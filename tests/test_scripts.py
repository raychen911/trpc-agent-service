import os
import signal
import socket
import stat
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_NAMES = (
    "build.sh",
    "start.sh",
    "stop.sh",
    "clean.sh",
    "format.sh",
    "lint_flake8.sh",
    "coverage.sh",
)


def _redis_is_available() -> bool:
    """Check if Redis is available either via env var or Docker."""
    if os.environ.get("TRPC_REDIS_URL"):
        return True
    try:
        result = subprocess.run(
            ["docker", "ps"],
            capture_output=True,
            timeout=5,
        )
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


requires_redis = pytest.mark.skipif(
    not _redis_is_available(),
    reason="Redis not available (set TRPC_REDIS_URL or install Docker)",
)


def test_operational_scripts_are_executable_and_valid_bash():
    for script_name in SCRIPT_NAMES:
        script = PROJECT_ROOT / script_name
        assert script.is_file(), f"missing {script_name}"
        assert script.stat().st_mode & stat.S_IXUSR, f"{script_name} is not executable"
        subprocess.run(["bash", "-n", str(script)], check=True)


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _unique_ports(n: int) -> list[int]:
    ports: list[int] = []
    while len(ports) < n:
        p = _free_port()
        if p not in ports:
            ports.append(p)
    return ports


@dataclass(frozen=True)
class _Services:
    environment: dict[str, str]
    worker_a_port: int
    worker_b_port: int
    gateway_port: int
    admin_port: int
    worker_a_pid_file: Path
    worker_a_log_file: Path
    worker_b_pid_file: Path
    worker_b_log_file: Path
    gateway_pid_file: Path
    gateway_log_file: Path
    admin_pid_file: Path
    admin_log_file: Path


def _service_fixture(tmp_path: Path) -> _Services:
    ports = _unique_ports(4)
    worker_a_port, worker_b_port, gateway_port, admin_port = ports

    worker_a_pid_file = tmp_path / "worker-a.pid"
    worker_a_log_file = tmp_path / "worker-a.log"
    worker_b_pid_file = tmp_path / "worker-b.pid"
    worker_b_log_file = tmp_path / "worker-b.log"
    gateway_pid_file = tmp_path / "gateway.pid"
    gateway_log_file = tmp_path / "gateway.log"
    admin_pid_file = tmp_path / "admin.pid"
    admin_log_file = tmp_path / "admin.log"
    redis_port = _free_port()
    redis_name = f"trpc-test-redis-{tmp_path.parent.name}-{tmp_path.name}"
    minio_port = _free_port()
    # ``tmp_path.name`` repeats across separate pytest invocations (for
    # example ``test_start...ma0``), while its parent carries the run-unique
    # ``pytest-N`` segment.  Include both so an interrupted earlier run can
    # never make a fresh invocation reuse unknown ephemeral MinIO credentials.
    minio_name = f"trpc-test-minio-{tmp_path.parent.name}-{tmp_path.name}"

    environment = os.environ.copy()
    environment.pop("TRPC_WORKER_BASE_URL", None)
    environment.pop("TRPC_WORKER_BASE_URLS", None)
    environment.update({
        "TRPC_SERVICE_PYTHON": sys.executable,
        "TRPC_WORKER_A_HOST": "127.0.0.1",
        "TRPC_WORKER_A_PORT": str(worker_a_port),
        "TRPC_WORKER_A_PID_FILE": str(worker_a_pid_file),
        "TRPC_WORKER_A_LOG_FILE": str(worker_a_log_file),
        "TRPC_WORKER_B_HOST": "127.0.0.1",
        "TRPC_WORKER_B_PORT": str(worker_b_port),
        "TRPC_WORKER_B_PID_FILE": str(worker_b_pid_file),
        "TRPC_WORKER_B_LOG_FILE": str(worker_b_log_file),
        "TRPC_GATEWAY_HOST": "127.0.0.1",
        "TRPC_GATEWAY_PORT": str(gateway_port),
        "TRPC_GATEWAY_PID_FILE": str(gateway_pid_file),
        "TRPC_GATEWAY_LOG_FILE": str(gateway_log_file),
        "TRPC_ADMIN_HOST": "127.0.0.1",
        "TRPC_ADMIN_PORT": str(admin_port),
        "TRPC_ADMIN_PID_FILE": str(admin_pid_file),
        "TRPC_ADMIN_LOG_FILE": str(admin_log_file),
        "TRPC_INTERNAL_TOKEN": "c" * 48,
        "TRPC_ADMIN_TOKEN": "d" * 48,
        "TRPC_REDIS_CONTAINER_NAME": redis_name,
        "TRPC_REDIS_PORT": str(redis_port),
        # The shared development MinIO name is intentionally not used by
        # tests.  A pre-existing developer container has credentials that a
        # fresh test process cannot know; reusing it makes backend-init wait
        # until the subprocess timeout instead of testing the script itself.
        "TRPC_MINIO_CONTAINER_NAME": minio_name,
        "TRPC_MINIO_PORT": str(minio_port),
        "TRPC_MINIO_MARKER_FILE": str(tmp_path / "minio.owned"),
        # Keep ownership markers scoped to the test too.  The defaults live in
        # the shared repository data directory and let one script test erase
        # another test's ownership proof during a full-suite run.
        "TRPC_REDIS_MARKER_FILE": str(tmp_path / "redis.owned"),
        "TRPC_POSTGRES_MARKER_FILE": str(tmp_path / "postgres.owned"),
    })
    return _Services(
        environment=environment,
        worker_a_port=worker_a_port,
        worker_b_port=worker_b_port,
        gateway_port=gateway_port,
        admin_port=admin_port,
        worker_a_pid_file=worker_a_pid_file,
        worker_a_log_file=worker_a_log_file,
        worker_b_pid_file=worker_b_pid_file,
        worker_b_log_file=worker_b_log_file,
        gateway_pid_file=gateway_pid_file,
        gateway_log_file=gateway_log_file,
        admin_pid_file=admin_pid_file,
        admin_log_file=admin_log_file,
    )


def _run_script(
    script_name: str,
    services: _Services,
    *,
    check: bool,
    environment: dict[str, str] | None = None,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(PROJECT_ROOT / script_name)],
        cwd=PROJECT_ROOT,
        env=environment or services.environment,
        check=check,
        capture_output=True,
        text=True,
        timeout=45,
    )


def _wait_for_health(port: int, log_file: Path, service_name: str) -> None:
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=1) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.2)
    log_text = log_file.read_text() if log_file.exists() else ""
    raise AssertionError(f"{service_name} did not become healthy\n{log_text}")


def _wait_for_services(services: _Services) -> None:
    _wait_for_health(services.worker_a_port, services.worker_a_log_file, "worker-a")
    _wait_for_health(services.worker_b_port, services.worker_b_log_file, "worker-b")
    _wait_for_health(services.gateway_port, services.gateway_log_file, "gateway")
    _wait_for_health(services.admin_port, services.admin_log_file, "admin")


def _read_pid(pid_file: Path) -> int:
    value = pid_file.read_text().strip()
    assert value.isdigit(), f"invalid PID file: {pid_file}"
    return int(value)


def _process_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _wait_for_process_exit(pid: int) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if not _process_is_running(pid):
            return
        time.sleep(0.1)
    raise AssertionError(f"process {pid} did not stop")


def _stop_services(services: _Services, environment: dict[str, str] | None = None) -> None:
    _run_script("stop.sh", services, check=False, environment=environment)


@requires_redis
def test_start_and_stop_scripts_manage_four_processes(tmp_path):
    services = _service_fixture(tmp_path)
    try:
        _run_script("start.sh", services, check=True)
        _wait_for_services(services)
    finally:
        _stop_services(services)

    assert services.worker_a_pid_file.exists() or not services.worker_a_pid_file.exists()
    assert not services.gateway_pid_file.exists()
    assert not services.worker_a_pid_file.exists()
    assert not services.worker_b_pid_file.exists()
    assert not services.admin_pid_file.exists()


@requires_redis
def test_start_produces_four_distinct_pids(tmp_path):
    services = _service_fixture(tmp_path)
    try:
        _run_script("start.sh", services, check=True)
        _wait_for_services(services)

        pid_a = _read_pid(services.worker_a_pid_file)
        pid_b = _read_pid(services.worker_b_pid_file)
        pid_gw = _read_pid(services.gateway_pid_file)
        pid_admin = _read_pid(services.admin_pid_file)

        assert len({pid_a, pid_b, pid_gw, pid_admin}) == 4
        assert _process_is_running(pid_a)
        assert _process_is_running(pid_b)
        assert _process_is_running(pid_gw)
        assert _process_is_running(pid_admin)
    finally:
        _stop_services(services)


@requires_redis
def test_start_with_existing_worker_pid_not_overwritten(tmp_path):
    services = _service_fixture(tmp_path)
    try:
        _run_script("start.sh", services, check=True)
        _wait_for_services(services)
        existing_pid_a = _read_pid(services.worker_a_pid_file)
        existing_pid_b = _read_pid(services.worker_b_pid_file)

        result = _run_script("start.sh", services, check=True)

        assert "worker-a already running" in (result.stdout + result.stderr).lower()
        assert _read_pid(services.worker_a_pid_file) == existing_pid_a
        assert _read_pid(services.worker_b_pid_file) == existing_pid_b
    finally:
        _stop_services(services)


@requires_redis
def test_start_replaces_exact_worker_pid_when_health_check_fails(tmp_path):
    services = _service_fixture(tmp_path)
    try:
        _run_script("start.sh", services, check=True)
        _wait_for_services(services)
        old_pid = _read_pid(services.worker_a_pid_file)

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        state_file = tmp_path / "worker-a-health-failed-once"
        real_curl = subprocess.run(["which", "curl"], capture_output=True, text=True, check=True).stdout.strip()
        fake_curl = fake_bin / "curl"
        fake_curl.write_text("#!/usr/bin/env bash\n"
                             f"if [[ \"$*\" == *\":{services.worker_a_port}/health\"* ]] && "
                             f"[[ ! -f \"{state_file}\" ]]; then\n"
                             f"  touch \"{state_file}\"\n"
                             "  exit 1\n"
                             "fi\n"
                             f"exec {real_curl} \"$@\"\n")
        fake_curl.chmod(0o755)
        environment = services.environment.copy()
        environment["PATH"] = f"{fake_bin}{os.pathsep}{environment['PATH']}"

        result = _run_script("start.sh", services, check=True, environment=environment)
        new_pid = _read_pid(services.worker_a_pid_file)

        assert "health check failed" in (result.stdout + result.stderr)
        assert new_pid != old_pid
        assert not _process_is_running(old_pid)
        assert _process_is_running(new_pid)
        _wait_for_health(services.worker_a_port, services.worker_a_log_file, "worker-a")
    finally:
        _stop_services(services)


@requires_redis
def test_start_with_stale_worker_pid(tmp_path):
    services = _service_fixture(tmp_path)
    stale_pid = "99999999"
    services.worker_a_pid_file.write_text(stale_pid)
    try:
        _run_script("start.sh", services, check=True)
        _wait_for_services(services)

        assert services.worker_a_pid_file.read_text().strip() != stale_pid
        assert _process_is_running(_read_pid(services.worker_a_pid_file))
    finally:
        _stop_services(services)


@requires_redis
def test_gateway_start_failure_rolls_back_workers(tmp_path):
    services = _service_fixture(tmp_path)
    try:
        _run_script("start.sh", services, check=True)
        _wait_for_services(services)
        worker_a_pid = _read_pid(services.worker_a_pid_file)
        worker_b_pid = _read_pid(services.worker_b_pid_file)

        gateway_pid = _read_pid(services.gateway_pid_file)
        os.kill(gateway_pid, signal.SIGTERM)
        _wait_for_process_exit(gateway_pid)
        services.gateway_pid_file.unlink()

        fake_bin = tmp_path / "bin"
        fake_bin.mkdir()
        fake_curl = fake_bin / "curl"
        fake_curl.write_text("#!/usr/bin/env bash\n"
                             "case \"$*\" in\n"
                             f"  *\":{services.worker_a_port}/health\"*) exit 0 ;;\n"
                             f"  *\":{services.worker_b_port}/health\"*) exit 0 ;;\n"
                             "  *) exit 1 ;;\n"
                             "esac\n")
        fake_curl.chmod(0o755)
        failing_environment = services.environment.copy()
        failing_environment["PATH"] = f"{fake_bin}{os.pathsep}{failing_environment['PATH']}"
        result = _run_script(
            "start.sh",
            services,
            check=False,
            environment=failing_environment,
        )

        assert result.returncode != 0
        assert "gateway did not become healthy" in (result.stdout + result.stderr)
        assert _read_pid(services.worker_a_pid_file) == worker_a_pid
        assert _read_pid(services.worker_b_pid_file) == worker_b_pid
        assert _process_is_running(worker_a_pid)
        assert _process_is_running(worker_b_pid)
        assert not services.gateway_pid_file.exists()
    finally:
        _stop_services(services)


@requires_redis
def test_worker_b_start_failure_rolls_back_worker_a(tmp_path):
    """If Worker B fails to start, Worker A (started this round) is rolled back."""
    services = _service_fixture(tmp_path)

    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    real_curl = subprocess.run(["which", "curl"], capture_output=True, text=True).stdout.strip()
    fake_curl = fake_bin / "curl"
    fake_curl.write_text("#!/usr/bin/env bash\n"
                         "case \"$*\" in\n"
                         f"  *\":{services.worker_a_port}/health\"*) exec {real_curl} \"$@\" ;;\n"
                         f"  *\":{services.worker_b_port}/health\"*) exit 1 ;;\n"
                         "  *) exec {real_curl} \"$@\" ;;\n"
                         "esac\n".replace("{real_curl}", real_curl))
    fake_curl.chmod(0o755)
    failing_environment = services.environment.copy()
    failing_environment["PATH"] = f"{fake_bin}{os.pathsep}{failing_environment['PATH']}"

    try:
        result = _run_script("start.sh", services, check=False, environment=failing_environment)
        assert result.returncode != 0
        assert not services.worker_a_pid_file.exists()
        assert not services.worker_b_pid_file.exists()
        assert not services.gateway_pid_file.exists()
    finally:
        _stop_services(services)


def test_stop_with_no_pid_files(tmp_path):
    services = _service_fixture(tmp_path)
    result = _run_script("stop.sh", services, check=True)

    assert "is not running" in (result.stdout + result.stderr)


def _docker_available() -> bool:
    try:
        r = subprocess.run(["docker", "ps"], capture_output=True, timeout=5)
        return r.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def _container_is_running(name: str) -> bool:
    r = subprocess.run(
        ["docker", "ps", "--format", "{{.Names}}"],
        capture_output=True,
        text=True,
        timeout=5,
    )
    return name in r.stdout.splitlines()


def _wait_for_container_running(name: str, timeout: float = 10.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _container_is_running(name):
            return
        time.sleep(0.2)
    raise AssertionError(f"container {name} did not start")


requires_docker = pytest.mark.skipif(
    not _docker_available(),
    reason="Docker not available",
)


@requires_docker
def test_stop_does_not_stop_preexisting_redis(tmp_path):
    """stop.sh should NOT stop a Redis container that was not started by start.sh."""
    import uuid
    container_name = f"trpc-preexist-{uuid.uuid4().hex[:8]}"
    redis_port = _free_port()

    subprocess.run(
        ["docker", "run", "-d", "--name", container_name, "-p", f"{redis_port}:6379", "redis:7"],
        capture_output=True,
        check=True,
        timeout=30,
    )
    try:
        _wait_for_container_running(container_name)

        services = _service_fixture(tmp_path)
        env = services.environment.copy()
        env["TRPC_REDIS_URL"] = f"redis://127.0.0.1:{redis_port}"
        env["TRPC_REDIS_CONTAINER_NAME"] = container_name

        try:
            _run_script("start.sh", services, check=True, environment=env)
            _wait_for_services(services)

            redis_marker = Path(env["TRPC_REDIS_MARKER_FILE"])
            assert not redis_marker.exists(), "marker file should not exist for pre-existing container"

            _stop_services(services)

            assert _container_is_running(container_name), \
                "pre-existing Redis container should still be running after stop.sh"
        finally:
            _stop_services(services)
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, timeout=10)


@requires_docker
def test_start_refuses_unowned_existing_redis(tmp_path):
    """An unmarked same-name Redis container must never become local state."""
    import uuid

    container_name = f"trpc-unowned-{uuid.uuid4().hex[:8]}"
    redis_port = _free_port()
    subprocess.run(
        ["docker", "run", "-d", "--name", container_name, "-p", f"{redis_port}:6379", "redis:7"],
        capture_output=True,
        check=True,
        timeout=30,
    )
    try:
        _wait_for_container_running(container_name)
        services = _service_fixture(tmp_path)
        env = services.environment.copy()
        env.pop("TRPC_REDIS_URL", None)
        env["TRPC_REDIS_CONTAINER_NAME"] = container_name
        env["TRPC_REDIS_PORT"] = str(redis_port)
        marker = Path(env["TRPC_REDIS_MARKER_FILE"])
        marker.unlink(missing_ok=True)

        result = _run_script("start.sh", services, check=False, environment=env)

        assert result.returncode != 0
        assert "Redis container exists but is not owned by start.sh" in (result.stdout + result.stderr)
        assert _container_is_running(container_name)
        assert not marker.exists()
    finally:
        _stop_services(services, environment=env)
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, timeout=10)


@requires_docker
def test_stop_removes_container_started_by_start(tmp_path):
    """stop.sh should stop a Redis container started by start.sh and remove the marker."""
    import uuid
    container_name = f"trpc-owned-{uuid.uuid4().hex[:8]}"
    redis_port = _free_port()

    subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, timeout=10)

    services = _service_fixture(tmp_path)
    env = services.environment.copy()
    env.pop("TRPC_REDIS_URL", None)
    env["TRPC_REDIS_CONTAINER_NAME"] = container_name
    env["TRPC_REDIS_PORT"] = str(redis_port)

    try:
        _run_script("start.sh", services, check=True, environment=env)
        _wait_for_services(services)

        assert _container_is_running(container_name), \
            "start.sh should have started the Redis container"

        _stop_services(services, environment=env)

        assert not _container_is_running(container_name), \
            "stop.sh should have stopped the Redis container it started"
    finally:
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True, timeout=10)


def test_start_without_admin_token_fails_fast(tmp_path):
    services = _service_fixture(tmp_path)
    environment = services.environment.copy()
    environment.pop("TRPC_ADMIN_TOKEN", None)

    result = _run_script("start.sh", services, check=False, environment=environment)

    assert result.returncode != 0
    assert "TRPC_ADMIN_TOKEN is not set" in (result.stdout + result.stderr)
    assert not services.worker_a_pid_file.exists()
    assert not services.worker_b_pid_file.exists()
    assert not services.gateway_pid_file.exists()
    assert not services.admin_pid_file.exists()


@requires_redis
def test_stop_stops_admin_before_gateway(tmp_path):
    services = _service_fixture(tmp_path)
    try:
        _run_script("start.sh", services, check=True)
        _wait_for_services(services)
    finally:
        result = _run_script("stop.sh", services, check=False)

    combined = result.stdout + result.stderr
    admin_pos = combined.find("admin stopped")
    gateway_pos = combined.find("gateway stopped")
    assert admin_pos != -1, f"admin not stopped: {combined}"
    assert gateway_pos != -1, f"gateway not stopped: {combined}"
    assert admin_pos < gateway_pos, "admin must stop before gateway"


@requires_redis
def test_admin_start_failure_rolls_back_this_run_processes(tmp_path):
    services = _service_fixture(tmp_path)
    try:
        _run_script("start.sh", services, check=True)
        _wait_for_services(services)
        worker_a_pid = _read_pid(services.worker_a_pid_file)
        worker_b_pid = _read_pid(services.worker_b_pid_file)
        gateway_pid = _read_pid(services.gateway_pid_file)

        admin_pid = _read_pid(services.admin_pid_file)
        os.kill(admin_pid, signal.SIGTERM)
        _wait_for_process_exit(admin_pid)
        services.admin_pid_file.unlink()

        fake_bin = tmp_path / "admin-fail-bin"
        fake_bin.mkdir()
        fake_curl = fake_bin / "curl"
        fake_curl.write_text("#!/usr/bin/env bash\n"
                             "case \"$*\" in\n"
                             f"  *\":{services.worker_a_port}/health\"*) exit 0 ;;\n"
                             f"  *\":{services.worker_b_port}/health\"*) exit 0 ;;\n"
                             f"  *\":{services.gateway_port}/health\"*) exit 0 ;;\n"
                             "  *) exit 1 ;;\n"
                             "esac\n")
        fake_curl.chmod(0o755)
        failing_environment = services.environment.copy()
        failing_environment["PATH"] = f"{fake_bin}{os.pathsep}{failing_environment['PATH']}"
        result = _run_script(
            "start.sh",
            services,
            check=False,
            environment=failing_environment,
        )

        assert result.returncode != 0
        assert "admin did not become healthy" in (result.stdout + result.stderr)
        assert _read_pid(services.worker_a_pid_file) == worker_a_pid
        assert _read_pid(services.worker_b_pid_file) == worker_b_pid
        assert _read_pid(services.gateway_pid_file) == gateway_pid
        assert _process_is_running(worker_a_pid)
        assert _process_is_running(worker_b_pid)
        assert _process_is_running(gateway_pid)
        assert not services.admin_pid_file.exists()
    finally:
        _stop_services(services)
