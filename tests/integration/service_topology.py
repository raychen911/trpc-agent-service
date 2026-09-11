"""Shared service topology for multi-process integration tests.

Provides ServiceTopology (PostgreSQL + Redis + 2 Workers + Gateway) and
helper utilities used by Stage 3C and Stage 4C integration tests.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import urllib.request
import uuid

import pytest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))


def docker_is_available() -> bool:
    try:
        result = subprocess.run(["docker", "ps"], capture_output=True, timeout=5)
        return result.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return False


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for_health(url: str, timeout: float = 15.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"{url}/health", timeout=1) as resp:
                if resp.status == 200:
                    return True
        except OSError:
            pass
        time.sleep(0.2)
    return False


def wait_for_redis(redis_url: str, timeout: float = 10.0) -> bool:
    import redis
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            r = redis.from_url(redis_url, socket_timeout=1.0, socket_connect_timeout=1.0)
            r.ping()
            r.close()
            return True
        except Exception:
            time.sleep(0.2)
    return False


def wait_for_postgres(database_url: str, timeout: float = 30.0) -> bool:
    import asyncio

    async def _check():
        import asyncpg
        conn = None
        try:
            conn = await asyncpg.connect(database_url, timeout=2.0)
            await conn.fetchval("SELECT 1")
            return True
        except Exception:
            return False
        finally:
            if conn:
                await conn.close()

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if asyncio.run(_check()):
                return True
        except Exception:
            pass
        time.sleep(0.5)
    return False


def wait_for_minio(endpoint: str, timeout: float = 30.0) -> bool:
    """Wait for the S3 endpoint required by the R1 backend profile."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(f"http://{endpoint}/minio/health/ready", timeout=1) as response:
                if response.status == 200:
                    return True
        except OSError:
            pass
        time.sleep(0.2)
    return False


def http_post_json(
    url: str,
    data: dict,
    headers: dict | None = None,
    timeout: float = 10.0,
) -> dict:
    body = json.dumps(data).encode()
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    if headers:
        for k, v in headers.items():
            req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except Exception as exc:
        return {"error": str(exc)}


def load_env_file() -> dict[str, str]:
    """Parse .env file and return key-value pairs. Ignores comments and empty lines."""
    env_vars: dict[str, str] = {}
    env_file = os.path.join(PROJECT_ROOT, ".env")
    if not os.path.isfile(env_file):
        return env_vars
    with open(env_file) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" in line:
                key, value = line.split("=", 1)
                env_vars[key.strip()] = value.strip()
    return env_vars


requires_docker = pytest.mark.skipif(
    not docker_is_available(),
    reason="Docker not available",
)


def _model_is_configured() -> bool:
    """Check if a real model API key is available for integration tests."""
    if os.environ.get("TRPC_MODEL_API_KEY") and os.environ.get("TRPC_MODEL_NAME"):
        return True
    env_file = os.path.join(PROJECT_ROOT, ".env")
    if os.path.isfile(env_file):
        with open(env_file) as f:
            for line in f:
                line = line.strip()
                if line.startswith("TRPC_MODEL_API_KEY=") and "=" in line:
                    val = line.split("=", 1)[1].strip()
                    if val:
                        return True
    return False


requires_model_and_docker = pytest.mark.skipif(
    not (docker_is_available() and _model_is_configured()),
    reason="Docker and real model configuration required",
)


def find_session_routing_to_worker(
    worker_a_url: str,
    worker_b_url: str,
    target_url: str,
    prefix: str = "det-routing",
    config_version: int = 2,
) -> str:
    """Find a session_id that deterministically routes to the target Worker URL.

    Uses the same Rendezvous hashing algorithm as the production router.
    """
    import hashlib

    target_normalized = target_url.rstrip("/")

    for i in range(10000):
        session_id = f"{prefix}-{i}"
        canonical = json.dumps(
            ["tenant_default", "app_demo", config_version, "web", "user_default", session_id],
            separators=(",", ":"),
            ensure_ascii=False,
        )
        score_a_input = json.dumps(
            [canonical, worker_a_url],
            separators=(",", ":"),
            ensure_ascii=False,
        )
        score_b_input = json.dumps(
            [canonical, worker_b_url],
            separators=(",", ":"),
            ensure_ascii=False,
        )
        score_a = hashlib.sha256(score_a_input.encode()).hexdigest()
        score_b = hashlib.sha256(score_b_input.encode()).hexdigest()

        winner = worker_a_url if score_a > score_b else worker_b_url
        if winner == target_normalized:
            return session_id

    raise RuntimeError(f"Could not find session routing to {target_url} after 10000 attempts")


class ServiceTopology:
    """Manages PostgreSQL + Redis + 2 Workers + Gateway for integration tests."""

    def __init__(self, tmp_path):
        self.tmp_path = tmp_path
        self.worker_a_port = free_port()
        self.worker_b_port = free_port()
        while self.worker_b_port == self.worker_a_port:
            self.worker_b_port = free_port()
        self.gateway_port = free_port()

        self.redis_container = f"trpc-3c-int-{uuid.uuid4().hex[:8]}"
        self.redis_port = free_port()
        self.redis_url = f"redis://127.0.0.1:{self.redis_port}"

        self.pg_container = f"trpc-3c-int-pg-{uuid.uuid4().hex[:8]}"
        self.pg_port = free_port()
        self.pg_user = "trpc_test"
        self.pg_password = "testpass"
        self.pg_db = "trpc_test"
        self.pg_url = (f"postgresql+asyncpg://{self.pg_user}:{self.pg_password}"
                       f"@127.0.0.1:{self.pg_port}/{self.pg_db}")
        self.pg_plain_url = (f"postgresql://{self.pg_user}:{self.pg_password}"
                             f"@127.0.0.1:{self.pg_port}/{self.pg_db}")

        self.minio_container = f"trpc-3c-int-minio-{uuid.uuid4().hex[:8]}"
        self.minio_port = free_port()
        self.minio_access_key = "topologyaccess"
        self.minio_secret_key = "topology-secret-0123456789"
        self.minio_endpoint = f"127.0.0.1:{self.minio_port}"

        self.worker_a_url = f"http://127.0.0.1:{self.worker_a_port}"
        self.worker_b_url = f"http://127.0.0.1:{self.worker_b_port}"
        self.gateway_url = f"http://127.0.0.1:{self.gateway_port}"

        self.worker_a_pid_file = tmp_path / "worker-a.pid"
        self.worker_b_pid_file = tmp_path / "worker-b.pid"
        self.gateway_pid_file = tmp_path / "gateway.pid"
        self.worker_a_log = tmp_path / "worker-a.log"
        self.worker_b_log = tmp_path / "worker-b.log"
        self.gateway_log = tmp_path / "gateway.log"

        self._pids: list[int] = []
        self._redis_started = False
        self._pg_started = False
        self._minio_started = False
        self._internal_token = uuid.uuid4().hex

    def start(self):
        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self.redis_container,
                "-p",
                f"{self.redis_port}:6379",
                "redis:7",
            ],
            capture_output=True,
            check=True,
            timeout=30,
        )
        self._redis_started = True
        assert wait_for_redis(self.redis_url), "Redis did not become ready"

        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self.pg_container,
                "-p",
                f"{self.pg_port}:5432",
                "-e",
                f"POSTGRES_USER={self.pg_user}",
                "-e",
                f"POSTGRES_PASSWORD={self.pg_password}",
                "-e",
                f"POSTGRES_DB={self.pg_db}",
                "postgres:16",
            ],
            capture_output=True,
            check=True,
            timeout=120,
        )
        self._pg_started = True
        assert wait_for_postgres(self.pg_plain_url), "PostgreSQL did not become ready"

        subprocess.run(
            [
                "docker",
                "run",
                "-d",
                "--name",
                self.minio_container,
                "-p",
                f"{self.minio_port}:9000",
                "-e",
                f"MINIO_ROOT_USER={self.minio_access_key}",
                "-e",
                f"MINIO_ROOT_PASSWORD={self.minio_secret_key}",
                "minio/minio:RELEASE.2025-04-22T22-12-26Z",
                "server",
                "/data",
            ],
            capture_output=True,
            check=True,
            timeout=60,
        )
        self._minio_started = True
        assert wait_for_minio(self.minio_endpoint), "MinIO did not become ready"

        migrate_env = os.environ.copy()
        migrate_env["TRPC_DATABASE_URL"] = self.pg_url
        result = subprocess.run(
            [sys.executable, "-m", "alembic", "upgrade", "head"],
            capture_output=True,
            text=True,
            env=migrate_env,
            cwd=PROJECT_ROOT,
            timeout=30,
        )
        assert result.returncode == 0, f"alembic upgrade failed: {result.stderr}"

        tenant_config = os.path.join(PROJECT_ROOT, "data", "tenants.json")
        if os.path.isfile(tenant_config):
            result = subprocess.run(
                [
                    sys.executable,
                    "-m",
                    "trpc_service._cli",
                    "tenant-config-import",
                    "--path",
                    tenant_config,
                ],
                capture_output=True,
                text=True,
                env=migrate_env,
                cwd=PROJECT_ROOT,
                timeout=30,
            )
            assert result.returncode == 0, f"tenant import failed: {result.stderr}"

        env = self._build_env()
        result = subprocess.run(
            [sys.executable, "-m", "trpc_service._cli", "backend-init"],
            capture_output=True,
            text=True,
            env=env,
            cwd=PROJECT_ROOT,
            timeout=30,
        )
        assert result.returncode == 0, f"backend initialization failed: {result.stderr}"

        for label, port, pid_file, log_file in [
            ("a", self.worker_a_port, self.worker_a_pid_file, self.worker_a_log),
            ("b", self.worker_b_port, self.worker_b_pid_file, self.worker_b_log),
        ]:
            worker_env = env.copy()
            worker_env["TRPC_INTERNAL_TOKEN"] = self._internal_token
            with open(log_file, "w") as lf:
                proc = subprocess.Popen(
                    [
                        sys.executable,
                        "-m",
                        "trpc_service._cli",
                        "worker",
                        "--host",
                        "127.0.0.1",
                        "--port",
                        str(port),
                    ],
                    env=worker_env,
                    stdout=lf,
                    stderr=subprocess.STDOUT,
                    cwd=PROJECT_ROOT,
                )
            pid_file.write_text(str(proc.pid))
            self._pids.append(proc.pid)

        for port in (self.worker_a_port, self.worker_b_port):
            assert wait_for_health(
                f"http://127.0.0.1:{port}"), \
                f"Worker on port {port} did not become healthy"

        gw_env = env.copy()
        gw_env["TRPC_INTERNAL_TOKEN"] = self._internal_token
        gw_env["TRPC_WORKER_BASE_URLS"] = (f"{self.worker_a_url},{self.worker_b_url}")
        with open(self.gateway_log, "w") as lf:
            gw_proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "trpc_service._cli",
                    "gateway",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(self.gateway_port),
                ],
                env=gw_env,
                stdout=lf,
                stderr=subprocess.STDOUT,
                cwd=PROJECT_ROOT,
            )
        self.gateway_pid_file.write_text(str(gw_proc.pid))
        self._pids.append(gw_proc.pid)

        assert wait_for_health(self.gateway_url), "Gateway did not become healthy"

    def _build_env(self) -> dict[str, str]:
        env = os.environ.copy()
        env_file_vars = load_env_file()
        for key, value in env_file_vars.items():
            if key.startswith("TRPC_MODEL_") and key not in env:
                env[key] = value
        env["TRPC_REDIS_URL"] = self.redis_url
        env["TRPC_DATABASE_URL"] = self.pg_url
        env["TRPC_S3_ENDPOINT"] = self.minio_endpoint
        env["TRPC_S3_ACCESS_KEY"] = self.minio_access_key
        env["TRPC_S3_SECRET_KEY"] = self.minio_secret_key
        env["TRPC_S3_BUCKET"] = "trpc-artifacts"
        env["TRPC_SERVICE_PYTHON"] = sys.executable
        env["TRPC_SESSION_LOCK_LEASE_SECONDS"] = "2"
        env["TRPC_SESSION_LOCK_RENEW_SECONDS"] = "0.5"
        return env

    def stop(self):
        for pid in reversed(self._pids):
            try:
                os.kill(pid, signal.SIGTERM)
            except OSError:
                pass
        for pid in self._pids:
            for _ in range(50):
                try:
                    os.kill(pid, 0)
                except OSError:
                    break
                time.sleep(0.1)
        self._pids.clear()

        if self._redis_started:
            subprocess.run(
                ["docker", "rm", "-f", self.redis_container],
                capture_output=True,
                timeout=10,
            )
            self._redis_started = False

        if self._pg_started:
            subprocess.run(
                ["docker", "rm", "-f", self.pg_container],
                capture_output=True,
                timeout=10,
            )
            self._pg_started = False

        if self._minio_started:
            subprocess.run(
                ["docker", "rm", "-f", self.minio_container],
                capture_output=True,
                timeout=10,
            )
            self._minio_started = False

    def kill_worker(self, which: str) -> int:
        pid_file = self.worker_a_pid_file if which == "a" \
            else self.worker_b_pid_file
        pid = int(pid_file.read_text().strip())
        os.kill(pid, signal.SIGKILL)
        for _ in range(50):
            try:
                os.kill(pid, 0)
            except OSError:
                break
            time.sleep(0.1)
        self._pids = [p for p in self._pids if p != pid]
        return pid

    def restart_worker(self, which: str) -> int:
        """Restart a killed Worker using the SAME internal token."""
        port = self.worker_a_port if which == "a" else self.worker_b_port
        pid_file = self.worker_a_pid_file if which == "a" \
            else self.worker_b_pid_file
        log_file = self.worker_a_log if which == "a" else self.worker_b_log

        env = self._build_env()
        env["TRPC_INTERNAL_TOKEN"] = self._internal_token
        with open(log_file, "a") as lf:
            proc = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "trpc_service._cli",
                    "worker",
                    "--host",
                    "127.0.0.1",
                    "--port",
                    str(port),
                ],
                env=env,
                stdout=lf,
                stderr=subprocess.STDOUT,
                cwd=PROJECT_ROOT,
            )
        pid_file.write_text(str(proc.pid))
        self._pids.append(proc.pid)
        assert wait_for_health(
            f"http://127.0.0.1:{port}"), \
            f"Restarted worker on port {port} did not become healthy"
        return proc.pid

    def count_session_in_log(self, which: str, session_id: str) -> int:
        """Count how many times a session executed (post-lock) by a Worker."""
        log_file = self.worker_a_log if which == "a" else self.worker_b_log
        if not log_file.exists():
            return 0
        count = 0
        with open(log_file) as f:
            for line in f:
                if f"session execution entered session_id={session_id}" in line:
                    count += 1
        return count

    def which_worker_handled(self, session_id: str) -> str | None:
        """Determine which Worker handled a session: 'a', 'b', or None."""
        a_count = self.count_session_in_log("a", session_id)
        b_count = self.count_session_in_log("b", session_id)
        if a_count > 0 and b_count == 0:
            return "a"
        if b_count > 0 and a_count == 0:
            return "b"
        return None


def is_transient_model_error(response: dict) -> bool:
    """Return True if the response indicates a transient model error eligible for retry.

    Only SAFE_ERROR_TEXT or error_code == "model_runtime" qualify.
    """
    from trpc_service.gateway.errors import SAFE_ERROR_TEXT
    response_text = response.get("response", "")
    error_code = response.get("error_code")
    return response_text == SAFE_ERROR_TEXT or error_code == "model_runtime"


def complete_model_seed(
    send_fn,
    max_attempts: int = 3,
) -> tuple[str, str, dict]:
    """Execute a model request with retry on transient errors.

    Each attempt generates a fresh message_id and session_id so that a
    failed seed never reuses the execution identity of a prior attempt.

    Args:
        send_fn: Callable(message_id, session_id) -> dict response
        max_attempts: Maximum retry attempts (default 3)

    Returns:
        (message_id, session_id, response) of successful execution

    Raises:
        pytest.fail: If all attempts fail or non-transient error occurs
    """
    from trpc_service.gateway.errors import SAFE_ERROR_TEXT

    for attempt in range(max_attempts):
        msg_id = f"msg-seed-{uuid.uuid4().hex[:8]}"
        session_id = f"sess-seed-{uuid.uuid4().hex[:8]}"

        response = send_fn(msg_id, session_id)
        response_text = response.get("response", "")

        if is_transient_model_error(response):
            if attempt < max_attempts - 1:
                continue
            else:
                pytest.fail(f"Model seed failed after {max_attempts} attempts with transient error. "
                            f"Last response: {response}")

        if "error" not in response and response_text != SAFE_ERROR_TEXT:
            return msg_id, session_id, response

        pytest.fail(f"Model seed failed with non-transient error on attempt {attempt + 1}: {response}")

    pytest.fail(f"Model seed did not succeed after {max_attempts} attempts")
