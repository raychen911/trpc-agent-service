"""Tests for PID identity verification in start.sh and stop.sh.

These tests verify that PID files are bound to specific service instances
(module + subcommand + host + port), preventing accidental kills of wrong instances.
"""

import subprocess
import sys
import time
import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent  # tests/.. = trpc-agent-service
SCRIPTS_DIR = PROJECT_ROOT / "scripts"


def _free_port() -> int:
    import socket
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _start_fake_service(host: str, port: int, subcommand: str) -> subprocess.Popen:
    """Start a fake tRPC service process for testing PID identity."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "tests.fake_trpc_service", subcommand, "--host", host, "--port",
         str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        cwd=PROJECT_ROOT,
    )
    # Give it time to start and initialize
    time.sleep(0.5)
    return proc


def _verify_pid_identity(pid: int, expected_module: str, expected_subcommand: str, expected_host: str,
                         expected_port: str) -> bool:
    """Call the shell function _verify_pid_identity and return result."""
    script = f"""
source {SCRIPTS_DIR / 'pid_identity.sh'}
if _verify_pid_identity {pid} {expected_module} {expected_subcommand} {expected_host} {expected_port}; then
    exit 0
else
    exit 1
fi
"""
    result = subprocess.run(["bash", "-c", script], capture_output=True, cwd=PROJECT_ROOT)
    return result.returncode == 0


class TestPIDIdentityVerification:
    """Test that PID identity verification correctly distinguishes service instances."""

    def test_worker_a_pid_file_pointing_to_worker_b_not_misidentified(self, tmp_path):
        """Worker A PID file pointing to Worker B process must not match."""
        worker_a_host = "127.0.0.1"
        worker_a_port = _free_port()
        worker_b_host = "127.0.0.1"
        worker_b_port = _free_port()

        # Start a process that looks like Worker B
        proc_b = _start_fake_service(worker_b_host, worker_b_port, "worker")
        try:
            # Verify it does NOT match Worker A's expected identity
            assert not _verify_pid_identity(proc_b.pid, "tests.fake_trpc_service", "worker", worker_a_host,
                                            str(worker_a_port)), "Worker B process should not match Worker A identity"

            # Verify it DOES match Worker B's expected identity
            assert _verify_pid_identity(proc_b.pid, "tests.fake_trpc_service", "worker", worker_b_host,
                                        str(worker_b_port)), "Worker B process should match Worker B identity"
        finally:
            proc_b.terminate()
            proc_b.wait(timeout=5)

    def test_admin_pid_file_pointing_to_different_port_not_miskilled(self, tmp_path):
        """Admin PID file pointing to Admin on different port must not match."""
        admin_port_1 = _free_port()
        admin_port_2 = _free_port()

        # Start a process that looks like Admin on port 2
        proc = _start_fake_service("127.0.0.1", admin_port_2, "admin")
        try:
            # Verify it does NOT match Admin on port 1
            assert not _verify_pid_identity(
                proc.pid, "tests.fake_trpc_service", "admin", "127.0.0.1",
                str(admin_port_1)), "Admin on port 2 should not match Admin on port 1 identity"

            # Verify it DOES match Admin on port 2
            assert _verify_pid_identity(proc.pid, "tests.fake_trpc_service", "admin", "127.0.0.1",
                                        str(admin_port_2)), "Admin on port 2 should match Admin on port 2 identity"
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_gateway_pid_file_pointing_to_different_port_not_misidentified(self, tmp_path):
        """Gateway PID file pointing to Gateway on different port must not match."""
        gateway_port_1 = _free_port()
        gateway_port_2 = _free_port()

        # Start a process that looks like Gateway on port 2
        proc = _start_fake_service("127.0.0.1", gateway_port_2, "gateway")
        try:
            # Verify it does NOT match Gateway on port 1
            assert not _verify_pid_identity(
                proc.pid, "tests.fake_trpc_service", "gateway", "127.0.0.1",
                str(gateway_port_1)), "Gateway on port 2 should not match Gateway on port 1 identity"
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_wrong_subcommand_not_matched(self, tmp_path):
        """Process with wrong subcommand must not match."""
        port = _free_port()

        # Start a worker process
        proc = _start_fake_service("127.0.0.1", port, "worker")
        try:
            # Verify it does NOT match gateway or admin
            assert not _verify_pid_identity(proc.pid, "tests.fake_trpc_service", "gateway", "127.0.0.1",
                                            str(port)), "Worker should not match gateway identity"
            assert not _verify_pid_identity(proc.pid, "tests.fake_trpc_service", "admin", "127.0.0.1",
                                            str(port)), "Worker should not match admin identity"
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_non_trpc_process_not_matched(self, tmp_path):
        """Non-tRPC process must not match."""
        # Start a simple Python process
        proc = subprocess.Popen(
            [sys.executable, "-c", "import time; time.sleep(300)"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            # Verify it does NOT match any tRPC identity
            assert not _verify_pid_identity(proc.pid, "tests.fake_trpc_service", "worker", "127.0.0.1",
                                            8001), "Non-tRPC process should not match worker identity"
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_dead_process_not_matched(self, tmp_path):
        """Dead process PID must not match."""
        # Use a PID that doesn't exist
        fake_pid = 999999
        assert not _verify_pid_identity(fake_pid, "tests.fake_trpc_service", "worker", "127.0.0.1",
                                        8001), "Dead process should not match any identity"

    def test_correct_identity_matches(self, tmp_path):
        """Correct process with exact identity must match."""
        host = "127.0.0.1"
        port = _free_port()

        proc = _start_fake_service(host, port, "worker")
        try:
            # Verify it DOES match its own identity
            assert _verify_pid_identity(proc.pid, "tests.fake_trpc_service", "worker", host,
                                        str(port)), "Process should match its own identity"
        finally:
            proc.terminate()
            proc.wait(timeout=5)


class TestStopScriptSafety:
    """Test that stop.sh only kills exact matching instances."""

    def test_stop_does_not_kill_wrong_port_instance(self, tmp_path):
        """stop.sh must not kill a worker on a different port than expected."""
        # This test verifies the logic without actually running stop.sh
        # We test the _verify_pid_identity function which stop.sh uses
        worker_port_1 = _free_port()
        worker_port_2 = _free_port()

        # Start worker on port 2
        proc = _start_fake_service("127.0.0.1", worker_port_2, "worker")
        try:
            # stop.sh for worker-a on port 1 should NOT match this process
            assert not _verify_pid_identity(proc.pid, "tests.fake_trpc_service", "worker", "127.0.0.1",
                                            str(worker_port_1)), "stop.sh should not kill worker on different port"
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_stop_does_not_kill_non_trpc_process(self, tmp_path):
        """stop.sh must not kill a non-tRPC process even if PID file exists."""
        # Start a simple sleep process
        proc = subprocess.Popen(
            ["sleep", "300"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            # stop.sh should NOT match this process
            assert not _verify_pid_identity(proc.pid, "tests.fake_trpc_service", "worker", "127.0.0.1",
                                            8001), "stop.sh should not kill non-tRPC process"
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_test_module_never_matches_production_module(self, tmp_path):
        """A test helper must not satisfy production's module identity."""
        port = _free_port()
        proc = _start_fake_service("127.0.0.1", port, "worker")
        try:
            assert not _verify_pid_identity(
                proc.pid,
                "trpc_service._cli",
                "worker",
                "127.0.0.1",
                str(port),
            )
        finally:
            proc.terminate()
            proc.wait(timeout=5)

    def test_stop_script_removes_stale_pid_without_killing_wrong_module(self, tmp_path):
        """The real stop script must preserve a live process from another module."""
        port = _free_port()
        proc = _start_fake_service("127.0.0.1", port, "worker")
        worker_a_pid = tmp_path / "worker-a.pid"
        worker_a_pid.write_text(str(proc.pid))
        environment = os.environ.copy()
        environment.update({
            "TRPC_WORKER_A_PID_FILE": str(worker_a_pid),
            "TRPC_WORKER_A_PORT": str(port),
            "TRPC_WORKER_B_PID_FILE": str(tmp_path / "worker-b.pid"),
            "TRPC_GATEWAY_PID_FILE": str(tmp_path / "gateway.pid"),
            "TRPC_ADMIN_PID_FILE": str(tmp_path / "admin.pid"),
            "TRPC_REDIS_MARKER_FILE": str(tmp_path / "redis.owned"),
            "TRPC_POSTGRES_MARKER_FILE": str(tmp_path / "postgres.owned"),
        })
        try:
            result = subprocess.run(
                [str(PROJECT_ROOT / "stop.sh")],
                cwd=PROJECT_ROOT,
                env=environment,
                capture_output=True,
                text=True,
                timeout=10,
            )
            assert result.returncode == 0
            assert proc.poll() is None
            assert not worker_a_pid.exists()
        finally:
            proc.terminate()
            proc.wait(timeout=5)
