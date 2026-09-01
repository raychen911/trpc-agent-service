from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen


def is_running(pid: int) -> bool:
    if os.name == "nt":
        completed = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/FO", "CSV", "/NH"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        return f'"{pid}"' in completed.stdout or f",{pid}," in completed.stdout
    try:
        os.kill(pid, 0)
    except (OSError, SystemError):
        return False
    return True


def wait_for_health(host: str, port: int, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with urlopen(f"http://{host}:{port}/health", timeout=2) as response:
                if response.status == 200:
                    return True
        except OSError:
            time.sleep(0.5)
    return False


def main() -> int:
    if sys.version_info < (3, 12):
        print("Python 3.12+ is required.", file=sys.stderr)
        return 2
    parser = argparse.ArgumentParser(description="Start the local tRPC-Agent Web UI")
    parser.add_argument("--host", default=os.getenv("HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.getenv("PORT", "18001")))
    parser.add_argument("--model", default=os.getenv("CPA_MODEL", "gpt-4.1"))
    parser.add_argument(
        "--runtime-mode",
        choices=("local", "trpc"),
        default=os.getenv("TRPC_AGENT_RUNTIME_MODE", "local"),
        help="Use dependency-free local demo mode or the tRPC-Agent-Python runtime.",
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parents[1]
    runtime_dir = root / ".run"
    runtime_dir.mkdir(exist_ok=True)
    pid_path = runtime_dir / "web-ui.pid"
    if pid_path.exists():
        try:
            old_pid = int(pid_path.read_text(encoding="ascii").strip())
        except ValueError:
            old_pid = 0
        if old_pid and is_running(old_pid):
            print(f"Web UI is already running: http://{args.host}:{args.port}/ui (pid {old_pid})")
            return 0
        pid_path.unlink(missing_ok=True)

    data_dir = root / "data"
    data_dir.mkdir(exist_ok=True)
    stdout_path = data_dir / "web-ui.out.log"
    stderr_path = data_dir / "web-ui.err.log"
    env = os.environ.copy()
    env.update(
        {
            # Keep the local smoke-test path dependency-free by default.
            # Real-model or Codex CLI verification can be enabled explicitly.
            "CPA_USE_CODEX_CLI": os.getenv("CPA_USE_CODEX_CLI", "0"),
            "CPA_MODEL": args.model,
            "TRPC_AGENT_RUNTIME_MODE": args.runtime_mode,
            "WORKER_QUEUE_URL": "",
            "WORKER_REMOTE": "0",
        }
    )

    stdout = stdout_path.open("ab")
    stderr = stderr_path.open("ab")
    creationflags = getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "uvicorn",
            "trpc_service.web.app:app",
            "--host",
            args.host,
            "--port",
            str(args.port),
        ],
        cwd=root,
        env=env,
        stdin=subprocess.DEVNULL,
        stdout=stdout,
        stderr=stderr,
        creationflags=creationflags,
        close_fds=True,
    )
    stdout.close()
    stderr.close()
    pid_path.write_text(str(process.pid), encoding="ascii")

    if not wait_for_health(args.host, args.port):
        print(f"Web UI failed to start. See {stderr_path}", file=sys.stderr)
        return 1
    print(f"Web UI: http://{args.host}:{args.port}/ui")
    print(f"PID: {process.pid}")
    print(f"Logs: {stderr_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
