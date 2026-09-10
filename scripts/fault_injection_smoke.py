"""Inject real Compose Redis/PostgreSQL failures and record recovery evidence."""

from __future__ import annotations

import argparse
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = [
    "docker",
    "compose",
    "--env-file",
    str(ROOT / ".env"),
    "-f",
    str(ROOT / "deployment" / "docker-compose.yml"),
]


def compose_command(project_name: str | None = None) -> list[str]:
    command = [*COMPOSE]
    if project_name:
        command.extend(["--project-name", project_name])
    return command


def run(*args: str, project_name: str | None = None, timeout: int = 60) -> dict[str, object]:
    command = [*compose_command(project_name), *args]
    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    return {
        "command": " ".join(command),
        "returncode": completed.returncode,
        "stdout": completed.stdout[-4000:],
        "stderr": completed.stderr[-4000:],
    }


def request(base_url: str, text: str, timeout: float = 12) -> dict[str, object]:
    started = time.perf_counter()
    payload = json.dumps(
        {
            "channel": "web",
            "account_id": "web_demo",
            "user_id": "fault-injection",
            "text": text,
        }
    ).encode("utf-8")
    req = Request(
        f"{base_url.rstrip('/')}/ui/api/chat",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8", errors="replace")
            return {
                "status": response.status,
                "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
                "body": body[:1000],
            }
    except (HTTPError, URLError, TimeoutError, OSError) as exc:
        return {
            "status": None,
            "elapsed_ms": round((time.perf_counter() - started) * 1000, 2),
            "error": str(exc)[:500],
        }


def wait_for_probe(command: list[str], expected: str, timeout: int = 45) -> dict[str, object]:
    deadline = time.time() + timeout
    attempts = 0
    last = ""
    while time.time() < deadline:
        attempts += 1
        completed = subprocess.run(command, cwd=ROOT, text=True, capture_output=True)
        last = (completed.stdout or completed.stderr).strip()
        if completed.returncode == 0 and expected in last:
            return {"ok": True, "attempts": attempts, "output": last[-500:]}
        time.sleep(2)
    return {"ok": False, "attempts": attempts, "output": last[-500:]}


def inject_service(
    base_url: str,
    service: str,
    probe: list[str],
    expected: str,
    project_name: str | None,
) -> dict[str, object]:
    result: dict[str, object] = {"service": service}
    result["stop"] = run("stop", service, project_name=project_name)
    stop_result = result["stop"]
    if isinstance(stop_result, dict) and stop_result.get("returncode") != 0:
        result["during_outage"] = {
            "status": None,
            "error": "service stop failed; outage was not injected",
        }
        result["start"] = {"returncode": 1, "error": "skipped because stop failed"}
        result["recovery_probe"] = {"ok": False, "error": "service stop failed"}
        result["after_recovery"] = {"status": None, "error": "service stop failed"}
        return result
    result["during_outage"] = request(base_url, f"fault injection: {service} stopped")
    result["start"] = run("start", service, project_name=project_name)
    result["recovery_probe"] = wait_for_probe(probe, expected)
    result["after_recovery"] = request(base_url, f"fault injection: {service} recovered")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--project-name", default=None)
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    redis_probe = [*compose_command(args.project_name), "exec", "-T", "redis", "redis-cli", "ping"]
    sql_probe = [
        *compose_command(args.project_name),
        "exec",
        "-T",
        "sql",
        "pg_isready",
        "-U",
        "trpc_agent",
        "-d",
        "trpc_agent",
    ]

    report: dict[str, object] = {
        "started_at": datetime.now(UTC).isoformat(),
        "base_url": args.base_url,
        "project_name": args.project_name,
        "baseline": request(args.base_url, "fault injection baseline"),
        "services": [],
    }
    report["services"].append(
        inject_service(
            args.base_url,
            "redis",
            redis_probe,
            "PONG",
            args.project_name,
        )
    )
    report["services"].append(
        inject_service(
            args.base_url,
            "sql",
            sql_probe,
            "accepting connections",
            args.project_name,
        )
    )
    report["finished_at"] = datetime.now(UTC).isoformat()
    if args.output:
        output = ROOT / args.output
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(report, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
