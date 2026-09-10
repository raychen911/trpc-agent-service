"""Run auditable online IM acceptance probes.

The probe deliberately separates platform acknowledgement from application
completion.  Real platform credentials are required for real-channel cases;
local fake servers may exercise the protocol but cannot be reported as a
production pass.
"""

from __future__ import annotations

import argparse
import json
import os
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

if __package__ in {None, ""}:
    import sys

    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.evidence_lineage import (
    ROOT,
    make_evidence,
    release_binding,
    write_json,
)


def _truthy(name: str, default: str = "0") -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _safe_error(value: object) -> str:
    text = str(value)
    for name in ("token", "secret", "password", "api_key", "authorization"):
        text = text.replace(os.getenv(name.upper(), "__missing__"), "[redacted]")
    return text[:500]


def request_json(
    url: str,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: float = 15,
) -> dict[str, Any]:
    request = Request(
        url,
        data=(json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None),
        headers={"Content-Type": "application/json", **(headers or {})},
        method="POST" if payload is not None else "GET",
    )
    with urlopen(request, timeout=timeout) as response:
        raw = response.read().decode("utf-8", errors="replace")
        try:
            body: Any = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            body = raw[:1000]
        return {"status": response.status, "body": body}


def _ack(result: dict[str, Any]) -> tuple[bool, str]:
    if not 200 <= int(result.get("status", 0)) < 300:
        return False, f"HTTP status {result.get('status')}"
    body = result.get("body")
    if isinstance(body, dict) and body.get("ok") is False:
        return False, "application ACK contains ok=false"
    if isinstance(body, dict) and body.get("accepted") is False:
        return False, "application ACK contains accepted=false"
    return True, "ok"


def _task_id(result: dict[str, Any]) -> str:
    body = result.get("body")
    return str(body.get("task_id", "")) if isinstance(body, dict) else ""


def _poll_task(
    base_url: str,
    task_id: str,
    headers: dict[str, str],
    timeout: float,
    interval: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout
    last: dict[str, Any] = {"status": 0, "body": {}}
    while time.monotonic() < deadline:
        try:
            last = request_json(
                f"{base_url}/admin/v1/webhook-tasks/{task_id}",
                headers=headers,
                timeout=min(10, timeout),
            )
            body = last.get("body")
            state = body.get("status") if isinstance(body, dict) else None
            if state in {"completed", "dead"}:
                return last
        except (HTTPError, URLError, OSError, ValueError) as exc:
            last = {"status": 0, "body": {}, "error": _safe_error(exc)}
        time.sleep(interval)
    return last


def _case_result(
    channel: str,
    first: dict[str, Any],
    second: dict[str, Any],
    first_state: dict[str, Any] | None,
    second_state: dict[str, Any] | None,
    require_durable: bool,
) -> dict[str, Any]:
    first_ok, first_reason = _ack(first)
    second_ok, second_reason = _ack(second)
    first_body = first_state.get("body", {}) if first_state else {}
    second_body = second_state.get("body", {}) if second_state else {}
    first_completed = not first_state or first_body.get("status") == "completed"
    second_completed = not second_state or second_body.get("status") == "completed"
    duplicate = second_body.get("result", {}).get("duplicate") is True if isinstance(second_body, dict) else False
    durable = bool(_task_id(first) and _task_id(second))
    ok = (
        first_ok
        and second_ok
        and first_completed
        and second_completed
        and duplicate
        and (durable or not require_durable)
    )
    reasons = [reason for passed, reason in ((first_ok, first_reason), (second_ok, second_reason)) if not passed]
    if not first_completed:
        reasons.append("first delivery did not complete")
    if not second_completed:
        reasons.append("replay did not complete")
    if not duplicate:
        reasons.append("replay was not reported as duplicate")
    if require_durable and not durable:
        reasons.append("durable task IDs are required")
    return {
        "channel": channel,
        "status": "pass" if ok else "fail",
        "checks": {
            "first_ack": {"status": "pass" if first_ok else "fail", "reason": first_reason},
            "replay_ack": {"status": "pass" if second_ok else "fail", "reason": second_reason},
            "first_completed": first_completed,
            "replay_completed": second_completed,
            "replay_duplicate": duplicate,
            "durable": durable,
        },
        "first": {"status": first.get("status"), "task_id": _task_id(first)},
        "replay": {"status": second.get("status"), "task_id": _task_id(second)},
        "first_state": first_body,
        "replay_state": second_body,
        "reason": "; ".join(reasons) if reasons else "ok",
    }


def probe_case(
    base_url: str,
    channel: str,
    account_id: str,
    payload: dict[str, Any],
    headers: dict[str, str] | None = None,
    admin_headers: dict[str, str] | None = None,
    require_durable: bool = True,
    timeout: float = 30,
    interval: float = 0.25,
    request: Callable[..., dict[str, Any]] = request_json,
) -> dict[str, Any]:
    endpoint = f"{base_url}/webhooks/{channel}/{account_id}"
    try:
        first = request(endpoint, payload=payload, headers=headers, timeout=timeout)
        second = request(endpoint, payload=payload, headers=headers, timeout=timeout)
        first_task = _task_id(first)
        second_task = _task_id(second)
        first_state = (
            _poll_task(base_url, first_task, admin_headers or {}, timeout, interval) if first_task else None
        )
        second_state = (
            _poll_task(base_url, second_task, admin_headers or {}, timeout, interval) if second_task else None
        )
        return _case_result(channel, first, second, first_state, second_state, require_durable)
    except (HTTPError, URLError, OSError, ValueError, TypeError) as exc:
        return {"channel": channel, "status": "fail", "reason": _safe_error(exc)}


def _load_payload(channel: str) -> tuple[str, dict[str, Any]]:
    path_value = os.getenv(f"ONLINE_IM_{channel.upper()}_PAYLOAD_FILE", "").strip()
    if not path_value:
        raise ValueError("payload file is not configured")
    payload = json.loads(Path(path_value).read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("payload file must contain a JSON object")
    account_id = str(payload.pop("_account_id"))
    return account_id, payload


def _binding() -> dict[str, Any] | None:
    context_path = os.getenv("TRPC_RELEASE_CONTEXT", "").strip()
    lock_path = os.getenv("TRPC_CANDIDATE_LOCK", "").strip()
    if not context_path or not lock_path:
        return None
    from scripts.candidate_lock import verify_lock
    from scripts.release_context import verify_context

    context = verify_context(Path(context_path), root=ROOT)
    lock = verify_lock(Path(context_path), Path(lock_path), root=ROOT)
    return release_binding(context, lock)


def run() -> dict[str, Any]:
    base_url = os.getenv("ONLINE_IM_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
    timeout = float(os.getenv("ONLINE_IM_TIMEOUT_SECONDS", "15"))
    probe_timeout = float(os.getenv("ONLINE_IM_PROBE_TIMEOUT_SECONDS", "30"))
    interval = float(os.getenv("ONLINE_IM_POLL_INTERVAL_SECONDS", "0.25"))
    admin_key = os.getenv("ONLINE_IM_ADMIN_API_KEY", os.getenv("ADMIN_API_KEY", "")).strip()
    admin_headers = {"X-Admin-API-Key": admin_key} if admin_key else {}
    checks: list[dict[str, Any]] = []
    try:
        health = request_json(f"{base_url}/health", timeout=timeout)
        health_ok = (
            health.get("status") == 200
            and isinstance(health.get("body"), dict)
            and health["body"].get("status") in {"ok", "degraded"}
        )
        checks.append({"channel": "health", "status": "pass" if health_ok else "fail", "result": health})
    except (HTTPError, URLError, OSError, ValueError) as exc:
        checks.append({"channel": "health", "status": "fail", "reason": _safe_error(exc)})
        return {"status": "fail", "checks": checks}

    web_payload = {
        "channel": "web",
        "account_id": os.getenv("ONLINE_IM_WEB_ACCOUNT", "web_demo"),
        "user_id": "online-im-gate",
        "message_id": os.getenv("ONLINE_IM_WEB_MESSAGE_ID", "online-im-gate-web"),
        "text": "online IM gate",
    }
    try:
        first = request_json(f"{base_url}/ui/api/chat", payload=web_payload, timeout=probe_timeout)
        first_ok, first_reason = _ack(first)
        # Durable async mode acknowledges the first request before the worker
        # completes it. Retry the replay until the idempotency record is
        # terminal, then require the API's explicit duplicate response.
        second: dict[str, Any] = {"status": 0, "body": {}}
        second_ok = False
        second_reason = "replay did not complete"
        duplicate = False
        deadline = time.monotonic() + probe_timeout
        while time.monotonic() < deadline:
            try:
                second = request_json(f"{base_url}/ui/api/chat", payload=web_payload, timeout=probe_timeout)
                second_ok, second_reason = _ack(second)
                second_body = second.get("body", {})
                duplicate = isinstance(second_body, dict) and second_body.get("duplicate") is True
                if duplicate:
                    break
            except (HTTPError, URLError, OSError, ValueError) as exc:
                second_reason = _safe_error(exc)
            time.sleep(0.25)
        web_ok = first_ok and second_ok and duplicate
        checks.append(
            {
                "channel": "web",
                "status": "pass" if web_ok else "fail",
                "checks": {
                    "first_ack": first_reason,
                    "replay_ack": second_reason,
                    "replay_duplicate": duplicate,
                },
            }
        )
    except (HTTPError, URLError, OSError, ValueError) as exc:
        checks.append({"channel": "web", "status": "fail", "reason": _safe_error(exc)})

    channels = [value.strip().lower() for value in os.getenv("ONLINE_IM_CHANNELS", "").split(",") if value.strip()]
    required = _truthy("ONLINE_IM_REQUIRED")
    for channel in channels:
        try:
            account_id, payload = _load_payload(channel)
            checks.append(
                probe_case(
                    base_url,
                    channel,
                    account_id,
                    payload,
                    headers={"X-Telegram-Bot-Api-Secret-Token": os.getenv("ONLINE_IM_TELEGRAM_CALLBACK_SECRET", "")}
                    if channel == "telegram"
                    else None,
                    admin_headers=admin_headers,
                    require_durable=_truthy("ONLINE_IM_REQUIRE_DURABLE", "1"),
                    timeout=probe_timeout,
                    interval=interval,
                )
            )
        except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
            checks.append({"channel": channel, "status": "fail", "reason": _safe_error(exc)})

    if not channels and required:
        checks.append(
            {"channel": "real_channels", "status": "fail", "reason": "ONLINE_IM_CHANNELS is empty"}
        )
    status = (
        "pass"
        if all(item.get("status") == "pass" for item in checks) and (not required or bool(channels))
        else "fail"
    )
    report: dict[str, Any] = {
        "generated_at": datetime.now(UTC).isoformat(),
        "status": status,
        "required": required,
        "checks": checks,
    }
    binding = _binding()
    if binding is not None:
        envelope = make_evidence("online-im", "scripts.online_im_gate", report, binding)
        output = Path(os.getenv("ONLINE_IM_EVIDENCE_OUTPUT", "data/online-im-evidence.json"))
        write_json(output, envelope)
        report["evidence"] = {
            "path": str(output),
            "evidence_sha256": envelope["evidence_sha256"],
            "release_binding": binding,
        }
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.parse_args(argv)
    try:
        report = run()
    except (OSError, ValueError, KeyError, json.JSONDecodeError) as exc:
        report = {"status": "fail", "reason": _safe_error(exc), "checks": []}
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report.get("status") == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
