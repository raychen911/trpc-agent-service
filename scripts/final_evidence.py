#!/usr/bin/env python3
"""Write and validate the deliberately small final-acceptance evidence schema."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

CHECKS = frozenset({
    "preflight",
    "compose_health",
    "worker_topology",
    "webhook_protocol",
    "tenant_isolation",
    "state_backends",
    "idempotency_ordering",
    "governance",
    "approval",
    "rate_budget",
    "audit_usage_trace",
    "rollout_rollback",
    "capacity_faults",
    "external_wecom",
    "external_feishu",
    "cleanup",
})
EXTERNAL_CHECKS = frozenset({"external_wecom", "external_feishu"})
MANDATORY_CHECKS = CHECKS - EXTERNAL_CHECKS
RESULTS = frozenset({"pass", "fail", "external_unavailable"})
COUNTERS = frozenset(
    {"requests", "events", "workers", "tenants", "artifacts", "traces", "deliveries", "failures", "replays", "faults"})


def _error(message: str) -> None:
    raise ValueError(message)


def _counter(value: str) -> tuple[str, int]:
    key, separator, raw_number = value.partition("=")
    if separator != "=" or key not in COUNTERS or not raw_number.isdecimal():
        _error("invalid evidence counter")
    return key, int(raw_number)


def _load(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"schema_version": 1, "checks": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError("invalid evidence document") from error
    _validate_document(payload, require_complete=False)
    return payload


def _validate_document(payload: Any, *, require_complete: bool) -> None:
    if not isinstance(payload, dict) or set(payload) != {"schema_version", "checks"}:
        _error("invalid evidence document")
    if payload["schema_version"] != 1 or not isinstance(payload["checks"], list):
        _error("invalid evidence document")
    seen: set[str] = set()
    for item in payload["checks"]:
        if not isinstance(item, dict) or set(item) != {"check", "result", "duration_ms", "counters"}:
            _error("invalid evidence check")
        check, result = item["check"], item["result"]
        if check not in CHECKS or check in seen or result not in RESULTS:
            _error("invalid evidence check")
        if result == "external_unavailable" and check not in EXTERNAL_CHECKS:
            _error("external result is not allowed for this check")
        if not isinstance(item["duration_ms"], int) or isinstance(item["duration_ms"], bool) or item["duration_ms"] < 0:
            _error("invalid evidence duration")
        counters = item["counters"]
        if not isinstance(counters, dict) or any(
                key not in COUNTERS or not isinstance(number, int) or isinstance(number, bool) or number < 0
                for key, number in counters.items()):
            _error("invalid evidence counters")
        seen.add(check)
    if require_complete and not MANDATORY_CHECKS <= seen:
        _error("mandatory evidence checks are missing")
    if require_complete and any(item["result"] == "fail" for item in payload["checks"]):
        _error("acceptance evidence contains a failed check")


def _atomic_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=".report-", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(payload, stream, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _record(arguments: argparse.Namespace) -> int:
    if arguments.check not in CHECKS or arguments.result not in RESULTS:
        _error("invalid evidence check")
    if arguments.result == "external_unavailable" and arguments.check not in EXTERNAL_CHECKS:
        _error("external result is not allowed for this check")
    if arguments.duration_ms < 0:
        _error("invalid evidence duration")
    counters = dict(_counter(value) for value in arguments.counter)
    if len(counters) != len(arguments.counter):
        _error("duplicate evidence counter")
    path = Path(arguments.path)
    payload = _load(path)
    checks = payload["checks"]
    if any(item["check"] == arguments.check for item in checks):
        _error("duplicate evidence check")
    checks.append({
        "check": arguments.check,
        "result": arguments.result,
        "duration_ms": arguments.duration_ms,
        "counters": counters,
    })
    _atomic_write(path, payload)
    return 0


def _validate(arguments: argparse.Namespace) -> int:
    _validate_document(_load(Path(arguments.path)), require_complete=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="strict, redacted final acceptance evidence")
    subcommands = parser.add_subparsers(dest="command", required=True)
    record = subcommands.add_parser("record")
    record.add_argument("--path", required=True)
    record.add_argument("--check", required=True)
    record.add_argument("--result", required=True)
    record.add_argument("--duration-ms", type=int, required=True)
    record.add_argument("--counter", action="append", default=[])
    validate = subcommands.add_parser("validate")
    validate.add_argument("--path", required=True)
    arguments = parser.parse_args()
    try:
        return _record(arguments) if arguments.command == "record" else _validate(arguments)
    except ValueError as error:
        print(str(error), file=os.sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
