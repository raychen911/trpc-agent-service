"""Structured JSON logging with mandatory redaction."""

from __future__ import annotations

import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

from trpc_service.log.redaction import redact, sanitize_text

_STANDARD_FIELDS = set(logging.makeLogRecord({}).__dict__) | {"message", "asctime"}


class RedactingJsonFormatter(logging.Formatter):
    """Emit one redacted JSON object per log record."""

    def format(self, record: logging.LogRecord) -> str:
        trace_id = record.__dict__.get("trace_id") or record.__dict__.get("trpc.trace_id")
        exc_info = record.exc_info
        has_exception = bool(exc_info)
        payload: dict[str, Any] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            # An exception log's formatted message often interpolates the
            # provider exception or user input.  Keep the operational event
            # but never serialize that caller-controlled text.
            "message": "operation failed" if has_exception else sanitize_text(record.getMessage()),
        }
        if trace_id:
            payload["trace_id"] = redact(trace_id, key="trace_id")
        for key, value in record.__dict__.items():
            if key not in _STANDARD_FIELDS and not key.startswith("_"):
                payload[key] = redact(value, key=key)
        if has_exception:
            # Exception text and tracebacks routinely contain user prompts,
            # provider responses, tool arguments, or credentials.  Keep only
            # bounded operational identifiers; the traceback remains in local
            # debugger state and is never exported by the service formatter.
            payload["exception"] = {
                "error_type": (
                    exc_info[0].__name__
                    if exc_info is not None and exc_info[0] is not None
                    else "Exception"
                ),
                "safe_code": record.__dict__.get("safe_code", "unhandled_exception"),
            }
        return json.dumps(redact(payload), ensure_ascii=False, separators=(",", ":"), default=str)


def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger exactly once for a service process."""

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(RedactingJsonFormatter())
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())


__all__ = ["RedactingJsonFormatter", "configure_logging"]
