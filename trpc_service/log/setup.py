from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timezone
from typing import Any

_REDACTED = "[REDACTED]"
_SENSITIVE_KEY = re.compile(
    r"(^|_)(authorization|cookie|password|passwd|secret|token|api_?key|private_?key)($|_)",
    re.IGNORECASE,
)
_SENSITIVE_VALUE_PATTERNS = (
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{8,}\b"),
    re.compile(r"(?i)(https?://[^:/\s]+:)[^@/\s]+@"),
    re.compile(r"(?i)(authorization|api_?key|token|secret|password)=([^&\s]+)"),
)


def _sanitize_text(value: str) -> str:
    sanitized = value
    for pattern in _SENSITIVE_VALUE_PATTERNS:
        if pattern.pattern.startswith("(?i)(https?"):
            sanitized = pattern.sub(r"\1[REDACTED]@", sanitized)
        elif pattern.groups >= 2:
            sanitized = pattern.sub(r"\1=[REDACTED]", sanitized)
        else:
            sanitized = pattern.sub(_REDACTED, sanitized)
    return sanitized


def sanitize_log_value(value: Any, key: str | None = None) -> Any:
    if key and _SENSITIVE_KEY.search(key):
        return _REDACTED
    if isinstance(value, str):
        return _sanitize_text(value)
    if isinstance(value, dict):
        return {
            str(item_key): sanitize_log_value(item, str(item_key))
            for item_key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [sanitize_log_value(item) for item in value]
    return value


class SensitiveDataFilter(logging.Filter):
    """Sanitize messages emitted by application and third-party SDK loggers."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg = _sanitize_text(str(record.getMessage()))
        record.args = ()
        return True


class JsonFormatter(logging.Formatter):
    """Small JSON formatter suitable for container log collection."""

    _standard_attributes = frozenset(
        {
            "args",
            "asctime",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "message",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "thread",
            "threadName",
            "taskName",
        }
    )
    _allowed_context = frozenset(
        {
            "agent_app_id",
            "channel",
            "code",
            "environment",
            "execution_id",
            "latency_ms",
            "message_id",
            "method",
            "node_id",
            "outbox_id",
            "path",
            "request_id",
            "service",
            "session_id",
            "status",
            "status_code",
            "tenant_id",
            "topic",
            "trace_id",
            "version",
        }
    )

    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": _sanitize_text(record.getMessage()),
        }
        payload.update(
            {
                key: value
                for key, value in record.__dict__.items()
                if key not in self._standard_attributes and key in self._allowed_context
            }
        )
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        payload = sanitize_log_value(payload)
        return json.dumps(payload, ensure_ascii=False, default=str)


def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger once for local and container execution."""

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(SensitiveDataFilter())

    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
    for logger_name in (
        "boto3",
        "botocore",
        "google",
        "httpcore",
        "httpx",
        "openai",
        "trpc_agent_sdk",
        "urllib3",
    ):
        logging.getLogger(logger_name).setLevel(logging.WARNING)
