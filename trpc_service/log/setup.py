"""JSON logging that prevents common credential and content leaks."""

from __future__ import annotations

import contextlib
import contextvars
import logging
import re
from collections.abc import Iterator, Mapping, Sequence
from typing import Any

from pythonjsonlogger.json import JsonFormatter

_REDACTED = "[REDACTED]"
_SENSITIVE_KEY = re.compile(
    r"(?:authorization|cookie|password|passwd|secret|token|api[_-]?key|"
    r"aes[_-]?key|response[_-]?url|ciphertext|signature|echostr|nonce|prompt|content)",
    re.IGNORECASE,
)
_BEARER_VALUE = re.compile(r"(?i)\bbearer\s+[A-Za-z0-9._~+/=-]+")
_URL_CREDENTIAL = re.compile(
    r"(?i)([?&](?:access_token|key|secret|signature|msg_signature|token|echostr|nonce)=)"
    r"[^&#\s]+"
)
_URL_AUTHORITY_CREDENTIAL = re.compile(r"(?i)(\b[a-z][a-z0-9+.-]*://[^\s/:@]+:)[^\s/@]+(@)")
_MAX_DEPTH = 6
_MAX_ITEMS = 100
_MAX_STRING = 2_048

_tenant_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "log_tenant_id", default=None
)
_request_id: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "log_request_id", default=None
)
_trace_id: contextvars.ContextVar[str | None] = contextvars.ContextVar("log_trace_id", default=None)


def redact(value: Any, *, key: str | None = None, _depth: int = 0) -> Any:
    """Return a bounded, recursively redacted logging-safe value."""

    if key is not None and _SENSITIVE_KEY.search(key):
        return _REDACTED
    if _depth >= _MAX_DEPTH:
        return "[TRUNCATED]"
    if isinstance(value, str):
        sanitized = _BEARER_VALUE.sub("Bearer [REDACTED]", value)
        sanitized = _URL_CREDENTIAL.sub(r"\1[REDACTED]", sanitized)
        sanitized = _URL_AUTHORITY_CREDENTIAL.sub(r"\1[REDACTED]\2", sanitized)
        if len(sanitized) > _MAX_STRING:
            return sanitized[:_MAX_STRING] + "…[TRUNCATED]"
        return sanitized
    if isinstance(value, Mapping):
        return {
            str(item_key): redact(
                item_value,
                key=str(item_key),
                _depth=_depth + 1,
            )
            for item_key, item_value in list(value.items())[:_MAX_ITEMS]
        }
    if isinstance(value, Sequence) and not isinstance(value, (bytes, bytearray)):
        return [redact(item, _depth=_depth + 1) for item in value[:_MAX_ITEMS]]
    if isinstance(value, bytes):
        return f"<bytes:{len(value)}>"
    return value


@contextlib.contextmanager
def bind_log_context(
    *,
    tenant_id: str | None = None,
    request_id: str | None = None,
    trace_id: str | None = None,
) -> Iterator[None]:
    """Bind correlation fields for the current async execution context."""

    tokens = (
        (_tenant_id, _tenant_id.set(tenant_id)),
        (_request_id, _request_id.set(request_id)),
        (_trace_id, _trace_id.set(trace_id)),
    )
    try:
        yield
    finally:
        for variable, token in reversed(tokens):
            variable.reset(token)


class _ContextAndRedactionFilter(logging.Filter):
    """Attach correlation data and sanitize every non-standard record field."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.tenant_id = _tenant_id.get()
        record.request_id = _request_id.get()
        record.trace_id = _trace_id.get()
        if record.exc_info is not None:
            exception_class = record.exc_info[0]
            record.exception_type = (
                exception_class.__name__ if exception_class is not None else "Exception"
            )
            record.exc_info = None
            record.exc_text = None
            record.stack_info = None
        record.msg = redact(record.msg)
        if isinstance(record.args, Mapping):
            record.args = redact(record.args)
        elif isinstance(record.args, tuple):
            record.args = tuple(redact(item) for item in record.args)
        for field_name, field_value in list(record.__dict__.items()):
            if field_name not in _STANDARD_LOG_RECORD_FIELDS:
                setattr(record, field_name, redact(field_value, key=field_name))
        return True


_STANDARD_LOG_RECORD_FIELDS = frozenset(logging.LogRecord("", 0, "", 0, "", (), None).__dict__) | {
    "tenant_id",
    "request_id",
    "trace_id",
}


def configure_logging(level: str = "INFO") -> None:
    """Configure a single JSON handler suitable for containers."""

    handler = logging.StreamHandler()
    handler.addFilter(_ContextAndRedactionFilter())
    handler.setFormatter(
        JsonFormatter(
            "%({})s %({})s %({})s %({})s %({})s %({})s %({})s".format(
                "asctime",
                "levelname",
                "name",
                "message",
                "tenant_id",
                "request_id",
                "trace_id",
            ),
            rename_fields={"levelname": "level", "asctime": "timestamp"},
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
