"""Structured process logging with async-safe request and execution context."""

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar, Token
from datetime import datetime, timezone
import json
import logging

from trpc_service.log.redaction import SensitiveDataRedactor

_LOG_CONTEXT: ContextVar[dict[str, str]] = ContextVar("trpc_log_context", default={})
_CONTEXT_FIELDS = (
    "service",
    "environment",
    "node_id",
    "node_role",
    "tenant_id",
    "agent_app_id",
    "request_id",
    "trace_id",
    "session_id",
)


@contextmanager
def bind_log_context(**fields: object) -> Iterator[None]:
    """Bind low-cardinality process fields and request correlation to this task."""

    current = dict(_LOG_CONTEXT.get())
    current.update({key: str(value) for key, value in fields.items() if value is not None})
    token: Token[dict[str, str]] = _LOG_CONTEXT.set(current)
    try:
        yield
    finally:
        _LOG_CONTEXT.reset(token)


class JsonLogFormatter(logging.Formatter):
    """Render one redacted JSON object suitable for local files and Loki."""

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: str = "%",
        validate: bool = True,
        *,
        defaults: Mapping[str, object] | None = None,
        use_colors: bool | None = None,
    ) -> None:
        del fmt, datefmt, style, validate, defaults, use_colors
        super().__init__()
        self._redactor = SensitiveDataRedactor()

    def format(self, record: logging.LogRecord) -> str:
        """Keep identifiers queryable while redacting the fully rendered message."""

        payload: dict[str, object] = {
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": self._redactor.redact_text(record.getMessage(), redact_pii=True),
        }
        context = _LOG_CONTEXT.get()
        for field in _CONTEXT_FIELDS:
            value = context.get(field)
            if value:
                payload[field] = value
        if record.exc_info:
            exception = self.formatException(record.exc_info)
            payload["exception"] = self._redactor.redact_text(exception, redact_pii=True)
            exception_type = record.exc_info[0]
            if exception_type is not None:
                payload["error_type"] = exception_type.__name__
        return json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
