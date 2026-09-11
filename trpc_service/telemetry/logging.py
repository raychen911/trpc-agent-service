"""JSON operational logs with an allowlist boundary, including SDK diagnostics."""

from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
import json
import logging
import math
import re

from opentelemetry import trace

_context = ContextVar("operational_log_context", default={})
_service = "trpc-service"
_identifiers = {"tenant_id", "request_id", "execution_id", "session_id", "channel", "stage", "error_type"}
_numbers = {"latency_ms", "config_version"}


def current_fields():
    return dict(_context.get())


def safe_fields(values):
    result = {}
    for key, value in values.items():
        key = key.replace(".", "_")
        if key in _identifiers and isinstance(value, str) and re.fullmatch(r"[A-Za-z0-9_.:-]{1,128}", value):
            result[key] = value
        elif key in _numbers and isinstance(value, (int, float)) and math.isfinite(value):
            result[key] = value
    return result


@contextmanager
def context(**values):
    token = _context.set({**_context.get(), **safe_fields(values)})
    try:
        yield
    finally:
        _context.reset(token)


class SafeJsonFormatter(logging.Formatter):
    """Never interpolate arbitrary dependency messages, extras or traceback bodies."""

    def format(self, record):
        event = getattr(record, "operational_event", "dependency.diagnostic")
        if not isinstance(event, str) or not re.fullmatch(r"[a-z][a-z0-9_.]{0,79}", event):
            event = "dependency.diagnostic"
        value = dict(timestamp=datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
                     severity=record.levelname,
                     service=_service,
                     event=event)
        value.update(safe_fields(_context.get()))
        value.update(safe_fields(getattr(record, "operational_fields", {})))
        span = trace.get_current_span().get_span_context()
        if span.is_valid:
            value.update(trace_id=f"{span.trace_id:032x}", span_id=f"{span.span_id:016x}")
        if record.exc_info and record.exc_info[0]:
            value["error_type"] = record.exc_info[0].__name__
        return json.dumps(value, ensure_ascii=True, allow_nan=False)


def emit(event, *, level=logging.INFO, **fields):
    logging.getLogger("trpc_service.operations").log(level,
                                                     event,
                                                     extra={
                                                         "operational_event": event,
                                                         "operational_fields": fields
                                                     })


def configure_logging(service, *, stream=None):
    global _service
    _service = service
    handler = logging.StreamHandler(stream)
    handler.setFormatter(SafeJsonFormatter())
    logging.basicConfig(level=logging.INFO, handlers=[handler], force=True)
    # Uvicorn may already have installed its own body-bearing formatters.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access"):
        logger = logging.getLogger(name)
        logger.handlers.clear()
        logger.propagate = True
    from .sdk_logging import configure_sdk_logging
    configure_sdk_logging()
