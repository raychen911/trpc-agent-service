"""Whitelist-based safe structured logging primitives (Stage 6B1).

``safe_log`` is the ONLY logging API product code may use.  It accepts a
fixed set of field names with strict runtime types — every field a ``str``
except ``duration_ms`` (a finite ``int``/``float``; ``bool`` and explicit
``None`` rejected; omit a field to leave it unset) — and never forwards
caller strings unchecked: control characters are pre-escaped, values are
length capped, and the record is rendered as single-line JSON by the handler
installed via :func:`trpc_service.log.config.configure_logging`.

Raw payloads, prompts, responses, tool arguments, headers, URL queries and
``str(exc)`` values must never be passed in — the whitelist makes that a
hard programming error (``ValueError``/``TypeError``).
"""

from __future__ import annotations

import logging
import math
from typing import Any, Final, TypedDict

# Fixed field whitelist — verbatim from the Stage 6B1 design spec.
SAFE_LOG_FIELD_NAMES: Final[frozenset[str]] = frozenset({
    "service",
    "event",
    "trace_id",
    "span_id",
    "request_id",
    "tenant_id",
    "channel",
    "operation",
    "status",
    "error_code",
    "exception_type",
    "duration_ms",
    "worker_endpoint",
})

MAX_FIELD_VALUE_CHARS: Final[int] = 512
TRUNCATION_SUFFIX: Final[str] = "[truncated]"

# Attribute carrying the validated payload on a LogRecord; read by
# SafeLogFormatter, scanned by SensitiveDataFilter.
_SAFE_RECORD_ATTR: Final[str] = "_trpc_safe_log_fields"

# Renders every Unicode "Cc" character (C0, DEL, C1) as a literal ``\uXXXX``
# text sequence.  ``json.dumps`` escapes C0/C1 on the wire already, but this
# also keeps the *decoded* value free of raw control characters.
_CONTROL_TRANSLATION = str.maketrans(
    {chr(code): f"\\u{code:04x}"
     for code in (*range(0x00, 0x20), 0x7f, *range(0x80, 0xa0))})


class SafeLogFields(TypedDict, total=False):
    """Typed shape of the keyword fields accepted by :func:`safe_log`."""

    service: str
    event: str
    trace_id: str
    span_id: str
    request_id: str
    tenant_id: str
    channel: str
    operation: str
    status: str
    error_code: str
    exception_type: str
    duration_ms: float
    worker_endpoint: str


def _clean_string(value: str) -> str:
    escaped = value.translate(_CONTROL_TRANSLATION)
    if len(escaped) <= MAX_FIELD_VALUE_CHARS:
        return escaped
    return escaped[:MAX_FIELD_VALUE_CHARS - len(TRUNCATION_SUFFIX)] + TRUNCATION_SUFFIX


def _clean_value(name: str, value: Any) -> Any:
    if name == "duration_ms":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise TypeError(f"safe log field 'duration_ms' requires a finite number, got {type(value).__name__}")
        if isinstance(value, float) and not math.isfinite(value):
            raise TypeError("safe log field 'duration_ms' requires a finite number")
        return value
    # Every other whitelisted field is strictly a string at runtime.  An
    # unset field is expressed by omitting it — explicit None is rejected so
    # "unknown" can never serialize as a bare null that downstream consumers
    # might conflate with an empty value.
    if not isinstance(value, str):
        raise TypeError(f"safe log field {name!r} requires a string (omit the field instead of passing "
                        f"None or a non-string), got {type(value).__name__}")
    return _clean_string(value)


def _resolve_level(level: Any) -> int:
    if isinstance(level, bool) or not isinstance(level, (int, str)):
        raise TypeError(f"level must be an int or a level name, got {type(level).__name__}")
    if isinstance(level, int):
        return level
    resolved = logging.getLevelName(level.upper())
    if not isinstance(resolved, int):
        raise ValueError(f"unknown log level name: {level!r}")
    return resolved


def safe_log(logger: logging.Logger, level: int | str, event: str, **fields: Any) -> None:
    """Emit one whitelisted, scalar-only, single-line JSON log record.

    Raises :class:`ValueError` for unknown field names and :class:`TypeError`
    for wrong runtime value types (non-``str`` fields, explicit ``None``
    where the field should simply be omitted, non-finite ``duration_ms``) or
    invalid levels — misuse is a hard, local programming error, never a
    silent leak.
    """
    resolved = _resolve_level(level)
    if not isinstance(event, str) or not event:
        raise TypeError("safe log event must be a non-empty string")
    payload: dict[str, Any] = {"event": _clean_string(event)}
    for name, value in fields.items():
        if name not in SAFE_LOG_FIELD_NAMES or name == "event":
            raise ValueError(f"unknown safe log field: {name!r}")
        payload[name] = _clean_value(name, value)
    logger.log(resolved, payload["event"], extra={_SAFE_RECORD_ATTR: payload})


__all__ = [
    "MAX_FIELD_VALUE_CHARS",
    "SAFE_LOG_FIELD_NAMES",
    "SafeLogFields",
    "safe_log",
]
