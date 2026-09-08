# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Secret masking for logs, traces and error reports.

The core principle (from the project application): IM tokens, model API keys
and database passwords must never appear in plaintext in logs, trace spans or
exception messages.
"""

from __future__ import annotations

import logging
import re
import threading
from collections.abc import Mapping
from typing import Any
from typing import Callable

_SECRET_PATTERNS: list[tuple[re.Pattern, str]] = [
    (re.compile(r"sk-[A-Za-z0-9_-]{8,}"), "sk-***"),
    (re.compile(r"Bearer\s+[A-Za-z0-9._~+/=-]+"), "Bearer ***"),
    (re.compile(r"(?i)\b(password|passwd|pwd|secret|token|api[_-]?key|authorization)\b\s*[=:]\s*[\"']?[^\s,;\"']+"),
     r"\1=***"),
    (re.compile(r"(?i)(postgres(?:ql)?|mysql|redis)://[^\s@]+:[^\s@]+@"), r"\1://***:***@"),
]
_SECRET_FIELD_PATTERN = re.compile(r"(?i)^(password|passwd|pwd|secret|token|api[_-]?key|authorization)$")
_REDACTING_FACTORY_MARKER = "_trpc_agent_redacting_factory"
_INSTALL_LOCK = threading.RLock()


def _is_secret_field(name: Any) -> bool:
    return isinstance(name, str) and _SECRET_FIELD_PATTERN.fullmatch(name) is not None


def _mask_structured_value(value: Any, *, field_name: Any = None, _seen: set[int] | None = None) -> Any:
    """Mask strings and common containers without mutating caller-owned data."""
    if _is_secret_field(field_name):
        return "***"
    if isinstance(value, str):
        return SecretMasker.mask_value(value)
    if not isinstance(value, (Mapping, tuple, list, set, frozenset)):
        return value

    seen = _seen if _seen is not None else set()
    value_id = id(value)
    if value_id in seen:
        return "<recursive>"
    seen.add(value_id)
    try:
        if isinstance(value, Mapping):
            return {key: _mask_structured_value(item, field_name=key, _seen=seen) for key, item in value.items()}
        if isinstance(value, tuple):
            return tuple(_mask_structured_value(item, _seen=seen) for item in value)
        if isinstance(value, list):
            return [_mask_structured_value(item, _seen=seen) for item in value]
        if isinstance(value, set):
            return {_mask_structured_value(item, _seen=seen) for item in value}
        return frozenset(_mask_structured_value(item, _seen=seen) for item in value)
    finally:
        seen.remove(value_id)


class _RedactingRecordData(dict[str, Any]):
    """A record dictionary that also redacts ``extra`` fields added later."""

    def __init__(self, values: Mapping[str, Any]) -> None:
        super().__init__()
        for key, value in values.items():
            self[key] = value

    def __setitem__(self, key: str, value: Any) -> None:
        super().__setitem__(key, _mask_structured_value(value, field_name=key))


class SecretMasker:
    """Applies secret-redaction patterns to arbitrary text."""

    _exact_secrets: set[str] = set()
    _lock = threading.RLock()

    @classmethod
    def register_secret(cls, value: str) -> None:
        """Register a resolved value for exact redaction in logs and errors."""
        if len(value) < 4:
            return
        with cls._lock:
            cls._exact_secrets.add(value)

    @classmethod
    def mask_value(cls, value: Any) -> Any:
        """Mask a string in place; pass non-strings through untouched."""
        if not isinstance(value, str):
            return value
        with cls._lock:
            exact = sorted(cls._exact_secrets, key=len, reverse=True)
        for secret in exact:
            value = value.replace(secret, "***")
        for pattern, replace in _SECRET_PATTERNS:
            value = pattern.sub(replace, value)
        return value


def _redact_log_record(record: logging.LogRecord) -> logging.LogRecord:
    """Redact every standard text-bearing part of a log record."""
    if not isinstance(record.msg, str):
        record.msg = _mask_structured_value(record.msg)
    preserve_access_args = (record.name.startswith("uvicorn.access") and isinstance(record.args, tuple)
                            and len(record.args) == 5)
    if preserve_access_args:
        # Uvicorn's AccessFormatter unpacks these five positional values after
        # filters run. Redact each value but retain the tuple structure.
        record.msg = SecretMasker.mask_value(record.msg)
        record.args = _mask_structured_value(record.args)
    else:
        if isinstance(record.args, Mapping):
            record.args = _mask_structured_value(record.args)
        try:
            rendered_message = record.getMessage()
        except Exception:  # noqa: BLE001 - logging formatting can raise arbitrary errors
            # Logging should not break application control flow. Drop arguments
            # that could not be rendered safely and retain a redacted template.
            try:
                fallback_message = str(record.msg)
            except Exception:  # noqa: BLE001 - hostile/lazy message objects are allowed
                fallback_message = "<unformattable log message>"
            record.msg = SecretMasker.mask_value(fallback_message)
            record.args = ()
        else:
            # Rendering first keeps %-style positional and mapping arguments valid
            # while allowing patterns such as ``token=%s`` to be redacted safely.
            record.msg = SecretMasker.mask_value(rendered_message)
            record.args = ()

    if record.exc_info:
        if record.exc_text is None:
            record.exc_text = logging.Formatter().formatException(record.exc_info)
        record.exc_text = SecretMasker.mask_value(record.exc_text)
    elif record.exc_text:
        record.exc_text = SecretMasker.mask_value(record.exc_text)
    if record.stack_info:
        record.stack_info = SecretMasker.mask_value(record.stack_info)

    # Logger.makeRecord adds ``extra`` fields after invoking the record
    # factory. Keeping a redacting dict here protects those late additions as
    # well as structured fields already supplied by a custom factory.
    if not isinstance(record.__dict__, _RedactingRecordData):
        record.__dict__ = _RedactingRecordData(record.__dict__)
    return record


def _redacting_factory(previous_factory: Callable[..., logging.LogRecord]) -> Callable[..., logging.LogRecord]:
    """Wrap, rather than replace, an application's existing record factory."""

    def factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        return _redact_log_record(previous_factory(*args, **kwargs))

    setattr(factory, _REDACTING_FACTORY_MARKER, True)
    return factory


class RedactingLogFilter(logging.Filter):
    """A :class:`logging.Filter` that masks secrets on every log record."""

    def filter(self, record: logging.LogRecord) -> bool:
        _redact_log_record(record)
        return True


def safe_error_message(exc: BaseException) -> str:
    """Return an exception message with secrets removed."""
    return SecretMasker.mask_value(str(exc))


def install_redacting_log_filter() -> None:
    """Install process-wide log redaction while preserving custom factories.

    The record factory covers child loggers, non-propagating loggers and
    handlers created by later ``dictConfig`` calls. The root filter remains as
    a compatibility fallback for records passed directly to the root logger.
    Repeated calls are safe.
    """
    with _INSTALL_LOCK:
        current_factory = logging.getLogRecordFactory()
        if not getattr(current_factory, _REDACTING_FACTORY_MARKER, False):
            logging.setLogRecordFactory(_redacting_factory(current_factory))

        root = logging.getLogger()
        if not any(isinstance(log_filter, RedactingLogFilter) for log_filter in root.filters):
            root.addFilter(RedactingLogFilter())
