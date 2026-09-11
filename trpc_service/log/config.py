"""Process-level safe logging configuration (Stage 6B1).

:func:`configure_logging` installs exactly one single-line JSON handler on
stderr at the root logger — replacing any pre-existing root handlers so no
raw handler can ever emit in parallel.  It is idempotent: repeated calls
replace the previous safe handler instead of growing the handler list.

Third-party takeover is **fail-closed by default**:

- Dropped entirely (raw logs never reach root, no heuristic can be trusted
  to recognise arbitrary user content): ``trpc_agent_sdk``, ``aibot``,
  ``Lark``, ``opentelemetry`` and ``uvicorn.access`` (export failures can
  embed connection details; access lines embed paths and query strings).
  Primary defense: a chained, idempotent **LogRecord
  factory** scrubs every record created on one of those names or its
  ``prefix.`` subtree *at creation time* — ``msg``/``args``/``exc_info``/
  ``exc_text``/``stack_info`` are replaced with the fixed category
  ``third_party_log_dropped`` before any handler can see them, so even a
  sub-logger an SDK invents later (with a force-appended raw handler, past
  ``Logger.disabled`` which only gates its own logger) emits nothing but
  that constant.  Supporting defenses: ``disabled = True`` and
  ``propagate = False`` (with a ``NullHandler``) on the named loggers, plus
  instance-level ``addHandler``/``removeHandler`` refusals as hygiene.  No
  original text is retained on factory, filter, or logger objects.
  Observability of SDK/transport errors is provided by the product adapter
  layer through :func:`trpc_service.log.safe.safe_log` (fixed event,
  error_code and exception_type only) — e.g. the WeCom ``_SafeWeComLogger``
  facade.
- Routed through the safe root path (fixed operational text only, never
  their own handlers): ``uvicorn``, ``uvicorn.error``, ``uvicorn.asgi``.

Verified against the installed SDKs:

- ``trpc_agent_sdk``: ``DefaultLogger`` wraps
  ``logging.getLogger("trpc_agent_sdk")`` and self-installs a stdout handler
  when that logger has none.
- ``Lark``: the Feishu SDK's log module (lark-channel-sdk 1.4.0) adds a
  stdout handler at import time, possibly after configuration.
- ``aibot``: wecom-aibot-python-sdk 1.0.2 prints via its own logger object;
  the product facade injects ``_SafeWeComLogger`` (fixed category text).

Uvicorn must be started with ``log_config=None`` and ``access_log=False``
(see :func:`trpc_service._cli._run_server`): ``uvicorn.run(log_config=None)``
silently substitutes the default dictConfig, which would replace the safe
root handlers with raw ones.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from typing import Any, Mapping

from .safe import _SAFE_RECORD_ATTR, _clean_string

THIRD_PARTY_LOGGER_NAMES = ("trpc_agent_sdk", "aibot", "Lark", "opentelemetry")
# Fail-closed dropped (see module docstring): raw third-party/user-shaped text.
DROP_LOGGER_NAMES = THIRD_PARTY_LOGGER_NAMES + ("uvicorn.access", )
# Kept visible but only through the safe root handler; never their own handlers.
OPERATIONAL_LOGGER_NAMES = ("uvicorn", "uvicorn.error", "uvicorn.asgi")
LOG_LEVEL_ENV_VAR = "TRPC_LOG_LEVEL"
_LEVEL_NAMES = ("CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG")

# Attribute carrying the exception type name after the filter has stripped
# exc_info/traceback detail from a record (never str(exc), never a stack).
_EXCEPTION_TYPE_ATTR = "_trpc_exception_type"

_SECRET_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(?:x[-_]api[-_]key|api[-_]key|proxy[-_]authorization|authorization|set[-_]cookie|cookie|"
    r"(?:tenant|app|user|refresh|access|verification)[-_]token|encrypt[-_]key|private[-_]key|"
    r"(?:app|client)[-_]secret|secret[-_]key|password|passwd|secret|token)\b[\"']?\s*[:=]")
_BODY_KEY_RE = re.compile(
    r"(?i)[\"'](?:text|content|prompt|messages|message|completion|arguments|args|query|body|input|user_input)"
    r"[\"']\s*[:=]")
_BODY_ASSIGNMENT_RE = re.compile(
    r"(?i)\b(?:text|content|prompt|messages|completion|arguments|query|body)\s*[:=]\s*[\{\[\"']")
_BEARER_RE = re.compile(r"(?i)\bbearer\s+[^\s,;\"']{6,}")
# ``scheme://user:password@`` — a non-empty password in a connection DSN.
_DSN_PASSWORD_RE = re.compile(r"(?i)[a-z][a-z0-9+.\-]{1,15}://[^/\s:@]+:[^/\s@]+@")
_API_KEY_VALUE_RE = re.compile(r"\bsk-[A-Za-z0-9_\-]{8,}")
_SENSITIVE_PATTERNS = (
    _SECRET_ASSIGNMENT_RE,
    _BODY_KEY_RE,
    _BODY_ASSIGNMENT_RE,
    _BEARER_RE,
    _DSN_PASSWORD_RE,
    _API_KEY_VALUE_RE,
)


class SensitiveDataFilter(logging.Filter):
    """Defensive block of third-party records that carry secret-shaped text.

    Matches known secret-ish field names, Bearer tokens, DSN passwords and
    request-body key names against the fully formatted message (and any
    ``safe_log`` string values).  A blocked record is dropped without being
    stored, quoted, or echoed: the filter keeps counters only.  Records
    carrying exception details are reduced to the exception **type name**:
    ``exc_info`` and any cached traceback are stripped before any handler
    can render ``str(exc)`` or a stack.  Never raises.
    """

    def __init__(self) -> None:
        super().__init__()
        self.blocked_count = 0
        self.exceptions_stripped = 0

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            self._strip_exception_detail(record)
            for text in self._texts(record):
                if any(pattern.search(text) for pattern in _SENSITIVE_PATTERNS):
                    self._scrub(record)
                    return False
            return True
        except Exception:
            # Fail closed: a filter bug must never leak data or break a request.
            self._scrub(record)
            return False

    def _scrub(self, record: logging.LogRecord) -> None:
        """Drop the record without keeping or echoing the original text."""
        self.blocked_count += 1
        record.msg = "[blocked]"
        record.args = None
        exc_info = record.exc_info
        if exc_info:
            exception = exc_info[1] if isinstance(exc_info, tuple) else exc_info
            if exception is not None and not getattr(record, _EXCEPTION_TYPE_ATTR, None):
                setattr(record, _EXCEPTION_TYPE_ATTR, type(exception).__name__)
            record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        payload = getattr(record, _SAFE_RECORD_ATTR, None)
        if isinstance(payload, dict):
            payload.clear()

    def _strip_exception_detail(self, record: logging.LogRecord) -> None:
        exc_info = record.exc_info
        if exc_info:
            exception = exc_info[1] if isinstance(exc_info, tuple) else exc_info
            if exception is not None and not getattr(record, _EXCEPTION_TYPE_ATTR, None):
                setattr(record, _EXCEPTION_TYPE_ATTR, type(exception).__name__)
                self.exceptions_stripped += 1
            record.exc_info = None
        if record.exc_text:
            record.exc_text = None
        if record.stack_info:
            record.stack_info = None

    @staticmethod
    def _texts(record: logging.LogRecord) -> list[str]:
        try:
            texts = [record.getMessage()]
        except Exception:
            texts = [str(record.msg)]
        payload = getattr(record, _SAFE_RECORD_ATTR, None)
        if isinstance(payload, dict):
            texts.extend(value for value in payload.values() if isinstance(value, str))
        return texts


class SafeLogFormatter(logging.Formatter):
    """Renders records as one line of escaped, capped JSON.

    ``safe_log`` records emit their validated whitelist payload.  Foreign
    records keep only framing (level, logger name) plus their message with
    the same control-char escaping and length cap — never tracebacks, never
    exception messages (see :class:`SensitiveDataFilter`).
    """

    def __init__(self, service_name: str) -> None:
        super().__init__()
        self._service = service_name

    def format(self, record: logging.LogRecord) -> str:
        try:
            line: dict[str, Any] = {
                "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(timespec="milliseconds"),
                "level": record.levelname,
                "service": self._service,
            }
            payload = getattr(record, _SAFE_RECORD_ATTR, None)
            if isinstance(payload, dict):
                line.update(payload)
            else:
                line["logger"] = _clean_string(record.name)
                line["message"] = _clean_string(record.getMessage())
                exception_type = getattr(record, _EXCEPTION_TYPE_ATTR, None)
                if exception_type:
                    line["exception_type"] = _clean_string(str(exception_type))
            return json.dumps(line, ensure_ascii=True)
        except Exception:
            # A formatter failure must never break a business request or
            # leak record contents: emit a fixed safe line only.
            return json.dumps(
                {
                    "level": "ERROR",
                    "service": self._service,
                    "event": "log.format_failed",
                    "error_code": "logging_unavailable",
                },
                ensure_ascii=True,
            )


class SafeLogStreamHandler(logging.StreamHandler):
    """stderr handler that fails silently instead of recursing into logging."""

    def __init__(self, service_name: str, data_filter: SensitiveDataFilter) -> None:
        super().__init__(sys.stderr)
        self.setFormatter(SafeLogFormatter(service_name))
        self.addFilter(data_filter)

    def handleError(self, record: logging.LogRecord) -> None:
        # Drop our own emit failures; never write diagnostics.
        return None


def _resolve_configured_level(environ: Mapping[str, str]) -> int:
    raw = str(environ.get(LOG_LEVEL_ENV_VAR, "") or "").strip().upper()
    if raw in _LEVEL_NAMES:
        return getattr(logging, raw)
    return logging.INFO


def _refuse_handler(handler: logging.Handler) -> None:
    """Instance-level stub shadowing ``Logger.addHandler``/``Logger.removeHandler``
    on dropped loggers so late SDK imports cannot attach or detach handlers.

    This is hygiene only — the fail-closed guarantees are the creation-time
    record factory and the per-logger ``disabled``/``propagate`` pair (see
    module docstring).
    """
    return None


# Marker attribute identifying our LogRecordFactory (idempotence check).
_SAFE_FACTORY_FLAG = "_trpc_safe_log_record_factory"

# Fixed replacement category for records born on a dropped logger subtree.
# A constant: the original text never survives record creation.
_DROP_CATEGORY_MESSAGE = "third_party_log_dropped"


def _is_dropped_logger(name: str) -> bool:
    """True for a dropped logger or anything in its ``prefix.`` subtree."""
    return any(name == prefix or name.startswith(prefix + ".") for prefix in DROP_LOGGER_NAMES)


def _install_safe_record_factory() -> None:
    """Scrub dropped-subtree records **at creation time** (primary defense).

    ``Logger.disabled`` only gates logging *on that logger*, and
    ``callHandlers`` runs a logger's own handlers before walking up the
    tree — so a sub-logger an SDK creates *after* configuration, with a raw
    ``StreamHandler`` force-appended, would otherwise emit arbitrary user
    text past every per-logger takeover and past the root filter.  The
    process-global record factory cannot be bypassed that way: any record
    whose name matches a dropped logger or its ``prefix.`` subtree has
    ``msg``/``args``/``exc_info``/``exc_text``/``stack_info`` replaced with
    the fixed :data:`_DROP_CATEGORY_MESSAGE` before any handler sees it.

    The factory is chained (it calls the previous factory) and idempotent
    (re-installing is a no-op while ours is current, so repeated
    ``configure_logging`` calls never wrap twice; if another library later
    replaces the factory entirely, the next ``configure_logging`` re-applies
    ours on top).  No original text is stored on the factory, filter, or
    logger objects at any point.
    """
    current = logging.getLogRecordFactory()
    if getattr(current, _SAFE_FACTORY_FLAG, False):
        return

    def safe_record_factory(*args: Any, **kwargs: Any) -> logging.LogRecord:
        record = current(*args, **kwargs)
        if _is_dropped_logger(record.name):
            record.msg = _DROP_CATEGORY_MESSAGE
            record.args = None
            record.exc_info = None
            record.exc_text = None
            record.stack_info = None
        return record

    setattr(safe_record_factory, _SAFE_FACTORY_FLAG, True)
    logging.setLogRecordFactory(safe_record_factory)


def configure_logging(service_name: str, environ: Mapping[str, str] | None = None) -> None:
    """Idempotently route all logging in this process through the safe path.

    ``service_name`` labels every emitted line; ``environ`` is read for
    ``TRPC_LOG_LEVEL`` (default ``INFO``).  Repeated calls replace the
    previously installed safe handler instead of adding another one.
    """
    if not isinstance(service_name, str) or not service_name:
        raise ValueError("service_name must be a non-empty string")
    env = os.environ if environ is None else environ
    level = _resolve_configured_level(env)
    data_filter = SensitiveDataFilter()

    # Creation-time scrub must be installed before any takeover: records on
    # dropped subtrees lose their raw text no matter which logger/handler an
    # SDK invents later.  Idempotent — repeated calls never double-wrap.
    _install_safe_record_factory()

    root = logging.getLogger()
    # Replace *all* pre-existing root handlers (foreign StreamHandlers from a
    # previous dictConfig included) so no unfiltered handler emits in parallel.
    # Assigning to the list bypasses any instance-level addHandler stubs.
    handler = SafeLogStreamHandler(service_name, data_filter)
    handler.setLevel(level)
    root.handlers[:] = [handler]
    if not any(isinstance(existing, SensitiveDataFilter) for existing in root.filters):
        root.addFilter(data_filter)
    root.setLevel(level)

    for name in DROP_LOGGER_NAMES:
        logger = logging.getLogger(name)
        logger.handlers[:] = [logging.NullHandler()]
        if not any(isinstance(existing, SensitiveDataFilter) for existing in logger.filters):
            logger.addFilter(data_filter)
        # Supporting defenses (the primary one is the record factory):
        # disabled=True blocks emit even if a handler is force-appended past
        # the refusal stubs, and propagate=False stops any sub-logger tree
        # from reaching root.  The refusal stubs are hygiene only.
        logger.propagate = False
        logger.disabled = True
        logger.setLevel(level)
        logger.__dict__["addHandler"] = _refuse_handler
        logger.__dict__["removeHandler"] = _refuse_handler

    for name in OPERATIONAL_LOGGER_NAMES:
        logger = logging.getLogger(name)
        # No handlers of their own — records travel to the safe root handler
        # only.  Re-enabled (disabled=False) so a previous dictConfig or a
        # disable_existing_loggers sweep cannot silence operational lines.
        logger.handlers[:] = []
        logger.propagate = True
        logger.disabled = False


__all__ = [
    "DROP_LOGGER_NAMES",
    "OPERATIONAL_LOGGER_NAMES",
    "THIRD_PARTY_LOGGER_NAMES",
    "SafeLogFormatter",
    "SafeLogStreamHandler",
    "SensitiveDataFilter",
    "configure_logging",
]
