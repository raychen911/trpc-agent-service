"""Safe structured logging boundary (Stage 6B1).

Product code logs only through :func:`safe_log` with the fixed
:class:`SafeLogFields` whitelist; :func:`configure_logging` wires root and
third-party SDK loggers to a defensive, single-line JSON stderr handler.
"""

from .config import SensitiveDataFilter, configure_logging
from .safe import MAX_FIELD_VALUE_CHARS, SAFE_LOG_FIELD_NAMES, SafeLogFields, safe_log

__all__ = [
    "MAX_FIELD_VALUE_CHARS",
    "SAFE_LOG_FIELD_NAMES",
    "SafeLogFields",
    "SensitiveDataFilter",
    "configure_logging",
    "safe_log",
]
