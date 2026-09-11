"""Privacy-first redaction for logs, traces and audit details."""

from __future__ import annotations

import re
from typing import Any

_SECRET_PATTERNS = (
    re.compile(r"(?i)([a-z][a-z0-9+.-]*://)[^\s/@]+:[^\s/@]+@"),
    re.compile(r"(?i)(api[_-]?key|token|secret|password)\s*[:=]\s*([^\s,;]+)"),
    re.compile(r"(?i)bearer\s+[a-z0-9._~+/=-]+"),
)


def redact_text(value: str) -> str:
    result = value
    for pattern in _SECRET_PATTERNS:
        if pattern is _SECRET_PATTERNS[0]:
            result = pattern.sub(r"\1[REDACTED]@", result)
            continue
        result = pattern.sub(
            lambda match: f"{match.group(1)}=[REDACTED]" if match.lastindex else "Bearer [REDACTED]", result
        )
    return result


def redact(value: Any) -> Any:
    if isinstance(value, str):
        return redact_text(value)
    if isinstance(value, dict):
        return {
            key: (
                "[REDACTED]"
                if any(word in str(key).lower() for word in ("secret", "token", "password", "api_key", "api-key", "authorization"))
                else redact(item)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    if isinstance(value, tuple):
        return tuple(redact(item) for item in value)
    return value
