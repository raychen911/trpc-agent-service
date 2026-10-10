"""Central redaction used by logs, errors, reports, and telemetry."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

REDACTED = "[REDACTED]"

_SENSITIVE_KEY = re.compile(
    r"(?:authorization|cookie|password|passwd|secret|token|api[_-]?key|"
    r"credential|private[_-]?key|client[_-]?secret|database[_-]?dsn|"
    r"message(?:_body)?|prompt|completion|tool[_-]?(?:args|result)|"
    r"(?:request|response)[_-]?(?:body|content)|input|output|state)$",
    re.IGNORECASE,
)
_BEARER = re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/=-]+")
_ASSIGNMENT = re.compile(r"(?i)\b(password|passwd|secret|token|api[_-]?key)\s*[:=]\s*([^\s,;]+)")


def is_sensitive_key(key: str) -> bool:
    """Return whether an attribute name represents content or a credential."""

    normalized = key.rsplit(".", 1)[-1]
    return bool(_SENSITIVE_KEY.search(normalized)) or any(
        marker in key.lower()
        for marker in (
            "gen_ai.prompt",
            "gen_ai.completion",
            "llm.request",
            "llm.response",
            ".llm_request",
            ".llm_response",
            "trpc.runner.input",
            "trpc.runner.output",
            "tool.arguments",
            "tool.response",
            ".tool_call_args",
            ".tool_response",
            ".state.begin",
            ".state.end",
            ".state.partial",
            ".stream_function_calls.raw",
            ".stream_function_calls.post_planner",
            "exception.message",
            "exception.stacktrace",
            "url.full",
            "http.url",
        )
    )


def sanitize_text(value: str) -> str:
    """Remove common inline credential forms without treating all text as content."""

    value = _BEARER.sub(f"Bearer {REDACTED}", value)
    return _ASSIGNMENT.sub(lambda match: f"{match.group(1)}={REDACTED}", value)


def redact(value: Any, *, key: str | None = None) -> Any:
    """Recursively sanitize structured data while preserving operational fields."""

    if key is not None and is_sensitive_key(key):
        return REDACTED
    if isinstance(value, Mapping):
        return {str(item_key): redact(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [redact(item) for item in value]
    if isinstance(value, str):
        return sanitize_text(value)
    return value


__all__ = ["REDACTED", "is_sensitive_key", "redact", "sanitize_text"]
