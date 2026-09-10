"""Small, deterministic controls applied before Agent execution."""

from __future__ import annotations

import asyncio
import re
from collections import defaultdict, deque
from time import monotonic
from typing import Any


class InputRejectedError(ValueError):
    pass


class InputPolicy:
    def __init__(self, max_chars: int = 20_000) -> None:
        self.max_chars = max_chars

    def validate(self, text: str, max_chars: int | None = None) -> str:
        normalized = text.strip()
        if not normalized:
            raise InputRejectedError("message is empty")
        effective_limit = self.max_chars if max_chars is None else min(self.max_chars, max_chars)
        if len(normalized) > effective_limit:
            raise InputRejectedError("message is too long")
        return normalized

    def authorize(self, principal_id: str, policy: dict[str, Any]) -> None:
        denied = {str(item) for item in policy.get("denied_users", [])}
        allowed = {str(item) for item in policy.get("allowed_users", [])}
        if principal_id in denied or (allowed and principal_id not in allowed):
            raise InputRejectedError("user is not authorized for this tenant")


class RequestRateLimiter:
    """Process-local fixed-window limiter for the single-node deployment."""

    def __init__(self) -> None:
        self._requests: dict[str, deque[float]] = defaultdict(deque)
        self._lock = asyncio.Lock()

    async def check(self, key: str, requests_per_minute: int | None) -> None:
        if requests_per_minute is None:
            return
        if requests_per_minute < 1:
            raise InputRejectedError("tenant request budget is disabled")
        cutoff = monotonic() - 60
        async with self._lock:
            timestamps = self._requests[key]
            while timestamps and timestamps[0] <= cutoff:
                timestamps.popleft()
            if len(timestamps) >= requests_per_minute:
                raise InputRejectedError("tenant request rate exceeded")
            timestamps.append(monotonic())


_SECRET_PATTERNS = (
    re.compile(r"(?i)(authorization\s*:\s*bearer\s+)[^\s]+"),
    re.compile(r"\bsk-[A-Za-z0-9_-]{12,}\b"),
)


def redact_sensitive(text: str) -> str:
    redacted = _SECRET_PATTERNS[0].sub(r"\1[REDACTED]", text)
    return _SECRET_PATTERNS[1].sub("[REDACTED]", redacted)


__all__ = [
    "InputPolicy",
    "InputRejectedError",
    "RequestRateLimiter",
    "redact_sensitive",
]
