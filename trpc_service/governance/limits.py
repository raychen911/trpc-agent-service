"""Atomic tenant rate limiting on Redis (Stage 6C).

One fixed-window counter per (tenant, UTC minute) driven by a single Lua
script: INCR, first-write EXPIRE (TTL covers the whole window plus drift
margin) and the allow/deny comparison happen ATOMICALLY inside Redis, so
two Gateway/Worker processes can never jointly exceed the configured
``requests_per_minute``.

Fail-closed contract: a Redis connect/script error raises
:class:`RateLimiterUnavailableError` — callers must reject the request with
the fixed "temporarily unavailable" text and NEVER fall through to
unlimited traffic.  Rejections themselves are not errors: ``acquire``
returns ``False``.  Keys embed only the validated tenant id and a UTC
minute; nothing else (user, session, message, token) ever enters a key,
and keys are never logged.
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Final, Protocol

# INCR then, exactly on window creation, EXPIRE — one atomic script.  The
# counter is compared against the limit in the same script so a caller can
# never observe "allowed" from a stale read.
_RATE_LUA: Final[str] = """
local count = redis.call('INCR', KEYS[1])
if count == 1 then
    redis.call('EXPIRE', KEYS[1], ARGV[2])
end
if count <= tonumber(ARGV[1]) then
    return 1
end
return 0
"""

WINDOW_SECONDS: Final[int] = 60
# TTL exceeds the minute window so a boundary-crossing minute can never
# resurrect a key of the previous window as its counter.
TTL_SECONDS: Final[int] = 120
_KEY_PREFIX: Final[str] = "trpc:rl:"


class RateLimiterUnavailableError(RuntimeError):
    """Redis rejected the limiter script; callers must fail closed."""


class TenantRateLimiter(Protocol):
    """Async protocol: one decision per request, no retry."""

    async def acquire(self, tenant_id: str, limit: int) -> bool:
        ...


def rate_limit_key(tenant_id: str, now: datetime | None = None) -> str:
    """Fixed-shape key: prefix + normalized tenant + UTC minute bucket."""
    current = now or datetime.now(timezone.utc)
    minute_bucket = int(current.timestamp()) // WINDOW_SECONDS
    return f"{_KEY_PREFIX}{tenant_id}:{minute_bucket}"


class RedisTenantRateLimiter:
    """``TenantRateLimiter`` over one shared redis.asyncio client."""

    def __init__(self, redis_client: object) -> None:
        # Script registered lazily against the connection pool (EVALSHA with
        # automatic NOSCRIPT retry handled by redis-py's Script wrapper).
        self._redis = redis_client
        self._script = None

    @classmethod
    def from_env(cls, environ: dict | None = None) -> "RedisTenantRateLimiter":
        import os

        import redis.asyncio as aioredis

        values = os.environ if environ is None else environ
        url = values.get("TRPC_REDIS_URL", "").strip()
        if not url:
            raise RateLimiterUnavailableError("TRPC_REDIS_URL is not configured")
        client = aioredis.from_url(
            url,
            socket_timeout=2.0,
            socket_connect_timeout=2.0,
            decode_responses=True,
        )
        return cls(client)

    async def acquire(self, tenant_id: str, limit: int) -> bool:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("limit must be a positive integer")
        script = self._script
        if script is None:
            script = self._redis.register_script(_RATE_LUA)  # type: ignore[attr-defined]
            self._script = script
        key = rate_limit_key(tenant_id)
        try:
            result = await script(keys=[key], args=[limit, TTL_SECONDS])
        except Exception:
            # Never surface the driver error (it can embed the URL); the
            # caller maps this to the fixed unavailable text.
            raise RateLimiterUnavailableError("rate limiter backend failed") from None
        return bool(int(result))

    async def check_ready(self) -> None:
        try:
            await self._redis.ping()  # type: ignore[attr-defined]
        except Exception:
            raise RateLimiterUnavailableError("rate limiter backend is not reachable") from None

    async def close(self) -> None:
        close = getattr(self._redis, "aclose", None) or getattr(self._redis, "close", None)
        if close is not None:
            try:
                await close()
            except Exception:
                pass


def utc_minute(now_monotonic: float | None = None) -> int:
    """Current UTC minute bucket (exported for tests/metrics)."""
    del now_monotonic  # present for API symmetry with time-based helpers
    return int(time.time()) // WINDOW_SECONDS


__all__ = [
    "RATE_LUA_SCRIPT",
    "RateLimiterUnavailableError",
    "RedisTenantRateLimiter",
    "TTL_SECONDS",
    "TenantRateLimiter",
    "WINDOW_SECONDS",
    "rate_limit_key",
]

# Exported alias so tests can pin the exact script text against drift.
RATE_LUA_SCRIPT: Final[str] = _RATE_LUA
