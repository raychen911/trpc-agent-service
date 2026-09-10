"""Small Redis owner-token lock used to serialize a Session across workers."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from time import monotonic
from uuid import uuid4

from redis.asyncio import Redis

_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""


class SessionLockTimeoutError(TimeoutError):
    pass


class RedisSessionLockManager:
    def __init__(self, redis_url: str, *, ttl_seconds: float, acquire_timeout: float = 30) -> None:
        self._redis = Redis.from_url(redis_url, decode_responses=True)
        self._ttl_ms = max(1_000, round(ttl_seconds * 1000))
        self._acquire_timeout = acquire_timeout

    @asynccontextmanager
    async def lock(self, tenant_id: str, session_id: str) -> AsyncIterator[None]:
        key = f"trpc-service:session-lock:{tenant_id}:{session_id}"
        token = uuid4().hex
        deadline = monotonic() + self._acquire_timeout
        while not await self._redis.set(key, token, nx=True, px=self._ttl_ms):
            if monotonic() >= deadline:
                raise SessionLockTimeoutError("session is busy on another worker")
            await asyncio.sleep(0.05)
        try:
            yield
        finally:
            await self._redis.eval(_RELEASE_SCRIPT, 1, key, token)

    async def close(self) -> None:
        await self._redis.aclose()


__all__ = ["RedisSessionLockManager", "SessionLockTimeoutError"]
