# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant-scoped callback rate limiting for the Agent Gateway."""

from __future__ import annotations

import time
from typing import Any
from typing import Optional


class LocalRateLimiter:
    """Process-local fixed-window limiter for tests and single-node demos."""

    def __init__(self, clock=time.monotonic) -> None:
        self._clock = clock
        self._windows: dict[str, tuple[float, int]] = {}

    async def allow(self, key: str, limit: int, window_seconds: int = 60) -> bool:
        """Consume one request and return whether it is within ``limit``."""
        if limit <= 0:
            return True
        now = self._clock()
        started, count = self._windows.get(key, (now, 0))
        if now - started >= window_seconds:
            started, count = now, 0
        count += 1
        self._windows[key] = (started, count)
        return count <= limit


class RedisRateLimiter:
    """Cross-node atomic fixed-window limiter backed by Redis."""

    _ALLOW_SCRIPT = """
local current = redis.call('INCR', KEYS[1])
if current == 1 then
  redis.call('EXPIRE', KEYS[1], ARGV[1])
end
if current <= tonumber(ARGV[2]) then
  return 1
end
return 0
"""

    def __init__(
        self,
        redis_url: Optional[str] = None,
        *,
        client: Any = None,
        prefix: str = "trpc-service:rate-limit",
    ) -> None:
        if client is not None:
            self._redis = client
        elif redis_url:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(redis_url, decode_responses=True)
        else:
            raise ValueError("RedisRateLimiter requires redis_url or client")
        self._prefix = prefix

    async def allow(self, key: str, limit: int, window_seconds: int = 60) -> bool:
        """Atomically consume one request in a Redis fixed window."""
        if limit <= 0:
            return True
        allowed = await self._redis.eval(
            self._ALLOW_SCRIPT,
            1,
            f"{self._prefix}:{key}",
            max(1, int(window_seconds)),
            limit,
        )
        return bool(allowed)

    async def close(self) -> None:
        await self._redis.aclose()


def build_rate_limiter(redis_url: Optional[str] = None):
    """Use Redis for multi-node enforcement and local memory for demos."""
    if redis_url:
        return RedisRateLimiter(redis_url)
    return LocalRateLimiter()
