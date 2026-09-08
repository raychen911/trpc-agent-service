# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Idempotency store for IM message de-duplication.

IM platforms may redeliver a callback when the gateway fails to ACK in time
(e.g. WeCom retries within ~5s). The gateway keys on ``tenant_id:channel:msg_id``
so duplicate callbacks are suppressed within the configured TTL after the task
has been durably accepted. External tool side effects still require their own
business idempotency keys.
"""

from __future__ import annotations

import time
from typing import Optional


class LocalIdempotencyStore:
    """Thread/async-safe in-memory idempotency store with TTL."""

    def __init__(self, ttl_seconds: float = 300.0) -> None:
        self._seen: dict[str, float] = {}
        self._ttl = ttl_seconds

    async def check_and_set(self, key: str) -> bool:
        """Return ``True`` if ``key`` was already seen (duplicate), else record it.

        Expired entries are lazily evicted.
        """
        now = time.monotonic()
        existing = self._seen.get(key)
        if existing is not None and now - existing < self._ttl:
            return True
        self._seen[key] = now
        return False

    async def release(self, key: str) -> None:
        """Release a reservation when dispatch did not durably accept the task."""
        self._seen.pop(key, None)


class RedisIdempotencyStore:
    """Redis-backed idempotency store using ``SET key NX EX ttl``."""

    def __init__(self, redis_url: str, ttl_seconds: float = 300.0) -> None:
        import redis.asyncio as aioredis

        self._redis = aioredis.from_url(redis_url)
        self._ttl = int(ttl_seconds)

    async def check_and_set(self, key: str) -> bool:
        was_set = await self._redis.set(key, "1", nx=True, ex=self._ttl)
        # ``set(nx=True)`` returns True only when the key did not already exist.
        return not bool(was_set)

    async def release(self, key: str) -> None:
        await self._redis.delete(key)

    async def close(self) -> None:
        await self._redis.aclose()


def build_idempotency_store(redis_url: Optional[str] = None, ttl_seconds: float = 300.0):
    """Build an idempotency store, preferring Redis when a URL is provided."""
    if redis_url:
        return RedisIdempotencyStore(redis_url, ttl_seconds)
    return LocalIdempotencyStore(ttl_seconds)
