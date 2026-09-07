# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Session-scoped execution locks used to serialize concurrent turns."""

from __future__ import annotations

import asyncio
import secrets
from contextlib import asynccontextmanager
from typing import Any
from typing import AsyncIterator
from typing import Optional
from redis.exceptions import WatchError


class LocalSessionLockManager:
    """Serialize turns in one process; intended for tests and local demos."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}

    @asynccontextmanager
    async def acquire(self, key: str) -> AsyncIterator[None]:
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            yield


class RedisSessionLockManager:
    """Redis lease lock with ownership-safe release."""

    def __init__(
        self,
        *,
        redis_url: Optional[str] = None,
        client: Any = None,
        lease_seconds: float = 180.0,
        acquire_timeout: float = 30.0,
        retry_interval: float = 0.05,
    ) -> None:
        if client is not None:
            self._client = client
        elif redis_url:
            import redis.asyncio as aioredis

            self._client = aioredis.from_url(redis_url, decode_responses=True)
        else:
            raise ValueError("RedisSessionLockManager requires redis_url or client")
        self._lease_ms = max(1, int(lease_seconds * 1000))
        self._acquire_timeout = acquire_timeout
        self._retry_interval = retry_interval

    @asynccontextmanager
    async def acquire(self, key: str) -> AsyncIterator[None]:
        lock_key = f"agent:session-lock:{key}"
        token = secrets.token_hex(16)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._acquire_timeout
        while not await self._client.set(lock_key, token, nx=True, px=self._lease_ms):
            if loop.time() >= deadline:
                raise TimeoutError(f"timed out acquiring session lock: {key}")
            await asyncio.sleep(self._retry_interval)
        try:
            yield
        finally:
            while True:
                try:
                    async with self._client.pipeline(transaction=True) as pipe:
                        await pipe.watch(lock_key)
                        if await pipe.get(lock_key) != token:
                            await pipe.unwatch()
                            break
                        pipe.multi()
                        pipe.delete(lock_key)
                        await pipe.execute()
                        break
                except WatchError:
                    continue

    async def close(self) -> None:
        await self._client.aclose()
