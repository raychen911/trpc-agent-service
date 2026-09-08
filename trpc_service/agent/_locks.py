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
from contextlib import suppress
from dataclasses import dataclass
from typing import Any
from typing import AsyncIterator
from typing import Optional
from redis.exceptions import WatchError


@dataclass
class SessionLease:
    """Lease metadata propagated to the Agent/storage execution context."""

    key: str
    owner_token: str
    fencing_token: int
    lost: bool = False

    def assert_valid(self) -> None:
        if self.lost:
            raise RuntimeError(f"session lease was lost while processing: {self.key}")


class LocalSessionLockManager:
    """Serialize turns in one process; intended for tests and local demos."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}

    @asynccontextmanager
    async def acquire(self, key: str) -> AsyncIterator[SessionLease]:
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            yield SessionLease(key=key, owner_token="local", fencing_token=0)


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
        renew_interval: Optional[float] = None,
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
        self._renew_interval = renew_interval if renew_interval is not None else max(0.05, lease_seconds / 3)

    async def _renew(self, lock_key: str, token: str) -> bool:
        """Extend a lease only when it is still owned by ``token``."""
        while True:
            try:
                async with self._client.pipeline(transaction=True) as pipe:
                    await pipe.watch(lock_key)
                    if await pipe.get(lock_key) != token:
                        await pipe.unwatch()
                        return False
                    pipe.multi()
                    pipe.pexpire(lock_key, self._lease_ms)
                    result = await pipe.execute()
                    return bool(result and result[0])
            except WatchError:
                continue

    async def _keep_alive(self, lease: SessionLease, lock_key: str) -> None:
        while True:
            await asyncio.sleep(self._renew_interval)
            try:
                if not await self._renew(lock_key, lease.owner_token):
                    lease.lost = True
                    return
            except Exception:  # noqa: BLE001 - fail closed when ownership cannot be established
                lease.lost = True
                return

    @asynccontextmanager
    async def acquire(self, key: str) -> AsyncIterator[SessionLease]:
        lock_key = f"agent:session-lock:{key}"
        fencing_key = f"{lock_key}:fencing"
        token = secrets.token_hex(16)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._acquire_timeout
        while not await self._client.set(lock_key, token, nx=True, px=self._lease_ms):
            if loop.time() >= deadline:
                raise TimeoutError(f"timed out acquiring session lock: {key}")
            await asyncio.sleep(self._retry_interval)
        fencing_token = int(await self._client.incr(fencing_key))
        lease = SessionLease(key=key, owner_token=token, fencing_token=fencing_token)
        keep_alive = asyncio.create_task(self._keep_alive(lease, lock_key))
        try:
            yield lease
        finally:
            keep_alive.cancel()
            with suppress(asyncio.CancelledError):
                await keep_alive
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
