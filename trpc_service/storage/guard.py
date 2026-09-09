# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Per-session execution guards.

SDK storage provides persistence, but a Worker still needs an external guard to
serialize the Runner's read-modify-write cycle for one session.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Protocol
from typing import Callable
from typing import Awaitable
from contextvars import ContextVar

from redis.asyncio import Redis
from redis.asyncio import from_url


class SessionLockTimeoutError(TimeoutError):
    """Raised when a request cannot enter its session before the deadline."""


class SessionLockLostError(RuntimeError):
    """Raised when a distributed lock is no longer owned by this execution."""


@dataclass(frozen=True, slots=True)
class SessionLease:
    """Observable ownership handle yielded to a Worker while it owns a session."""

    token: str
    lost: asyncio.Event
    verifier: Callable[[], Awaitable[bool]] | None = None
    epoch: int = 0
    key: str = ""

    async def verify(self) -> None:
        self.assert_owned()
        if self.verifier:
            try:
                owned = await asyncio.wait_for(self.verifier(), timeout=2)
            except Exception:
                self.lost.set()
                raise SessionLockLostError("session ownership could not be verified") from None
            if not owned:
                self.lost.set()
        self.assert_owned()

    @property
    def is_lost(self) -> bool:
        return self.lost.is_set()

    def assert_owned(self, key: str = "") -> None:
        if self.is_lost:
            suffix = f": {key}" if key else ""
            raise SessionLockLostError(f"session lock ownership was lost{suffix}")


class SessionExecutionGuard(Protocol):
    """Serialize all executions that mutate the same session."""

    def hold(self, key: str, *, wait_timeout: float,
             lease_seconds: float) -> contextlib.AbstractAsyncContextManager[SessionLease]:
        """Return a context manager yielding an observable lease handle."""


_current_lease: ContextVar[SessionLease | None] = ContextVar("session_lease", default=None)


@contextlib.contextmanager
def lease_scope(lease: SessionLease):
    token = _current_lease.set(lease)
    try:
        yield lease
    finally:
        _current_lease.reset(token)


async def verify_current_lease() -> None:
    lease = _current_lease.get()
    if lease:
        await lease.verify()


def current_lease():
    return _current_lease.get()


class InMemorySessionExecutionGuard:
    """Process-local guard for tests and single-node development."""

    def __init__(self) -> None:
        self._locks: dict[str, asyncio.Lock] = {}
        self._map_lock = asyncio.Lock()
        self._epochs: dict[str, int] = {}

    @contextlib.asynccontextmanager
    async def hold(self,
                   key: str,
                   *,
                   wait_timeout: float = 5.0,
                   lease_seconds: float = 30.0) -> AsyncIterator[SessionLease]:
        del lease_seconds
        async with self._map_lock:
            lock = self._locks.setdefault(key, asyncio.Lock())
        try:
            await asyncio.wait_for(lock.acquire(), timeout=wait_timeout)
        except asyncio.TimeoutError as error:
            raise SessionLockTimeoutError(f"timed out waiting for session lock: {key}") from error
        self._epochs[key] = self._epochs.get(key, 0) + 1
        lease = SessionLease(token=uuid.uuid4().hex, lost=asyncio.Event(), epoch=self._epochs[key], key=key)
        try:
            yield lease
        finally:
            lock.release()
            async with self._map_lock:
                if not lock.locked() and not getattr(lock, "_waiters", None):
                    self._locks.pop(key, None)


_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
end
return 0
"""

_RENEW_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('pexpire', KEYS[1], ARGV[2])
end
return 0
"""

_ACQUIRE_SCRIPT = """
if redis.call('exists', KEYS[1]) == 1 then return 0 end
local epoch = redis.call('incr', KEYS[2])
redis.call('set', KEYS[1], ARGV[1], 'PX', ARGV[2])
return epoch
"""


class RedisSessionExecutionGuard:
    """Redis token lock with bounded waiting, renewal and owner-checked release."""

    def __init__(self,
                 redis_url: str,
                 *,
                 prefix: str = "trpc-service:session-lock",
                 client: Redis | None = None) -> None:
        self._redis = client or from_url(redis_url, decode_responses=True, socket_timeout=2, socket_connect_timeout=2)
        self._owns_client = client is None
        self._prefix = prefix.rstrip(":")

    @contextlib.asynccontextmanager
    async def hold(self,
                   key: str,
                   *,
                   wait_timeout: float = 5.0,
                   lease_seconds: float = 30.0) -> AsyncIterator[SessionLease]:
        redis_key = f"{self._prefix}:{key}"
        token = uuid.uuid4().hex
        lease_ms = max(1000, int(lease_seconds * 1000))
        deadline = asyncio.get_running_loop().time() + wait_timeout
        epoch = 0
        while not epoch:
            epoch = await self._redis.eval(_ACQUIRE_SCRIPT, 2, redis_key, f"{redis_key}:epoch", token, lease_ms)
            if epoch:
                break
            if asyncio.get_running_loop().time() >= deadline:
                raise SessionLockTimeoutError(f"timed out waiting for session lock: {key}")
            await asyncio.sleep(min(0.1, max(0.01, wait_timeout / 20)))

        async def verify() -> bool:
            return await self._redis.get(redis_key) == token

        lease = SessionLease(token=token, lost=asyncio.Event(), verifier=verify, epoch=int(epoch), key=redis_key)
        renewal = asyncio.create_task(self._renew(redis_key, token, lease_ms, lease.lost))
        try:
            yield lease
            lease.assert_owned(key)
        finally:
            renewal.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await renewal
            # The lease may have been lost because Redis itself is unavailable;
            # release is best effort and must not hide SessionLockLostError.
            with contextlib.suppress(Exception):
                await self._redis.eval(_RELEASE_SCRIPT, 1, redis_key, token)

    async def _renew(self, key: str, token: str, lease_ms: int, lost: asyncio.Event) -> None:
        interval = max(0.25, lease_ms / 3000)
        while True:
            await asyncio.sleep(interval)
            try:
                renewed = await self._redis.eval(_RENEW_SCRIPT, 1, key, token, lease_ms)
            except Exception:
                lost.set()
                return
            if not renewed:
                lost.set()
                return

    async def close(self) -> None:
        if self._owns_client:
            await self._redis.aclose()

    async def ping(self) -> bool:
        return bool(await self._redis.ping())
