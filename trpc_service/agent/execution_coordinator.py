"""Redis-based session execution coordinator: distributed lock for cross-process serialization."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import secrets
import time
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, Protocol

logger = logging.getLogger(__name__)

_LOCK_KEY_PREFIX = "trpc:session-lock:"
_DEFAULT_WAIT = 5.0
_DEFAULT_LEASE = 30.0
_DEFAULT_RENEW = 10.0

_LUA_CAS_RENEW = """
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
    redis.call('PEXPIRE', KEYS[1], ARGV[2])
    return 1
else
    return 0
end
"""

_LUA_CAS_DELETE = """
local current = redis.call('GET', KEYS[1])
if current == ARGV[1] then
    redis.call('DEL', KEYS[1])
    return 1
else
    return 0
end
"""


class SessionBusyError(Exception):
    """Raised when a session lock cannot be acquired within the wait timeout."""


class SessionExecutionLostError(Exception):
    """Raised when the session lease is lost (renewal failed or ownership lost)."""


@dataclass(frozen=True)
class SessionExecutionIdentity:
    """Identity fields for session lock key computation."""

    tenant_id: str
    app_id: str
    config_version: int
    sdk_user_id: str
    session_id: str

    @property
    def digest(self) -> str:
        canonical = json.dumps(
            [
                self.tenant_id,
                self.app_id,
                self.config_version,
                self.sdk_user_id,
                self.session_id,
            ],
            separators=(",", ":"),
            ensure_ascii=False,
        )
        return hashlib.sha256(canonical.encode()).hexdigest()


@dataclass(frozen=True)
class SessionLease:
    """Represents an active session execution lease."""

    _token: str
    _key: str
    _coordinator: RedisSessionExecutionCoordinator

    def ensure_valid(self) -> None:
        """Raise SessionExecutionLostError if the lease is no longer valid."""
        if not self._coordinator._is_lease_valid(self._key, self._token):
            raise SessionExecutionLostError("session lease lost")


class SessionExecutionCoordinator(Protocol):
    """Protocol for session execution coordination."""

    @asynccontextmanager
    def acquire(self, identity: SessionExecutionIdentity) -> AsyncIterator[SessionLease]:
        """Acquire a session lock. Yields a SessionLease."""
        ...

    async def close(self) -> None:
        """Close the coordinator and release resources."""
        ...


class RedisSessionExecutionCoordinator:
    """Redis-based session execution coordinator using distributed locks."""

    def __init__(
        self,
        redis_url: str,
        wait_seconds: float,
        lease_seconds: float,
        renew_seconds: float,
        _redis: Any = None,
    ) -> None:
        if not math.isfinite(wait_seconds) or wait_seconds <= 0:
            raise ValueError("wait_seconds must be a positive finite number")
        if not math.isfinite(lease_seconds) or lease_seconds <= 0:
            raise ValueError("lease_seconds must be a positive finite number")
        if not math.isfinite(renew_seconds) or renew_seconds <= 0:
            raise ValueError("renew_seconds must be a positive finite number")
        if renew_seconds >= lease_seconds / 2:
            raise ValueError("renew_seconds must be less than half of lease_seconds")

        self._redis_url = redis_url
        self.wait_seconds = wait_seconds
        self.lease_seconds = lease_seconds
        self.renew_seconds = renew_seconds
        self._closed = False
        self._lease_valid: dict[str, str] = {}

        if _redis is not None:
            self._redis = _redis
            self._owns_redis = False
        else:
            import redis.asyncio as aioredis
            self._redis = aioredis.from_url(
                redis_url,
                decode_responses=True,
                socket_connect_timeout=2.0,
                socket_timeout=2.0,
            )
            self._owns_redis = True

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> RedisSessionExecutionCoordinator:
        if environ is None:
            import os
            environ = os.environ

        redis_url = environ.get("TRPC_REDIS_URL", "").strip()
        if not redis_url:
            raise ValueError("TRPC_REDIS_URL is required for session execution coordinator")

        wait = _parse_positive_float(
            environ.get("TRPC_SESSION_LOCK_WAIT_SECONDS", "").strip(),
            "TRPC_SESSION_LOCK_WAIT_SECONDS",
            default=_DEFAULT_WAIT,
        )
        lease = _parse_positive_float(
            environ.get("TRPC_SESSION_LOCK_LEASE_SECONDS", "").strip(),
            "TRPC_SESSION_LOCK_LEASE_SECONDS",
            default=_DEFAULT_LEASE,
        )
        renew = _parse_positive_float(
            environ.get("TRPC_SESSION_LOCK_RENEW_SECONDS", "").strip(),
            "TRPC_SESSION_LOCK_RENEW_SECONDS",
            default=_DEFAULT_RENEW,
        )

        return cls(
            redis_url=redis_url,
            wait_seconds=wait,
            lease_seconds=lease,
            renew_seconds=renew,
        )

    def _is_lease_valid(self, key: str, token: str) -> bool:
        return self._lease_valid.get(key) == token

    async def _cas_renew(self, key: str, token: str, lease_ms: int) -> bool:
        result = await self._redis.eval(_LUA_CAS_RENEW, 1, key, token, lease_ms)
        return result == 1

    async def _cas_delete(self, key: str, token: str) -> bool:
        result = await self._redis.eval(_LUA_CAS_DELETE, 1, key, token)
        return result == 1

    @asynccontextmanager
    async def acquire(self, identity: SessionExecutionIdentity) -> AsyncIterator[SessionLease]:
        key = f"{_LOCK_KEY_PREFIX}{identity.digest}"
        token = secrets.token_urlsafe(32)
        lease_ms = int(self.lease_seconds * 1000)

        deadline = time.monotonic() + self.wait_seconds
        acquired = False

        while time.monotonic() < deadline:
            try:
                result = await self._redis.set(key, token, nx=True, px=lease_ms)
                if result:
                    acquired = True
                    break
            except Exception:
                logger.warning("session lock acquire error")
                raise
            jitter = secrets.randbelow(50) / 1000.0
            await asyncio.sleep(min(0.1 + jitter, max(0, deadline - time.monotonic())))

        if not acquired:
            raise SessionBusyError("session is busy, could not acquire lock within timeout")

        self._lease_valid[key] = token
        renew_task: asyncio.Task[None] | None = None

        async def _renew_loop() -> None:
            try:
                while not self._closed:
                    await asyncio.sleep(self.renew_seconds)
                    if self._closed:
                        break
                    try:
                        ok = await self._cas_renew(key, token, lease_ms)
                        if not ok:
                            logger.warning("session lease renewal failed (ownership lost)")
                            self._lease_valid.pop(key, None)
                            return
                    except Exception:
                        logger.warning("session lease renewal error")
                        self._lease_valid.pop(key, None)
                        return
            except asyncio.CancelledError:
                return

        try:
            renew_task = asyncio.create_task(_renew_loop())
            lease = SessionLease(_token=token, _key=key, _coordinator=self)
            yield lease
        finally:
            if renew_task is not None:
                renew_task.cancel()
                try:
                    await renew_task
                except (asyncio.CancelledError, Exception):
                    pass
            try:
                released = await self._cas_delete(key, token)
                if not released:
                    self._lease_valid.pop(key, None)
                    raise SessionExecutionLostError("session lock release failed (ownership lost)")
            except SessionExecutionLostError:
                raise
            except Exception:
                self._lease_valid.pop(key, None)
                raise SessionExecutionLostError("session lock release error") from None
            self._lease_valid.pop(key, None)

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._owns_redis:
            try:
                await self._redis.aclose()
            except Exception:
                pass


def _parse_positive_float(raw: str, name: str, *, default: float) -> float:
    if not raw:
        return default
    try:
        value = float(raw)
    except (ValueError, TypeError):
        raise ValueError(f"{name} must be a positive finite number") from None
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number")
    return value


__all__ = [
    "RedisSessionExecutionCoordinator",
    "SessionBusyError",
    "SessionExecutionCoordinator",
    "SessionExecutionIdentity",
    "SessionExecutionLostError",
    "SessionLease",
]
