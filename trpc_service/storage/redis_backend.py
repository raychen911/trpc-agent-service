import asyncio
import json
import time
import uuid
from collections.abc import Mapping
from contextlib import AbstractAsyncContextManager, suppress
from datetime import datetime, timezone
from typing import Any

from redis.asyncio import Redis

from trpc_service.storage.contracts import (
    IdempotencyRecord,
    RateLimitDecision,
    SessionIdentity,
    SessionSnapshot,
)
from trpc_service.storage.exceptions import LockNotAcquiredError, VersionConflictError
from trpc_service.storage.keys import session_lock_key

_release_lock_script = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

_renew_lock_script = """
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
"""

_abandon_idempotency_script = """
local raw = redis.call('GET', KEYS[1])
if not raw then
  return 0
end
local decoded = cjson.decode(raw)
if decoded['status'] == 'processing' then
  return redis.call('DEL', KEYS[1])
end
return 0
"""

_cas_session_script = """
local raw = redis.call('GET', KEYS[1])
local expected = tonumber(ARGV[1])
local current = 0
if raw then
  local decoded = cjson.decode(raw)
  current = tonumber(decoded['version'])
end
if current ~= expected then
  return {0, current}
end
redis.call('SET', KEYS[1], ARGV[2])
return {1, expected + 1}
"""

_rate_limit_script = """
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
local member = ARGV[4]
redis.call('ZREMRANGEBYSCORE', key, '-inf', now - window)
local count = redis.call('ZCARD', key)
if count >= limit then
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  return {0, 0, oldest[2] + window - now}
end
redis.call('ZADD', key, now, member)
redis.call('PEXPIRE', key, window)
return {1, limit - count - 1, 0}
"""


def _decode(value: bytes | str | None) -> str | None:
    if value is None:
        return None
    return value.decode("utf-8") if isinstance(value, bytes) else value


class _RedisLock(AbstractAsyncContextManager[str]):
    def __init__(
        self,
        redis: Redis,
        key: str,
        ttl_seconds: float,
        wait_timeout_seconds: float,
    ) -> None:
        self._redis = redis
        self._key = key
        self._token = str(uuid.uuid4())
        self._ttl_ms = max(1, int(ttl_seconds * 1000))
        self._wait_timeout = wait_timeout_seconds
        self._renew_task: asyncio.Task[None] | None = None

    async def __aenter__(self) -> str:
        deadline = time.monotonic() + self._wait_timeout
        while True:
            acquired = await self._redis.set(self._key, self._token, nx=True, px=self._ttl_ms)
            if acquired:
                self._renew_task = asyncio.create_task(self._renew_lease())
                return self._token
            if time.monotonic() >= deadline:
                raise LockNotAcquiredError(f"timed out acquiring lock {self._key}")
            await asyncio.sleep(0.05)

    async def _renew_lease(self) -> None:
        interval = max(0.05, self._ttl_ms / 3000)
        while True:
            await asyncio.sleep(interval)
            renewed = await self._redis.eval(
                _renew_lock_script,
                1,
                self._key,
                self._token,
                self._ttl_ms,
            )
            if not renewed:
                return

    async def __aexit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._renew_task is not None:
            self._renew_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._renew_task
        await self._redis.eval(_release_lock_script, 1, self._key, self._token)


class RedisCoordinationStore:
    """Redis-backed locks, idempotency, sliding-window limits, and short state."""

    def __init__(self, redis: Redis, key_prefix: str = "trpc") -> None:
        self._redis = redis
        self._prefix = key_prefix.rstrip(":")

    @classmethod
    def from_url(cls, url: str, key_prefix: str = "trpc") -> "RedisCoordinationStore":
        return cls(Redis.from_url(url, decode_responses=False), key_prefix)

    def _key(self, kind: str, key: str) -> str:
        return f"{self._prefix}:{kind}:{key}"

    def acquire(
        self, key: str, *, ttl_seconds: float = 30, wait_timeout_seconds: float = 5
    ) -> AbstractAsyncContextManager[str]:
        return _RedisLock(
            self._redis,
            self._key("lock", key),
            ttl_seconds,
            wait_timeout_seconds,
        )

    async def claim(self, key: str, *, ttl_seconds: int = 86_400) -> bool:
        payload = json.dumps({"status": "processing", "result": None})
        result = await self._redis.set(
            self._key("idempotency", key), payload, nx=True, ex=ttl_seconds
        )
        return bool(result)

    async def complete(
        self, key: str, result: Mapping[str, Any], *, ttl_seconds: int = 86_400
    ) -> None:
        payload = json.dumps({"status": "completed", "result": dict(result)}, ensure_ascii=False)
        await self._redis.set(self._key("idempotency", key), payload, ex=ttl_seconds)

    async def get(self, key: str) -> IdempotencyRecord | None:
        raw = _decode(await self._redis.get(self._key("idempotency", key)))
        if raw is None:
            return None
        payload = json.loads(raw)
        return IdempotencyRecord(key, payload["status"], payload.get("result"))

    async def abandon(self, key: str) -> None:
        await self._redis.eval(
            _abandon_idempotency_script,
            1,
            self._key("idempotency", key),
        )

    async def check(self, key: str, *, limit: int, window_seconds: int) -> RateLimitDecision:
        if limit < 1 or window_seconds < 1:
            raise ValueError("limit and window_seconds must be positive")
        now_ms = int(time.time() * 1000)
        result = await self._redis.eval(
            _rate_limit_script,
            1,
            self._key("rate", key),
            now_ms,
            window_seconds * 1000,
            limit,
            f"{now_ms}:{uuid.uuid4()}",
        )
        return RateLimitDecision(
            allowed=bool(result[0]),
            remaining=int(result[1]),
            retry_after_seconds=max(0.0, float(result[2]) / 1000),
        )

    async def set_state(self, key: str, value: Mapping[str, Any], *, ttl_seconds: int) -> None:
        await self._redis.set(
            self._key("state", key),
            json.dumps(dict(value), ensure_ascii=False),
            ex=ttl_seconds,
        )

    async def get_state(self, key: str) -> Mapping[str, Any] | None:
        raw = _decode(await self._redis.get(self._key("state", key)))
        return json.loads(raw) if raw is not None else None

    async def delete_state(self, key: str) -> None:
        await self._redis.delete(self._key("state", key))

    async def close(self) -> None:
        await self._redis.aclose()


class RedisSessionStore:
    """Redis hot-session state with atomic Lua compare-and-swap."""

    def __init__(self, redis: Redis, key_prefix: str = "trpc") -> None:
        self._redis = redis
        self._prefix = key_prefix.rstrip(":")

    @classmethod
    def from_url(cls, url: str, key_prefix: str = "trpc") -> "RedisSessionStore":
        return cls(Redis.from_url(url, decode_responses=False), key_prefix)

    def _key(self, identity: SessionIdentity) -> str:
        lock_key = session_lock_key(identity.tenant_id, identity.agent_app_id, identity.session_id)
        return f"{self._prefix}:hot:{lock_key}"

    @staticmethod
    def _serialize(
        identity: SessionIdentity, state: Mapping[str, Any], version: int, updated_at: datetime
    ) -> str:
        return json.dumps(
            {
                "tenant_id": identity.tenant_id,
                "agent_app_id": identity.agent_app_id,
                "user_id": identity.user_id,
                "session_id": identity.session_id,
                "state": dict(state),
                "version": version,
                "updated_at": updated_at.isoformat(),
            },
            ensure_ascii=False,
        )

    @staticmethod
    def _deserialize(raw: str) -> SessionSnapshot:
        payload = json.loads(raw)
        identity = SessionIdentity(
            payload["tenant_id"],
            payload["agent_app_id"],
            payload["user_id"],
            payload["session_id"],
        )
        return SessionSnapshot(
            identity,
            payload["state"],
            int(payload["version"]),
            datetime.fromisoformat(payload["updated_at"]),
        )

    async def get_session(self, identity: SessionIdentity) -> SessionSnapshot | None:
        raw = _decode(await self._redis.get(self._key(identity)))
        return self._deserialize(raw) if raw is not None else None

    async def create_session(self, identity: SessionIdentity) -> SessionSnapshot:
        snapshot = SessionSnapshot(identity, {}, 0, datetime.now(timezone.utc))
        await self._redis.set(
            self._key(identity),
            self._serialize(identity, snapshot.state, snapshot.version, snapshot.updated_at),
            nx=True,
        )
        return await self.get_session(identity) or snapshot

    async def compare_and_swap_state(
        self,
        identity: SessionIdentity,
        expected_version: int,
        next_state: Mapping[str, Any],
    ) -> SessionSnapshot:
        updated_at = datetime.now(timezone.utc)
        payload = self._serialize(identity, next_state, expected_version + 1, updated_at)
        result = await self._redis.eval(
            _cas_session_script,
            1,
            self._key(identity),
            expected_version,
            payload,
        )
        if not bool(result[0]):
            raise VersionConflictError(
                f"expected session version {expected_version}, got {int(result[1])}"
            )
        return SessionSnapshot(identity, dict(next_state), expected_version + 1, updated_at)

    async def close(self) -> None:
        await self._redis.aclose()
