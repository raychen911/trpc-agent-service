"""Redis session projection with monotonic sequence CAS."""

from __future__ import annotations

import base64
import json
from typing import Any, Protocol


class RedisProjectionClient(Protocol):
    async def eval(self, script: str, key_count: int, *args: Any) -> Any: ...

    async def hmget(self, key: str, *fields: str) -> list[Any]: ...


_PUT_LUA = """
local current = redis.call('HGET', KEYS[1], 'sequence')
if current and tonumber(current) > tonumber(ARGV[1]) then
  return 0
end
if current and tonumber(current) == tonumber(ARGV[1]) then
  local current_payload = redis.call('HGET', KEYS[1], 'payload')
  if current_payload ~= ARGV[2] then
    return 0
  end
  redis.call('EXPIRE', KEYS[1], ARGV[3])
  return 1
end
redis.call('HSET', KEYS[1], 'sequence', ARGV[1], 'payload', ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1
"""


class RedisProjectionStore:
    def __init__(self, redis: RedisProjectionClient, *, ttl_seconds: int = 86400) -> None:
        self._redis = redis
        self._ttl_seconds = ttl_seconds

    @staticmethod
    def key(tenant_id: str, session_id: str) -> str:
        # A reversible, delimiter-safe encoding prevents ``tenant:a`` + ``b``
        # colliding with ``tenant`` + ``a:b`` while retaining enough
        # information for migration tooling to inspect the key.
        def encode(value: str) -> str:
            return base64.urlsafe_b64encode(value.encode()).decode().rstrip("=")

        return f"trpc:projection:session:v2:{encode(tenant_id)}.{encode(session_id)}"

    async def put_session(
        self, tenant_id: str, session_id: str, *, sequence: int, value: dict[str, object]
    ) -> None:
        applied = await self._redis.eval(
            _PUT_LUA,
            1,
            self.key(tenant_id, session_id),
            sequence,
            json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True),
            self._ttl_seconds,
        )
        if applied != 1:
            raise ValueError("projection attempted to move backwards")

    async def get_session(
        self, tenant_id: str, session_id: str, *, minimum_sequence: int
    ) -> dict[str, object] | None:
        values = await self._redis.hmget(self.key(tenant_id, session_id), "sequence", "payload")
        if not values or values[0] is None or values[1] is None:
            return None
        sequence = int(values[0])
        if sequence < minimum_sequence:
            return None
        payload = values[1].decode() if isinstance(values[1], bytes) else values[1]
        value = json.loads(payload)
        if not isinstance(value, dict):
            raise ValueError("invalid Redis session projection")
        return value


__all__ = ["RedisProjectionStore"]
