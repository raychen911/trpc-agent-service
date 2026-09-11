"""Redis versioned projections; PostgreSQL remains authoritative."""

from __future__ import annotations

import json
from typing import Any

_SET_IF_NEWER = """
local current = redis.call('HGET', KEYS[1], 'version')
if current and tonumber(current) > tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'version', ARGV[1], 'payload', ARGV[2])
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 1
"""


class RedisProjectionStore:
    def __init__(self, redis_url: str, *, ttl_seconds: int = 3600, client: Any | None = None) -> None:
        if client is None:
            from redis.asyncio import Redis

            client = Redis.from_url(redis_url, decode_responses=True)
        self.client = client
        self.ttl_seconds = ttl_seconds

    @staticmethod
    def key(tenant_id: str, kind: str, identifier: str) -> str:
        return f"trpc:{tenant_id}:{kind}:{identifier}"

    async def put(self, tenant_id: str, kind: str, identifier: str, version: int, payload: dict[str, Any]) -> bool:
        value = await self.client.eval(
            _SET_IF_NEWER,
            1,
            self.key(tenant_id, kind, identifier),
            version,
            json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str),
            self.ttl_seconds,
        )
        return bool(value)

    async def get(self, tenant_id: str, kind: str, identifier: str) -> tuple[int, dict[str, Any]] | None:
        value = await self.client.hgetall(self.key(tenant_id, kind, identifier))
        if not value:
            return None
        payload = json.loads(value["payload"])
        return int(value["version"]), payload

    async def invalidate(self, tenant_id: str, kind: str, identifier: str) -> None:
        await self.client.delete(self.key(tenant_id, kind, identifier))

    async def close(self) -> None:
        await self.client.aclose()
