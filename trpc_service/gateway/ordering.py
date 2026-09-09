"""Best-effort channel sequence observation without silently reordering content."""

from __future__ import annotations

import asyncio

from redis.asyncio import Redis
from redis.asyncio import from_url


class InMemoryOrderingStore:

    def __init__(self) -> None:
        self._last: dict[str, int] = {}
        self._lock = asyncio.Lock()

    async def observe(self, scope: str, sequence: int) -> bool:
        """Return True when sequence is older/equal; messages still process by arrival."""
        async with self._lock:
            previous = self._last.get(scope)
            self._last[scope] = max(sequence, previous or sequence)
            return previous is not None and sequence <= previous


_OBSERVE = """
local previous = redis.call('get', KEYS[1])
if not previous or tonumber(ARGV[1]) > tonumber(previous) then
  redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[2])
  return 0
end
return 1
"""


class RedisOrderingStore:

    def __init__(self, redis_url: str, client: Redis | None = None) -> None:
        self._redis = client or from_url(redis_url, decode_responses=True)
        self._owns_client = client is None

    async def observe(self, scope: str, sequence: int) -> bool:
        key = f"trpc-service:channel-order:{scope}"
        return bool(await self._redis.eval(_OBSERVE, 1, key, sequence, 604800))

    async def close(self) -> None:
        if self._owns_client:
            await self._redis.aclose()
