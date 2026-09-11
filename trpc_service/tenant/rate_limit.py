"""Tenant-scoped admission rate limiting."""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections import defaultdict, deque
from typing import Any


class RateLimitExceeded(RuntimeError):
    pass


class InMemoryRateLimiter:
    def __init__(self) -> None:
        self._values: dict[str, deque[float]] = defaultdict(deque)
        self._accepted: dict[tuple[str, str], float] = {}
        self._lock = asyncio.Lock()

    async def allow(
        self, tenant_id: str, limit: int, window_seconds: int = 60, request_key: str | None = None
    ) -> bool:
        if limit <= 0:
            return False
        cutoff = time.monotonic() - window_seconds
        async with self._lock:
            if request_key is not None and self._accepted.get((tenant_id, request_key), 0) > cutoff:
                return True
            values = self._values[tenant_id]
            while values and values[0] <= cutoff:
                values.popleft()
            if len(values) >= limit:
                return False
            now = time.monotonic()
            values.append(now)
            if request_key is not None:
                self._accepted[(tenant_id, request_key)] = now
            if len(self._accepted) > 10_000:
                self._accepted = {key: timestamp for key, timestamp in self._accepted.items() if timestamp > cutoff}
            return True


_FIXED_WINDOW = """
if ARGV[3] ~= '' and redis.call('EXISTS', KEYS[2]) == 1 then return 1 end
local value = redis.call('INCR', KEYS[1])
if value == 1 then redis.call('EXPIRE', KEYS[1], ARGV[2]) end
if value > tonumber(ARGV[1]) then
  redis.call('DECR', KEYS[1])
  return 0
end
if ARGV[3] ~= '' then redis.call('SET', KEYS[2], '1', 'EX', ARGV[2]) end
return 1
"""


class RedisRateLimiter:
    def __init__(self, client: Any, *, prefix: str = "trpc:tenant-rate:v1") -> None:
        self.client = client
        self.prefix = prefix

    async def allow(
        self, tenant_id: str, limit: int, window_seconds: int = 60, request_key: str | None = None
    ) -> bool:
        if limit <= 0:
            return False
        bucket = int(time.time()) // window_seconds
        identity = hashlib.sha256(request_key.encode()).hexdigest() if request_key else "none"
        value = await self.client.eval(
            _FIXED_WINDOW,
            2,
            f"{self.prefix}:{tenant_id}:{bucket}",
            f"{self.prefix}:accepted:{tenant_id}:{identity}",
            limit,
            window_seconds + 1,
            request_key or "",
        )
        return bool(value)
