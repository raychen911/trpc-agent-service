# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Durable task results used to make IM reply retries side-effect safe."""

from __future__ import annotations

from typing import Any
from typing import Optional


class LocalTaskResultStore:

    def __init__(self) -> None:
        self._results: dict[str, str] = {}

    async def get(self, key: str) -> Optional[str]:
        return self._results.get(key)

    async def put(self, key: str, result: str) -> None:
        self._results[key] = result


class RedisTaskResultStore:
    """Share completed Agent results so redelivery only retries IM sending."""

    def __init__(
        self,
        *,
        redis_url: Optional[str] = None,
        client: Any = None,
        ttl_seconds: int = 86400,
    ) -> None:
        if client is not None:
            self._redis = client
        elif redis_url:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(redis_url, decode_responses=True)
        else:
            raise ValueError("RedisTaskResultStore requires redis_url or client")
        self._ttl = ttl_seconds

    @staticmethod
    def _key(key: str) -> str:
        return f"agent:task-result:{key}"

    async def get(self, key: str) -> Optional[str]:
        return await self._redis.get(self._key(key))

    async def put(self, key: str, result: str) -> None:
        await self._redis.set(self._key(key), result, ex=self._ttl)

    async def close(self) -> None:
        await self._redis.aclose()
