# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Redis-backed, cross-node atomic tenant budget accounting."""

from __future__ import annotations

from typing import Any
from typing import Optional

from redis.exceptions import WatchError

from trpc_service.tenant import Tenant
from ._budget import ModelPricing
from ._budget import today_str


class RedisBudgetTracker:
    """Persist token reservations and committed usage in Redis hashes."""

    def __init__(
        self,
        *,
        redis_url: Optional[str] = None,
        client: Any = None,
        pricing: Optional[dict[str, ModelPricing]] = None,
        retention_seconds: int = 35 * 86400,
    ) -> None:
        if client is not None:
            self._redis = client
        elif redis_url:
            import redis.asyncio as aioredis

            self._redis = aioredis.from_url(redis_url, decode_responses=True)
        else:
            raise ValueError("RedisBudgetTracker requires redis_url or client")
        self._pricing = pricing or {}
        self._retention = retention_seconds

    @staticmethod
    def _key(tenant_id: str, date_str: Optional[str]) -> str:
        return f"agent:budget:{tenant_id}:{date_str or today_str()}"

    def set_pricing(self, model_name: str, pricing: ModelPricing) -> None:
        self._pricing[model_name] = pricing

    async def reserve(self, tenant: Tenant, estimated_tokens: int, date_str: Optional[str] = None) -> bool:
        budget = tenant.budget.daily_token_budget
        if budget is None:
            return True
        key = self._key(tenant.tenant_id, date_str)
        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    values = await pipe.hmget(key, "input", "output", "reserved")
                    committed = sum(int(float(value or 0)) for value in values)
                    if committed + estimated_tokens > budget:
                        await pipe.unwatch()
                        return False
                    pipe.multi()
                    pipe.hincrby(key, "reserved", estimated_tokens)
                    pipe.expire(key, self._retention)
                    await pipe.execute()
                    return True
            except WatchError:
                continue

    async def release(self, tenant_id: str, estimated_tokens: int, date_str: Optional[str] = None) -> None:
        key = self._key(tenant_id, date_str)
        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    current = int(float(await pipe.hget(key, "reserved") or 0))
                    pipe.multi()
                    pipe.hset(key, "reserved", max(0, current - estimated_tokens))
                    pipe.expire(key, self._retention)
                    await pipe.execute()
                    return
            except WatchError:
                continue

    async def record(self,
                     tenant_id: str,
                     model_name: str,
                     input_tokens: int,
                     output_tokens: int,
                     date_str: Optional[str] = None) -> float:
        pricing = self._pricing.get(model_name)
        cost = 0.0
        if pricing is not None:
            cost = (input_tokens * pricing.input_per_mtok + output_tokens * pricing.output_per_mtok) / 1_000_000
        key = self._key(tenant_id, date_str)
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.hincrby(key, "input", input_tokens)
            pipe.hincrby(key, "output", output_tokens)
            pipe.hincrbyfloat(key, "cost", cost)
            pipe.expire(key, self._retention)
            await pipe.execute()
        return cost

    async def usage(self, tenant_id: str, date_str: Optional[str] = None) -> dict[str, float]:
        raw = await self._redis.hgetall(self._key(tenant_id, date_str))
        return {
            "input": float(raw.get("input", raw.get(b"input", 0)) or 0),
            "output": float(raw.get("output", raw.get(b"output", 0)) or 0),
            "reserved": float(raw.get("reserved", raw.get(b"reserved", 0)) or 0),
            "cost": float(raw.get("cost", raw.get(b"cost", 0)) or 0),
        }

    async def is_within_budget(self, tenant: Tenant, date_str: Optional[str] = None) -> bool:
        usage = await self.usage(tenant.tenant_id, date_str)
        budget = tenant.budget
        if budget.daily_token_budget is not None:
            if usage["input"] + usage["output"] >= budget.daily_token_budget:
                return False
        if budget.daily_cost_limit is not None and usage["cost"] >= budget.daily_cost_limit:
            return False
        return True

    async def close(self) -> None:
        await self._redis.aclose()
