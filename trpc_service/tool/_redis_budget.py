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
        self._pricing: dict[tuple[Optional[str], str], ModelPricing] = {
            (None, model_name): value
            for model_name, value in (pricing or {}).items()
        }
        self._retention = retention_seconds

    @staticmethod
    def _key(tenant_id: str, date_str: Optional[str]) -> str:
        return f"agent:budget:{tenant_id}:{date_str or today_str()}"

    def set_pricing(
        self,
        model_name: str,
        pricing: ModelPricing,
        *,
        tenant_id: Optional[str] = None,
    ) -> None:
        self._pricing[(tenant_id, model_name)] = pricing

    def replace_tenant_pricing(self, tenant_id: str, pricing: dict[str, ModelPricing]) -> None:
        """Replace one tenant's table after a hot configuration reload."""
        self._pricing = {key: value for key, value in self._pricing.items() if key[0] != tenant_id}
        self._pricing.update({(tenant_id, model_name): value for model_name, value in pricing.items()})

    def _model_pricing(self, tenant_id: str, model_name: str) -> Optional[ModelPricing]:
        return self._pricing.get((tenant_id, model_name)) or self._pricing.get((None, model_name))

    def estimate_cost(self, tenant_id: str, model_name: str, tokens: int) -> Optional[float]:
        pricing = self._model_pricing(tenant_id, model_name)
        if pricing is None:
            return None
        return tokens * max(pricing.input_per_mtok, pricing.output_per_mtok) / 1_000_000

    async def reserve(
        self,
        tenant: Tenant,
        estimated_tokens: int,
        date_str: Optional[str] = None,
        *,
        model_name: str = "",
    ) -> bool:
        token_budget = tenant.budget.daily_token_budget
        cost_budget = tenant.budget.daily_cost_limit
        if token_budget is None and cost_budget is None:
            return True
        estimated_cost = self.estimate_cost(tenant.tenant_id, model_name, estimated_tokens)
        if cost_budget is not None and estimated_cost is None:
            return False
        key = self._key(tenant.tenant_id, date_str)
        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    values = await pipe.hmget(key, "input", "output", "reserved", "cost", "reserved_cost")
                    committed_tokens = sum(int(float(value or 0)) for value in values[:3])
                    committed_cost = sum(float(value or 0) for value in values[3:])
                    if token_budget is not None and committed_tokens + estimated_tokens > token_budget:
                        await pipe.unwatch()
                        return False
                    if cost_budget is not None and committed_cost + float(estimated_cost or 0.0) > cost_budget:
                        await pipe.unwatch()
                        return False
                    pipe.multi()
                    pipe.hincrby(key, "reserved", estimated_tokens)
                    pipe.hincrbyfloat(key, "reserved_cost", float(estimated_cost or 0.0))
                    pipe.expire(key, self._retention)
                    await pipe.execute()
                    return True
            except WatchError:
                continue

    async def release(
        self,
        tenant_id: str,
        estimated_tokens: int,
        date_str: Optional[str] = None,
        *,
        model_name: str = "",
    ) -> None:
        key = self._key(tenant_id, date_str)
        estimated_cost = self.estimate_cost(tenant_id, model_name, estimated_tokens) or 0.0
        while True:
            try:
                async with self._redis.pipeline(transaction=True) as pipe:
                    await pipe.watch(key)
                    values = await pipe.hmget(key, "reserved", "reserved_cost")
                    current = int(float(values[0] or 0))
                    current_cost = float(values[1] or 0)
                    pipe.multi()
                    pipe.hset(key, "reserved", max(0, current - estimated_tokens))
                    pipe.hset(key, "reserved_cost", max(0.0, current_cost - estimated_cost))
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
        pricing = self._model_pricing(tenant_id, model_name)
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
            "reserved_cost": float(raw.get("reserved_cost", raw.get(b"reserved_cost", 0)) or 0),
        }

    async def is_within_budget(self, tenant: Tenant, date_str: Optional[str] = None) -> bool:
        usage = await self.usage(tenant.tenant_id, date_str)
        budget = tenant.budget
        if budget.daily_token_budget is not None:
            if usage["input"] + usage["output"] + usage["reserved"] >= budget.daily_token_budget:
                return False
        if (budget.daily_cost_limit is not None and usage["cost"] + usage["reserved_cost"] >= budget.daily_cost_limit):
            return False
        return True

    async def close(self) -> None:
        await self._redis.aclose()
