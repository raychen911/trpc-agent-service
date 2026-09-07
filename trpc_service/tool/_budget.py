# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Per-tenant budget tracking and a MODEL filter that enforces it.

Token usage is recorded after each LLM call (from ``usage_metadata``) and cost
is derived from an injectable pricing table. The filter short-circuits a model
call *before* it happens when the tenant is over budget.
"""

from __future__ import annotations

import contextvars
import inspect
import threading
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Callable
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict

from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.abc import FilterType
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.filter import BaseFilter

from trpc_service.tenant import Tenant
from ._exceptions import BudgetExceededError

TenantResolver = Callable[[str], Optional[Tenant]]


def today_str() -> str:
    """Return today's date as ``YYYY-MM-DD`` in UTC (consistent across nodes)."""
    return datetime.now(timezone.utc).date().isoformat()


class ModelPricing(BaseModel):
    """Cost per million tokens for a model."""

    model_config = ConfigDict(extra="forbid")

    input_per_mtok: float = 0.0
    output_per_mtok: float = 0.0


class BudgetTracker:
    """Tracks per-tenant, per-day token usage and cost (in-memory, injectable pricing).

    Usage is partitioned by ``(tenant_id, date_str)`` so daily budgets roll over
    automatically on the UTC date boundary — no reset task is required and no
    cross-node reset coordination is needed.
    """

    def __init__(self, pricing: Optional[dict[str, ModelPricing]] = None) -> None:
        self._pricing = pricing or {}
        self._usage: dict[tuple[str, str], dict[str, float]] = {}
        self._reserved: dict[tuple[str, str], int] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _resolve_date(date_str: Optional[str]) -> str:
        return date_str or today_str()

    def set_pricing(self, model_name: str, pricing: ModelPricing) -> None:
        self._pricing[model_name] = pricing

    def record(
        self,
        tenant_id: str,
        model_name: str,
        input_tokens: int,
        output_tokens: int,
        date_str: Optional[str] = None,
    ) -> None:
        date_str = self._resolve_date(date_str)
        pricing = self._pricing.get(model_name)
        cost = 0.0
        if pricing is not None:
            cost = (input_tokens * pricing.input_per_mtok + output_tokens * pricing.output_per_mtok) / 1_000_000
        with self._lock:
            usage = self._usage.setdefault((tenant_id, date_str), {"input": 0.0, "output": 0.0, "cost": 0.0})
            usage["input"] += input_tokens
            usage["output"] += output_tokens
            usage["cost"] += cost

    def reserve(self, tenant: Tenant, estimated_tokens: int, date_str: Optional[str] = None) -> bool:
        """Atomically reserve an estimated token amount against the token budget.

        Returns ``False`` (without reserving) when the reservation would exceed
        ``budget.daily_token_budget`` for the given (default: today's) day. This
        closes the check-then-act race where concurrent model calls could
        otherwise all pass the budget check.
        """
        tenant_id = tenant.tenant_id
        budget = tenant.budget.daily_token_budget
        if budget is None:
            return True
        date_str = self._resolve_date(date_str)
        with self._lock:
            usage = self._usage.get((tenant_id, date_str), {})
            committed = usage.get("input", 0) + usage.get("output", 0) + self._reserved.get((tenant_id, date_str), 0)
            if committed + estimated_tokens > budget:
                return False
            self._reserved[(tenant_id, date_str)] = self._reserved.get((tenant_id, date_str), 0) + estimated_tokens
            return True

    def release(self, tenant_id: str, estimated_tokens: int, date_str: Optional[str] = None) -> None:
        """Release a previously reserved token amount (paired with :meth:`reserve`)."""
        date_str = self._resolve_date(date_str)
        with self._lock:
            self._reserved[(tenant_id, date_str)] = max(0,
                                                        self._reserved.get((tenant_id, date_str), 0) - estimated_tokens)

    def reserved_tokens(self, tenant_id: str, date_str: Optional[str] = None) -> int:
        date_str = self._resolve_date(date_str)
        with self._lock:
            return self._reserved.get((tenant_id, date_str), 0)

    def input_tokens(self, tenant_id: str, date_str: Optional[str] = None) -> int:
        date_str = self._resolve_date(date_str)
        with self._lock:
            return int(self._usage.get((tenant_id, date_str), {}).get("input", 0))

    def output_tokens(self, tenant_id: str, date_str: Optional[str] = None) -> int:
        date_str = self._resolve_date(date_str)
        with self._lock:
            return int(self._usage.get((tenant_id, date_str), {}).get("output", 0))

    def total_tokens(self, tenant_id: str, date_str: Optional[str] = None) -> int:
        date_str = self._resolve_date(date_str)
        return self.input_tokens(tenant_id, date_str) + self.output_tokens(tenant_id, date_str)

    def cost(self, tenant_id: str, date_str: Optional[str] = None) -> float:
        date_str = self._resolve_date(date_str)
        with self._lock:
            return float(self._usage.get((tenant_id, date_str), {}).get("cost", 0.0))

    def cost_by_date(self, tenant_id: str) -> dict[str, float]:
        """Return per-day cost history for a tenant (used for billing)."""
        with self._lock:
            return {
                date_str: usage.get("cost", 0.0)
                for (tid, date_str), usage in self._usage.items() if tid == tenant_id
            }

    def is_within_budget(self, tenant: Tenant, date_str: Optional[str] = None) -> bool:
        """Check the tenant's token and cost budgets for the given day (``None`` = unlimited)."""
        date_str = self._resolve_date(date_str)
        budget = tenant.budget
        if (budget.daily_token_budget is not None
                and self.total_tokens(tenant.tenant_id, date_str) >= budget.daily_token_budget):
            return False
        if budget.daily_cost_limit is not None and self.cost(tenant.tenant_id, date_str) >= budget.daily_cost_limit:
            return False
        return True

    def reset(self, tenant_id: str, date_str: Optional[str] = None) -> None:
        date_str = self._resolve_date(date_str)
        with self._lock:
            self._usage.pop((tenant_id, date_str), None)
            self._reserved.pop((tenant_id, date_str), None)


class ModelBudgetFilter(BaseFilter):
    """MODEL filter that blocks out-of-budget tenants and records usage.

    Budget enforcement uses an atomic ``reserve`` in ``_before`` (with an
    estimated per-call token count) and records actual usage + releases the
    reservation in ``_after``.
    """

    def __init__(
        self,
        tracker: BudgetTracker,
        *,
        resolver: Optional[TenantResolver] = None,
        tenant: Optional[Tenant] = None,
        estimated_tokens_per_call: int = 4096,
    ) -> None:
        super().__init__()
        self._type = FilterType.MODEL
        self._name = "tenant_model_budget"
        self._tracker = tracker
        self._resolver = resolver
        self._tenant = tenant
        self._estimated_tokens_per_call = estimated_tokens_per_call
        self._reserved_var: contextvars.ContextVar[int] = contextvars.ContextVar(
            f"tenant_model_budget_reserved_{id(self)}", default=0)

    def _resolve(self, tenant_id: str) -> Optional[Tenant]:
        if self._resolver is not None:
            return self._resolver(tenant_id)
        return self._tenant

    async def _before(self, ctx: AgentContext, req: Any, rsp: FilterResult):
        tenant_id = ctx.get_metadata("tenant_id")
        tenant = self._resolve(tenant_id)
        if tenant is None:
            return None
        reserved = self._tracker.reserve(tenant, self._estimated_tokens_per_call)
        if inspect.isawaitable(reserved):
            reserved = await reserved
        if not reserved:
            rsp.error = BudgetExceededError(f"tenant '{tenant_id}' has exceeded its budget")
            rsp.is_continue = False
            return None
        self._reserved_var.set(self._estimated_tokens_per_call)
        return None

    async def _after(self, ctx: AgentContext, req: Any, rsp: FilterResult):
        tenant_id = ctx.get_metadata("tenant_id")
        reserved = self._reserved_var.get()
        self._reserved_var.set(0)
        if reserved:
            released = self._tracker.release(tenant_id, reserved)
            if inspect.isawaitable(released):
                await released
        if rsp.error:
            return None
        response = rsp.rsp
        if response is None or getattr(response, "usage_metadata", None) is None:
            return None
        usage = response.usage_metadata
        prompt = usage.prompt_token_count or 0
        total = usage.total_token_count or 0
        output = max(0, total - prompt)
        if not tenant_id:
            return None
        model_name = getattr(req, "model", "") or ""
        recorded = self._tracker.record(tenant_id, model_name, prompt, output)
        if inspect.isawaitable(recorded):
            await recorded
        return None
