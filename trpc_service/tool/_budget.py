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
from pydantic import Field

from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.abc import FilterType
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.filter import BaseFilter

from trpc_service.metrics import EnterpriseMetrics
from trpc_service.metrics import get_enterprise_metrics
from trpc_service.tenant import Tenant
from ._exceptions import BudgetExceededError

TenantResolver = Callable[[str], Optional[Tenant]]


def today_str() -> str:
    """Return today's date as ``YYYY-MM-DD`` in UTC (consistent across nodes)."""
    return datetime.now(timezone.utc).date().isoformat()


class ModelPricing(BaseModel):
    """Cost per million tokens for a model."""

    model_config = ConfigDict(extra="forbid")

    input_per_mtok: float = Field(default=0.0, ge=0)
    output_per_mtok: float = Field(default=0.0, ge=0)


class BudgetTracker:
    """Tracks per-tenant, per-day token usage and cost (in-memory, injectable pricing).

    Usage is partitioned by ``(tenant_id, date_str)`` so daily budgets roll over
    automatically on the UTC date boundary — no reset task is required and no
    cross-node reset coordination is needed.
    """

    def __init__(self, pricing: Optional[dict[str, ModelPricing]] = None) -> None:
        self._pricing: dict[tuple[Optional[str], str], ModelPricing] = {
            (None, model_name): value
            for model_name, value in (pricing or {}).items()
        }
        self._usage: dict[tuple[str, str], dict[str, float]] = {}
        self._reserved: dict[tuple[str, str], int] = {}
        self._reserved_cost: dict[tuple[str, str], float] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _resolve_date(date_str: Optional[str]) -> str:
        return date_str or today_str()

    def set_pricing(
        self,
        model_name: str,
        pricing: ModelPricing,
        *,
        tenant_id: Optional[str] = None,
    ) -> None:
        with self._lock:
            self._pricing[(tenant_id, model_name)] = pricing

    def replace_tenant_pricing(self, tenant_id: str, pricing: dict[str, ModelPricing]) -> None:
        """Replace one tenant's table so removed prices cannot survive a config reload."""
        with self._lock:
            self._pricing = {key: value for key, value in self._pricing.items() if key[0] != tenant_id}
            self._pricing.update({(tenant_id, model_name): value for model_name, value in pricing.items()})

    def _model_pricing(self, tenant_id: str, model_name: str) -> Optional[ModelPricing]:
        with self._lock:
            return self._pricing.get((tenant_id, model_name)) or self._pricing.get((None, model_name))

    def estimate_cost(self, tenant_id: str, model_name: str, tokens: int) -> Optional[float]:
        """Conservatively price a reservation with an unknown input/output split."""
        pricing = self._model_pricing(tenant_id, model_name)
        if pricing is None:
            return None
        return tokens * max(pricing.input_per_mtok, pricing.output_per_mtok) / 1_000_000

    def record(
        self,
        tenant_id: str,
        model_name: str,
        input_tokens: int,
        output_tokens: int,
        date_str: Optional[str] = None,
    ) -> float:
        date_str = self._resolve_date(date_str)
        pricing = self._model_pricing(tenant_id, model_name)
        cost = 0.0
        if pricing is not None:
            cost = (input_tokens * pricing.input_per_mtok + output_tokens * pricing.output_per_mtok) / 1_000_000
        with self._lock:
            usage = self._usage.setdefault((tenant_id, date_str), {"input": 0.0, "output": 0.0, "cost": 0.0})
            usage["input"] += input_tokens
            usage["output"] += output_tokens
            usage["cost"] += cost
        return cost

    def reserve(
        self,
        tenant: Tenant,
        estimated_tokens: int,
        date_str: Optional[str] = None,
        *,
        model_name: str = "",
    ) -> bool:
        """Atomically reserve estimated tokens and cost against both budgets.

        Cost limits fail closed when the selected model has no pricing
        configuration, rather than silently treating every call as free.
        """
        tenant_id = tenant.tenant_id
        token_budget = tenant.budget.daily_token_budget
        cost_budget = tenant.budget.daily_cost_limit
        if token_budget is None and cost_budget is None:
            return True
        estimated_cost = self.estimate_cost(tenant_id, model_name, estimated_tokens)
        if cost_budget is not None and estimated_cost is None:
            return False
        date_str = self._resolve_date(date_str)
        key = (tenant_id, date_str)
        with self._lock:
            usage = self._usage.get(key, {})
            committed_tokens = usage.get("input", 0) + usage.get("output", 0) + self._reserved.get(key, 0)
            committed_cost = usage.get("cost", 0.0) + self._reserved_cost.get(key, 0.0)
            if token_budget is not None and committed_tokens + estimated_tokens > token_budget:
                return False
            if cost_budget is not None and committed_cost + float(estimated_cost or 0.0) > cost_budget:
                return False
            self._reserved[key] = self._reserved.get(key, 0) + estimated_tokens
            self._reserved_cost[key] = self._reserved_cost.get(key, 0.0) + float(estimated_cost or 0.0)
            return True

    def release(
        self,
        tenant_id: str,
        estimated_tokens: int,
        date_str: Optional[str] = None,
        *,
        model_name: str = "",
    ) -> None:
        """Release previously reserved token and estimated-cost capacity."""
        date_str = self._resolve_date(date_str)
        key = (tenant_id, date_str)
        estimated_cost = self.estimate_cost(tenant_id, model_name, estimated_tokens) or 0.0
        with self._lock:
            self._reserved[key] = max(0, self._reserved.get(key, 0) - estimated_tokens)
            self._reserved_cost[key] = max(0.0, self._reserved_cost.get(key, 0.0) - estimated_cost)

    def reserved_tokens(self, tenant_id: str, date_str: Optional[str] = None) -> int:
        date_str = self._resolve_date(date_str)
        with self._lock:
            return self._reserved.get((tenant_id, date_str), 0)

    def reserved_cost(self, tenant_id: str, date_str: Optional[str] = None) -> float:
        date_str = self._resolve_date(date_str)
        with self._lock:
            return self._reserved_cost.get((tenant_id, date_str), 0.0)

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
                and self.total_tokens(tenant.tenant_id, date_str) + self.reserved_tokens(tenant.tenant_id, date_str)
                >= budget.daily_token_budget):
            return False
        if (budget.daily_cost_limit is not None
                and self.cost(tenant.tenant_id, date_str) + self.reserved_cost(tenant.tenant_id, date_str)
                >= budget.daily_cost_limit):
            return False
        return True

    def reset(self, tenant_id: str, date_str: Optional[str] = None) -> None:
        date_str = self._resolve_date(date_str)
        with self._lock:
            self._usage.pop((tenant_id, date_str), None)
            self._reserved.pop((tenant_id, date_str), None)
            self._reserved_cost.pop((tenant_id, date_str), None)


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
        metrics: Optional[EnterpriseMetrics] = None,
    ) -> None:
        super().__init__()
        self._type = FilterType.MODEL
        self._name = "tenant_model_budget"
        self._tracker = tracker
        self._resolver = resolver
        self._tenant = tenant
        self._estimated_tokens_per_call = estimated_tokens_per_call
        self._metrics = metrics or get_enterprise_metrics()
        self._reserved_var: contextvars.ContextVar[tuple[int, str, str, str]] = contextvars.ContextVar(
            f"tenant_model_budget_reserved_{id(self)}", default=(0, "", "", ""))
        self._stream_response_var: contextvars.ContextVar[Any] = contextvars.ContextVar(
            f"tenant_model_budget_stream_response_{id(self)}", default=None)

    def _resolve(self, tenant_id: str) -> Optional[Tenant]:
        if self._resolver is not None:
            return self._resolver(tenant_id)
        return self._tenant

    async def _budget_usage(self, tenant_id: str) -> dict[str, float]:
        usage_method = getattr(self._tracker, "usage", None)
        if callable(usage_method):
            usage = usage_method(tenant_id)
            if inspect.isawaitable(usage):
                usage = await usage
            return {
                "input": float(usage.get("input", 0)),
                "output": float(usage.get("output", 0)),
                "reserved": float(usage.get("reserved", 0)),
                "cost": float(usage.get("cost", 0)),
                "reserved_cost": float(usage.get("reserved_cost", 0)),
            }

        return {
            "input": float(self._tracker.input_tokens(tenant_id)),
            "output": float(self._tracker.output_tokens(tenant_id)),
            "reserved": float(self._tracker.reserved_tokens(tenant_id)),
            "cost": float(self._tracker.cost(tenant_id)),
            "reserved_cost": float(self._tracker.reserved_cost(tenant_id)),
        }

    async def _publish_budget_metrics(self, tenant: Tenant) -> None:
        attributes = {"tenant_id": tenant.tenant_id}
        usage = await self._budget_usage(tenant.tenant_id)
        self._metrics.set_gauge(
            "agent_budget_tokens_used",
            usage["input"] + usage["output"],
            **attributes,
        )
        self._metrics.set_gauge("agent_budget_tokens_reserved", usage["reserved"], **attributes)
        self._metrics.set_gauge("agent_budget_cost_used", usage["cost"], **attributes)
        self._metrics.set_gauge("agent_budget_cost_reserved", usage["reserved_cost"], **attributes)
        if tenant.budget.daily_token_budget is not None:
            self._metrics.set_gauge(
                "agent_budget_daily_token_limit",
                tenant.budget.daily_token_budget,
                **attributes,
            )
        if tenant.budget.daily_cost_limit is not None:
            self._metrics.set_gauge(
                "agent_budget_daily_cost_limit",
                tenant.budget.daily_cost_limit,
                **attributes,
            )

    @staticmethod
    def _token_counts(response: Any) -> tuple[int, int]:
        usage = getattr(response, "usage_metadata", None)
        if usage is None:
            return 0, 0
        prompt = int(getattr(usage, "prompt_token_count", None) or 0)
        candidates = getattr(usage, "candidates_token_count", None)
        if candidates is not None:
            return prompt, max(0, int(candidates))
        total = int(getattr(usage, "total_token_count", None) or 0)
        return prompt, max(0, total - prompt)

    async def _before(self, ctx: AgentContext, req: Any, rsp: FilterResult):
        self._stream_response_var.set(None)
        tenant_id = ctx.get_metadata("tenant_id")
        tenant = self._resolve(tenant_id)
        if tenant is None:
            return None
        tenant_id = tenant.tenant_id
        model_name = str(getattr(req, "model", "") or tenant.model.model_name)
        reservation_model = self._reservation_model(tenant, model_name)
        reserved = self._tracker.reserve(
            tenant,
            self._estimated_tokens_per_call,
            model_name=reservation_model,
        )
        if inspect.isawaitable(reserved):
            reserved = await reserved
        if not reserved:
            await self._publish_budget_metrics(tenant)
            self._metrics.increment(
                "agent_budget_rejection_total",
                tenant_id=tenant_id,
                model=getattr(req, "model", None),
            )
            rsp.error = BudgetExceededError(f"tenant '{tenant_id}' has exceeded its budget")
            rsp.is_continue = False
            return None
        self._reserved_var.set((self._estimated_tokens_per_call, reservation_model, model_name, tenant_id))
        await self._publish_budget_metrics(tenant)
        return None

    async def _after_every_stream(self, ctx: AgentContext, req: Any, rsp: FilterResult) -> None:
        response = rsp.rsp
        if response is not None and getattr(response, "usage_metadata", None) is not None:
            self._stream_response_var.set(response)

    async def _after(self, ctx: AgentContext, req: Any, rsp: FilterResult):
        tenant_id = ctx.get_metadata("tenant_id")
        tenant = self._resolve(tenant_id)
        reserved, reserved_model, requested_model, reserved_tenant_id = self._reserved_var.get()
        self._reserved_var.set((0, "", "", ""))
        account_tenant_id = reserved_tenant_id or tenant_id
        if rsp.error:
            await self._release(account_tenant_id, reserved, reserved_model)
            if tenant is not None:
                await self._publish_budget_metrics(tenant)
            return None
        response = rsp.rsp or self._stream_response_var.get()
        self._stream_response_var.set(None)
        if response is None or getattr(response, "usage_metadata", None) is None:
            await self._release(account_tenant_id, reserved, reserved_model)
            if tenant is not None:
                await self._publish_budget_metrics(tenant)
            return None
        prompt, output = self._token_counts(response)
        if not account_tenant_id:
            return None
        response_model = getattr(response, "model", "") or getattr(req, "model", "") or ""
        model_name = str(response_model) or requested_model
        try:
            cost = self._tracker.record(account_tenant_id, model_name, prompt, output)
            if inspect.isawaitable(cost):
                cost = await cost
        finally:
            await self._release(account_tenant_id, reserved, reserved_model)
        self._metrics.increment("agent_llm_input_tokens_total", prompt, tenant_id=account_tenant_id, model=model_name)
        self._metrics.increment("agent_llm_output_tokens_total", output, tenant_id=account_tenant_id, model=model_name)
        self._metrics.increment("agent_llm_cost_total",
                                float(cost or 0.0),
                                tenant_id=account_tenant_id,
                                model=model_name)
        if tenant is not None:
            await self._publish_budget_metrics(tenant)
        return None

    async def _release(self, tenant_id: str, reserved: int, model_name: str) -> None:
        if not tenant_id or not reserved:
            return
        released = self._tracker.release(tenant_id, reserved, model_name=model_name)
        if inspect.isawaitable(released):
            await released

    def _reservation_model(self, tenant: Tenant, requested_model: str) -> str:
        """Reserve for the most expensive possible primary/fallback model."""
        if tenant.budget.daily_cost_limit is None or requested_model != tenant.model.model_name:
            return requested_model
        candidates = [requested_model]
        if tenant.model.fallback_model:
            candidates.append(tenant.model.fallback_model)
        priced: list[tuple[float, str]] = []
        for model_name in candidates:
            cost = self._tracker.estimate_cost(
                tenant.tenant_id,
                model_name,
                self._estimated_tokens_per_call,
            )
            if cost is None:
                return model_name
            priced.append((float(cost), model_name))
        return max(priced)[1]
