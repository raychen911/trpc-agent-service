# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for budget tracking, HITL confirmation and streaming replies."""

from __future__ import annotations

import asyncio
import time
from types import SimpleNamespace

import fakeredis.aioredis as faioredis

from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.context import new_agent_context
from trpc_service import EnterpriseMetrics
from trpc_service.tool import BudgetTracker
from trpc_service.tool import ConfirmationManager
from trpc_service.tool import ModelBudgetFilter
from trpc_service.tool import ModelPricing
from trpc_service.tool import RedisBudgetTracker
from trpc_service.tool import RedisConfirmationManager
from trpc_service.tool import ToolAllowlistFilter
from trpc_service.tool import ToolConfirmationRequired
from trpc_service.tenant import BudgetConfig
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import Tenant
from trpc_service.tenant import ToolPermissions

# ------------------------------------------------------------------ budget


def _tenant(token_budget=None, cost_limit=None) -> Tenant:
    tenant = Tenant(tenant_id="tenant_a", name="t", model=ModelEndpoint(model_name="gpt-4o"))
    tenant.budget = BudgetConfig(daily_token_budget=token_budget, daily_cost_limit=cost_limit)
    return tenant


def test_budget_tracker_records_tokens_and_cost():
    tracker = BudgetTracker(pricing={"gpt-4o": ModelPricing(input_per_mtok=2.5, output_per_mtok=10.0)})
    cost = tracker.record("tenant_a", "gpt-4o", input_tokens=1000, output_tokens=500)
    assert tracker.input_tokens("tenant_a") == 1000
    assert tracker.output_tokens("tenant_a") == 500
    assert tracker.total_tokens("tenant_a") == 1500
    # (1000*2.5 + 500*10.0) / 1e6 = (2500 + 5000)/1e6 = 0.0075
    assert cost == 0.0075
    assert tracker.cost("tenant_a") == 0.0075


def test_budget_tracker_without_pricing_records_zero_cost():
    tracker = BudgetTracker()
    tracker.record("tenant_a", "unknown_model", 100, 50)
    assert tracker.total_tokens("tenant_a") == 150
    assert tracker.cost("tenant_a") == 0.0


def test_budget_tracker_is_within_budget():
    tracker = BudgetTracker()
    tracker.record("tenant_a", "m", 100, 50)
    assert tracker.is_within_budget(_tenant(token_budget=200)) is True
    assert tracker.is_within_budget(_tenant(token_budget=150)) is False  # reached exactly


def test_budget_reserve_is_atomic():
    tracker = BudgetTracker()
    tenant = _tenant(token_budget=100)
    assert tracker.reserve(tenant, 60) is True  # committed=60
    assert tracker.reserve(tenant, 60) is False  # 120 > 100, must not reserve
    tracker.release(tenant.tenant_id, 60)
    assert tracker.reserve(tenant, 50) is True  # committed=50 after release


def test_budget_daily_rollover():
    tracker = BudgetTracker(pricing={"m": ModelPricing(input_per_mtok=1.0, output_per_mtok=1.0)})
    tracker.record("tenant_a", "m", 100, 0, date_str="2026-01-01")
    tracker.record("tenant_a", "m", 200, 0, date_str="2026-01-02")

    # Each day's usage is isolated.
    assert tracker.total_tokens("tenant_a", date_str="2026-01-01") == 100
    assert tracker.total_tokens("tenant_a", date_str="2026-01-02") == 200
    # Today (a different date) has no usage — the budget has effectively reset.
    assert tracker.total_tokens("tenant_a") == 0
    # Per-day cost history is retained for billing.
    assert set(tracker.cost_by_date("tenant_a").keys()) == {"2026-01-01", "2026-01-02"}


def test_reserve_scoped_to_day():
    tracker = BudgetTracker()
    tenant = _tenant(token_budget=100)
    assert tracker.reserve(tenant, 60, date_str="2026-01-01") is True
    # A new day is unaffected by yesterday's reservation.
    assert tracker.reserve(tenant, 60, date_str="2026-01-02") is True
    # The same day still enforces the budget.
    assert tracker.reserve(tenant, 60, date_str="2026-01-02") is False


async def test_model_budget_filter_blocks_over_budget():
    tracker = BudgetTracker()
    tracker.record("tenant_a", "gpt-4o", 100, 50)
    f = ModelBudgetFilter(tracker, tenant=_tenant(token_budget=100))
    rsp = FilterResult()
    await f._before(new_agent_context(metadata={"tenant_id": "tenant_a"}), None, rsp)
    assert rsp.error is not None
    assert rsp.is_continue is False


async def test_model_budget_filter_records_usage_after():
    tracker = BudgetTracker()
    metrics = EnterpriseMetrics(meter=False)
    f = ModelBudgetFilter(tracker, tenant=_tenant(token_budget=10000), metrics=metrics)
    rsp = FilterResult()
    rsp.rsp = SimpleNamespace(usage_metadata=SimpleNamespace(prompt_token_count=80, total_token_count=120))
    await f._after(new_agent_context(metadata={"tenant_id": "tenant_a"}), SimpleNamespace(model="gpt-4o"), rsp)
    assert tracker.total_tokens("tenant_a") == 120
    counters = {item["name"]: item for item in metrics.snapshot("tenant_a")["counters"]}
    assert counters["agent_llm_input_tokens_total"]["value"] == 80
    assert counters["agent_llm_output_tokens_total"]["value"] == 40
    assert counters["agent_llm_cost_total"]["value"] == 0
    assert counters["agent_llm_input_tokens_total"]["attributes"]["model"] == "gpt-4o"


async def test_model_budget_filter_records_streamed_usage_and_budget_gauges():
    tracker = BudgetTracker(pricing={"gpt-4o": ModelPricing()})
    metrics = EnterpriseMetrics(meter=False)
    tenant = _tenant(token_budget=10000, cost_limit=5)
    model_filter = ModelBudgetFilter(tracker, tenant=tenant, metrics=metrics)
    ctx = new_agent_context(metadata={"tenant_id": "tenant_a"})
    response = SimpleNamespace(
        model="gpt-4o",
        usage_metadata=SimpleNamespace(
            prompt_token_count=80,
            candidates_token_count=45,
            total_token_count=125,
        ),
    )

    async def handle():
        yield FilterResult(rsp=response)

    events = [event async for event in model_filter.run_stream(ctx, SimpleNamespace(model="gpt-4o"), handle)]

    assert events[0].rsp is response
    assert tracker.total_tokens("tenant_a") == 125
    snapshot = metrics.snapshot("tenant_a")
    counters = {item["name"]: item["value"] for item in snapshot["counters"]}
    gauges = {item["name"]: item["value"] for item in snapshot["gauges"]}
    assert counters["agent_llm_input_tokens_total"] == 80
    assert counters["agent_llm_output_tokens_total"] == 45
    assert gauges == {
        "agent_budget_cost_reserved": 0,
        "agent_budget_cost_used": 0,
        "agent_budget_daily_cost_limit": 5,
        "agent_budget_daily_token_limit": 10000,
        "agent_budget_tokens_reserved": 0,
        "agent_budget_tokens_used": 125,
    }


async def test_model_budget_filter_records_priced_cost_and_rejections():
    metrics = EnterpriseMetrics(meter=False)
    tracker = BudgetTracker(pricing={"gpt-4o": ModelPricing(input_per_mtok=2.5, output_per_mtok=10.0)})
    f = ModelBudgetFilter(tracker, tenant=_tenant(token_budget=10000), metrics=metrics)
    rsp = FilterResult()
    rsp.rsp = SimpleNamespace(usage_metadata=SimpleNamespace(prompt_token_count=1000, total_token_count=1500))

    await f._after(new_agent_context(metadata={"tenant_id": "tenant_a"}), SimpleNamespace(model="gpt-4o"), rsp)

    cost = next(item for item in metrics.snapshot("tenant_a")["counters"] if item["name"] == "agent_llm_cost_total")
    assert cost["value"] == 0.0075
    assert cost["unit"] == "USD"

    blocked = ModelBudgetFilter(tracker, tenant=_tenant(token_budget=1), metrics=metrics)
    blocked_rsp = FilterResult()
    await blocked._before(new_agent_context(metadata={"tenant_id": "tenant_a"}), SimpleNamespace(model="gpt-4o"),
                          blocked_rsp)
    rejection = next(item for item in metrics.snapshot("tenant_a")["counters"]
                     if item["name"] == "agent_budget_rejection_total")
    assert rejection["value"] == 1
    assert blocked_rsp.is_continue is False


async def test_cost_budget_reserves_before_call_and_missing_price_fails_closed():
    metrics = EnterpriseMetrics(meter=False)
    tenant = _tenant(cost_limit=0.009)
    priced = BudgetTracker(pricing={"gpt-4o": ModelPricing(input_per_mtok=2, output_per_mtok=10)})
    model_filter = ModelBudgetFilter(priced, tenant=tenant, estimated_tokens_per_call=1000, metrics=metrics)
    rsp = FilterResult()

    await model_filter._before(
        new_agent_context(metadata={"tenant_id": "tenant_a"}),
        SimpleNamespace(model="gpt-4o"),
        rsp,
    )

    assert rsp.is_continue is False
    assert priced.reserved_cost("tenant_a") == 0

    missing_price = ModelBudgetFilter(BudgetTracker(), tenant=_tenant(cost_limit=1), metrics=metrics)
    missing_rsp = FilterResult()
    await missing_price._before(
        new_agent_context(metadata={"tenant_id": "tenant_a"}),
        SimpleNamespace(model="gpt-4o"),
        missing_rsp,
    )
    assert missing_rsp.is_continue is False


async def test_cost_budget_records_actual_cost_and_releases_reservation():
    tracker = BudgetTracker(pricing={"gpt-4o": ModelPricing(input_per_mtok=2, output_per_mtok=10)})
    tenant = _tenant(cost_limit=1)
    model_filter = ModelBudgetFilter(tracker, tenant=tenant, estimated_tokens_per_call=1000)
    ctx = new_agent_context(metadata={"tenant_id": "tenant_a"})
    response = SimpleNamespace(
        model="gpt-4o",
        usage_metadata=SimpleNamespace(prompt_token_count=1000, candidates_token_count=500),
    )

    async def handle():
        assert tracker.reserved_cost("tenant_a") == 0.01
        yield FilterResult(rsp=response)

    events = [event async for event in model_filter.run_stream(ctx, SimpleNamespace(model="gpt-4o"), handle)]

    assert events[0].rsp is response
    assert tracker.cost("tenant_a") == 0.007
    assert tracker.reserved_cost("tenant_a") == 0


async def test_cost_budget_reserves_for_fallback_and_records_model_used():
    tracker = BudgetTracker(
        pricing={
            "primary": ModelPricing(input_per_mtok=1, output_per_mtok=1),
            "backup": ModelPricing(input_per_mtok=10, output_per_mtok=10),
        })
    tenant = _tenant(cost_limit=0.009)
    tenant.model.model_name = "primary"
    tenant.model.fallback_model = "backup"
    model_filter = ModelBudgetFilter(tracker, tenant=tenant, estimated_tokens_per_call=1000)
    ctx = new_agent_context(metadata={"tenant_id": "tenant_a"})
    blocked = FilterResult()

    await model_filter._before(ctx, SimpleNamespace(model="primary"), blocked)

    assert blocked.is_continue is False

    tenant.budget.daily_cost_limit = 1
    response = SimpleNamespace(
        model="backup",
        usage_metadata=SimpleNamespace(prompt_token_count=1000, candidates_token_count=0),
    )

    async def handle():
        assert tracker.reserved_cost("tenant_a") == 0.01
        yield FilterResult(rsp=response)

    events = [event async for event in model_filter.run_stream(ctx, SimpleNamespace(model="primary"), handle)]

    assert events[0].rsp is response
    assert tracker.cost("tenant_a") == 0.01
    assert tracker.reserved_cost("tenant_a") == 0


def test_budget_pricing_is_tenant_scoped():
    tracker = BudgetTracker()
    tracker.set_pricing("shared", ModelPricing(input_per_mtok=1), tenant_id="tenant_a")
    tracker.set_pricing("shared", ModelPricing(input_per_mtok=3), tenant_id="tenant_b")

    assert tracker.record("tenant_a", "shared", 1_000_000, 0) == 1
    assert tracker.record("tenant_b", "shared", 1_000_000, 0) == 3


async def test_redis_budget_tracker_is_atomic_across_concurrent_workers():
    client = faioredis.FakeRedis(decode_responses=True)
    tracker_a = RedisBudgetTracker(client=client)
    tracker_b = RedisBudgetTracker(client=client)
    tenant = _tenant(token_budget=100)

    results = await asyncio.gather(*[(tracker_a if index % 2 else tracker_b).reserve(tenant, 60, date_str="2026-01-01")
                                     for index in range(10)])
    assert results.count(True) == 1
    usage = await tracker_a.usage("tenant_a", date_str="2026-01-01")
    assert usage["reserved"] == 60

    await tracker_b.release("tenant_a", 60, date_str="2026-01-01")
    cost = await tracker_a.record("tenant_a", "gpt-4o", 40, 10, date_str="2026-01-01")
    assert cost == 0
    usage = await tracker_a.usage("tenant_a", date_str="2026-01-01")
    assert usage["reserved"] == 0
    assert usage["input"] + usage["output"] == 50


async def test_redis_cost_budget_is_atomic_across_workers():
    client = faioredis.FakeRedis(decode_responses=True)
    pricing = {"gpt-4o": ModelPricing(input_per_mtok=2, output_per_mtok=10)}
    tracker_a = RedisBudgetTracker(client=client, pricing=pricing)
    tracker_b = RedisBudgetTracker(client=client, pricing=pricing)
    tenant = _tenant(cost_limit=0.015)

    results = await asyncio.gather(*[(tracker_a if index % 2 else tracker_b).reserve(
        tenant,
        1000,
        date_str="2026-01-01",
        model_name="gpt-4o",
    ) for index in range(10)])

    assert results.count(True) == 1
    usage = await tracker_a.usage("tenant_a", date_str="2026-01-01")
    assert usage["reserved_cost"] == 0.01


# ------------------------------------------------------------------- hitl


def test_confirmation_manager_request_get_resolve():
    mgr = ConfirmationManager()
    pending = mgr.request("tenant_a", "cancel_order", {"order_id": 1})
    assert pending.token
    assert mgr.get(pending.token).tool_name == "cancel_order"

    resolved = mgr.resolve(pending.token, approve=True)
    assert resolved.tool_name == "cancel_order"
    # A token can only be used once.
    assert mgr.get(pending.token) is None


def test_confirmation_manager_expiry():
    mgr = ConfirmationManager(ttl_seconds=0.001)
    pending = mgr.request("tenant_a", "cancel_order")
    time.sleep(0.01)
    assert mgr.get(pending.token) is None


async def test_redis_confirmation_is_shared_one_time_and_identity_bound():
    client = faioredis.FakeRedis(decode_responses=True)
    manager_a = RedisConfirmationManager(client=client)
    manager_b = RedisConfirmationManager(client=client)
    pending = await manager_a.request(
        "tenant_a",
        "cancel_order",
        {"order_id": 1},
        user_id="u1",
        session_id="s1",
    )

    assert (await manager_b.get(pending.token)).user_id == "u1"
    resolved = await manager_b.resolve(pending.token, approve=True)
    assert resolved.session_id == "s1"
    assert await manager_a.resolve(pending.token, approve=True) is None


async def test_dangerous_tool_requests_confirmation_token():
    mgr = ConfirmationManager()
    perms = ToolPermissions(dangerous_tools=["cancel_order"])
    f = ToolAllowlistFilter(permissions=perms, confirmation_manager=mgr)
    rsp = FilterResult()
    await f._before(new_agent_context(metadata={"tenant_id": "tenant_a"}), {"tool_name": "cancel_order"}, rsp)
    assert isinstance(rsp.error, ToolConfirmationRequired)
    assert rsp.is_continue is False
    token = rsp.error.token
    assert mgr.get(token).tool_name == "cancel_order"


async def test_confirmed_tool_is_allowed():
    mgr = ConfirmationManager()
    perms = ToolPermissions(dangerous_tools=["cancel_order"])
    f = ToolAllowlistFilter(permissions=perms, confirmation_manager=mgr)
    ctx = new_agent_context(metadata={"tenant_id": "tenant_a", "confirmed_tools": ["cancel_order"]})
    rsp = FilterResult()
    await f._before(ctx, {"tool_name": "cancel_order"}, rsp)
    assert rsp.error is None
    assert rsp.is_continue is True
