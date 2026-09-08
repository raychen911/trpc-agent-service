# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for tenant governance filters and redaction."""

from __future__ import annotations

import asyncio
import pytest

from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.agents import BaseAgent
from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.context import new_agent_context
from trpc_service.tool import SensitiveDataRedactor
from trpc_service.tool import ChannelUserAuthorizationFilter
from trpc_service.tool import ToolAllowlistFilter
from trpc_service.tool import ToolOutputRedactionFilter
from trpc_service.tool import ToolCallLimitFilter
from trpc_service.tool import ToolExecutionTimeoutFilter
from trpc_service.tool import ToolMetricsFilter
from trpc_service.tool import GovernedToolSet
from trpc_service.tool import apply_tenant_governance
from trpc_service.tool import build_governance_filters
from trpc_service.metrics import EnterpriseMetrics
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import IMAccessPolicy
from trpc_service.tenant import Tenant
from trpc_service.tenant import ToolPermissions
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.tools import FunctionTool
from trpc_agent_sdk.tools import BaseToolSet


def _make_tenant() -> Tenant:
    tenant = Tenant(tenant_id="tenant_a", name="t", model=ModelEndpoint(model_name="gpt-4o"))
    tenant.tool_permissions = ToolPermissions(
        tool_whitelist=["query_order", "query_logistics"],
        tool_denylist=["delete_order"],
        dangerous_tools=["cancel_order"],
    )
    return tenant


def _ctx():
    return new_agent_context(metadata={"tenant_id": "tenant_a"})


async def test_allowlisted_tool_is_allowed():
    f = ToolAllowlistFilter(permissions=_make_tenant().tool_permissions)
    rsp = FilterResult()
    await f._before(_ctx(), {"tool_name": "query_order"}, rsp)
    assert rsp.error is None
    assert rsp.is_continue is True


async def test_denylisted_tool_is_blocked():
    f = ToolAllowlistFilter(permissions=_make_tenant().tool_permissions)
    rsp = FilterResult()
    await f._before(_ctx(), {"tool_name": "delete_order"}, rsp)
    assert isinstance(rsp.error, PermissionError)
    assert rsp.is_continue is False


async def test_non_whitelisted_tool_is_blocked():
    f = ToolAllowlistFilter(permissions=_make_tenant().tool_permissions)
    rsp = FilterResult()
    await f._before(_ctx(), {"tool_name": "refund_order"}, rsp)
    assert isinstance(rsp.error, PermissionError)
    assert rsp.is_continue is False


async def test_dangerous_tool_requires_confirmation():
    f = ToolAllowlistFilter(permissions=_make_tenant().tool_permissions)
    rsp = FilterResult()
    await f._before(_ctx(), {"tool_name": "cancel_order"}, rsp)
    assert isinstance(rsp.error, PermissionError)
    assert rsp.is_continue is False


async def test_no_permissions_means_allow():
    f = ToolAllowlistFilter(permissions=None)
    rsp = FilterResult()
    await f._before(_ctx(), {"tool_name": "anything"}, rsp)
    assert rsp.error is None
    assert rsp.is_continue is True


async def test_redaction_filter_masks_sensitive_output():
    f = ToolOutputRedactionFilter()
    rsp = FilterResult()
    rsp.rsp = {"msg": "call 13812345678 or use key sk-abcdefghijklmnop"}
    await f._after(_ctx(), {}, rsp)
    assert "13812345678" not in rsp.rsp["msg"]
    assert "sk-abcdefghijklmnop" not in rsp.rsp["msg"]


def test_sensitive_data_redactor_default_rules():
    redactor = SensitiveDataRedactor()
    text = "phone 13812345678, key sk-abcdefghijklmnop, Bearer abc.def"
    out = redactor.redact(text)
    assert "13812345678" not in out
    assert "sk-abcdefghijklmnop" not in out
    assert "Bearer abc.def" not in out


def test_build_governance_filters_returns_chain():
    filters = build_governance_filters(_make_tenant())
    assert len(filters) == 2
    assert isinstance(filters[0], ToolAllowlistFilter)
    assert isinstance(filters[1], ToolOutputRedactionFilter)


async def test_tool_call_limit_blocks_calls_above_turn_quota():
    quota = ToolCallLimitFilter(max_calls=2)
    for _ in range(2):
        allowed = FilterResult()
        await quota._before(_ctx(), {"tool_name": "query_order"}, allowed)
        assert allowed.error is None

    denied = FilterResult()
    await quota._before(_ctx(), {"tool_name": "query_order"}, denied)
    assert isinstance(denied.error, PermissionError)
    assert denied.is_continue is False


async def test_zero_tool_call_limit_means_unlimited():
    quota = ToolCallLimitFilter(max_calls=0)
    result = FilterResult()
    await quota._before(_ctx(), {"tool_name": "query_order"}, result)
    assert result.error is None


class _ToolAgent(BaseAgent):

    async def _run_async_impl(self, ctx):
        if False:
            yield None


async def test_tool_filter_blocks_real_function_tool_execution():
    calls = []

    def delete_order(order_id: str) -> str:
        """Delete an order."""
        calls.append(order_id)
        return "deleted"

    session_service = InMemorySessionService()
    session = await session_service.create_session(app_name="app", user_id="u1", session_id="s1")
    agent_context = new_agent_context(metadata={"tenant_id": "tenant_a"})
    invocation = InvocationContext(
        session_service=session_service,
        invocation_id="inv1",
        agent=_ToolAgent(name="agent"),
        agent_context=agent_context,
        session=session,
        branch="",
    )
    tool = FunctionTool(
        delete_order,
        filters=[ToolAllowlistFilter(permissions=_make_tenant().tool_permissions)],
    )

    with pytest.raises(PermissionError):
        await tool.run_async(tool_context=invocation, args={"order_id": "o1"})
    assert calls == []


async def test_tool_filter_redacts_real_function_tool_output():

    def query_order() -> str:
        """Return an order owner."""
        return "owner phone is 13812345678"

    session_service = InMemorySessionService()
    session = await session_service.create_session(app_name="app", user_id="u1", session_id="s1")
    invocation = InvocationContext(
        session_service=session_service,
        invocation_id="inv2",
        agent=_ToolAgent(name="agent"),
        agent_context=new_agent_context(metadata={"tenant_id": "tenant_a"}),
        session=session,
        branch="",
    )
    tool = FunctionTool(query_order, filters=[ToolOutputRedactionFilter()])

    result = await tool.run_async(tool_context=invocation, args={})
    assert "13812345678" not in result


async def test_im_user_authorization_checks_verified_user_and_group():
    tenant = _make_tenant()
    tenant.im_access_policy = IMAccessPolicy(
        require_verified_identity=True,
        allowed_users=["u1"],
        allowed_groups=["g1"],
    )
    policy_filter = ChannelUserAuthorizationFilter(tenant=tenant)

    allowed = FilterResult()
    await policy_filter._before(
        new_agent_context(metadata={
            "tenant_id": "tenant_a",
            "channel_user_id": "u1",
            "channel_chat_id": "g1",
            "channel_user_verified": True,
        }), None, allowed)
    assert allowed.error is None

    denied = FilterResult()
    await policy_filter._before(
        new_agent_context(metadata={
            "tenant_id": "tenant_a",
            "channel_user_id": "u2",
            "channel_chat_id": "g1",
            "channel_user_verified": False,
        }), None, denied)
    assert isinstance(denied.error, PermissionError)


def test_apply_tenant_governance_wraps_callable_and_attaches_filters():

    def query_order() -> str:
        """Query one order."""
        return "ok"

    class Agent:

        def __init__(self):
            self.tools = [query_order]
            self.filters = []

        def add_one_filter(self, item):
            self.filters.append(item)

    agent = apply_tenant_governance(Agent(), _make_tenant())

    assert isinstance(agent.tools[0], FunctionTool)
    assert any(isinstance(item, ToolAllowlistFilter) for item in agent.tools[0].filters)
    assert any(isinstance(item, ToolCallLimitFilter) for item in agent.tools[0].filters)
    assert any(isinstance(item, ToolExecutionTimeoutFilter) for item in agent.tools[0].filters)
    assert any(isinstance(item, ToolMetricsFilter) for item in agent.tools[0].filters)
    assert any(isinstance(item, ToolOutputRedactionFilter) for item in agent.tools[0].filters)
    # Agent-level channel authorization is owned by the deployment factory;
    # applying tool governance must not add a duplicate filter.
    assert not any(isinstance(item, ChannelUserAuthorizationFilter) for item in agent.filters)


def test_apply_tenant_governance_rejects_unknown_tool_type():
    sentinel = object()

    class Agent:
        tools = [sentinel]

    with pytest.raises(TypeError, match="unsupported tenant tool type"):
        apply_tenant_governance(Agent(), _make_tenant())


async def test_governed_toolset_applies_filters_after_dynamic_expansion():

    def query_order() -> str:
        """Query one order."""
        return "ok"

    class DynamicToolSet(BaseToolSet):

        def __init__(self):
            super().__init__(name="dynamic")
            self.closed = False
            self.tool = FunctionTool(query_order)

        async def get_tools(self, invocation_context=None):
            return [self.tool]

        async def close(self):
            self.closed = True

    inner = DynamicToolSet()

    class Agent:
        tools = [inner]

    agent = apply_tenant_governance(Agent(), _make_tenant())
    assert isinstance(agent.tools[0], GovernedToolSet)
    tools = await agent.tools[0].get_tools()
    assert len(tools) == 1
    assert any(isinstance(item, ToolAllowlistFilter) for item in tools[0].filters)
    assert any(isinstance(item, ToolExecutionTimeoutFilter) for item in tools[0].filters)
    filter_count = len(tools[0].filters)
    assert (await agent.tools[0].get_tools())[0] is tools[0]
    assert len(tools[0].filters) == filter_count
    await agent.tools[0].close()
    assert inner.closed is True


async def test_governed_toolset_rejects_non_tool_results():

    class InvalidToolSet(BaseToolSet):

        async def get_tools(self, invocation_context=None):
            return [object()]

    class Agent:
        tools = [InvalidToolSet(name="invalid")]

    governed = apply_tenant_governance(Agent(), _make_tenant())
    with pytest.raises(TypeError, match="returned unsupported tool type"):
        await governed.tools[0].get_tools()


async def test_tool_execution_timeout_cancels_slow_tool():

    async def slow_tool() -> str:
        """Wait longer than the tenant permits."""
        await asyncio.sleep(0.05)
        return "late"

    session_service = InMemorySessionService()
    session = await session_service.create_session(app_name="app", user_id="u1", session_id="s1")
    invocation = InvocationContext(
        session_service=session_service,
        invocation_id="timeout",
        agent=_ToolAgent(name="agent"),
        agent_context=new_agent_context(metadata={"tenant_id": "tenant_a"}),
        session=session,
        branch="",
    )
    tool = FunctionTool(slow_tool, filters=[ToolExecutionTimeoutFilter(0.005)])

    with pytest.raises(TimeoutError, match="tool execution exceeded"):
        await tool.run_async(tool_context=invocation, args={})


async def test_tool_metrics_filter_records_sync_and_stream_outcomes():
    metrics = EnterpriseMetrics(meter=False)
    filter_ = ToolMetricsFilter(metrics)
    ctx = new_agent_context(metadata={"tenant_id": "tenant_a"})

    async def success():
        return "ok"

    result = await filter_.run(ctx, {"tool_name": "query_order"}, success)
    assert result.rsp == "ok"

    async def stream_error():
        yield FilterResult(error=ValueError("failed"), is_continue=False)

    events = [event async for event in filter_.run_stream(ctx, {"tool_name": "query_order"}, stream_error)]
    assert isinstance(events[0].error, ValueError)

    snapshot = metrics.snapshot("tenant_a")
    calls = [item for item in snapshot["counters"] if item["name"] == "agent_tool_call_total"]
    assert {(item["attributes"]["outcome"], item["value"])
            for item in calls} == {
                ("success", 1),
                ("error", 1),
            }
    durations = [item for item in snapshot["histograms"] if item["name"] == "agent_tool_call_duration_ms"]
    assert sum(item["count"] for item in durations) == 2
