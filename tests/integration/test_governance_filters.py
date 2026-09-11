"""Exercise the public SDK filter runners rather than calling lifecycle methods directly."""

from unittest.mock import AsyncMock

import pytest
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.filter import run_filters, run_stream_filters
from trpc_service.governance.filters import TenantBoundaryFilter, ToolGovernanceFilter
from trpc_service.tenant import ToolPolicy


def context(tenant="tenant_acme"):
    result = AgentContext()
    result.with_metadata("tenant_id", tenant)
    result.with_metadata("agent_app_id", "support")
    return result


@pytest.mark.asyncio
async def test_agent_filter_rejects_tenant_before_model_handler():
    called = []

    async def model():
        called.append(True)
        yield "response"

    policy = TenantBoundaryFilter("tenant_acme", "support")
    with pytest.raises(PermissionError):
        _ = [value async for value in run_stream_filters(context("tenant_other"), None, [policy], model)]
    assert called == []
    assert [value async for value in run_stream_filters(context(), None, [policy], model)] == ["response"]


@pytest.mark.asyncio
async def test_tool_arguments_cannot_supply_permission_or_confirmation():
    policy = ToolPolicy(allow={"read", "delete"}, require_confirmation={"delete"})
    operation = AsyncMock(return_value={"result": "done"})
    for name in ("unregistered", "delete"):
        guard = ToolGovernanceFilter("tenant_acme", "support", name, policy)
        with pytest.raises(PermissionError):
            await run_filters(context(), {"tool_name": "read", "confirmed": True}, [guard], operation)
    operation.assert_not_awaited()
    guard = ToolGovernanceFilter("tenant_acme", "support", "read", policy)
    assert await run_filters(context(), {}, [guard], operation) == {"result": "done"}


@pytest.mark.asyncio
async def test_filter_rechecks_revoked_authorization_each_invocation():
    authorize = AsyncMock()
    guard = TenantBoundaryFilter("tenant_acme", "support", authorize=authorize)
    operation = AsyncMock(return_value="done")
    assert await run_filters(context(), None, [guard], operation) == "done"
    authorize.side_effect = PermissionError("membership revoked")
    with pytest.raises(PermissionError):
        await run_filters(context(), None, [guard], operation)
    assert operation.await_count == 1
