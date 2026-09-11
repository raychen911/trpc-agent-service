"""Public FunctionTool execution persists results and refuses ambiguous replays."""

import asyncio
from types import SimpleNamespace

import pytest
from trpc_agent_sdk.context import AgentContext

from trpc_service.persistence import Database
from trpc_service.reliability import InboxRepository
from trpc_service.reliability.tool_execution import DurableTools, ToolOutcomeUnknown
from trpc_service.storage import ConflictError


@pytest.fixture
def tool_execution(tmp_path):
    database = Database(f"sqlite:///{tmp_path / 'tools.db'}")
    database.create_schema()
    receipt = InboxRepository(database).accept(
        tenant_id="tenant_acme", channel_binding_id="binding_primary",
        external_message_id="tool-input", payload={}, request_id="request", trace_id="trace", partition_key="p",
        config_version=1, storage_revision=1)
    yield database, receipt.execution_id
    database.close()


@pytest.mark.asyncio
async def test_official_function_tool_replays_saved_result_and_checks_scope(tool_execution):
    database, execution = tool_execution
    calls = []

    async def reserve_stock(sku: str, tool_context):
        """Reserve a stock item."""
        calls.append(sku)
        return {"reservation": "receipt-42"}

    context = SimpleNamespace(function_call_id="call-1", agent_context=AgentContext(), agent=SimpleNamespace())
    context.agent_context.with_metadata("tenant_id", "tenant_acme")
    context.agent_context.with_metadata("execution_id", execution)
    first = DurableTools(database).wrap(reserve_stock, tenant_id="tenant_acme")
    assert await first.run_async(args={"sku": "item-1"}, tool_context=context) == {"reservation": "receipt-42"}
    # Reconstruct the wrapper, as a restarted worker would.
    second = DurableTools(database).wrap(reserve_stock, tenant_id="tenant_acme")
    assert await second.run_async(args={"sku": "item-1"}, tool_context=context) == {"reservation": "receipt-42"}
    assert calls == ["item-1"]
    with pytest.raises(ConflictError):
        await second.run_async(args={"sku": "changed"}, tool_context=context)
    context.agent_context.with_metadata("tenant_id", "tenant_other")
    with pytest.raises(PermissionError):
        await second.run_async(args={"sku": "item-1"}, tool_context=context)


@pytest.mark.asyncio
async def test_concurrent_claim_budget_and_unknown_reconciliation(tool_execution):
    database, execution = tool_execution
    tools = DurableTools(database)
    entered, release = asyncio.Event(), asyncio.Event()
    calls = 0

    async def external(key):
        nonlocal calls
        calls += 1
        entered.set()
        await release.wait()
        raise TimeoutError("private remote response")

    first = asyncio.create_task(tools.invoke("tenant_acme", execution, "one", "reserve", {}, external, max_calls=1))
    await entered.wait()
    with pytest.raises(ToolOutcomeUnknown):
        await tools.invoke("tenant_acme", execution, "one", "reserve", {}, external)
    with pytest.raises(PermissionError):
        await tools.invoke("tenant_acme", execution, "two", "reserve", {}, external, max_calls=1)
    release.set()
    with pytest.raises(TimeoutError):
        await first
    with pytest.raises(ToolOutcomeUnknown):
        await tools.invoke("tenant_acme", execution, "one", "reserve", {}, external)

    async def reconcile(key, arguments):
        assert key == execution + ":one"
        return True, {"confirmed": True}

    assert await tools.invoke("tenant_acme", execution, "one", "reserve", {}, external,
                              recover=reconcile) == {"confirmed": True}
    assert calls == 1
