from dataclasses import replace

import pytest

from tests.test_trpc_agent_runner import _context
from trpc_service.agent.contracts import AgentToolCall, AgentToolKind, AgentToolResult
from trpc_service.agent.ledger import InMemoryToolLedger, ToolLedgerConflict, ToolLedgerStatus


def _call(arguments=None) -> AgentToolCall:
    return AgentToolCall(
        call_id="request-1:0:ticket.create",
        name="ticket.create",
        kind=AgentToolKind.TOOL,
        logical_call_index=0,
        arguments=arguments or {"title": "incident"},
    )


@pytest.mark.anyio
async def test_tool_ledger_replays_completed_result_without_reexecution() -> None:
    ledger = InMemoryToolLedger()
    context = _context()
    call = _call()

    prepared = await ledger.prepare(context, call)
    await ledger.complete(context, call, AgentToolResult(call.call_id, content="T-1"))
    replay = await ledger.prepare(context, call)

    assert prepared.should_execute
    assert replay.status is ToolLedgerStatus.SUCCEEDED
    assert replay.result == AgentToolResult(call.call_id, content="T-1")
    assert not replay.should_execute


@pytest.mark.anyio
async def test_tool_ledger_rejects_call_id_reuse_with_changed_arguments() -> None:
    ledger = InMemoryToolLedger()
    context = _context()
    await ledger.prepare(context, _call())

    with pytest.raises(ToolLedgerConflict):
        await ledger.prepare(context, _call({"title": "different"}))


@pytest.mark.anyio
async def test_unknown_tool_outcome_stays_quarantined() -> None:
    ledger = InMemoryToolLedger()
    context = replace(_context(), attributes={"node_id": "worker-1"})
    call = _call()
    await ledger.prepare(context, call)

    await ledger.mark_unknown(context, call, "provider acknowledgement timed out")
    replay = await ledger.prepare(context, call)

    assert replay.status is ToolLedgerStatus.UNKNOWN
    assert not replay.should_execute
