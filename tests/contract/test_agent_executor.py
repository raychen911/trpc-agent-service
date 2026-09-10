from __future__ import annotations

import asyncio
import pytest

from tests.support import inbound_message_data
from trpc_service.channels.contracts import InboundMessage
from trpc_service.config.settings import build_demo_settings
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.session_backend import SessionBackendFactory
from trpc_service.tenant.session_identity import derive_session_identity
from trpc_service.storage.contracts import AgentExecutionFailed, OutcomeUnknown
from trpc_service.worker.service import AgentExecutor, PreparedAgentRun


async def test_agent_executor_uses_official_runner_and_selects_one_final_event() -> None:
    settings = build_demo_settings()
    adapters = InMemoryPlatformAdapters(settings)
    context = adapters.context_for_test("binding-alpha", "user-001", inbound_message_data()["trace_id"])
    identity = derive_session_identity(context, "direct", "conversation-001")
    executor = AgentExecutor(SessionBackendFactory())

    prepared = await executor.prepare(context, identity, "Remember validation token ALPHA.")
    assert prepared.execution_started is False
    result = await prepared.execute(timeout_seconds=30)

    assert result.final_text == "stored:ALPHA"
    assert result.event_count >= 1
    assert result.final_response_count == 1
    assert executor.external_model_calls == 0
    await executor.close()


def _identity():
    settings = build_demo_settings()
    adapters = InMemoryPlatformAdapters(settings)
    context = adapters.context_for_test("binding-alpha", "user-001", inbound_message_data()["trace_id"])
    return derive_session_identity(context, "direct", "conversation-001")


async def test_prepared_run_rejects_missing_final_event_and_maps_vendor_error() -> None:
    class Event:
        visible = True
        def is_final_response(self): return False
        def get_text(self): return "intermediate"

    class NoFinalRunner:
        async def run_async(self, **_kwargs):
            yield Event()

    class FailedRunner:
        async def run_async(self, **_kwargs):
            raise RuntimeError("private vendor detail")
            yield Event()

    with pytest.raises(AgentExecutionFailed, match="Agent execution failed"):
        await PreparedAgentRun(NoFinalRunner(), _identity(), "hello").execute()
    with pytest.raises(AgentExecutionFailed, match="Agent execution failed"):
        await PreparedAgentRun(FailedRunner(), _identity(), "hello").execute()


async def test_prepared_run_timeout_and_cancellation_are_outcome_unknown() -> None:
    entered = asyncio.Event()

    class SlowRunner:
        async def run_async(self, **_kwargs):
            entered.set()
            await asyncio.Event().wait()
            if False:
                yield None

    with pytest.raises(OutcomeUnknown):
        await PreparedAgentRun(SlowRunner(), _identity(), "hello").execute(timeout_seconds=0.001)

    entered.clear()
    task = asyncio.create_task(PreparedAgentRun(SlowRunner(), _identity(), "hello").execute())
    await entered.wait()
    task.cancel()
    with pytest.raises(OutcomeUnknown):
        await task
