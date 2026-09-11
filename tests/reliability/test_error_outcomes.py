"""Errors remain failures across normal execution and both durable recovery paths."""

from datetime import datetime, timezone

import pytest
from sqlalchemy import select
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, Part

from tests.reliability.test_pipeline import FakeRunner
from trpc_service.agent import AgentChannelEvent, AgentChannelEventType, RunnerRegistry
from trpc_service.channels import ChannelType, ConversationType, NormalizedInboundMessage
from trpc_service.persistence import Database
from trpc_service.persistence.models import ExecutionAttemptRow, InboundMessageRow
from trpc_service.reliability import (
    DurableExecutionPipeline, InboxRepository, OutboxRepository, PostTurnRepository, SessionCoordinator,
)
from trpc_service.tenant import ResolvedRoute


class ErrorRunner(FakeRunner):
    def __init__(self, tool_called):
        super().__init__()
        self.tool_called = tool_called

    async def run_async(self, **kwargs):
        self.calls += 1
        if self.tool_called:
            yield Event(id="tool", author="agent", content=Content(parts=[
                Part.from_function_call(name="write_order", args={}),
            ]))
        yield Event(id="failure", author="agent", error_code="timeout", error_message="private detail")


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["normal", "outbox", "committed"])
@pytest.mark.parametrize("tool_called", [False, True])
async def test_error_outcome_survives_recovery(tmp_path, path, tool_called):
    database = Database(f"sqlite+pysqlite:///{tmp_path / 'errors.db'}")
    database.create_schema()
    inbox, outbox = InboxRepository(database), OutboxRepository(database)
    tasks, coordinator = PostTurnRepository(database), SessionCoordinator(database)
    pipeline = DurableExecutionPipeline(inbox, outbox, tasks, coordinator)
    route = ResolvedRoute(tenant_id="tenant_acme", config_version=3, storage_revision=4,
                          agent_app_id="support_app", channel_binding_id="binding_primary",
                          actor_id="actor_1", internal_user_id="usr_1", session_id="ses_1",
                          partition_key="tenant_acme:ses_1")
    message = NormalizedInboundMessage(
        channel=ChannelType.WECOM, webhook_public_id="callback_acme", external_message_id="error-1",
        external_user_id="user-1", conversation_type=ConversationType.DIRECT, text="hello",
        received_at=datetime.now(timezone.utc), request_id="request-1",
    )
    receipt = pipeline.ingest(message, route, trace_id="trace-1")
    registry, runner = RunnerRegistry(), ErrorRunner(tool_called)
    registry.register("tenant_acme", "support_app", 3, runner)
    work = inbox.claim(worker_id="worker", lease_seconds=30)
    try:
        if path != "normal":
            # A previous worker persisted its final error, then died before status commit.
            lease = await coordinator.acquire("tenant_acme", "support_app", "usr_1", "ses_1", "old", 30,
                                              execution_id=receipt.execution_id)
            coordinator.start_attempt(receipt.execution_id, "old", 3, 4)
            events = []
            if tool_called:
                events.append(AgentChannelEvent(type=AgentChannelEventType.TOOL_CALL, event_id="tool",
                                                tool_name="write_order"))
            events.append(AgentChannelEvent(type=AgentChannelEventType.ERROR, event_id="failure", final=True,
                                            error_type="agent_error", text="Agent execution failed"))
            revision = 0
            for index, event in enumerate(events):
                payload = event.model_dump(mode="json")
                revision = await coordinator.commit(lease, expected_revision=revision, event_id=event.event_id,
                                                    event_payload=payload, event_type="channel_event", author="agent")
                if path == "outbox":
                    outbox.enqueue("tenant_acme", receipt.inbound_message_id, index, payload)
            await coordinator.release(lease)
        outputs = await pipeline.execute(work, registry, worker_id="worker")
        expected = "unknown_outcome" if tool_called else "failed_final"
        assert outputs[-1].type is AgentChannelEventType.ERROR
        assert runner.calls == (1 if path == "normal" else 0)
        assert "private detail" not in repr(outputs)
        with database.sessions() as session:
            row = session.get(InboundMessageRow, receipt.inbound_message_id)
            assert (row.status, row.error_type, row.lease_owner) == (expected, "agent_error", None)
            attempts = list(session.scalars(select(ExecutionAttemptRow)))
            assert len(attempts) == 1
            assert attempts[0].status == expected
        assert inbox.claim(worker_id="no-replay", lease_seconds=30) is None
        assert not tasks.claim(limit=10, worker_id="post", lease_seconds=30)
        assert len(outbox.list_for_inbound("tenant_acme", receipt.inbound_message_id)) == len(outputs)
    finally:
        database.close()
