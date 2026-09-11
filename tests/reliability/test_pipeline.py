"""Durable callback-to-runner-to-outbox vertical slice."""

from datetime import datetime
from datetime import timezone

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part

from trpc_service.agent import RunnerRegistry
from trpc_service.agent import tenant_app_name
from trpc_service.channels import ChannelType
from trpc_service.channels import ConversationType
from trpc_service.channels import NormalizedInboundMessage
from trpc_service.persistence import Database
from trpc_service.reliability import DurableExecutionPipeline
from trpc_service.reliability import InboxDisposition
from trpc_service.reliability import InboxRepository
from trpc_service.reliability import OutboxRepository
from trpc_service.reliability import PostTurnRepository
from trpc_service.reliability import SessionCoordinator
from trpc_service.storage import ConflictError
from trpc_service.tenant import ResolvedRoute


class FakeRunner:
    def __init__(self):
        self.app_name = tenant_app_name("tenant_acme", "support_app")
        self.calls = 0
        self.last_context = None

    async def run_async(self, **kwargs):
        self.calls += 1
        self.last_context = kwargs["agent_context"]
        yield Event(id="reply", author="support", content=Content(parts=[Part.from_text(text="safe reply")]))


class PartialFailureRunner(FakeRunner):
    async def run_async(self, **kwargs):
        self.calls += 1
        yield Event(
            id="partial",
            author="support",
            partial=True,
            content=Content(parts=[Part.from_text(text="partial reply")]),
        )
        raise TimeoutError("model timed out after a partial response")


@pytest.mark.asyncio
async def test_duplicate_callback_runs_once_and_creates_durable_outputs(tmp_path) -> None:
    database = Database(f"sqlite+pysqlite:///{tmp_path / 'pipeline.db'}")
    database.create_schema()
    inbox = InboxRepository(database)
    outbox = OutboxRepository(database)
    tasks = PostTurnRepository(database)
    pipeline = DurableExecutionPipeline(inbox, outbox, tasks, SessionCoordinator(database))
    route = ResolvedRoute(
        tenant_id="tenant_acme",
        config_version=3,
        storage_revision=4,
        agent_app_id="support_app",
        channel_binding_id="binding_primary",
        actor_id="actor_1",
        internal_user_id="usr_1",
        session_id="ses_1",
        partition_key="tenant_acme:ses_1",
    )
    message = NormalizedInboundMessage(
        channel=ChannelType.WECOM,
        webhook_public_id="callback_acme",
        external_message_id="external-1",
        external_user_id="user-1",
        conversation_type=ConversationType.DIRECT,
        text="hello",
        received_at=datetime.now(timezone.utc),
        request_id="request-1",
    )
    first = pipeline.ingest(message, route, trace_id="trace-1")
    duplicate = pipeline.ingest(message, route, trace_id="trace-2")
    assert first.disposition is InboxDisposition.ACCEPTED
    assert duplicate.disposition is InboxDisposition.DUPLICATE

    registry = RunnerRegistry()
    runner = FakeRunner()
    registry.register("tenant_acme", "support_app", 3, runner)
    work = inbox.claim(worker_id="worker-a", lease_seconds=30)
    outputs = await pipeline.execute(work, registry, worker_id="worker-a")
    assert runner.calls == 1
    assert runner.last_context.metadata["trace_id"] == "trace-1"
    assert runner.last_context.metadata["execution_id"] == first.execution_id
    assert runner.last_context.metadata["fencing_token"] >= 1
    assert outputs[-1].text == "safe reply"
    assert inbox.claim(worker_id="worker-b", lease_seconds=30) is None
    assert len(outbox.claim(limit=10, worker_id="dispatcher", lease_seconds=30)) == 1
    assert len(tasks.claim(limit=10, worker_id="post", lease_seconds=30)) == 2
    database.close()


@pytest.mark.asyncio
async def test_partial_execution_is_not_blindly_replayed_after_timeout(tmp_path) -> None:
    database = Database(f"sqlite+pysqlite:///{tmp_path / 'partial.db'}")
    database.create_schema()
    inbox = InboxRepository(database)
    pipeline = DurableExecutionPipeline(
        inbox,
        OutboxRepository(database),
        PostTurnRepository(database),
        SessionCoordinator(database),
    )
    route = ResolvedRoute(
        tenant_id="tenant_acme",
        config_version=3,
        storage_revision=4,
        agent_app_id="support_app",
        channel_binding_id="binding_primary",
        actor_id="actor_1",
        internal_user_id="usr_1",
        session_id="ses_partial",
        partition_key="tenant_acme:ses_partial",
    )
    message = NormalizedInboundMessage(
        channel=ChannelType.WECOM,
        webhook_public_id="callback_acme",
        external_message_id="external-partial",
        external_user_id="user-1",
        conversation_type=ConversationType.DIRECT,
        text="hello",
        received_at=datetime.now(timezone.utc),
        request_id="request-partial",
    )
    pipeline.ingest(message, route, trace_id="trace-partial")
    registry = RunnerRegistry()
    runner = PartialFailureRunner()
    registry.register("tenant_acme", "support_app", 3, runner)

    first = inbox.claim(worker_id="worker-a", lease_seconds=30)
    with pytest.raises(TimeoutError):
        await pipeline.execute(first, registry, worker_id="worker-a")
    inbox.retry(first.inbound_message_id, "worker-a", "model_timeout", delay_seconds=-1)

    recovered = inbox.claim(worker_id="worker-b", lease_seconds=30)
    with pytest.raises(ConflictError, match="partial prior execution"):
        await pipeline.execute(recovered, registry, worker_id="worker-b")
    assert runner.calls == 1
    database.close()
