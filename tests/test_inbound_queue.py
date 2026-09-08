import asyncio

import pytest
from sqlalchemy import select

from trpc_service.gateway import AgentMessageService, AgentReply, GatewayResponse, NormalizedMessage
from trpc_service.gateway.queue import (
    ExecutionLedger,
    ExecutionUncertainError,
    InboundWorker,
    SqlInboundQueue,
)
from trpc_service.storage import Database
from trpc_service.storage.coordinator import TurnCoordinator
from trpc_service.storage.inmemory import (
    InMemoryConversationStore,
    InMemoryCoordinationStore,
)
from trpc_service.storage.models import AgentExecution, InboundMessage, OutboxMessage, Tenant


def message(external_id: str = "update-1") -> NormalizedMessage:
    return NormalizedMessage(
        "tenant-1",
        "app-1",
        "telegram",
        "bot-1",
        external_id,
        "user-1",
        "chat-1",
        "direct",
        "hello",
        "trace-1",
    )


def database() -> Database:
    db = Database("sqlite+pysqlite:///:memory:")
    db.create_schema()
    with db.session_factory.begin() as session:
        session.add(
            Tenant(
                id="tenant-1",
                slug="inbound",
                name="Inbound",
                key_namespace="tenant/inbound",
            )
        )
    return db


class FakeRouter:
    async def dispatch(self, request: NormalizedMessage) -> GatewayResponse:
        return GatewayResponse(
            "processed", "node-1", request.session_id, request.trace_id, "reply", 1
        )


def test_inbound_enqueue_dedupe_worker_and_delivery_outbox() -> None:
    db = database()

    async def scenario() -> None:
        queue = SqlInboundQueue(db.session_factory)
        first = await queue.enqueue(message())
        duplicate = await queue.enqueue(message())
        assert first.created
        assert not duplicate.created
        assert duplicate.record.id == first.record.id

        worker = InboundWorker(queue, FakeRouter(), "worker-1")
        assert await worker.poll_once() == 1
        assert await worker.poll_once() == 0

    try:
        asyncio.run(scenario())
        with db.session_factory() as session:
            inbound = session.scalar(select(InboundMessage))
            execution = session.scalar(select(AgentExecution))
            outbox = session.scalar(select(OutboxMessage))
            assert inbound is not None and inbound.status == "completed"
            assert execution is not None and execution.status == "delivery_enqueued"
            assert outbox is not None and outbox.topic == "im.reply.telegram"
    finally:
        db.dispose()


class CountingExecutor:
    def __init__(self, fail: bool = False) -> None:
        self.calls = 0
        self.fail = fail

    async def execute(self, _message: NormalizedMessage) -> AgentReply:
        self.calls += 1
        if self.fail:
            raise TimeoutError("model timeout")
        return AgentReply("cached reply")


class FailFirstConversationStore(InMemoryConversationStore):
    def __init__(self) -> None:
        super().__init__()
        self.fail_first = True

    async def commit_turn(self, commit):
        if self.fail_first:
            self.fail_first = False
            raise OSError("database temporarily unavailable")
        return await super().commit_turn(commit)


def test_runner_result_is_reused_after_platform_commit_failure() -> None:
    db = database()

    async def scenario() -> None:
        executor = CountingExecutor()
        store = FailFirstConversationStore()
        service = AgentMessageService(
            executor,
            TurnCoordinator(store, InMemoryCoordinationStore()),
            execution_ledger=ExecutionLedger(db.session_factory),
        )
        with pytest.raises(OSError, match="temporarily unavailable"):
            await service.handle(message("recoverable"), "node-1")
        result = await service.handle(message("recoverable"), "node-1")
        assert result.reply_text == "cached reply"
        assert executor.calls == 1

    try:
        asyncio.run(scenario())
    finally:
        db.dispose()


def test_uncertain_runner_attempt_is_not_automatically_replayed() -> None:
    db = database()

    async def scenario() -> None:
        executor = CountingExecutor(fail=True)
        service = AgentMessageService(
            executor,
            TurnCoordinator(InMemoryConversationStore(), InMemoryCoordinationStore()),
            execution_ledger=ExecutionLedger(db.session_factory),
        )
        with pytest.raises(ExecutionUncertainError):
            await service.handle(message("uncertain"), "node-1")
        with pytest.raises(ExecutionUncertainError, match="manual recovery"):
            await service.handle(message("uncertain"), "node-1")
        assert executor.calls == 1

    try:
        asyncio.run(scenario())
    finally:
        db.dispose()
