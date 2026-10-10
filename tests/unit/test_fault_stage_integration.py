from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import uuid4

import pytest

from tests.conftest import envelope, repository
from trpc_service.agent.worker import AgentWorker, ProcessStatus, WorkerResult
from trpc_service.faults import FaultStage, FaultStageEvent
from trpc_service.queue.redis_streams import QueueMessage
from trpc_service.queue.worker_consumer import WorkerConsumer
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import SessionLease, SessionSnapshot, StoredEvent, TurnCommit
from trpc_service.storage.postgres import PostgresRuntimeRepository
from trpc_service.storage.protocols import FencingConflict, RuntimeRepository
from trpc_service.tenant.models import ToolRisk
from trpc_service.tool.execution import (
    ExecutionRecord,
    ExecutionStatus,
    ToolExecutor,
)


class RecordingController:
    def __init__(self, order: list[str]) -> None:
        self.events: list[FaultStageEvent] = []
        self._order = order

    async def checkpoint(self, event: FaultStageEvent) -> bool:
        self._order.append("checkpoint")
        self.events.append(event)
        return False


class QueueDouble:
    def __init__(self, order: list[str]) -> None:
        self.acked: list[QueueMessage] = []
        self._order = order

    async def ack(self, message: QueueMessage) -> None:
        self._order.append("ack")
        self.acked.append(message)

    async def defer(self, _message: QueueMessage, *, consumer_id: str) -> bool:
        self._order.append("defer")
        return False

    async def heartbeat(self, _message: QueueMessage, *, consumer_id: str, stop_event) -> bool:
        await stop_event.wait()
        return True


class ConsumerRepository:
    def __init__(
        self, acceptance: Any, order: list[str], *, error: BaseException | None = None
    ) -> None:
        self.acceptance = acceptance
        self._order = order
        self._error = error

    async def get_acceptance(self, _tenant_id: str, _inbound_id: str) -> Any:
        self._order.append("acceptance")
        if self._error is not None:
            raise self._error
        return self.acceptance


class ConsumerWorker:
    def __init__(self, order: list[str], *, error: BaseException | None = None) -> None:
        self._order = order
        self._error = error

    async def process(self, _acceptance: Any) -> WorkerResult:
        self._order.append("process")
        if self._error is not None:
            raise self._error
        return WorkerResult(ProcessStatus.COMMITTED)


class LedgerDouble:
    def __init__(self, order: list[str]) -> None:
        self._order = order
        self.finished: list[ExecutionStatus] = []

    async def begin(
        self,
        execution_key: str,
        *,
        tenant_id: str,
        turn_id: str,
        tool_name: str,
        risk: ToolRisk,
        arguments_hash: str,
        owner_id: str | None = None,
        fencing_token: int | None = None,
    ) -> ExecutionRecord:
        del tenant_id, turn_id, tool_name, risk, arguments_hash, owner_id, fencing_token
        self._order.append("begin")
        return ExecutionRecord(execution_key, ExecutionStatus.STARTED, fresh=True)

    async def finish(
        self,
        _execution_key: str,
        *,
        tenant_id: str,
        status: ExecutionStatus,
        result: Any = None,
        owner_id: str | None = None,
        fencing_token: int | None = None,
    ) -> None:
        del tenant_id, result, owner_id, fencing_token
        self._order.append("finish")
        self.finished.append(status)


class ConnectionDouble:
    def __init__(self, rows: list[Any], order: list[str]) -> None:
        self._rows = rows
        self.calls: list[tuple[str, tuple[Any, ...]]] = []
        self._order = order

    def transaction(self) -> ConnectionDouble:
        return self

    async def __aenter__(self) -> ConnectionDouble:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def fetchrow(self, *args: Any) -> Any:
        self._order.append("fetchrow")
        self.calls.append(("fetchrow", args))
        return self._rows.pop(0) if self._rows else None

    async def fetchval(self, *args: Any) -> datetime:
        self.calls.append(("fetchval", args))
        return datetime.now(UTC)

    async def execute(self, *args: Any) -> str:
        self._order.append("execute")
        self.calls.append(("execute", args))
        return "UPDATE 1"


class PoolDouble:
    def __init__(self, connection: ConnectionDouble) -> None:
        self.connection = connection

    def acquire(self) -> ConnectionDouble:
        return self.connection


async def _make_acceptance() -> Any:
    value = repository()
    return await TenantRuntime(value, routing_key=b"f" * 32).accept(
        "binding-unpredictable-a", envelope("fault-stage")
    )


@pytest.mark.asyncio
async def test_enqueue_checkpoint_is_after_claim_entry_before_acceptance_and_ack() -> None:
    acceptance = await _make_acceptance()
    order: list[str] = []
    controller = RecordingController(order)
    message = QueueMessage(
        stream_id="42-0",
        outbox_id="outbox-1",
        tenant_id=acceptance.context.tenant_id,
        event_type="inbound.accepted",
        aggregate_id=acceptance.inbound_id,
        payload={},
        trace_headers={},
    )
    queue = QueueDouble(order)
    consumer = WorkerConsumer(
        cast(RuntimeRepository, ConsumerRepository(acceptance, order)),
        queue,  # type: ignore[arg-type]
        cast(AgentWorker, ConsumerWorker(order)),
        consumer_id="worker-a",
        fault_stages=controller,
    )

    await consumer.process_message(message)

    assert order == ["checkpoint", "acceptance", "process", "ack"]
    assert queue.acked == [message]
    assert controller.events
    event = controller.events[0]
    assert event.stage is FaultStage.ENQUEUE
    assert event.tenant_id == acceptance.context.tenant_id
    assert event.inbound_id == acceptance.inbound_id
    assert event.stream_id == "42-0"
    assert event.worker_id == "worker-a"


@pytest.mark.asyncio
async def test_enqueue_error_does_not_ack_and_unconfigured_path_has_no_checkpoint() -> None:
    acceptance = await _make_acceptance()
    order: list[str] = []
    message = QueueMessage(
        stream_id="42-1",
        outbox_id="outbox-2",
        tenant_id=acceptance.context.tenant_id,
        event_type="inbound.accepted",
        aggregate_id=acceptance.inbound_id,
        payload={},
        trace_headers={},
    )
    queue = QueueDouble(order)
    consumer = WorkerConsumer(
        cast(
            RuntimeRepository,
            ConsumerRepository(acceptance, order, error=RuntimeError("repository down")),
        ),
        queue,  # type: ignore[arg-type]
        cast(AgentWorker, ConsumerWorker(order)),
        consumer_id="worker-a",
    )

    with pytest.raises(RuntimeError, match="repository down"):
        await consumer.process_message(message)
    assert queue.acked == []
    assert order == ["acceptance"]


@pytest.mark.asyncio
async def test_enqueue_cancellation_does_not_ack() -> None:
    acceptance = await _make_acceptance()
    order: list[str] = []
    controller = RecordingController(order)
    message = QueueMessage(
        stream_id="42-2",
        outbox_id="outbox-3",
        tenant_id=acceptance.context.tenant_id,
        event_type="inbound.accepted",
        aggregate_id=acceptance.inbound_id,
        payload={},
        trace_headers={},
    )
    queue = QueueDouble(order)
    consumer = WorkerConsumer(
        cast(RuntimeRepository, ConsumerRepository(acceptance, order)),
        queue,  # type: ignore[arg-type]
        cast(AgentWorker, ConsumerWorker(order, error=asyncio.CancelledError())),
        consumer_id="worker-a",
        fault_stages=controller,
    )

    with pytest.raises(asyncio.CancelledError):
        await consumer.process_message(message)
    assert queue.acked == []
    assert order == ["checkpoint", "acceptance", "process"]


@pytest.mark.asyncio
async def test_tool_checkpoint_is_after_ledger_begin_before_call() -> None:
    accepted = await _make_acceptance()
    order: list[str] = []
    controller = RecordingController(order)
    ledger = LedgerDouble(order)
    executor = ToolExecutor(b"t" * 32, ledger, fault_stages=controller, worker_id="worker-b")

    async def call() -> str:
        order.append("call")
        return "ok"

    result = await executor.execute(
        accepted.context,
        turn_id="turn-1",
        tool_name="lookup",
        arguments={"q": "x"},
        risk=ToolRisk.IDEMPOTENT,
        call=call,
    )

    assert result == "ok"
    assert order == ["begin", "checkpoint", "call", "finish"]
    event = controller.events[0]
    assert event.stage is FaultStage.TOOL
    assert event.tenant_id == accepted.context.tenant_id
    assert event.turn_id == "turn-1"
    assert event.execution_key is not None
    assert event.worker_id == "worker-b"


@pytest.mark.asyncio
async def test_tool_error_finishes_ledger_without_retrying_call() -> None:
    accepted = await _make_acceptance()
    order: list[str] = []
    controller = RecordingController(order)
    ledger = LedgerDouble(order)
    executor = ToolExecutor(b"t" * 32, ledger, fault_stages=controller, worker_id="worker-b")
    calls = 0

    async def call() -> str:
        nonlocal calls
        calls += 1
        order.append("call")
        raise RuntimeError("tool failed")

    with pytest.raises(RuntimeError, match="tool failed"):
        await executor.execute(
            accepted.context,
            turn_id="turn-2",
            tool_name="lookup",
            arguments={},
            risk=ToolRisk.IDEMPOTENT,
            call=call,
        )
    assert calls == 1
    assert ledger.finished == [ExecutionStatus.FAILED]
    assert order == ["begin", "checkpoint", "call", "finish"]


@pytest.mark.asyncio
async def test_commit_checkpoint_is_after_fencing_and_before_first_business_write() -> None:
    accepted = await _make_acceptance()
    order: list[str] = []
    controller = RecordingController(order)
    now = datetime.now(UTC)
    lease = SessionLease(
        tenant_id=accepted.context.tenant_id,
        session_id=accepted.context.session_id,
        turn_id=str(uuid4()),
        inbound_id=accepted.inbound_id,
        worker_id="worker-c",
        fencing_token=3,
        expires_at=now + timedelta(minutes=1),
        snapshot=SessionSnapshot(
            tenant_id=accepted.context.tenant_id,
            app_id=accepted.context.app_id,
            session_id=accepted.context.session_id,
            principal_id=accepted.context.principal_id,
        ),
    )
    connection = ConnectionDouble(
        [
            {
                "lease_owner": lease.worker_id,
                "lease_epoch": lease.fencing_token,
                "lease_expires_at": lease.expires_at,
                "next_sequence": 1,
            },
            {"fencing_token": lease.fencing_token, "status": "processing"},
        ],
        order,
    )
    repository = PostgresRuntimeRepository(PoolDouble(connection), fault_stages=controller)
    result = await repository.commit(
        TurnCommit(
            context=accepted.context,
            lease=lease,
            state={"done": True},
            events=(StoredEvent(event_id="event-1", author="agent", timestamp=1, event={}),),
        )
    )

    assert result.first_sequence == 1
    assert order[:4] == ["execute", "fetchrow", "fetchrow", "checkpoint"]
    assert order[4] == "execute"
    assert not any("fault_stage" in str(args).lower() for _, args in connection.calls)
    event = controller.events[0]
    assert event.stage is FaultStage.COMMIT_TXN_OPEN
    assert event.tenant_id == lease.tenant_id
    assert event.inbound_id == lease.inbound_id
    assert event.turn_id == lease.turn_id
    assert event.worker_id == lease.worker_id
    assert event.fencing_token == lease.fencing_token


@pytest.mark.asyncio
async def test_commit_invalid_fencing_does_not_checkpoint_or_write_business_rows() -> None:
    accepted = await _make_acceptance()
    order: list[str] = []
    controller = RecordingController(order)
    now = datetime.now(UTC)
    lease = SessionLease(
        tenant_id=accepted.context.tenant_id,
        session_id=accepted.context.session_id,
        turn_id=str(uuid4()),
        inbound_id=accepted.inbound_id,
        worker_id="worker-c",
        fencing_token=3,
        expires_at=now + timedelta(minutes=1),
        snapshot=SessionSnapshot(
            tenant_id=accepted.context.tenant_id,
            app_id=accepted.context.app_id,
            session_id=accepted.context.session_id,
            principal_id=accepted.context.principal_id,
        ),
    )
    connection = ConnectionDouble([None, None], order)
    repository = PostgresRuntimeRepository(PoolDouble(connection), fault_stages=controller)

    with pytest.raises(FencingConflict):
        await repository.commit(
            TurnCommit(context=accepted.context, lease=lease, state={}, events=())
        )

    assert controller.events == []
    # v1 commit now takes the optional mailbox lock before session/turn so it
    # cannot deadlock with a v2 terminal transition during scheduler cutover.
    assert order == ["execute", "fetchrow", "fetchrow", "fetchrow"]
