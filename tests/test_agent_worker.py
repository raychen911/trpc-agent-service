import asyncio
from dataclasses import replace
from datetime import datetime
from pathlib import Path

import anyio
import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from tests.test_agent_task_queue import _request
from trpc_service.agent.contracts import (
    AgentExecutionReceipt,
    AgentTaskClaim,
    AgentTaskStatus,
    PolicyAction,
    PolicyDecision,
)
from trpc_service.agent.pipeline import AgentExecutionRejected
from trpc_service.agent.queue import PostgreSQLAgentTaskQueue
from trpc_service.agent.recovery_state import RunnerAttemptSnapshot, RunnerAttemptStatus
from trpc_service.agent.worker import AgentWorkerService
from trpc_service.config import LeasedWorkerConfig
from trpc_service.storage import SessionSnapshot
from trpc_service.storage.orm import Base
from trpc_service.storage.runtime_orm import InboxMessageRow, OutboxMessageRow


class StubPipeline:

    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.calls = 0

    async def execute(self, request):  # type: ignore[no-untyped-def]
        self.calls += 1
        if self.error is not None:
            raise self.error
        return AgentExecutionReceipt(session=SessionSnapshot(session_id=request.session_id,
                                                             version=1), )


class BlockingPipeline:
    """Represent a long model call and record task-level cancellation."""

    def __init__(self) -> None:
        self.cancelled = asyncio.Event()

    async def execute(self, request):  # type: ignore[no-untyped-def]
        del request
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()
        raise AssertionError("unreachable")


class LeaseLosingQueue:
    """Return one task, then deny its first Worker lease renewal."""

    def __init__(self) -> None:
        self.claimed = False
        self.completed = False
        self.failed = False

    async def claim(self, worker_id: str, *, lease_until: datetime):  # type: ignore[no-untyped-def]
        del worker_id, lease_until
        if self.claimed:
            return None
        self.claimed = True
        return AgentTaskClaim(
            task_id="task-1",
            request=_request(),
            status=AgentTaskStatus.RUNNING,
            attempt_count=1,
        )

    async def renew(
        self,
        task_id: str,
        *,
        worker_id: str,
        fencing_token: int,
        lease_until: datetime,
    ) -> bool:
        del task_id, worker_id, fencing_token, lease_until
        return False

    async def complete(self, task_id: str, *, worker_id: str, fencing_token: int) -> None:
        del task_id, worker_id, fencing_token
        self.completed = True

    async def fail(self, task_id: str, **kwargs):  # type: ignore[no-untyped-def]
        del task_id, kwargs
        self.failed = True


@pytest.mark.anyio
async def test_worker_nodes_retry_and_complete_shared_tasks(tmp_path: Path) -> None:
    """A second stateless Worker can finish work released by a failed node."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'worker.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    queue = PostgreSQLAgentTaskQueue(async_sessionmaker(engine, expire_on_commit=False))
    request = _request()
    await queue.enqueue(request)

    failing = StubPipeline(RuntimeError("provider unavailable"))
    worker_a = AgentWorkerService(
        queue=queue,
        pipeline=failing,
        node_id="worker-node-a",
        concurrency=1,
        runtime=LeasedWorkerConfig(
            lease_seconds=30,
            poll_interval_seconds=0.01,
            retry_base_seconds=0,
            retry_max_seconds=60,
            retry_jitter_ratio=0.2,
            max_attempts=3,
        ),
    )
    assert await worker_a.run_once(0)
    retry = await queue.get(
        request.tenant,
        request.channel.binding_id,
        request.incoming.external_message_id,
    )
    assert retry is not None
    assert retry.status is AgentTaskStatus.RETRYABLE_FAILED

    succeeding = StubPipeline()
    worker_b = AgentWorkerService(
        queue=queue,
        pipeline=succeeding,
        node_id="worker-node-b",
        concurrency=1,
        runtime=LeasedWorkerConfig(
            lease_seconds=30,
            poll_interval_seconds=0.01,
            retry_base_seconds=0,
            retry_max_seconds=60,
            retry_jitter_ratio=0.2,
            max_attempts=3,
        ),
    )
    assert await worker_b.run_once(0)
    completed = await queue.get(
        request.tenant,
        request.channel.binding_id,
        request.incoming.external_message_id,
    )

    assert completed is not None
    assert completed.status is AgentTaskStatus.SUCCEEDED
    assert completed.attempt_count == 2
    assert failing.calls == 1
    assert succeeding.calls == 1

    await worker_b.start()
    await worker_b.start()
    await anyio.sleep(0.02)
    await worker_b.close()
    await worker_b.close()

    await engine.dispose()


@pytest.mark.anyio
async def test_worker_cancels_pipeline_after_task_lease_loss() -> None:
    """The queue fence also stops a stale Worker before terminal mutation."""

    queue = LeaseLosingQueue()
    pipeline = BlockingPipeline()
    worker = AgentWorkerService(
        queue=queue,  # type: ignore[arg-type]
        pipeline=pipeline,
        node_id="worker-node-a",
        concurrency=1,
        runtime=LeasedWorkerConfig(
            lease_seconds=3,
            poll_interval_seconds=0.01,
            retry_base_seconds=0,
            retry_max_seconds=60,
            retry_jitter_ratio=0.2,
            max_attempts=3,
        ),
    )

    assert await worker.run_once(0)
    assert pipeline.cancelled.is_set()
    assert not queue.completed
    assert not queue.failed


@pytest.mark.anyio
async def test_worker_does_not_retry_permanent_configuration_failure() -> None:
    """Invalid immutable configuration is terminal rather than retried by every node."""

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    queue = PostgreSQLAgentTaskQueue(async_sessionmaker(engine, expire_on_commit=False))
    request = _request()
    await queue.enqueue(request)
    worker = AgentWorkerService(
        queue=queue,
        pipeline=StubPipeline(ValueError("invalid model configuration")),
        node_id="worker-node-a",
        concurrency=1,
        runtime=LeasedWorkerConfig(
            lease_seconds=30,
            poll_interval_seconds=0.01,
            retry_base_seconds=0,
            retry_max_seconds=60,
            retry_jitter_ratio=0.2,
            max_attempts=5,
        ),
    )

    assert await worker.run_once(0)
    snapshot = await queue.get(
        request.tenant,
        request.channel.binding_id,
        request.incoming.external_message_id,
    )

    assert snapshot is not None
    assert snapshot.status is AgentTaskStatus.PERMANENT_FAILED
    assert snapshot.safe_error == "Agent task cannot be executed"
    assert not await worker.run_once(0)

    # A visible IM progress message must always be closed by a durable terminal
    # reply, even when the Agent cannot produce its normal result.
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as database:
        failure_reply = await database.scalar(
            select(OutboxMessageRow).where(
                OutboxMessageRow.request_id == request.tenant.request_id,
                OutboxMessageRow.category == "IM_REPLY",
            ))
        inbox = await database.scalar(
            select(InboxMessageRow).where(InboxMessageRow.request_id == request.tenant.request_id, )
        )
    assert failure_reply is not None
    assert failure_reply.status == "PENDING"
    assert failure_reply.payload["text"] == "请求处理失败，请检查 Agent 配置或联系管理员后重试。"
    # The outer task owns the final retry budget and must reconcile a nested
    # execution row that may still be marked retryable.
    if inbox is not None:
        assert inbox.status == "PERMANENT_FAILED"
    await engine.dispose()


class RecordingQueue:
    """Expose one queue claim and record its terminal Worker transition."""

    def __init__(self, *, attempt_count: int = 1) -> None:
        self.pending_claim: AgentTaskClaim | None = AgentTaskClaim(
            task_id="task-recovery",
            request=_request(),
            status=AgentTaskStatus.RUNNING,
            attempt_count=attempt_count,
            fencing_token=3,
        )
        self.completed = False
        self.failure: dict[str, object] | None = None

    async def claim(self, worker_id: str, *, lease_until: datetime):  # type: ignore[no-untyped-def]
        del worker_id, lease_until
        claim, self.pending_claim = self.pending_claim, None
        return claim

    async def renew(self, task_id: str, **kwargs):  # type: ignore[no-untyped-def]
        del task_id, kwargs
        return True

    async def complete(self, task_id: str, **kwargs):  # type: ignore[no-untyped-def]
        del task_id, kwargs
        self.completed = True

    async def fail(self, task_id: str, **kwargs):  # type: ignore[no-untyped-def]
        del task_id
        self.failure = kwargs


class RecordingRecoveryStore:
    """Record the recovery state machine calls made by the Worker boundary."""

    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    async def start(self, claim: AgentTaskClaim, *, node_id: str) -> RunnerAttemptSnapshot:
        self.events.append(("start", node_id))
        return RunnerAttemptSnapshot(
            attempt_id="attempt-1",
            task_id=claim.task_id,
            request_id=claim.request.tenant.request_id,
            attempt_no=claim.attempt_count,
            fencing_token=claim.fencing_token,
            node_id=node_id,
            status=RunnerAttemptStatus.RUNNING,
        )

    async def checkpoint(
        self,
        attempt: RunnerAttemptSnapshot,
        *,
        stage: str,
        state_ref: str | None,
        occurred_at: datetime | None = None,
    ) -> object:
        del attempt, state_ref, occurred_at
        self.events.append(("checkpoint", stage))
        return object()

    async def complete(self, attempt: RunnerAttemptSnapshot) -> RunnerAttemptSnapshot:
        self.events.append(("complete", attempt.attempt_id))
        return replace(attempt, status=RunnerAttemptStatus.SUCCEEDED)

    async def fail(
        self,
        attempt: RunnerAttemptSnapshot,
        summary: str,
        *,
        retryable: bool,
    ) -> RunnerAttemptSnapshot:
        self.events.append(("fail", retryable))
        return replace(
            attempt,
            status=(RunnerAttemptStatus.RETRYABLE_FAILED
                    if retryable else RunnerAttemptStatus.PERMANENT_FAILED),
            error_summary=summary,
        )

    async def mark_unknown(
        self,
        attempt: RunnerAttemptSnapshot,
        summary: str,
    ) -> RunnerAttemptSnapshot:
        self.events.append(("unknown", summary))
        return replace(attempt, status=RunnerAttemptStatus.UNKNOWN, error_summary=summary)


def _worker(
    queue: RecordingQueue,
    pipeline: StubPipeline,
    recovery: RecordingRecoveryStore,
) -> AgentWorkerService:
    return AgentWorkerService(
        queue=queue,  # type: ignore[arg-type]
        pipeline=pipeline,
        node_id="worker-recovery",
        concurrency=1,
        runtime=LeasedWorkerConfig(
            lease_seconds=30,
            poll_interval_seconds=0.01,
            retry_base_seconds=0,
            retry_max_seconds=60,
            retry_jitter_ratio=0,
            max_attempts=3,
        ),
        recovery_store=recovery,  # type: ignore[arg-type]
    )


@pytest.mark.anyio
async def test_worker_records_success_rejection_and_failure_recovery_states() -> None:
    """Every terminal pipeline outcome advances the durable Runner state machine."""

    success_queue = RecordingQueue()
    success_recovery = RecordingRecoveryStore()
    assert await _worker(success_queue, StubPipeline(), success_recovery).run_once(0)
    assert success_queue.completed
    assert success_recovery.events == [
        ("start", "worker-recovery:0"),
        ("checkpoint", "PIPELINE_STARTED"),
        ("checkpoint", "RESULT_COMMITTED"),
        ("complete", "attempt-1"),
    ]

    rejected_queue = RecordingQueue()
    rejected_recovery = RecordingRecoveryStore()
    rejected = AgentExecutionRejected(
        PolicyDecision(PolicyAction.DENY, reason="blocked"),
        AgentExecutionReceipt(session=SessionSnapshot("session-1", 0)),
    )
    assert await _worker(
        rejected_queue,
        StubPipeline(rejected),
        rejected_recovery,
    ).run_once(0)
    assert rejected_queue.completed
    assert ("checkpoint", "POLICY_REJECTION_COMMITTED") in rejected_recovery.events

    failed_queue = RecordingQueue(attempt_count=3)
    failed_recovery = RecordingRecoveryStore()
    assert await _worker(
        failed_queue,
        StubPipeline(RuntimeError("provider unavailable")),
        failed_recovery,
    ).run_once(0)
    assert failed_queue.failure is not None
    assert failed_queue.failure["next_attempt_at"] is None
    assert ("fail", True) in failed_recovery.events


def test_worker_rejects_invalid_identity_concurrency_and_slot() -> None:
    queue = RecordingQueue()
    recovery = RecordingRecoveryStore()
    runtime = LeasedWorkerConfig(
        lease_seconds=30,
        poll_interval_seconds=0.01,
        retry_base_seconds=0,
        retry_max_seconds=60,
        retry_jitter_ratio=0,
        max_attempts=3,
    )

    with pytest.raises(ValueError, match="node ID cannot be empty"):
        AgentWorkerService(
            queue=queue,  # type: ignore[arg-type]
            pipeline=StubPipeline(),
            node_id=" ",
            concurrency=1,
            runtime=runtime,
        )
    with pytest.raises(ValueError, match="concurrency must be positive"):
        AgentWorkerService(
            queue=queue,  # type: ignore[arg-type]
            pipeline=StubPipeline(),
            node_id="worker",
            concurrency=0,
            runtime=runtime,
        )

    worker = _worker(queue, StubPipeline(), recovery)
    with pytest.raises(ValueError, match="outside configured concurrency"):
        asyncio.run(worker.run_once(1))
