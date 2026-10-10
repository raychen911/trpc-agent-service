"""Small, deterministic branch gates for runtime ownership and recovery.

These tests intentionally exercise failure edges without starting a service or
contacting Redis/PostgreSQL.  The assertions focus on the ownership contract:
an expired/lost lease must not be acknowledged or committed, and recovery
components must release their own resources when a cycle fails or is stopped.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any, cast

import pytest
from trpc_agent_sdk.agents import BaseAgent

import trpc_service.agent.worker as worker_module
import trpc_service.faults.controller as fault_module
import trpc_service.queue.worker_consumer as consumer_module
from tests.conftest import envelope, repository
from trpc_service.agent.mailbox_runtime import MailboxReadyClaimer
from trpc_service.agent.session_recovery import LeaseSweeper, SessionRecoveryService
from trpc_service.agent.worker import AgentWorker, ProcessStatus, WorkerResult
from trpc_service.config.settings import SchedulerVersion
from trpc_service.faults import (
    FaultStage,
    FaultStageControlError,
    FaultStageEvent,
    PostgresFaultStageController,
)
from trpc_service.queue.redis_streams import QueueMessage
from trpc_service.queue.session_ready import SessionReady
from trpc_service.queue.worker_consumer import WorkerConsumer
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import (
    MailboxClaimStatus,
    SessionClaim,
    SessionMailbox,
)
from trpc_service.storage.protocols import FencingConflict
from trpc_service.tenant.models import TenantConfig

# ---------------------------------------------------------------------------
# Fault controller: invalid transitions and bounded waits.


class _FaultTransaction:
    async def __aenter__(self) -> _FaultTransaction:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class _FaultAcquire:
    def __init__(self, connection: _FaultConnection) -> None:
        self.connection = connection

    async def __aenter__(self) -> _FaultConnection:
        return self.connection

    async def __aexit__(self, *_args: object) -> None:
        return None


class _FaultConnection:
    """Minimal query-aware fake for controller wait-state branches."""

    def __init__(self) -> None:
        self.rows: dict[str, dict[str, Any]] = {}

    def transaction(self) -> _FaultTransaction:
        return _FaultTransaction()

    async def execute(self, query: str, *args: Any) -> str:
        if query.lstrip().startswith("SELECT set_config"):
            return "SELECT 1"
        if "SET status='expired'" in query:
            row = self.rows.get(str(args[1]))
            if row is not None and row["status"] == "entered":
                row["status"] = "expired"
            return "UPDATE 1"
        raise AssertionError(f"unexpected execute query: {query}")

    async def fetchrow(self, query: str, *args: Any) -> dict[str, Any] | None:
        if query.lstrip().startswith("SELECT status, expires_at"):
            row = self.rows.get(str(args[1]))
            if row is None:
                return None
            return {"status": row["status"], "expires_at": row["expires_at"]}
        raise AssertionError(f"unexpected fetchrow query: {query}")


class _FaultPool:
    def __init__(self) -> None:
        self.connection = _FaultConnection()

    def acquire(self) -> _FaultAcquire:
        return _FaultAcquire(self.connection)


def _fault_event(**updates: Any) -> FaultStageEvent:
    values: dict[str, Any] = {
        "stage": FaultStage.TOOL,
        "tenant_id": "tenant-a",
        "worker_id": "worker-a",
        "inbound_id": "inbound-a",
        "turn_id": "turn-a",
        "execution_key": "execution-a",
        "stream_id": "stream-a",
        "fencing_token": 1,
    }
    values.update(updates)
    return FaultStageEvent(**values)


def _fault_controller(pool: _FaultPool, **updates: Any) -> PostgresFaultStageController:
    values: dict[str, Any] = {
        "run_id": "run-a",
        "run_token": "test-only-fault-token-0123456789abcdef",
        "poll_interval_seconds": 0.001,
    }
    values.update(updates)
    return PostgresFaultStageController(pool, **values)


def test_fault_controller_rejects_blank_values_and_unsafe_limits() -> None:
    with pytest.raises(ValueError, match="cannot be blank"):
        _fault_event(worker_id=" ")
    with pytest.raises(ValueError, match="cannot be blank"):
        _fault_event(execution_key=" ")

    pool = _FaultPool()
    with pytest.raises(ValueError, match="poll interval"):
        _fault_controller(pool, poll_interval_seconds=0)
    with pytest.raises(ValueError, match="TTL"):
        _fault_controller(pool, ttl_seconds=0)
    with pytest.raises(FaultStageControlError, match="cannot be empty"):
        _fault_controller(pool, run_id=" ")


@pytest.mark.asyncio
async def test_fault_controller_poll_states_fail_closed_and_mark_expiry() -> None:
    pool = _FaultPool()
    controller = _fault_controller(pool, wait_timeout_seconds=0.001)
    event = _fault_event()
    fingerprint = fault_module._target_fingerprint(event)

    # A deleted/unknown control cannot release a checkpoint.
    assert (
        await controller._poll_until_released(
            event, control_id="missing", fingerprint=fingerprint, expires_at=None
        )
        is False
    )

    control_id = "control-a"
    pool.connection.rows[control_id] = {
        "status": "armed",
        "expires_at": datetime.now(UTC) + timedelta(seconds=1),
    }
    assert (
        await controller._poll_until_released(
            event, control_id=control_id, fingerprint=fingerprint, expires_at=None
        )
        is False
    )

    pool.connection.rows[control_id]["status"] = "unexpected"
    assert (
        await controller._poll_until_released(
            event, control_id=control_id, fingerprint=fingerprint, expires_at=None
        )
        is False
    )

    pool.connection.rows[control_id].update(
        {"status": "entered", "expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    )
    assert (
        await controller._poll_until_released(
            event,
            control_id=control_id,
            fingerprint=fingerprint,
            expires_at=datetime.now(UTC),
        )
        is False
    )
    assert pool.connection.rows[control_id]["status"] == "expired"

    pool.connection.rows[control_id].update(
        {"status": "entered", "expires_at": datetime.now(UTC) + timedelta(seconds=1)}
    )
    assert (
        await controller._poll_until_released(
            event,
            control_id=control_id,
            fingerprint=fingerprint,
            expires_at=datetime.now(UTC) + timedelta(seconds=1),
        )
        is False
    )
    assert pool.connection.rows[control_id]["status"] == "expired"


@pytest.mark.asyncio
async def test_fault_controller_invalid_address_and_cleanup_limits_fail_closed() -> None:
    controller = _fault_controller(_FaultPool())
    with pytest.raises(TypeError, match="FaultStageEvent"):
        await controller.checkpoint(object())  # type: ignore[arg-type]
    with pytest.raises(FaultStageControlError, match="cannot be empty"):
        await controller.release("", tenant_id="tenant-a")
    with pytest.raises(FaultStageControlError, match="cannot be empty"):
        await controller.release("control-a", tenant_id=" ")
    with pytest.raises(ValueError, match="between 1 and 1000"):
        await controller.cleanup_expired(tenant_id="tenant-a", limit=0)
    with pytest.raises(ValueError, match="between 1 and 1000"):
        await controller.cleanup_expired(tenant_id="tenant-a", limit=1001)


def test_fault_controller_datetime_normalization_is_explicit() -> None:
    assert fault_module._as_datetime(None) is None
    assert fault_module._as_datetime("not-a-date") is None
    naive = datetime(2026, 8, 23, 12, 0)
    assert fault_module._as_datetime(naive) == naive.replace(tzinfo=UTC)


def test_agent_worker_rejects_non_positive_attempt_budget() -> None:
    with pytest.raises(ValueError, match="max_turn_attempts"):
        AgentWorker(
            repository(),
            worker_id="worker-a",
            agent_loader=cast(Any, _deterministic_loader),
            max_turn_attempts=0,
        )


# ---------------------------------------------------------------------------
# Agent worker: cancellation, v2 fencing, and duplicate resolution.


async def _claim_v2(
    repo: Any,
    message_id: str,
    *,
    owner_id: str = "worker-a",
    lease_for: timedelta | None = None,
) -> tuple[Any, Any]:
    accepted = await TenantRuntime(
        repo,
        routing_key=b"runtime-recovery-branch-gate" * 2,
        scheduler_version=SchedulerVersion.V2,
    ).accept("binding-unpredictable-a", envelope(message_id))
    mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert mailbox is not None
    ready_event = repo.mailbox.outbox[-1]
    return accepted, await MailboxReadyClaimer(
        repo,
        owner_id=owner_id,
        lease_for=lease_for or timedelta(seconds=1),
    ).claim(
        SessionReady(
            event_id=ready_event.outbox_id,
            tenant_id=accepted.context.tenant_id,
            session_id=accepted.context.session_id,
            generation=mailbox.queue_generation,
            priority=mailbox.priority,
            trace_id=accepted.context.trace_id,
            created_at=datetime.now(UTC),
        )
    )


async def _deterministic_loader(_config: TenantConfig) -> BaseAgent:
    from trpc_service.agent.fake import DeterministicAgent

    return cast(BaseAgent, DeterministicAgent(name="branch-gate-agent", response="done"))


@pytest.mark.asyncio
async def test_agent_worker_rejects_non_executable_mailbox_claim() -> None:
    repo = repository()
    worker = AgentWorker(repo, worker_id="worker-a", agent_loader=cast(Any, _deterministic_loader))
    claim = SessionClaim(
        status=MailboxClaimStatus.EMPTY,
        mailbox=SessionMailbox(tenant_id="tenant-a", session_id="session-a"),
    )
    with pytest.raises(ValueError, match="execution lease"):
        await worker.process_claimed(claim)


@pytest.mark.asyncio
async def test_agent_worker_v1_refreshes_duplicate_before_returning_busy() -> None:
    repo = repository()
    accepted = await TenantRuntime(repo, routing_key=b"duplicate-refresh" * 2).accept(
        "binding-unpredictable-a", envelope("duplicate-refresh")
    )
    duplicate = accepted.model_copy(update={"duplicate": True})

    class NoLeaseRepository:
        async def acquire(self, **_kwargs: Any) -> None:
            return None

        async def get_acceptance(self, *_args: Any) -> Any:
            return duplicate

    result = await AgentWorker(
        cast(Any, NoLeaseRepository()),
        worker_id="worker-a",
        agent_loader=cast(Any, _deterministic_loader),
    ).process(accepted)
    assert result.status == ProcessStatus.DUPLICATE


@pytest.mark.asyncio
async def test_agent_worker_cancellation_moves_v2_turn_to_retry_without_leaks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = repository()
    accepted, claim = await _claim_v2(
        repo,
        "cancelled-turn",
        lease_for=timedelta(seconds=2),
    )
    started = asyncio.Event()
    stop = asyncio.Event()

    class BlockingRunner:
        state: dict[str, Any]
        buffered_events = ()

        def __init__(self, **_kwargs: Any) -> None:
            self.state = {}

        async def run(self, *_args: Any, **_kwargs: Any) -> AsyncIterator[Any]:
            started.set()
            await stop.wait()
            if False:
                yield None

    monkeypatch.setattr(worker_module, "TenantRunner", BlockingRunner)
    task = asyncio.create_task(
        AgentWorker(
            repo,
            worker_id="worker-a",
            agent_loader=cast(Any, _deterministic_loader),
        ).process_claimed(claim)
    )
    await asyncio.wait_for(started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)

    mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert mailbox is not None
    assert mailbox.status.value == "RETRY_WAIT"
    assert mailbox.processing_inbound_id is None
    assert (accepted.context.tenant_id, accepted.context.session_id) not in repo._leases


@pytest.mark.asyncio
async def test_agent_worker_v2_heartbeat_loss_releases_fenced_turn_without_commit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repo = repository()
    accepted, claim = await _claim_v2(
        repo,
        "heartbeat-loss",
        lease_for=timedelta(milliseconds=300),
    )
    cancelled = asyncio.Event()

    async def lose_lease(*_args: Any, **_kwargs: Any) -> None:
        raise FencingConflict("lease lost")

    repo.renew_session_ready = lose_lease  # type: ignore[assignment]

    class SlowRunner:
        state: dict[str, Any]
        buffered_events = ()

        def __init__(self, **_kwargs: Any) -> None:
            self.state = {}

        async def run(self, *_args: Any, **_kwargs: Any) -> AsyncIterator[Any]:
            try:
                await asyncio.sleep(0.2)
            except asyncio.CancelledError:
                cancelled.set()
                raise
            if False:
                yield None

    monkeypatch.setattr(worker_module, "TenantRunner", SlowRunner)
    with pytest.raises(FencingConflict, match="heartbeat failed"):
        await AgentWorker(
            repo,
            worker_id="worker-a",
            agent_loader=cast(Any, _deterministic_loader),
            lease_for=timedelta(milliseconds=300),
        ).process_claimed(claim)

    assert cancelled.is_set()
    mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert mailbox is not None and mailbox.status.value == "RETRY_WAIT"
    assert not any(record.event_type.startswith("outbound.") for record in repo._outbox.values())


# ---------------------------------------------------------------------------
# Legacy worker consumer: ownership and cleanup edges retained for v1 drain.


class _LegacyQueue:
    def __init__(self) -> None:
        self.acked: list[QueueMessage] = []
        self.deferred: list[QueueMessage] = []
        self.defer_result = False
        self.heartbeat_error: BaseException | None = None
        self.heartbeat_started = asyncio.Event()
        self.heartbeat_cancelled = asyncio.Event()

    async def ack(self, message: QueueMessage) -> None:
        self.acked.append(message)

    async def defer(
        self,
        message: QueueMessage,
        *,
        consumer_id: str,
        retry_delay_seconds: float,
    ) -> bool:
        del consumer_id, retry_delay_seconds
        self.deferred.append(message)
        return self.defer_result

    async def heartbeat(
        self,
        message: QueueMessage,
        *,
        consumer_id: str,
        stop_event: asyncio.Event,
    ) -> bool:
        del message, consumer_id
        self.heartbeat_started.set()
        if self.heartbeat_error is not None:
            raise self.heartbeat_error
        try:
            await stop_event.wait()
        except asyncio.CancelledError:
            self.heartbeat_cancelled.set()
            raise
        return True


def _legacy_message() -> QueueMessage:
    return QueueMessage(
        "1-0",
        "outbox-1",
        "tenant-a",
        "inbound.accepted",
        "inbound-1",
        {},
        {},
    )


@pytest.mark.asyncio
async def test_legacy_consumer_fault_checkpoint_receives_identity_only() -> None:
    message = _legacy_message()
    queue = _LegacyQueue()
    accepted = await TenantRuntime(repository(), routing_key=b"legacy-gate" * 4).accept(
        "binding-unpredictable-a", envelope("inbound-1")
    )
    seen: list[FaultStageEvent] = []

    class Controller:
        async def checkpoint(self, event: FaultStageEvent) -> bool:
            seen.append(event)
            return False

    async def _present(*_args: Any) -> Any:
        return accepted

    consumer = WorkerConsumer(
        cast(Any, SimpleNamespace(get_acceptance=_present)),
        cast(Any, queue),
        cast(Any, SimpleNamespace(process=lambda _accepted: _committed())),
        consumer_id="worker-a",
        fault_stages=Controller(),
    )
    await consumer.process_message(message)
    assert seen[0].stage == FaultStage.ENQUEUE
    assert seen[0].inbound_id == message.aggregate_id
    assert seen[0].stream_id == message.stream_id
    assert queue.acked == [message]


async def _committed() -> WorkerResult:
    return WorkerResult(ProcessStatus.COMMITTED)


@pytest.mark.asyncio
async def test_legacy_consumer_defer_loss_does_not_ack() -> None:
    message = _legacy_message()
    queue = _LegacyQueue()
    accepted = SimpleNamespace(duplicate=False)

    async def busy(_accepted: Any) -> WorkerResult:
        return WorkerResult(ProcessStatus.BUSY)

    async def present(*_args: Any) -> Any:
        return accepted

    consumer = WorkerConsumer(
        cast(Any, SimpleNamespace(get_acceptance=present)),
        cast(Any, queue),
        cast(Any, SimpleNamespace(process=busy)),
        consumer_id="worker-a",
    )
    await consumer.process_message(message)
    assert queue.deferred == [message]
    assert queue.acked == []


@pytest.mark.asyncio
async def test_legacy_consumer_heartbeat_error_is_not_acknowledged() -> None:
    message = _legacy_message()
    queue = _LegacyQueue()
    queue.heartbeat_error = RuntimeError("heartbeat unavailable")
    accepted = SimpleNamespace(duplicate=False)

    async def committed(_accepted: Any) -> WorkerResult:
        await asyncio.sleep(0)
        return WorkerResult(ProcessStatus.COMMITTED)

    async def present(*_args: Any) -> Any:
        return accepted

    consumer = WorkerConsumer(
        cast(Any, SimpleNamespace(get_acceptance=present)),
        cast(Any, queue),
        cast(Any, SimpleNamespace(process=committed)),
        consumer_id="worker-a",
    )
    with pytest.raises(RuntimeError, match="heartbeat unavailable"):
        await consumer.process_message(message)
    assert queue.acked == []


@pytest.mark.asyncio
async def test_legacy_consumer_owned_operation_cancel_releases_body_task() -> None:
    queue = _LegacyQueue()
    consumer = WorkerConsumer(
        cast(Any, SimpleNamespace()),
        cast(Any, queue),
        cast(Any, SimpleNamespace()),
        consumer_id="worker-a",
    )
    heartbeat = asyncio.create_task(asyncio.Event().wait())
    body_started = asyncio.Event()
    body_cancelled = asyncio.Event()

    async def body() -> None:
        body_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            body_cancelled.set()
            raise

    operation = consumer._run_owned_operation(body(), heartbeat)
    task = asyncio.create_task(operation)
    await asyncio.wait_for(body_started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    heartbeat.cancel()
    await asyncio.gather(heartbeat, return_exceptions=True)
    assert body_cancelled.is_set()


@pytest.mark.asyncio
async def test_legacy_consumer_finished_unhealthy_heartbeat_never_starts_body() -> None:
    consumer = WorkerConsumer(
        cast(Any, SimpleNamespace()),
        cast(Any, _LegacyQueue()),
        cast(Any, SimpleNamespace()),
        consumer_id="worker-a",
    )
    body_started = False

    async def body() -> None:
        nonlocal body_started
        body_started = True

    heartbeat = asyncio.create_task(asyncio.sleep(0, result=False))
    await heartbeat
    with pytest.raises(consumer_module._OwnershipLost):
        await consumer._run_owned_operation(body(), heartbeat)
    assert body_started is False


@pytest.mark.asyncio
async def test_legacy_consumer_heartbeat_exception_is_wrapped_and_cleanup_errors_surface() -> None:
    consumer = WorkerConsumer(
        cast(Any, SimpleNamespace()),
        cast(Any, _LegacyQueue()),
        cast(Any, SimpleNamespace()),
        consumer_id="worker-a",
    )

    async def failing_heartbeat() -> bool:
        raise RuntimeError("heartbeat backend down")

    heartbeat = asyncio.create_task(failing_heartbeat())
    with pytest.raises(RuntimeError):
        await heartbeat
    with pytest.raises(consumer_module._HeartbeatFailed, match="heartbeat backend down"):
        await consumer._run_owned_operation(asyncio.sleep(0), heartbeat)

    async def raising_cleanup() -> bool:
        raise RuntimeError("cleanup failed")

    cleanup_task = asyncio.create_task(raising_cleanup())
    ok, error = await consumer._stop_heartbeat(asyncio.Event(), cleanup_task)
    assert ok is False
    assert isinstance(error, RuntimeError)


@pytest.mark.asyncio
async def test_legacy_consumer_heartbeat_cleanup_timeout_cancels_task() -> None:
    consumer = WorkerConsumer(
        cast(Any, SimpleNamespace()),
        cast(Any, _LegacyQueue()),
        cast(Any, SimpleNamespace()),
        consumer_id="worker-a",
    )
    stop = asyncio.Event()
    heartbeat_cancelled = asyncio.Event()

    async def heartbeat() -> bool:
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            heartbeat_cancelled.set()
            raise
        return True

    task = asyncio.create_task(heartbeat())
    original = worker_module  # keep this test independent of timing patches
    del original
    # The production timeout is bounded to 250 ms; this is an isolated cleanup
    # check and does not perform any load.
    ok, error = await consumer._stop_heartbeat(stop, task)
    assert ok is False
    assert isinstance(error, TimeoutError)
    assert heartbeat_cancelled.is_set()


# ---------------------------------------------------------------------------
# Recovery loops: partial failure, cancellation, and duplicate start.


class _RecoveryRepository:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.block = asyncio.Event()
        self.block_enabled = False
        self.fail = False
        self.result: Any = 1

    async def sweep_expired_leases(self, *, owner_id: str, limit: int) -> int:
        del owner_id, limit
        self.calls.append("lease_sweeper")
        return cast(int, await self._result())

    async def schedule_retries(self, *, owner_id: str, limit: int) -> int:
        del owner_id, limit
        self.calls.append("retry_scheduler")
        return cast(int, await self._result())

    async def reconcile_sessions(self, *, owner_id: str, limit: int) -> int:
        del owner_id, limit
        self.calls.append("session_reconciler")
        return cast(int, await self._result())

    async def _result(self) -> Any:
        if self.block_enabled:
            await self.block.wait()
        if self.fail:
            raise RuntimeError("recovery backend unavailable")
        return self.result


@pytest.mark.asyncio
async def test_recovery_cycle_preserves_partial_failure_and_runs_later_components() -> None:
    repository = _RecoveryRepository()
    service = SessionRecoveryService(repository, owner_id="recovery-a", batch_size=2)
    original = repository.schedule_retries

    async def fail_retry(**kwargs: Any) -> int:
        del kwargs
        repository.calls.append("retry_scheduler_failed")
        raise RuntimeError("retry failed")

    repository.schedule_retries = fail_retry  # type: ignore[method-assign]
    result = await service.run_once()
    repository.schedule_retries = original  # type: ignore[method-assign]
    assert result.status == "fail"
    assert result.counts["lease_sweeper"] == 1
    assert result.counts["retry_scheduler"] == 0
    assert result.counts["session_reconciler"] == 1
    assert result.failures == {"retry_scheduler": "RuntimeError"}


@pytest.mark.asyncio
async def test_recovery_component_cancellation_propagates_without_marking_partial_failure() -> None:
    repository = _RecoveryRepository()
    repository.block_enabled = True
    service = SessionRecoveryService(repository, owner_id="recovery-a", poll_seconds=1)
    task = asyncio.create_task(service.loops[0].run_once())
    await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.asyncio
@pytest.mark.parametrize("result", [True, -1])
async def test_recovery_rejects_invalid_repository_count(result: Any) -> None:
    repository = _RecoveryRepository()
    repository.result = result
    loop = LeaseSweeper(repository, owner_id="recovery-a", batch_size=2)
    with pytest.raises(TypeError, match="non-negative int"):
        await loop.run_once()


@pytest.mark.asyncio
async def test_recovery_loop_retries_after_component_failure_until_stopped() -> None:
    repository = _RecoveryRepository()
    repository.fail = True
    service = SessionRecoveryService(repository, owner_id="recovery-a", poll_seconds=0.001)
    task = asyncio.create_task(service._run_loop(service.loops[0]))
    await asyncio.sleep(0.01)
    service.stop()
    await asyncio.wait_for(task, timeout=1)
    assert repository.calls
    assert all(call == "lease_sweeper" for call in repository.calls)


@pytest.mark.asyncio
async def test_recovery_service_rejects_duplicate_run_and_cleans_tasks_on_cancel() -> None:
    repository = _RecoveryRepository()
    service = SessionRecoveryService(repository, owner_id="recovery-a", poll_seconds=0.01)
    task = asyncio.create_task(service.run())
    await asyncio.sleep(0.02)
    with pytest.raises(RuntimeError, match="already running"):
        await service.run()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert service._tasks == ()
