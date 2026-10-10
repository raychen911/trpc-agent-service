"""Offline acceptance checks for the Session Mailbox architecture.

These tests describe the scheduler boundaries rather than the model itself:
Redis is a wake-up, PostgreSQL/in-memory mailbox state is authoritative, and a
session can have only one executable turn at a time.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from tests.conftest import envelope, repository
from tests.unit.test_session_worker_consumer import (
    FakeClaim,
    FakeExecutor,
    FakeQueue,
    FakeReclaimer,
    delivery,
)
from trpc_service.agent.worker import AgentWorker, ProcessStatus
from trpc_service.config.settings import SchedulerVersion
from trpc_service.queue.session_worker_consumer import SessionWorkerConsumer
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.mailbox import InMemorySessionMailboxStore
from trpc_service.storage.models import MailboxClaimStatus, MailboxStatus
from trpc_service.storage.protocols import FencingConflict


class SequenceClaimer:
    """Return one executable claim, then a RUNNING result for the hot session."""

    def __init__(self) -> None:
        self.calls = 0
        self.second_call = asyncio.Event()

    async def claim(self, _message):
        self.calls += 1
        if self.calls == 1:
            return FakeClaim(MailboxClaimStatus.CLAIMED)
        self.second_call.set()
        return FakeClaim(MailboxClaimStatus.RUNNING)


class BlockingExecutor(FakeExecutor):
    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()

    async def execute(self, claim: FakeClaim) -> None:
        self.calls.append(claim)
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.started.set()
        try:
            await self.release.wait()
        finally:
            self.active -= 1
            self.finished.set()


@pytest.mark.asyncio
async def test_hot_session_uses_one_execution_slot_and_running_delivery_is_acked() -> None:
    """Duplicate wake-ups for one session must not create two model turns."""

    queue = FakeQueue([])
    claimer = SequenceClaimer()
    executor = BlockingExecutor()
    consumer = SessionWorkerConsumer(
        queue,
        FakeReclaimer(),
        claimer,
        executor,
        consumer_id="worker-a",
        concurrency=2,
    )
    stop = asyncio.Event()
    permits = [await consumer._take_permit(stop) for _ in range(2)]
    assert all(permit is not None for permit in permits)
    tasks = [
        asyncio.create_task(
            consumer._process_delivery(item, permit),
        )
        for item, permit in zip(
            (delivery("hot-session"), delivery("hot-session")), permits, strict=True
        )
    ]
    await asyncio.wait_for(executor.started.wait(), timeout=1)
    await asyncio.wait_for(claimer.second_call.wait(), timeout=1)
    await asyncio.sleep(0)
    assert executor.max_active == 1
    assert len(executor.calls) == 1
    executor.release.set()
    await asyncio.gather(*tasks)

    assert claimer.calls == 2
    assert executor.max_active == 1
    assert len(executor.calls) == 1
    assert len(queue.acks) == 2
    assert consumer.concurrency_available == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [MailboxClaimStatus.RUNNING, MailboxClaimStatus.STALE],
)
async def test_non_claimed_delivery_is_acked_once_without_busy_sleep_or_retry(status) -> None:
    """A wake-up that loses the PG race is finished immediately in its slot."""

    queue = FakeQueue([])
    claimer = _FixedClaimer(status)
    executor = FakeExecutor()
    consumer = SessionWorkerConsumer(
        queue,
        FakeReclaimer(),
        claimer,
        executor,
        consumer_id="worker-a",
        concurrency=1,
    )
    permit = await consumer._take_permit(asyncio.Event())
    assert permit is not None
    started = asyncio.get_running_loop().time()
    await consumer._process_delivery(delivery("non-claim"), permit)
    elapsed = asyncio.get_running_loop().time() - started

    assert elapsed < 0.2
    assert claimer.calls == 1
    assert len(queue.acks) == 1
    assert executor.calls == []
    assert consumer.concurrency_available == 1


class _FixedClaimer:
    def __init__(self, status: MailboxClaimStatus, *, order: list[str] | None = None) -> None:
        self.status = status
        self.calls = 0
        self.order = order

    async def claim(self, _message) -> FakeClaim:
        self.calls += 1
        if self.order is not None:
            self.order.append("claim")
        return FakeClaim(self.status)


class _BusyRepository:
    async def acquire(self, **_kwargs):
        return None

    async def get_acceptance(self, _tenant_id: str, _inbound_id: str):
        return None


@pytest.mark.asyncio
async def test_legacy_busy_result_is_immediate_and_has_no_retry_sleep(monkeypatch) -> None:
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=b"legacy-busy-acceptance" * 2)
    accepted = await runtime.accept("binding-unpredictable-a", envelope("legacy-busy"))

    async def forbidden_sleep(*_args, **_kwargs):
        raise AssertionError("BUSY must not sleep in an execution slot")

    monkeypatch.setattr("trpc_service.agent.worker.asyncio.sleep", forbidden_sleep)
    worker = AgentWorker(
        _BusyRepository(),
        worker_id="worker-a",
        agent_loader=lambda _config: None,
    )
    result = await worker.process(accepted)
    assert result.status == ProcessStatus.BUSY


@pytest.mark.asyncio
async def test_claim_ack_precedes_execution() -> None:
    order: list[str] = []
    queue = FakeQueue([delivery("ack-order")])
    queue.order = order
    executor = FakeExecutor(order=order)
    consumer = SessionWorkerConsumer(
        queue,
        FakeReclaimer(),
        _FixedClaimer(MailboxClaimStatus.CLAIMED, order=order),
        executor,
        consumer_id="worker-a",
    )
    permit = await consumer._take_permit(asyncio.Event())
    assert permit is not None
    await consumer._process_delivery(delivery("ack-order"), permit)
    assert order == ["claim", "ack", "execute"]


@pytest.mark.asyncio
async def test_retry_wait_is_only_reawakened_by_scheduler() -> None:
    repo = repository()
    runtime = TenantRuntime(
        repo,
        routing_key=b"mailbox-acceptance" * 2,
        scheduler_version=SchedulerVersion.V2,
    )
    accepted = await runtime.accept("binding-unpredictable-a", envelope("retry-scheduler"))
    mailbox = await repo.mailbox.get("tenant-a", accepted.context.session_id)
    assert mailbox is not None
    claim = await repo.claim_session_ready(
        "tenant-a",
        accepted.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert claim.execution_lease is not None
    await repo.retry_session_ready(
        claim.execution_lease, error_type="temporary", delay=timedelta(seconds=0)
    )
    waiting = await repo.mailbox.get("tenant-a", accepted.context.session_id)
    assert waiting is not None and waiting.status == MailboxStatus.RETRY_WAIT
    outbox_count = len(repo.mailbox.outbox)

    handled = await repo.schedule_retries(owner_id="scheduler-a", limit=1)
    awakened = await repo.mailbox.get("tenant-a", accepted.context.session_id)
    assert handled == 1
    assert awakened is not None and awakened.status == MailboxStatus.QUEUED
    assert len(repo.mailbox.outbox) == outbox_count + 1


@pytest.mark.asyncio
async def test_expired_lease_cannot_be_renewed() -> None:
    repo = repository()
    runtime = TenantRuntime(
        repo,
        routing_key=b"mailbox-expired" * 3,
        scheduler_version=SchedulerVersion.V2,
    )
    accepted = await runtime.accept("binding-unpredictable-a", envelope("expired-renew"))
    mailbox = await repo.mailbox.get("tenant-a", accepted.context.session_id)
    assert mailbox is not None
    claim = await repo.claim_session_ready(
        "tenant-a",
        accepted.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert claim.execution_lease is not None
    key = ("tenant-a", accepted.context.session_id)
    current = repo.mailbox._mailboxes[key]
    repo.mailbox._mailboxes[key] = current.model_copy(
        update={"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    )
    with pytest.raises(FencingConflict):
        await repo.renew_session_ready(claim.execution_lease, lease_for=timedelta(seconds=30))


@pytest.mark.asyncio
async def test_permanent_failure_advances_resolved_sequence_and_wakes_next_item() -> None:
    repo = repository()
    runtime = TenantRuntime(
        repo,
        routing_key=b"mailbox-failure" * 3,
        scheduler_version=SchedulerVersion.V2,
    )
    first = await runtime.accept("binding-unpredictable-a", envelope("poison"))
    await repo.accept_inbound_v2(
        context=first.context,
        envelope=envelope("after-poison"),
        trace_headers={},
    )
    mailbox = await repo.mailbox.get("tenant-a", first.context.session_id)
    assert mailbox is not None
    claim = await repo.claim_session_ready(
        "tenant-a",
        first.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert claim.execution_lease is not None
    await repo.fail_session_ready(claim.execution_lease, error_type="permanent_tool_error")
    updated = await repo.mailbox.get("tenant-a", first.context.session_id)
    assert updated is not None
    assert updated.resolved_sequence == 1
    assert updated.status == MailboxStatus.QUEUED
    assert len(repo.mailbox.outbox) == 2


@pytest.mark.asyncio
async def test_v1_and_v2_share_one_fence_in_both_directions() -> None:
    repo = repository()
    v1_runtime = TenantRuntime(repo, routing_key=b"mailbox-fence" * 3)
    v1 = await v1_runtime.accept("binding-unpredictable-a", envelope("v1-active"))
    v1_lease = await repo.acquire(
        acceptance=v1,
        worker_id="v1-worker",
        lease_for=timedelta(seconds=30),
    )
    assert v1_lease is not None
    await repo.accept_inbound_v2(
        context=v1.context,
        envelope=envelope("v2-during-v1"),
        trace_headers={},
    )
    v2_result = await repo.claim_session_ready(
        "tenant-a",
        v1.context.session_id,
        owner_id="v2-worker",
        lease_for=timedelta(seconds=30),
    )
    assert v2_result.status == MailboxClaimStatus.RUNNING
    assert v2_result.execution_lease is None

    repo = repository()
    v2_runtime = TenantRuntime(
        repo,
        routing_key=b"mailbox-fence" * 3,
        scheduler_version=SchedulerVersion.V2,
    )
    v2 = await v2_runtime.accept("binding-unpredictable-a", envelope("v2-active"))
    mailbox = await repo.mailbox.get("tenant-a", v2.context.session_id)
    assert mailbox is not None
    v2_claim = await repo.claim_session_ready(
        "tenant-a",
        v2.context.session_id,
        owner_id="v2-worker",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert v2_claim.execution_lease is not None
    assert (
        await repo.acquire(
            acceptance=v2,
            worker_id="v1-worker",
            lease_for=timedelta(seconds=30),
        )
        is None
    )


@pytest.mark.asyncio
async def test_reconciler_rebuilds_lost_session_ready_notification() -> None:
    store = InMemorySessionMailboxStore()
    base = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)
    await store.accept(
        "tenant-a",
        "session-a",
        "inbound-a",
        now=base,
        trace_id="trace-a",
    )
    assert len(store.outbox) == 1
    store._outbox.clear()
    rebuilt = await store.reconcile("tenant-a", "session-a", now=base + timedelta(seconds=6))
    assert rebuilt is not None and rebuilt.status == MailboxStatus.QUEUED
    assert len(store.outbox) == 1
