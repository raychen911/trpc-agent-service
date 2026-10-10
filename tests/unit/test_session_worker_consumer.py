from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime

import pytest

from trpc_service.faults import FaultStage, FaultStageEvent
from trpc_service.queue.session_ready import SessionReady, SessionReadyDelivery
from trpc_service.queue.session_worker_consumer import SessionWorkerConsumer
from trpc_service.storage.models import MailboxClaimStatus


def delivery(name: str = "event-1") -> SessionReadyDelivery:
    return SessionReadyDelivery(
        stream_id=f"{name}-1",
        message=SessionReady(
            event_id=name,
            tenant_id="tenant-a",
            session_id=f"session-{name}",
            generation=1,
            priority=0,
            trace_id=f"trace-{name}",
            created_at=datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
        ),
    )


@dataclass
class FakeClaim:
    status: MailboxClaimStatus


class FakeQueue:
    def __init__(self, deliveries: list[SessionReadyDelivery]) -> None:
        self.deliveries = list(deliveries)
        self.receive_calls: list[dict[str, object]] = []
        self.acks: list[SessionReadyDelivery] = []
        self.ack_error: Exception | None = None
        self.order: list[str] = []
        self.ack_event = asyncio.Event()

    async def receive_new(
        self,
        *,
        consumer: str,
        count: int,
        block_ms: int,
    ) -> tuple[SessionReadyDelivery, ...]:
        self.receive_calls.append({"consumer": consumer, "count": count, "block_ms": block_ms})
        await asyncio.sleep(0.005)
        if self.deliveries:
            return (self.deliveries.pop(0),)
        return ()

    async def ack(self, item: SessionReadyDelivery) -> bool:
        self.order.append("ack")
        if self.ack_error is not None:
            raise self.ack_error
        self.acks.append(item)
        self.ack_event.set()
        return True


class BlockingEmptyQueue(FakeQueue):
    def __init__(self, block_seconds: float) -> None:
        super().__init__([])
        self.block_seconds = block_seconds

    async def receive_new(
        self,
        *,
        consumer: str,
        count: int,
        block_ms: int,
    ) -> tuple[SessionReadyDelivery, ...]:
        self.receive_calls.append({"consumer": consumer, "count": count, "block_ms": block_ms})
        await asyncio.sleep(self.block_seconds)
        return ()


class NeverAckQueue(FakeQueue):
    def __init__(self, deliveries: list[SessionReadyDelivery]) -> None:
        super().__init__(deliveries)
        self.ack_started = asyncio.Event()
        self.ack_cancelled = asyncio.Event()

    async def ack(self, item: SessionReadyDelivery) -> bool:
        del item
        self.ack_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.ack_cancelled.set()
            raise
        return False


class FakeReclaimer:
    def __init__(self, deliveries: list[SessionReadyDelivery] | None = None) -> None:
        self.deliveries = list(deliveries or [])
        self.calls = 0
        self.called = asyncio.Event()
        self.returned = asyncio.Event()

    async def reclaim(self) -> tuple[SessionReadyDelivery, ...]:
        self.calls += 1
        self.called.set()
        await asyncio.sleep(0.005)
        if self.deliveries:
            result = (self.deliveries.pop(0),)
            self.returned.set()
            return result
        self.returned.set()
        return ()


class FakeClaimer:
    def __init__(
        self,
        status: MailboxClaimStatus = MailboxClaimStatus.CLAIMED,
        *,
        error: Exception | None = None,
        order: list[str] | None = None,
    ) -> None:
        self.status = status
        self.error = error
        self.calls: list[SessionReady] = []
        self.order = order if order is not None else []
        self.called = asyncio.Event()

    async def claim(self, message: SessionReady) -> FakeClaim:
        self.calls.append(message)
        self.order.append("claim")
        self.called.set()
        if self.error is not None:
            raise self.error
        return FakeClaim(self.status)


class FakeExecutor:
    def __init__(self, *, order: list[str] | None = None) -> None:
        self.calls: list[FakeClaim] = []
        self.order = order if order is not None else []
        self.error: Exception | None = None
        self.started = asyncio.Event()
        self.finished = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.active = 0
        self.max_active = 0

    async def execute(self, claim: FakeClaim) -> None:
        self.calls.append(claim)
        self.order.append("execute")
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        self.started.set()
        try:
            if self.error is not None:
                raise self.error
            await asyncio.sleep(0)
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        finally:
            self.active -= 1
            self.finished.set()


class FakeFaultStages:
    def __init__(
        self,
        *,
        order: list[str] | None = None,
        error: Exception | None = None,
        wait_for_release: bool = False,
    ) -> None:
        self.events: list[FaultStageEvent] = []
        self.order = order if order is not None else []
        self.error = error
        self.called = asyncio.Event()
        self.cancelled = asyncio.Event()
        self.release = asyncio.Event()
        self.wait_for_release = wait_for_release

    async def checkpoint(self, event: FaultStageEvent) -> bool:
        self.events.append(event)
        self.order.append("fault")
        self.called.set()
        try:
            if self.wait_for_release:
                await self.release.wait()
            if self.error is not None:
                raise self.error
        except asyncio.CancelledError:
            self.cancelled.set()
            raise
        return True


class OrderedQueue(FakeQueue):
    async def receive_new(
        self,
        *,
        consumer: str,
        count: int,
        block_ms: int,
    ) -> tuple[SessionReadyDelivery, ...]:
        if self.deliveries:
            self.order.append("receive")
        return await super().receive_new(
            consumer=consumer,
            count=count,
            block_ms=block_ms,
        )


async def stop_after_processed(
    consumer: SessionWorkerConsumer,
    stop_event: asyncio.Event,
    predicate: asyncio.Event,
) -> None:
    task = asyncio.create_task(consumer.run(stop_event))
    await asyncio.wait_for(predicate.wait(), timeout=1)
    stop_event.set()
    await asyncio.wait_for(task, timeout=1)


@pytest.mark.asyncio
async def test_claim_ack_execute_order_and_single_item_receive() -> None:
    order: list[str] = []
    queue = FakeQueue([delivery()])
    queue.order = order
    claimer = FakeClaimer(order=order)
    executor = FakeExecutor(order=order)
    reclaimer = FakeReclaimer()
    consumer = SessionWorkerConsumer(
        queue,
        reclaimer,
        claimer,
        executor,
        consumer_id="worker-a",
        concurrency=2,
    )

    stop = asyncio.Event()
    await stop_after_processed(consumer, stop, executor.finished)

    assert order == ["claim", "ack", "execute"]
    assert len(claimer.calls) == 1
    assert len(queue.acks) == 1
    assert queue.receive_calls[0] == {
        "consumer": "worker-a",
        "count": 1,
        "block_ms": 5_000,
    }


@pytest.mark.asyncio
async def test_default_receive_block_stops_cleanly_and_releases_reclaimer_permit() -> None:
    queue = FakeQueue([])
    reclaimer = FakeReclaimer()
    consumer = SessionWorkerConsumer(
        queue,
        reclaimer,
        FakeClaimer(),
        FakeExecutor(),
        consumer_id="worker-a",
        concurrency=1,
        reclaimer_poll_seconds=0.01,
    )

    stop = asyncio.Event()
    task = asyncio.create_task(consumer.run(stop))
    await asyncio.wait_for(reclaimer.called.wait(), timeout=1)

    stop.set()
    await asyncio.wait_for(task, timeout=1)

    assert reclaimer.calls > 0
    assert queue.receive_calls
    assert all(call["block_ms"] == 5_000 for call in queue.receive_calls)
    assert consumer.concurrency_available == 1
    assert consumer._in_flight == {}


@pytest.mark.asyncio
async def test_empty_queue_polling_has_a_bounded_call_rate() -> None:
    queue = BlockingEmptyQueue(0.05)
    reclaimer = FakeReclaimer()
    consumer = SessionWorkerConsumer(
        queue,
        reclaimer,
        FakeClaimer(),
        FakeExecutor(),
        consumer_id="worker-a",
        concurrency=1,
        receive_block_ms=50,
        reclaimer_poll_seconds=0.05,
    )

    stop = asyncio.Event()
    task = asyncio.create_task(consumer.run(stop))
    await asyncio.sleep(0.23)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    # An empty Redis receive blocks for the configured interval; neither
    # source is allowed to turn an empty queue into a tight polling loop.
    assert 1 <= len(queue.receive_calls) <= 6
    assert 1 <= reclaimer.calls <= 6
    assert all(call["block_ms"] == 50 for call in queue.receive_calls)


@pytest.mark.asyncio
async def test_v2_enqueue_fault_checkpoint_is_after_receive_before_pg_claim() -> None:
    order: list[str] = []
    queue = OrderedQueue([delivery()])
    queue.order = order
    fault_stages = FakeFaultStages(order=order)
    claimer = FakeClaimer(order=order)
    executor = FakeExecutor(order=order)
    consumer = SessionWorkerConsumer(
        queue,
        FakeReclaimer(),
        claimer,
        executor,
        consumer_id="worker-a",
        concurrency=1,
        fault_stages=fault_stages,
        fault_injection_enabled=True,
        test_environment=True,
    )

    stop = asyncio.Event()
    await stop_after_processed(consumer, stop, executor.finished)

    assert order == ["receive", "fault", "claim", "ack", "execute"]
    assert len(fault_stages.events) == 1
    event = fault_stages.events[0]
    assert event.stage is FaultStage.ENQUEUE
    assert event.tenant_id == "tenant-a"
    assert event.worker_id == "worker-a"
    assert event.inbound_id == "event-1"
    assert event.stream_id == "event-1-1"


@pytest.mark.asyncio
async def test_v2_enqueue_fault_error_leaves_redis_delivery_unacked() -> None:
    queue = FakeQueue([delivery()])
    fault_stages = FakeFaultStages(error=RuntimeError("fault checkpoint failed"))
    claimer = FakeClaimer()
    executor = FakeExecutor()
    consumer = SessionWorkerConsumer(
        queue,
        FakeReclaimer(),
        claimer,
        executor,
        consumer_id="worker-a",
        concurrency=1,
        fault_stages=fault_stages,
        fault_injection_enabled=True,
        test_environment=True,
    )

    stop = asyncio.Event()
    await stop_after_processed(consumer, stop, fault_stages.called)

    assert claimer.calls == []
    assert queue.acks == []
    assert executor.calls == []
    assert consumer.concurrency_available == 1


@pytest.mark.asyncio
async def test_v2_enqueue_fault_cancellation_releases_permit_and_leaves_pel() -> None:
    queue = FakeQueue([delivery()])
    fault_stages = FakeFaultStages(wait_for_release=True)
    claimer = FakeClaimer()
    executor = FakeExecutor()
    consumer = SessionWorkerConsumer(
        queue,
        FakeReclaimer(),
        claimer,
        executor,
        consumer_id="worker-a",
        concurrency=1,
        fault_stages=fault_stages,
        fault_injection_enabled=True,
        test_environment=True,
    )

    task = asyncio.create_task(consumer.run())
    await asyncio.wait_for(fault_stages.called.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)

    assert fault_stages.cancelled.is_set()
    assert claimer.calls == []
    assert queue.acks == []
    assert executor.calls == []
    assert consumer.concurrency_available == 1


@pytest.mark.parametrize(
    ("fault_injection_enabled", "test_environment"),
    [(False, True), (True, False), (False, False)],
)
def test_v2_enqueue_fault_controller_requires_explicit_test_mode(
    fault_injection_enabled: bool,
    test_environment: bool,
) -> None:
    with pytest.raises(ValueError, match="fault stages require"):
        SessionWorkerConsumer(
            FakeQueue([]),
            FakeReclaimer(),
            FakeClaimer(),
            FakeExecutor(),
            consumer_id="worker-a",
            fault_stages=FakeFaultStages(),
            fault_injection_enabled=fault_injection_enabled,
            test_environment=test_environment,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status",
    [MailboxClaimStatus.STALE, MailboxClaimStatus.RUNNING, MailboxClaimStatus.EMPTY],
)
async def test_non_claim_status_is_acked_without_execution(status: MailboxClaimStatus) -> None:
    queue = FakeQueue([delivery()])
    claimer = FakeClaimer(status)
    executor = FakeExecutor()
    consumer = SessionWorkerConsumer(
        queue,
        FakeReclaimer(),
        claimer,
        executor,
        consumer_id="worker-a",
    )

    stop = asyncio.Event()
    await stop_after_processed(consumer, stop, queue.ack_event)
    assert len(claimer.calls) == 1
    assert len(queue.acks) == 1
    assert executor.calls == []


@pytest.mark.asyncio
async def test_claim_error_leaves_delivery_unacked() -> None:
    queue = FakeQueue([delivery()])
    executor = FakeExecutor()
    claimer = FakeClaimer(error=RuntimeError("database unavailable"))
    consumer = SessionWorkerConsumer(
        queue,
        FakeReclaimer(),
        claimer,
        executor,
        consumer_id="worker-a",
    )

    stop = asyncio.Event()
    await stop_after_processed(consumer, stop, claimer.called)
    assert queue.acks == []
    assert executor.calls == []


@pytest.mark.asyncio
async def test_ack_failure_does_not_undo_claim_or_skip_execution() -> None:
    order: list[str] = []
    queue = FakeQueue([delivery()])
    queue.order = order
    queue.ack_error = RuntimeError("redis unavailable")
    executor = FakeExecutor(order=order)
    consumer = SessionWorkerConsumer(
        queue,
        FakeReclaimer(),
        FakeClaimer(order=order),
        executor,
        consumer_id="worker-a",
    )

    stop = asyncio.Event()
    await stop_after_processed(consumer, stop, executor.finished)
    assert order == ["claim", "ack", "execute"]
    assert len(executor.calls) == 1


@pytest.mark.asyncio
async def test_one_execution_error_does_not_stop_later_delivery() -> None:
    queue = FakeQueue([delivery("first"), delivery("second")])
    executor = FakeExecutor()
    calls = 0

    class PerCallExecutor(FakeExecutor):
        async def execute(self, claim: FakeClaim) -> None:
            nonlocal calls
            calls += 1
            self.calls.append(claim)
            if calls == 1:
                raise RuntimeError("turn failed")
            self.finished.set()

    executor = PerCallExecutor()
    consumer = SessionWorkerConsumer(
        queue,
        FakeReclaimer(),
        FakeClaimer(),
        executor,
        consumer_id="worker-a",
        concurrency=2,
    )
    stop = asyncio.Event()
    await stop_after_processed(consumer, stop, executor.finished)
    assert calls >= 2
    assert len(queue.acks) == 2


@pytest.mark.asyncio
async def test_reclaimer_gets_a_slot_with_concurrency_one() -> None:
    queue = FakeQueue([])
    reclaimer = FakeReclaimer()
    consumer = SessionWorkerConsumer(
        queue,
        reclaimer,
        FakeClaimer(),
        FakeExecutor(),
        consumer_id="worker-a",
        concurrency=1,
        receive_block_ms=10,
        reclaimer_poll_seconds=0.01,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(consumer.run(stop))
    await asyncio.wait_for(reclaimer.called.wait(), timeout=1)
    stop.set()
    await asyncio.wait_for(task, timeout=1)
    assert reclaimer.calls > 0
    assert queue.receive_calls
    assert all(call["count"] == 1 for call in queue.receive_calls)
    assert all(call["block_ms"] == 10 for call in queue.receive_calls)


@pytest.mark.asyncio
async def test_reclaimer_waits_for_execution_slot_before_xautoclaim() -> None:
    queue = FakeQueue([])
    reclaimer = FakeReclaimer([delivery("reclaimed")])
    claimer = FakeClaimer()
    consumer = SessionWorkerConsumer(
        queue,
        reclaimer,
        claimer,
        FakeExecutor(),
        consumer_id="worker-a",
        concurrency=1,
    )
    held = await consumer._take_permit(asyncio.Event())
    assert held is not None

    stop = asyncio.Event()
    task = asyncio.create_task(consumer._reclaim_loop(stop))
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(reclaimer.called.wait(), timeout=0.05)
    assert claimer.calls == []
    assert queue.acks == []

    held.release()
    await asyncio.wait_for(reclaimer.called.wait(), timeout=1)
    await asyncio.wait_for(reclaimer.returned.wait(), timeout=1)
    await asyncio.wait_for(claimer.called.wait(), timeout=1)
    stop.set()
    await asyncio.wait_for(task, timeout=1)
    assert len(claimer.calls) == 1


@pytest.mark.asyncio
async def test_reclaimer_cancellation_before_capacity_does_not_claim_redis() -> None:
    queue = FakeQueue([])
    reclaimer = FakeReclaimer([delivery("cancelled-reclaim")])
    claimer = FakeClaimer()
    consumer = SessionWorkerConsumer(
        queue,
        reclaimer,
        claimer,
        FakeExecutor(),
        consumer_id="worker-a",
        concurrency=1,
    )
    held = await consumer._take_permit(asyncio.Event())
    assert held is not None

    task = asyncio.create_task(consumer._reclaim_loop(asyncio.Event()))
    with pytest.raises(TimeoutError):
        await asyncio.wait_for(reclaimer.called.wait(), timeout=0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)

    # Capacity is reserved before XAUTOCLAIM, so cancellation cannot transfer
    # Redis ownership while the worker is already saturated.
    assert reclaimer.calls == 0
    assert claimer.calls == []
    assert queue.acks == []
    assert consumer.concurrency_available == 0
    held.release()
    assert consumer.concurrency_available == 1
    assert consumer._in_flight == {}


@pytest.mark.asyncio
async def test_ack_deadline_allows_execution_and_leaves_redis_delivery_unacked() -> None:
    queue = NeverAckQueue([delivery("ack-timeout")])
    executor = FakeExecutor()
    consumer = SessionWorkerConsumer(
        queue,
        FakeReclaimer(),
        FakeClaimer(),
        executor,
        consumer_id="worker-a",
        concurrency=1,
        ack_timeout_seconds=0.01,
    )

    stop = asyncio.Event()
    await stop_after_processed(consumer, stop, executor.finished)

    assert queue.ack_started.is_set()
    assert queue.ack_cancelled.is_set()
    assert queue.acks == []
    assert len(executor.calls) == 1
    assert consumer.concurrency_available == 1


def test_ack_timeout_must_be_positive_and_finite() -> None:
    arguments = dict(
        queue=FakeQueue([]),
        reclaimer=FakeReclaimer(),
        claimer=FakeClaimer(),
        executor=FakeExecutor(),
        consumer_id="worker-a",
    )
    for value in (0, -1, float("inf"), float("nan")):
        with pytest.raises(ValueError, match="ack_timeout_seconds"):
            SessionWorkerConsumer(**arguments, ack_timeout_seconds=value)


@pytest.mark.asyncio
async def test_cancellation_awaits_inflight_execution_and_releases_permit() -> None:
    queue = FakeQueue([delivery()])
    executor = FakeExecutor()
    gate = asyncio.Event()

    async def blocking_execute(_: FakeClaim) -> None:
        executor.started.set()
        try:
            await gate.wait()
        except asyncio.CancelledError:
            executor.cancelled.set()
            raise
        finally:
            executor.finished.set()

    executor.execute = blocking_execute  # type: ignore[method-assign]
    consumer = SessionWorkerConsumer(
        queue,
        FakeReclaimer(),
        FakeClaimer(),
        executor,
        consumer_id="worker-a",
        concurrency=1,
    )
    task = asyncio.create_task(consumer.run())
    await asyncio.wait_for(executor.started.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)
    assert executor.cancelled.is_set()
    assert executor.finished.is_set()
    assert consumer.concurrency_available == 1


@pytest.mark.asyncio
async def test_done_callback_releases_permit_when_delivery_never_starts() -> None:
    consumer = SessionWorkerConsumer(
        FakeQueue([]),
        FakeReclaimer(),
        FakeClaimer(),
        FakeExecutor(),
        consumer_id="worker-a",
        concurrency=1,
    )
    permit = await consumer._take_permit(asyncio.Event())
    assert permit is not None
    task = asyncio.create_task(asyncio.sleep(0))
    consumer._in_flight[task] = permit
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    consumer._finish_delivery_task(task)

    assert consumer.concurrency_available == 1
