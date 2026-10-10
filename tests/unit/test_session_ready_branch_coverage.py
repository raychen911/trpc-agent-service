from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import pytest
from redis.exceptions import ResponseError

import trpc_service.queue.session_ready as ready_module
import trpc_service.queue.session_worker_consumer as worker_module
from trpc_service.queue.session_ready import (
    SessionReady,
    SessionReadyCodec,
    SessionReadyDelivery,
    SessionReadyQueue,
    SessionReadyReclaimer,
)
from trpc_service.queue.session_worker_consumer import (
    SessionWorkerConsumer,
    _wait_or_yield,
)
from trpc_service.storage.models import MailboxClaimStatus


def message(event_id: str = "event-1", *, created_at: datetime | None = None) -> SessionReady:
    return SessionReady(
        event_id=event_id,
        tenant_id="tenant-a",
        session_id=f"session-{event_id}",
        generation=1,
        priority=0,
        trace_id=f"trace-{event_id}",
        created_at=created_at or datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
    )


def delivery(event_id: str = "event-1") -> SessionReadyDelivery:
    return SessionReadyDelivery(f"{event_id}-1", message(event_id))


def encoded_fields(value: SessionReady | None = None) -> dict[bytes, bytes]:
    return {
        key.encode(): item.encode()
        for key, item in SessionReadyCodec.encode(value or message()).items()
    }


class BranchRedis:
    def __init__(self) -> None:
        self.read_result: Any = []
        self.read_error: BaseException | None = None
        self.ack_result: int = 1
        self.ack_error: BaseException | None = None
        self.reclaim_results: list[Any] = []
        self.reclaim_error: BaseException | None = None
        self.read_calls = 0
        self.reclaim_calls = 0
        self.read_called = asyncio.Event()
        self.reclaim_called = asyncio.Event()
        self.group_create_calls = 0
        self.group_error: BaseException | None = None

    async def xgroup_create(self, *_args: Any, **_kwargs: Any) -> None:
        self.group_create_calls += 1
        if self.group_error is not None:
            raise self.group_error
        return None

    async def xadd(self, *_args: Any, **_kwargs: Any) -> bytes:
        return b"1-0"

    async def xreadgroup(self, *_args: Any, **_kwargs: Any) -> Any:
        self.read_calls += 1
        self.read_called.set()
        if self.read_error is not None:
            raise self.read_error
        return self.read_result

    async def xack(self, *_args: Any, **_kwargs: Any) -> int:
        if self.ack_error is not None:
            raise self.ack_error
        return self.ack_result

    async def xautoclaim(self, *_args: Any, **_kwargs: Any) -> Any:
        self.reclaim_calls += 1
        self.reclaim_called.set()
        if self.reclaim_error is not None:
            raise self.reclaim_error
        if self.reclaim_results:
            return self.reclaim_results.pop(0)
        return (b"0-0", [], [])


@pytest.mark.asyncio
async def test_codec_normalizes_naive_datetime_and_queue_empty_block_options() -> None:
    naive = message(created_at=datetime(2026, 8, 23, 12, 0))
    encoded = SessionReadyCodec.encode(naive)
    assert encoded["created_at"].endswith("Z")

    redis = BranchRedis()
    queue = SessionReadyQueue(redis)
    assert await queue.receive_new(consumer="worker", block_ms=None) == ()
    assert redis.read_calls == 1
    assert redis.read_called.is_set()
    # Negative BLOCK values are intentionally also non-blocking.
    assert await queue.receive_new(consumer="worker", block_ms=-1) == ()
    assert await queue.receive_new(consumer="worker", block_ms=10) == ()

    redis.read_result = [(b"trpc:session-ready:v2", [(b"2-0", encoded_fields())])]
    decoded = await queue.receive(consumer="worker", block_ms=0)
    assert decoded[0].stream_id == "2-0"
    assert decoded[0].message.event_id == "event-1"

    with pytest.raises(ValueError, match="consumer"):
        await queue.receive_new(consumer="")
    with pytest.raises(ValueError, match="count"):
        await queue.receive_new(consumer="worker", count=0)


def test_codec_rejects_missing_extra_and_invalid_values() -> None:
    fields = SessionReadyCodec.encode(message())
    missing = dict(fields)
    missing.pop("trace_id")
    with pytest.raises(ValueError, match="missing=trace_id"):
        SessionReadyCodec.decode(missing)

    extra = {**fields, "unexpected": "value"}
    with pytest.raises(ValueError, match="extra=unexpected"):
        SessionReadyCodec.decode(extra)

    invalid = {**fields, "generation": "not-an-int"}
    with pytest.raises(ValueError, match="field value"):
        SessionReadyCodec.decode(invalid)


@pytest.mark.asyncio
async def test_queue_receive_and_ack_propagate_errors_and_cancellation() -> None:
    redis = BranchRedis()
    queue = SessionReadyQueue(redis)
    assert await queue.publish(message()) == "1-0"
    redis.read_error = RuntimeError("read failed")
    with pytest.raises(RuntimeError, match="read failed"):
        await queue.receive_new(consumer="worker", block_ms=0)

    redis.read_error = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await queue.receive_new(consumer="worker", block_ms=0)

    redis.read_error = ResponseError("WRONGTYPE stream has the wrong kind of value")
    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await queue.receive_new(consumer="worker", block_ms=0)

    item = delivery()
    redis.read_error = None
    redis.ack_result = 0
    assert await queue.ack(item) is False
    redis.ack_error = RuntimeError("ack failed")
    with pytest.raises(RuntimeError, match="ack failed"):
        await queue.ack(item)
    redis.ack_error = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await queue.ack(item)


@pytest.mark.asyncio
async def test_queue_and_reclaimer_recreate_group_after_total_redis_loss() -> None:
    redis = BranchRedis()
    queue = SessionReadyQueue(redis)
    original_read = redis.xreadgroup
    first_read = True

    async def no_group_once(*args: Any, **kwargs: Any) -> Any:
        nonlocal first_read
        if first_read:
            first_read = False
            redis.read_calls += 1
            raise ResponseError("NOGROUP consumer group does not exist")
        return await original_read(*args, **kwargs)

    redis.xreadgroup = no_group_once  # type: ignore[method-assign]
    assert await queue.receive_new(consumer="worker", block_ms=0) == ()
    assert redis.read_calls == 2
    assert redis.group_create_calls == 1

    redis.group_error = ResponseError("BUSYGROUP Consumer Group name already exists")
    await queue.ensure_group()
    redis.group_error = ResponseError("ERR cannot create group")
    with pytest.raises(ResponseError, match="cannot create"):
        await queue.ensure_group()

    reclaim_redis = BranchRedis()
    reclaim_redis.reclaim_error = ResponseError("NOGROUP no such key")
    reclaimer = SessionReadyReclaimer(reclaim_redis, consumer="reclaimer")
    assert await reclaimer.reclaim() == ()
    assert reclaimer.cursor == "0-0"
    assert reclaim_redis.group_create_calls == 1
    assert await reclaimer.reclaim_once() == ()
    reclaim_redis.reclaim_error = ResponseError("WRONGTYPE stream has the wrong kind of value")
    with pytest.raises(ResponseError, match="WRONGTYPE"):
        await reclaimer.reclaim()


@pytest.mark.asyncio
async def test_reclaimer_validation_and_xautoclaim_result_shapes() -> None:
    redis = BranchRedis()
    with pytest.raises(ValueError, match="consumer"):
        SessionReadyReclaimer(redis, consumer="")
    with pytest.raises(ValueError, match="min_idle"):
        SessionReadyReclaimer(redis, consumer="worker", min_idle_ms=-1)
    with pytest.raises(ValueError, match="count"):
        SessionReadyReclaimer(redis, consumer="worker", count=0)
    with pytest.raises(ValueError, match="poll_seconds"):
        SessionReadyReclaimer(redis, consumer="worker", poll_seconds=-1)

    reclaimer = SessionReadyReclaimer(redis, consumer="worker")
    redis.reclaim_results = [(), (b"",), (b"2-0",)]
    assert await reclaimer.reclaim() == ()
    assert reclaimer.cursor == "0-0"
    assert await reclaimer.reclaim() == ()
    assert reclaimer.cursor == "0-0"
    assert await reclaimer.reclaim() == ()
    assert reclaimer.cursor == "2-0"

    class TruthyEmpty:
        def __bool__(self) -> bool:
            return True

        def __len__(self) -> int:
            return 0

    assert ready_module._unpack_xautoclaim(TruthyEmpty()) == ("0-0", ())

    with pytest.raises(RuntimeError, match="on_delivery"):
        await reclaimer.run(asyncio.Event())


@pytest.mark.asyncio
async def test_reclaimer_error_handler_and_cancellation_paths() -> None:
    redis = BranchRedis()
    reclaimer = SessionReadyReclaimer(
        redis,
        consumer="worker",
        on_delivery=lambda _: _raise(RuntimeError("handler")),
    )
    fields = SessionReadyCodec.encode(message())
    redis.reclaim_results = [
        (b"0-0", [(b"1-0", {key.encode(): value.encode() for key, value in fields.items()})], []),
    ]
    with pytest.raises(RuntimeError, match="handler"):
        await reclaimer.reclaim()

    redis.reclaim_error = RuntimeError("redis reclaim")
    with pytest.raises(RuntimeError, match="redis reclaim"):
        await reclaimer.reclaim()
    redis.reclaim_error = asyncio.CancelledError()
    with pytest.raises(asyncio.CancelledError):
        await reclaimer.reclaim()


async def _raise(error: BaseException) -> None:
    raise error


@pytest.mark.asyncio
async def test_reclaimer_run_covers_empty_sleep_and_stop_wait() -> None:
    redis = BranchRedis()
    reclaimer = SessionReadyReclaimer(
        redis,
        consumer="worker",
        on_delivery=lambda _: _raise(RuntimeError()),
        poll_seconds=0.01,
    )
    task = asyncio.create_task(reclaimer.run())
    await asyncio.wait_for(redis.reclaim_called.wait(), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    redis = BranchRedis()
    stop = asyncio.Event()
    reclaimer = SessionReadyReclaimer(
        redis,
        consumer="worker",
        on_delivery=lambda _: _raise(RuntimeError()),
        poll_seconds=0.005,
    )
    task = asyncio.create_task(reclaimer.run(stop))
    await asyncio.sleep(0.02)
    stop.set()
    await asyncio.wait_for(task, timeout=1)

    redis = BranchRedis()
    stop = asyncio.Event()
    original = redis.xautoclaim

    async def stop_after_empty(*args: Any, **kwargs: Any) -> Any:
        result = await original(*args, **kwargs)
        stop.set()
        return result

    redis.xautoclaim = stop_after_empty  # type: ignore[method-assign]
    reclaimer = SessionReadyReclaimer(
        redis,
        consumer="worker",
        on_delivery=lambda _: _raise(RuntimeError()),
        poll_seconds=0,
    )
    await reclaimer.run(stop)


@pytest.mark.asyncio
async def test_worker_constructor_and_permit_guard_paths() -> None:
    queue = SilentQueue()
    reclaim = SilentReclaimer()
    claimer = SilentClaimer()
    executor = SilentExecutor()
    for kwargs, pattern in [
        ({"consumer_id": ""}, "consumer_id"),
        ({"consumer_id": "w", "concurrency": 0}, "concurrency"),
        ({"consumer_id": "w", "receive_block_ms": 0}, "receive_block_ms"),
        ({"consumer_id": "w", "reclaimer_poll_seconds": -1}, "reclaimer_poll_seconds"),
        ({"consumer_id": "w", "ack_timeout_seconds": 0}, "ack_timeout_seconds"),
        ({"consumer_id": "w", "ack_timeout_seconds": float("inf")}, "ack_timeout_seconds"),
    ]:
        with pytest.raises(ValueError, match=pattern):
            SessionWorkerConsumer(queue, reclaim, claimer, executor, **kwargs)

    consumer = SessionWorkerConsumer(
        queue,
        reclaim,
        claimer,
        executor,
        consumer_id="w",
        concurrency=1,
    )
    stop = asyncio.Event()
    stop.set()
    assert await consumer._take_permit(stop) is None
    assert consumer.concurrency_available == 1

    permit = await consumer._take_permit(asyncio.Event())
    assert permit is not None
    waiting = asyncio.create_task(consumer._take_permit(stop_event := asyncio.Event()))
    await asyncio.sleep(0)
    stop_event.set()
    permit.release()
    assert await waiting is None
    assert consumer.concurrency_available == 1


@pytest.mark.asyncio
async def test_worker_receive_and_reclaim_error_and_empty_paths() -> None:
    stop = asyncio.Event()
    queue = ErrorOnceQueue(stop)
    consumer = SessionWorkerConsumer(
        queue,
        SilentReclaimer(),
        SilentClaimer(),
        SilentExecutor(),
        consumer_id="w",
    )
    await consumer._receive_loop(stop)
    assert consumer.concurrency_available == 10

    stop = asyncio.Event()
    queue = CancelQueue()
    consumer = SessionWorkerConsumer(
        queue,
        SilentReclaimer(),
        SilentClaimer(),
        SilentExecutor(),
        consumer_id="w",
    )
    with pytest.raises(asyncio.CancelledError):
        await consumer._receive_loop(stop)
    assert consumer.concurrency_available == 10

    stop = asyncio.Event()
    consumer = SessionWorkerConsumer(
        EmptyQueue(stop),
        SilentReclaimer(),
        SilentClaimer(),
        SilentExecutor(),
        consumer_id="w",
    )
    await consumer._receive_loop(stop)

    stop = asyncio.Event()
    consumer = SessionWorkerConsumer(
        SilentQueue(),
        ErrorOnceReclaimer(stop),
        SilentClaimer(),
        SilentExecutor(),
        consumer_id="w",
    )
    await consumer._reclaim_loop(stop)

    # If the receive loop currently owns the only permit, the reclaimer can
    # observe shutdown immediately after it acquires that permit.
    consumer = SessionWorkerConsumer(
        SilentQueue(),
        SilentReclaimer(),
        SilentClaimer(),
        SilentExecutor(),
        consumer_id="w",
        concurrency=1,
    )
    held = await consumer._take_permit(asyncio.Event())
    assert held is not None
    stop = asyncio.Event()
    waiting_reclaimer = asyncio.create_task(consumer._reclaim_loop(stop))
    await asyncio.sleep(0)
    stop.set()
    held.release()
    await waiting_reclaimer

    stop = asyncio.Event()
    consumer = SessionWorkerConsumer(
        SilentQueue(),
        CancelReclaimer(),
        SilentClaimer(),
        SilentExecutor(),
        consumer_id="w",
    )
    with pytest.raises(asyncio.CancelledError):
        await consumer._reclaim_loop(stop)
    assert consumer.concurrency_available == 10

    stop = asyncio.Event()
    consumer = SessionWorkerConsumer(
        SilentQueue(),
        EmptyReclaimer(stop),
        SilentClaimer(),
        SilentExecutor(),
        consumer_id="w",
        reclaimer_poll_seconds=0,
    )
    await consumer._reclaim_loop(stop)


@pytest.mark.asyncio
async def test_worker_task_creation_completion_and_outer_error_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumer = SessionWorkerConsumer(
        SilentQueue(),
        SilentReclaimer(),
        SilentClaimer(),
        SilentExecutor(),
        consumer_id="w",
    )
    permit = await consumer._take_permit(asyncio.Event())
    assert permit is not None

    def fail_create(*_args: Any, **_kwargs: Any) -> Any:
        raise RuntimeError("create failed")

    monkeypatch.setattr(worker_module.asyncio, "create_task", fail_create)
    with pytest.raises(RuntimeError, match="create failed"):
        consumer._start_delivery(delivery(), permit)
    assert consumer.concurrency_available == 10

    async def boom(_: SessionReadyDelivery) -> None:
        raise RuntimeError("unexpected")

    monkeypatch.undo()
    permit = await consumer._take_permit(asyncio.Event())
    assert permit is not None
    monkeypatch.setattr(consumer, "_claim_ack_execute", boom)
    await consumer._process_delivery(delivery(), permit)
    assert consumer.concurrency_available == 10

    async def succeeds() -> None:
        return None

    success = asyncio.create_task(succeeds())
    await success
    consumer._finish_delivery_task(success)

    async def fails() -> None:
        raise RuntimeError("task failed")

    failed = asyncio.create_task(fails())
    await asyncio.gather(failed, return_exceptions=True)
    consumer._finish_delivery_task(failed)


@pytest.mark.asyncio
async def test_worker_claim_ack_cancel_and_done_inflight_paths() -> None:
    consumer = SessionWorkerConsumer(
        SilentQueue(),
        SilentReclaimer(),
        CancelClaimer(),
        SilentExecutor(),
        consumer_id="w",
    )
    with pytest.raises(asyncio.CancelledError):
        await consumer._claim_ack_execute(delivery())

    queue = AckFalseQueue()
    consumer = SessionWorkerConsumer(
        queue,
        SilentReclaimer(),
        ClaimedClaimer(),
        SilentExecutor(),
        consumer_id="w",
    )
    await consumer._claim_ack_execute(delivery())
    assert queue.ack_calls == 1

    queue = CancelAckQueue()
    consumer = SessionWorkerConsumer(
        queue,
        SilentReclaimer(),
        ClaimedClaimer(),
        SilentExecutor(),
        consumer_id="w",
    )
    with pytest.raises(asyncio.CancelledError):
        await consumer._ack_safely(delivery())

    consumer = SessionWorkerConsumer(
        SilentQueue(),
        SilentReclaimer(),
        SilentClaimer(),
        SilentExecutor(),
        consumer_id="w",
        concurrency=1,
    )
    permit = await consumer._take_permit(asyncio.Event())
    assert permit is not None
    done = asyncio.create_task(asyncio.sleep(0))
    await done
    consumer._in_flight[done] = permit
    await consumer._cancel_in_flight()
    assert consumer.concurrency_available == 1


@pytest.mark.asyncio
async def test_worker_reclaimer_starts_a_delivery_and_releases_its_slot() -> None:
    stop = asyncio.Event()
    consumer = SessionWorkerConsumer(
        SilentQueue(),
        OneDeliveryReclaimer(stop),
        ClaimedClaimer(),
        SilentExecutor(),
        consumer_id="w",
    )
    await consumer._reclaim_loop(stop)
    await consumer._cancel_in_flight()
    assert consumer.concurrency_available == 10


@pytest.mark.asyncio
async def test_worker_run_cancels_source_tasks() -> None:
    queue = BlockingQueue()
    reclaim = BlockingReclaimer()
    consumer = SessionWorkerConsumer(
        queue, reclaim, SilentClaimer(), SilentExecutor(), consumer_id="w"
    )
    task = asyncio.create_task(consumer.run())
    await asyncio.wait_for(asyncio.gather(queue.started.wait(), reclaim.started.wait()), timeout=1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=1)


@pytest.mark.asyncio
async def test_worker_run_cancels_peer_source_after_source_failure() -> None:
    peer_started = asyncio.Event()
    peer_cancelled = asyncio.Event()

    async def fail_source(_stop: asyncio.Event) -> None:
        raise RuntimeError("source failed")

    async def blocking_source(_stop: asyncio.Event) -> None:
        peer_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            peer_cancelled.set()
            raise

    consumer = SessionWorkerConsumer(
        SilentQueue(), SilentReclaimer(), SilentClaimer(), SilentExecutor(), consumer_id="w"
    )
    consumer._receive_loop = fail_source  # type: ignore[method-assign]
    consumer._reclaim_loop = blocking_source  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="source failed"):
        await consumer.run()
    assert peer_started.is_set()
    assert peer_cancelled.is_set()


@pytest.mark.asyncio
async def test_wait_or_yield_zero_and_timeout() -> None:
    await _wait_or_yield(asyncio.Event(), 0)
    await _wait_or_yield(asyncio.Event(), 0.001)


class SilentQueue:
    async def receive_new(self, **_kwargs: Any) -> tuple[SessionReadyDelivery, ...]:
        return ()

    async def ack(self, _delivery: SessionReadyDelivery) -> bool:
        return True


class EmptyQueue(SilentQueue):
    def __init__(self, stop: asyncio.Event) -> None:
        self.stop = stop

    async def receive_new(self, **_kwargs: Any) -> tuple[SessionReadyDelivery, ...]:
        self.stop.set()
        return ()


class ErrorOnceQueue(SilentQueue):
    def __init__(self, stop: asyncio.Event) -> None:
        self.stop = stop

    async def receive_new(self, **_kwargs: Any) -> tuple[SessionReadyDelivery, ...]:
        self.stop.set()
        raise RuntimeError("receive failed")


class CancelQueue(SilentQueue):
    async def receive_new(self, **_kwargs: Any) -> tuple[SessionReadyDelivery, ...]:
        raise asyncio.CancelledError()


class SilentReclaimer:
    async def reclaim(self) -> tuple[SessionReadyDelivery, ...]:
        return ()


class EmptyReclaimer(SilentReclaimer):
    def __init__(self, stop: asyncio.Event) -> None:
        self.stop = stop

    async def reclaim(self) -> tuple[SessionReadyDelivery, ...]:
        self.stop.set()
        return ()


class OneDeliveryReclaimer(SilentReclaimer):
    def __init__(self, stop: asyncio.Event) -> None:
        self.stop = stop

    async def reclaim(self) -> tuple[SessionReadyDelivery, ...]:
        self.stop.set()
        return (delivery("reclaimed"),)


class ErrorOnceReclaimer(SilentReclaimer):
    def __init__(self, stop: asyncio.Event) -> None:
        self.stop = stop

    async def reclaim(self) -> tuple[SessionReadyDelivery, ...]:
        self.stop.set()
        raise RuntimeError("reclaim failed")


class CancelReclaimer(SilentReclaimer):
    async def reclaim(self) -> tuple[SessionReadyDelivery, ...]:
        raise asyncio.CancelledError()


class SilentClaimer:
    async def claim(self, _message: SessionReady) -> Any:
        return type("Claim", (), {"status": MailboxClaimStatus.RUNNING})()


class ClaimedClaimer:
    async def claim(self, _message: SessionReady) -> Any:
        return type("Claim", (), {"status": MailboxClaimStatus.CLAIMED})()


class CancelClaimer:
    async def claim(self, _message: SessionReady) -> Any:
        raise asyncio.CancelledError()


class SilentExecutor:
    async def execute(self, _claim: Any) -> None:
        return None


class AckFalseQueue(SilentQueue):
    def __init__(self) -> None:
        self.ack_calls = 0

    async def ack(self, _delivery: SessionReadyDelivery) -> bool:
        self.ack_calls += 1
        return False


class CancelAckQueue(SilentQueue):
    async def ack(self, _delivery: SessionReadyDelivery) -> bool:
        raise asyncio.CancelledError()


class BlockingQueue(SilentQueue):
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def receive_new(self, **_kwargs: Any) -> tuple[SessionReadyDelivery, ...]:
        self.started.set()
        await asyncio.Event().wait()
        return ()


class BlockingReclaimer(SilentReclaimer):
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def reclaim(self) -> tuple[SessionReadyDelivery, ...]:
        self.started.set()
        await asyncio.Event().wait()
        return ()
