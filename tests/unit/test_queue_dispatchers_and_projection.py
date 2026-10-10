from __future__ import annotations

import asyncio
from datetime import timedelta
from types import SimpleNamespace

import asyncpg
import pytest
from cryptography.exceptions import InvalidTag
from redis.exceptions import ResponseError

from tests.conftest import envelope, repository, tenant_config
from trpc_service.agent.worker import ProcessStatus, WorkerResult
from trpc_service.channels.dispatcher import ChannelDispatcher
from trpc_service.channels.envelopes import (
    DeliveryReceipt,
    DeliveryStatus,
    OutboundEnvelope,
)
from trpc_service.config.settings import SchedulerVersion
from trpc_service.metrics.prometheus import CALLBACKS, QUEUE_DEPTH
from trpc_service.queue.dispatcher import OutboxDispatcher
from trpc_service.queue.emergency import EmergencyQueue, EmergencyQueueDrainer
from trpc_service.queue.redis_streams import QueueMessage, RedisStreamQueue
from trpc_service.queue.worker_consumer import WorkerConsumer
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import OutboxRecord, SessionSnapshot
from trpc_service.storage.projector import PostTurnProjector
from trpc_service.tenant.models import Channel


def record(event_type: str = "inbound.accepted", *, attempts: int = 0) -> OutboxRecord:
    return OutboxRecord(
        outbox_id="outbox-1",
        tenant_id="tenant-a",
        event_type=event_type,
        aggregate_id="inbound-1",
        payload={"inbound_id": "inbound-1"},
        trace_headers={"traceparent": "trace"},
        attempts=attempts,
    )


class RedisFake:
    def __init__(self) -> None:
        self.group_error = None
        self.eval_value = b"1-0"
        self.acked = []
        self.added = []
        self.closed = False
        self.rows = []
        self.claimed = (b"0-0", [], [])
        self.eval_calls = []
        self.eval_results = []
        self.eval_events = []
        self.xreadgroup_calls = []
        self.xautoclaim_calls = []
        self.xreadgroup_rows = []
        self.xautoclaim_results = []

    async def xgroup_create(self, *args, **kwargs):
        if self.group_error:
            raise self.group_error

    async def eval(self, *args):
        self.eval_calls.append(args)
        self.eval_events.append("eval")
        if self.eval_results:
            return self.eval_results.pop(0)
        return self.eval_value

    async def xreadgroup(self, *args, **kwargs):
        self.xreadgroup_calls.append((args, kwargs))
        if self.xreadgroup_rows:
            return self.xreadgroup_rows.pop(0)
        return self.rows

    async def xautoclaim(self, *args, **kwargs):
        self.xautoclaim_calls.append((args, kwargs))
        if self.xautoclaim_results:
            return self.xautoclaim_results.pop(0)
        return self.claimed

    async def xack(self, *args):
        self.acked.append(args)

    async def xadd(self, *args):
        self.added.append(args)
        return b"2-0"

    async def aclose(self):
        self.closed = True


@pytest.mark.asyncio
async def test_redis_stream_queue_contract(monkeypatch) -> None:
    redis = RedisFake()
    queue = RedisStreamQueue(redis, stream="stream", group="group")
    await queue.ensure_group()
    redis.group_error = ResponseError("BUSYGROUP already exists")
    await queue.ensure_group()
    redis.group_error = ResponseError("denied")
    with pytest.raises(ResponseError):
        await queue.ensure_group()
    redis.group_error = None

    assert await queue.publish(record()) == "1-0"
    redis.eval_value = b""
    assert await queue.publish(record()) is None
    redis.rows = [
        (
            b"stream",
            [
                (
                    b"3-0",
                    {
                        b"outbox_id": b"o",
                        b"tenant_id": b"t",
                        b"event_type": b"inbound.accepted",
                        b"aggregate_id": b"a",
                        b"payload": b'{"x":1}',
                        b"trace_headers": b'{"traceparent":"p"}',
                    },
                )
            ],
        )
    ]
    messages = await queue.consume(consumer="worker")
    assert messages[0].payload == {"x": 1}
    await queue.ack(messages[0])

    async def no_sleep(_delay: float) -> None:
        return None

    monkeypatch.setattr(asyncio, "sleep", no_sleep)
    redis.eval_results = [1]
    assert await queue.defer(messages[0], consumer_id="worker") is True
    await queue.requeue(messages[0])
    assert len(redis.acked) == 2 and redis.added

    redis.claimed = (
        b"0-0",
        [
            (
                b"4-0",
                {
                    b"outbox_id": b"claimed",
                    b"tenant_id": b"t",
                    b"event_type": b"inbound.accepted",
                    b"aggregate_id": b"a",
                    b"payload": b"{}",
                    b"trace_headers": b"{}",
                },
            )
        ],
        [],
    )
    assert (await queue.consume(consumer="recovery"))[0].outbox_id == "claimed"
    await queue.close()
    assert redis.closed


@pytest.mark.asyncio
async def test_redis_stream_queue_defer_refreshes_only_when_current_owner_matches(
    monkeypatch,
) -> None:
    redis = RedisFake()
    queue = RedisStreamQueue(redis, stream="stream", group="group")
    message = QueueMessage("1-0", "outbox-1", "tenant-a", "inbound.accepted", "a", {}, {})
    sleep_calls: list[float] = []

    async def fake_sleep(delay: float) -> None:
        sleep_calls.append(delay)
        redis.eval_events.append("sleep")

    monkeypatch.setattr(asyncio, "sleep", fake_sleep)
    # The Lua script returns one for a still-owned pending entry and zero for
    # an owner change or an already acknowledged entry.
    redis.eval_results = [1, 0, 0]
    assert await queue.defer(message, consumer_id="worker", retry_delay_seconds=0.1) is True
    assert await queue.defer(message, consumer_id="worker", retry_delay_seconds=0.2) is False
    assert await queue.defer(message, consumer_id="worker", retry_delay_seconds=0.9) is False

    assert redis.acked == []
    assert redis.added == []
    assert len(sleep_calls) == 3
    assert sleep_calls == [0.1, 0.2, 0.25]
    assert redis.eval_events == ["sleep", "eval", "sleep", "eval", "sleep", "eval"]
    assert len(redis.eval_calls) == 3

    for args in redis.eval_calls:
        script, key_count, *values = args
        assert key_count >= 1
        assert "XPENDING" in script.upper()
        assert "XCLAIM" in script.upper()
        assert "JUSTID" in script.upper()
        assert "pending[1][2] ~= ARGV[3]" in script
        assert values == ["stream", "group", message.stream_id, "worker"]


@pytest.mark.asyncio
async def test_redis_stream_queue_reclaims_non_deferred_delivery_with_xautoclaim_cursor() -> None:
    redis = RedisFake()
    queue = RedisStreamQueue(redis)

    def row(stream_id: bytes, outbox_id: bytes) -> tuple[bytes, dict[bytes, bytes]]:
        return stream_id, {
            b"outbox_id": outbox_id,
            b"tenant_id": b"tenant-a",
            b"event_type": b"inbound.accepted",
            b"aggregate_id": b"aggregate-a",
            b"payload": b"{}",
            b"trace_headers": b"{}",
        }

    claimed_rows = [row(b"8-0", b"crashed")]
    redis.xautoclaim_results = [
        (b"9-0", claimed_rows, []),
        (b"0-0", [], []),
        (b"0-0", [], []),
    ]
    redis.xreadgroup_rows = [[], []]

    messages = await queue.consume(consumer="replacement", count=1, block_ms=0)
    assert [message.outbox_id for message in messages] == ["crashed"]
    assert redis.xautoclaim_calls[0][1]["start_id"] == "0-0"
    assert redis.xautoclaim_calls[0][1]["min_idle_time"] == 60_000

    assert await queue.consume(consumer="replacement", count=1, block_ms=0) == ()
    assert redis.xautoclaim_calls[1][1]["start_id"] == "9-0"

    assert await queue.consume(consumer="replacement", count=1, block_ms=0) == ()
    assert redis.xautoclaim_calls[2][1]["start_id"] == "0-0"
    assert redis.eval_calls == []
    assert redis.xreadgroup_calls
    assert all(call[1]["streams"] == {"trpc:inbound:v1": ">"} for call in redis.xreadgroup_calls)


@pytest.mark.asyncio
async def test_redis_stream_queue_does_not_scan_or_requeue_other_inflight_pel() -> None:
    redis = RedisFake()
    queue = RedisStreamQueue(redis, reclaim_after_ms=60_000)
    redis.xautoclaim_results = [(b"0-0", [], [])]
    redis.xreadgroup_rows = [[]]

    assert await queue.consume(consumer="worker", count=2, block_ms=0) == ()
    assert redis.eval_calls == []
    assert len(redis.xautoclaim_calls) == 1
    assert redis.xautoclaim_calls[0][1]["count"] == 2


@pytest.mark.asyncio
async def test_redis_stream_queue_filters_active_reclaimed_messages_without_ack() -> None:
    redis = RedisFake()
    queue = RedisStreamQueue(redis, reclaim_after_ms=60_000)

    def row(stream_id: bytes, outbox_id: bytes) -> tuple[bytes, dict[bytes, bytes]]:
        return stream_id, {
            b"outbox_id": outbox_id,
            b"tenant_id": b"tenant-a",
            b"event_type": b"inbound.accepted",
            b"aggregate_id": b"aggregate-a",
            b"payload": b"{}",
            b"trace_headers": b"{}",
        }

    redis.xautoclaim_results = [
        (
            b"0-0",
            [row(b"1-0", b"active"), row(b"2-0", b"available")],
            [],
        )
    ]

    messages = await queue.consume(
        consumer="worker",
        count=2,
        block_ms=0,
        active_stream_ids={"1-0"},
    )

    assert [message.outbox_id for message in messages] == ["available"]
    assert redis.acked == []
    assert redis.xreadgroup_calls == []


@pytest.mark.asyncio
async def test_redis_stream_queue_heartbeat_refreshes_multiple_times_then_stops(
    monkeypatch,
) -> None:
    redis = RedisFake()
    queue = RedisStreamQueue(redis, stream="stream", group="group", reclaim_after_ms=300)
    message = QueueMessage("1-0", "outbox-1", "tenant-a", "inbound.accepted", "a", {}, {})
    stop_event = asyncio.Event()
    redis.eval_results = [1, 1, 1]
    waits: list[float] = []

    async def fake_wait_for(awaitable, timeout: float):  # noqa: ASYNC109
        waits.append(timeout)
        awaitable.close()
        if len(waits) <= 3:
            raise TimeoutError
        stop_event.set()
        return None

    monkeypatch.setattr(asyncio, "wait_for", fake_wait_for)

    assert await queue.heartbeat(message, "worker", stop_event) is True
    assert len(redis.eval_calls) == 3
    assert waits == [pytest.approx(0.1)] * 4
    assert stop_event.is_set()
    for args in redis.eval_calls:
        assert args[2:] == ("stream", "group", message.stream_id, "worker")


@pytest.mark.asyncio
async def test_redis_stream_queue_heartbeat_stops_without_refresh_when_signaled() -> None:
    redis = RedisFake()
    queue = RedisStreamQueue(redis, stream="stream", group="group", reclaim_after_ms=300)
    message = QueueMessage("1-0", "outbox-1", "tenant-a", "inbound.accepted", "a", {}, {})
    stop_event = asyncio.Event()
    stop_event.set()

    assert await queue.heartbeat(message, "worker", stop_event) is True
    assert redis.eval_calls == []


@pytest.mark.asyncio
async def test_emergency_queue_encrypts_and_authenticates() -> None:
    redis = RedisFake()
    with pytest.raises(ValueError, match="32"):
        EmergencyQueue(redis, b"short")
    queue = EmergencyQueue(redis, b"e" * 32)
    redis.group_error = ResponseError("BUSYGROUP already exists")
    await queue.ensure_group()
    redis.group_error = ResponseError("denied")
    with pytest.raises(ResponseError):
        await queue.ensure_group()
    redis.group_error = None

    inbound = envelope()
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=b"e" * 32)
    route = await repo.resolve_binding("binding-unpredictable-a")
    assert route is not None
    prepared = runtime.prepare(route, inbound)
    assert await queue.enqueue(prepared) == "2-0"
    fields = redis.added[-1][1]
    assert (
        queue.decrypt(
            "binding-unpredictable-a",
            fields["nonce"],
            fields["payload"],
        )
        == prepared
    )
    with pytest.raises(InvalidTag):
        queue.decrypt("other", fields["nonce"], fields["payload"])

    redis.claimed = (b"0-0", [(b"3-0", fields)], [])
    claimed = await queue.consume(consumer="drainer")
    assert claimed[0].message_id == "3-0"
    redis.claimed = (b"0-0", [], [])
    assert await queue.consume(consumer="drainer", block_ms=1) == ()

    redis.rows = [(b"trpc:emergency:v1", [(b"4-0", fields)])]
    messages = await queue.consume(consumer="drainer")
    assert messages[0].prepared.context.config_version == prepared.context.config_version

    repo.add_config(tenant_config(version=2))
    repo.add_route(route.model_copy(update={"active_config_version": 2}))
    changed_route = await repo.resolve_binding("binding-unpredictable-a")
    assert changed_route is not None
    assert runtime.prepare(changed_route, envelope("message-2")).context.config_version == 2

    drainer = EmergencyQueueDrainer(repo, queue, consumer_id="drainer")
    assert await drainer.drain_once() == 1
    assert redis.acked[-1][-1] == "4-0"
    accepted = tuple(repo._acceptances.values())
    assert len(accepted) == 1 and accepted[0].context.config_version == 1

    # A reclaimed delivery is harmless: durable acceptance is idempotent and is
    # acknowledged only after PostgreSQL returns the existing record.
    assert await drainer.drain_once() == 1
    assert len(repo._acceptances) == 1


@pytest.mark.asyncio
async def test_emergency_queue_is_physically_separated_by_scheduler_version() -> None:
    redis = RedisFake()
    queue = EmergencyQueue(
        redis,
        b"e" * 32,
        scheduler_version=SchedulerVersion.V2,
    )
    repo = repository()
    runtime = TenantRuntime(
        repo,
        routing_key=b"e" * 32,
        scheduler_version=SchedulerVersion.V2,
    )
    route = await repo.resolve_binding("binding-unpredictable-a")
    assert route is not None

    await queue.enqueue(runtime.prepare(route, envelope()))
    assert redis.added[-1][0] == "trpc:emergency:v2"
    await queue.consume(consumer="v2-drainer", block_ms=1)
    assert redis.xautoclaim_calls[-1][0][:2] == (
        "trpc:emergency:v2",
        "trpc-emergency-drainers-v2",
    )


@pytest.mark.asyncio
async def test_emergency_drainer_does_not_ack_while_postgres_is_unavailable() -> None:
    redis = RedisFake()
    queue = EmergencyQueue(redis, b"e" * 32)
    repo = repository()
    route = await repo.resolve_binding("binding-unpredictable-a")
    assert route is not None
    prepared = TenantRuntime(repo, routing_key=b"e" * 32).prepare(route, envelope())
    await queue.enqueue(prepared)
    redis.rows = [(b"trpc:emergency:v1", [(b"5-0", redis.added[-1][1])])]

    class UnavailableRepository:
        async def get_config(self, *args, **kwargs):
            raise asyncpg.PostgresError("down")

        async def accept_inbound(self, **kwargs):
            raise AssertionError("accept must not run without the pinned config")

    drainer = EmergencyQueueDrainer(UnavailableRepository(), queue, consumer_id="drainer")
    with pytest.raises(asyncpg.PostgresError):
        await drainer.drain_once()
    assert redis.acked == []


class DispatchRepository:
    def __init__(self, records=()) -> None:
        self.records = records
        self.marked = []
        self.released = []
        self.receipts = []
        self.dead_lettered = []
        self.route = None
        self.snapshot_value = None

    async def claim_outbox(self, **kwargs):
        return tuple(self.records)

    async def mark_outbox_published(self, tenant_id, outbox_id, *, owner_id):
        self.marked.append((tenant_id, outbox_id, owner_id))

    async def release_outbox(self, tenant_id, outbox_id, **kwargs):
        self.released.append((tenant_id, outbox_id, kwargs))

    async def resolve_binding(self, binding_id):
        return self.route

    async def record_delivery(self, tenant_id, receipt, *, retrying=False):
        self.receipts.append((tenant_id, receipt, retrying))

    async def dead_letter_outbox(self, record, *, owner_id, reason):
        self.dead_lettered.append((record, owner_id, reason))

    async def get_session_snapshot(self, tenant_id, session_id):
        return self.snapshot_value


class PublishQueue:
    def __init__(self, fail=False):
        self.fail = fail
        self.values = []
        self.group = False

    async def publish(self, value):
        if self.fail:
            raise ConnectionError("redis down")
        self.values.append(value)

    async def ensure_group(self):
        self.group = True


@pytest.mark.asyncio
async def test_outbox_dispatch_success_failure_and_idle_run(monkeypatch) -> None:
    repo = DispatchRepository([record(attempts=3)])
    success = OutboxDispatcher(repo, PublishQueue(), owner_id="owner")
    assert await success.dispatch_once() == 1
    assert repo.marked

    repo = DispatchRepository([record(attempts=3)])
    failure = OutboxDispatcher(repo, PublishQueue(fail=True), owner_id="owner")
    assert await failure.dispatch_once() == 0
    assert repo.released[0][2]["delay"] == timedelta(seconds=8)

    empty_repo = DispatchRepository()
    idle = OutboxDispatcher(empty_repo, PublishQueue(), owner_id="owner")
    stop_event = asyncio.Event()

    async def stop_after_wait(event, _seconds):
        event.set()

    monkeypatch.setattr("trpc_service.queue.dispatcher._wait_or_stop", stop_after_wait)
    await idle.run(stop_event=stop_event)


def outbound_payload(channel=Channel.FEISHU):
    return OutboundEnvelope(
        outbound_id="out",
        tenant_id="tenant-a",
        binding_id="binding-unpredictable-a",
        channel=channel,
        target_id="user",
        session_id="session",
        text="reply",
    ).model_dump(mode="json")


class Adapter:
    def __init__(self, receipt):
        self.receipt = receipt

    async def send(self, envelope, binding):
        return self.receipt


@pytest.mark.asyncio
async def test_channel_dispatcher_all_delivery_decisions(monkeypatch) -> None:
    base = record("outbound.feishu.ready").model_copy(update={"payload": outbound_payload()})
    repo = DispatchRepository([base])
    dispatcher = ChannelDispatcher(repo, {}, owner_id="owner", event_type=base.event_type)
    assert await dispatcher.dispatch_once() == 0
    assert repo.released[-1][2]["error_type"] == "binding_unavailable"

    repo.route = repository()._routes["binding-unpredictable-a"]
    assert await dispatcher.dispatch_once() == 0
    assert repo.released[-1][2]["error_type"] == "adapter_unavailable"

    failed = DeliveryReceipt(
        outbound_id="out",
        status=DeliveryStatus.FAILED,
        retryable=True,
        provider_code="rate_limit",
    )
    dispatcher = ChannelDispatcher(
        repo,
        {Channel.FEISHU: Adapter(failed)},
        owner_id="owner",
        event_type=base.event_type,
    )
    assert await dispatcher.dispatch_once() == 0
    assert repo.receipts[-1][2] is True

    ambiguous = failed.model_copy(
        update={"status": DeliveryStatus.AMBIGUOUS, "provider_code": "transport_unknown"}
    )
    dispatcher = ChannelDispatcher(
        repo,
        {Channel.FEISHU: Adapter(ambiguous)},
        owner_id="owner",
        event_type=base.event_type,
    )
    assert await dispatcher.dispatch_once() == 1
    assert repo.dead_lettered[-1][2] == "transport_unknown"

    exhausted_repo = DispatchRepository([base.model_copy(update={"attempts": 5})])
    exhausted_repo.route = repository()._routes["binding-unpredictable-a"]
    dispatcher = ChannelDispatcher(
        exhausted_repo,
        {Channel.FEISHU: Adapter(failed)},
        owner_id="owner",
        event_type=base.event_type,
        max_attempts=5,
    )
    assert await dispatcher.dispatch_once() == 1
    assert exhausted_repo.dead_lettered[-1][2] == "rate_limit"

    exhausted_repo.records = []

    async def stop(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", stop)
    with pytest.raises(asyncio.CancelledError):
        await dispatcher.run()


class Projection:
    def __init__(self):
        self.values = []

    async def put_session(self, *args, **kwargs):
        self.values.append((args, kwargs))


@pytest.mark.asyncio
async def test_projector_missing_success_and_idle(monkeypatch) -> None:
    item = record("post_turn.ready").model_copy(update={"payload": {"session_id": "session"}})
    repo = DispatchRepository([item])
    projection = Projection()
    projector = PostTurnProjector(repo, projection, owner_id="projector")
    assert await projector.project_once() == 0
    assert repo.released[-1][2]["error_type"] == "session_not_visible"
    repo.snapshot_value = SessionSnapshot(
        tenant_id="tenant-a",
        app_id="app-unit",
        session_id="session",
        principal_id="principal",
        next_sequence=4,
    )
    assert await projector.project_once() == 1
    assert projection.values[-1][1]["sequence"] == 3

    repo.records = []

    async def stop(_seconds):
        raise asyncio.CancelledError

    monkeypatch.setattr(asyncio, "sleep", stop)
    with pytest.raises(asyncio.CancelledError):
        await projector.run()


@pytest.mark.asyncio
async def test_projector_stop_event_does_not_claim_new_records() -> None:
    item = record("post_turn.ready").model_copy(update={"payload": {"session_id": "session"}})
    repo = DispatchRepository([item])
    projector = PostTurnProjector(repo, Projection(), owner_id="projector")
    stop_event = asyncio.Event()
    stop_event.set()

    assert await projector.project_once(stop_event=stop_event) == 0
    assert repo.records == [item]


class ConsumerQueue:
    def __init__(self):
        self.acked = []
        self.deferred = []
        self.deferred_consumers = []
        self.defer_delays = []
        self.defer_results: list[bool] = []
        self.heartbeat_calls = []
        self.heartbeat_stops = []
        self.heartbeat_started = asyncio.Event()
        self.heartbeat_result = True
        self.heartbeat_error: BaseException | None = None
        self.heartbeat_error_on_cancel: BaseException | None = None
        self.group = False
        self.messages = []
        self.consume_counts = []

    async def ensure_group(self):
        self.group = True

    async def ack(self, message):
        self.acked.append(message)

    async def requeue(self, message):
        raise AssertionError("busy messages must not be duplicated")

    async def defer(self, message, *, consumer_id, retry_delay_seconds=0.1) -> bool:
        self.deferred.append(message)
        self.deferred_consumers.append(consumer_id)
        self.defer_delays.append(retry_delay_seconds)
        if self.defer_results:
            return self.defer_results.pop(0)
        return False

    async def heartbeat(self, message, *, consumer_id, stop_event) -> bool:
        self.heartbeat_calls.append((message, consumer_id))
        self.heartbeat_started.set()
        try:
            if self.heartbeat_error is not None:
                raise self.heartbeat_error
            await stop_event.wait()
        except asyncio.CancelledError:
            if self.heartbeat_error_on_cancel is not None:
                raise self.heartbeat_error_on_cancel from None
            raise
        self.heartbeat_stops.append(message)
        return self.heartbeat_result

    async def consume(self, *, consumer, count, active_stream_ids=()):
        del active_stream_ids
        self.consume_counts.append(count)
        if not self.messages:
            raise asyncio.CancelledError
        values, self.messages = self.messages, []
        return values


@pytest.mark.asyncio
async def test_worker_consumer_missing_busy_committed_and_run() -> None:
    message = QueueMessage("1", "o", "tenant-a", "inbound.accepted", "missing", {}, {})
    queue = ConsumerQueue()
    repo = SimpleNamespace(get_acceptance=lambda *args: None)

    async def missing(*args):
        return None

    repo.get_acceptance = missing
    worker = SimpleNamespace(process=None)
    consumer = WorkerConsumer(repo, queue, worker, consumer_id="worker")
    await consumer.process_message(message)
    assert queue.acked == [message]

    accepted = await TenantRuntimeForTest.acceptance()

    async def present(*args):
        return accepted

    repo.get_acceptance = present

    async def busy(_acceptance):
        return WorkerResult(status=ProcessStatus.BUSY)

    worker.process = busy
    await consumer.process_message(message)
    assert queue.deferred == [message]
    assert queue.deferred_consumers == ["worker"]

    async def committed(_acceptance):
        return WorkerResult(status=ProcessStatus.COMMITTED)

    worker.process = committed
    await consumer.process_message(message)
    assert queue.acked[-1] == message
    queue.messages = [message]
    with pytest.raises(asyncio.CancelledError):
        await consumer.run()
    assert queue.group
    assert queue.consume_counts == [1, 1]


@pytest.mark.asyncio
async def test_worker_consumer_retries_busy_in_same_process_message_when_defer_succeeds() -> None:
    message = QueueMessage("1", "o", "tenant-a", "inbound.accepted", "missing", {}, {})
    queue = ConsumerQueue()
    queue.defer_results = [True, True]
    accepted = await TenantRuntimeForTest.acceptance()

    async def present(*args):
        return accepted

    repo = SimpleNamespace(get_acceptance=present)
    statuses = iter((ProcessStatus.BUSY, ProcessStatus.BUSY, ProcessStatus.COMMITTED))
    process_calls = 0

    async def busy_then_committed(_acceptance):
        nonlocal process_calls
        process_calls += 1
        return WorkerResult(status=next(statuses))

    consumer = WorkerConsumer(
        repo,
        queue,
        SimpleNamespace(process=busy_then_committed),
        consumer_id="worker",
    )

    await consumer.process_message(message)

    assert process_calls == 3
    assert queue.deferred == [message, message]
    assert queue.deferred_consumers == ["worker", "worker"]
    assert queue.defer_delays == [0.1, 0.2]
    assert queue.acked == [message]


@pytest.mark.asyncio
async def test_worker_consumer_starts_and_stops_heartbeat_for_long_process() -> None:
    message = QueueMessage("1", "o", "tenant-a", "inbound.accepted", "missing", {}, {})
    queue = ConsumerQueue()
    accepted = await TenantRuntimeForTest.acceptance()
    started = asyncio.Event()
    release = asyncio.Event()

    async def present(*args):
        return accepted

    async def long_process(_acceptance):
        started.set()
        await release.wait()
        return WorkerResult(status=ProcessStatus.COMMITTED)

    consumer = WorkerConsumer(
        SimpleNamespace(get_acceptance=present),
        queue,
        SimpleNamespace(process=long_process),
        consumer_id="worker",
    )
    task = asyncio.create_task(consumer.process_message(message))
    await asyncio.wait_for(started.wait(), timeout=1)
    await asyncio.wait_for(queue.heartbeat_started.wait(), timeout=1)
    assert queue.heartbeat_calls == [(message, "worker")]
    assert queue.heartbeat_stops == []

    release.set()
    await asyncio.wait_for(task, timeout=1)
    assert queue.heartbeat_stops == [message]
    assert queue.acked == [message]


@pytest.mark.asyncio
async def test_worker_consumer_heartbeat_failure_does_not_ack_successful_process() -> None:
    message = QueueMessage("1", "o", "tenant-a", "inbound.accepted", "missing", {}, {})
    queue = ConsumerQueue()
    queue.heartbeat_error = RuntimeError("heartbeat failed")
    accepted = await TenantRuntimeForTest.acceptance()

    async def present(*args):
        return accepted

    async def successful_process(_acceptance):
        await asyncio.sleep(0)
        return WorkerResult(status=ProcessStatus.COMMITTED)

    consumer = WorkerConsumer(
        SimpleNamespace(get_acceptance=present),
        queue,
        SimpleNamespace(process=successful_process),
        consumer_id="worker",
    )

    with pytest.raises(RuntimeError, match="heartbeat failed"):
        await consumer.process_message(message)
    assert queue.acked == []


@pytest.mark.parametrize("body_error", [RuntimeError("worker failed"), asyncio.CancelledError()])
@pytest.mark.asyncio
async def test_worker_consumer_body_error_is_not_masked_by_heartbeat_cancel_error(
    body_error,
) -> None:
    message = QueueMessage("1", "o", "tenant-a", "inbound.accepted", "missing", {}, {})
    queue = ConsumerQueue()
    queue.heartbeat_error_on_cancel = RuntimeError("heartbeat cancellation failed")
    accepted = await TenantRuntimeForTest.acceptance()
    body_started = asyncio.Event()

    async def present(*args):
        return accepted

    async def failing_process(_acceptance):
        await asyncio.sleep(0)
        body_started.set()
        raise body_error

    consumer = WorkerConsumer(
        SimpleNamespace(get_acceptance=present),
        queue,
        SimpleNamespace(process=failing_process),
        consumer_id="worker",
    )

    if isinstance(body_error, asyncio.CancelledError):
        with pytest.raises(asyncio.CancelledError):
            await consumer.process_message(message)
    else:
        with pytest.raises(RuntimeError, match="worker failed"):
            await consumer.process_message(message)
    assert body_started.is_set()
    assert queue.acked == []


class RacingQueue:
    def __init__(self, heartbeat_mode: str) -> None:
        self.heartbeat_mode = heartbeat_mode
        self.acked: list[QueueMessage] = []
        self.heartbeat_started = asyncio.Event()
        self.heartbeat_cancelled = asyncio.Event()
        self.defer_started = asyncio.Event()
        self.defer_cancelled = asyncio.Event()

    async def ack(self, message: QueueMessage) -> None:
        self.acked.append(message)

    async def defer(
        self,
        _message: QueueMessage,
        *,
        consumer_id: str,
        retry_delay_seconds: float = 0.1,
    ) -> bool:
        del consumer_id, retry_delay_seconds
        self.defer_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.defer_cancelled.set()
            raise

    async def heartbeat(
        self,
        _message: QueueMessage,
        *,
        consumer_id: str,
        stop_event: asyncio.Event,
    ) -> bool:
        del consumer_id, stop_event
        self.heartbeat_started.set()
        if self.heartbeat_mode == "false":
            return False
        if self.heartbeat_mode == "error":
            await self.defer_started.wait()
            raise RuntimeError("heartbeat failed")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.heartbeat_cancelled.set()
            return True


@pytest.mark.asyncio
async def test_worker_consumer_heartbeat_loss_cancels_inflight_worker_without_ack() -> None:
    message = QueueMessage("1", "o", "tenant-a", "inbound.accepted", "missing", {}, {})
    queue = RacingQueue("false")
    accepted = await TenantRuntimeForTest.acceptance()
    worker_cancelled = asyncio.Event()

    async def present(*_args):
        return accepted

    async def process(_acceptance):
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            worker_cancelled.set()
            raise

    consumer = WorkerConsumer(
        SimpleNamespace(get_acceptance=present),
        queue,
        SimpleNamespace(process=process),
        consumer_id="worker",
    )

    await asyncio.wait_for(consumer.process_message(message), timeout=1)

    assert worker_cancelled.is_set()
    assert queue.acked == []


@pytest.mark.asyncio
async def test_worker_consumer_heartbeat_error_cancels_defer_without_ack() -> None:
    message = QueueMessage("1", "o", "tenant-a", "inbound.accepted", "missing", {}, {})
    queue = RacingQueue("error")
    accepted = await TenantRuntimeForTest.acceptance()
    statuses = iter((ProcessStatus.BUSY, ProcessStatus.COMMITTED))

    async def present(*_args):
        return accepted

    async def process(_acceptance):
        return WorkerResult(next(statuses))

    consumer = WorkerConsumer(
        SimpleNamespace(get_acceptance=present),
        queue,
        SimpleNamespace(process=process),
        consumer_id="worker",
    )

    with pytest.raises(RuntimeError, match="heartbeat failed"):
        await asyncio.wait_for(consumer.process_message(message), timeout=1)

    assert queue.defer_started.is_set()
    assert queue.defer_cancelled.is_set()
    assert queue.acked == []


@pytest.mark.asyncio
async def test_worker_consumer_body_error_wins_when_heartbeat_fails_concurrently() -> None:
    message = QueueMessage("1", "o", "tenant-a", "inbound.accepted", "missing", {}, {})
    queue = RacingQueue("error")
    accepted = await TenantRuntimeForTest.acceptance()

    async def present(*_args):
        return accepted

    async def process(_acceptance):
        queue.defer_started.set()
        raise RuntimeError("worker failed")

    consumer = WorkerConsumer(
        SimpleNamespace(get_acceptance=present),
        queue,
        SimpleNamespace(process=process),
        consumer_id="worker",
    )

    with pytest.raises(RuntimeError, match="worker failed"):
        await asyncio.wait_for(consumer.process_message(message), timeout=1)

    assert queue.acked == []


@pytest.mark.asyncio
async def test_worker_consumer_heartbeat_cleanup_cancels_stuck_task() -> None:
    message = QueueMessage("1", "o", "tenant-a", "inbound.accepted", "missing", {}, {})
    queue = RacingQueue("stuck")
    accepted = await TenantRuntimeForTest.acceptance()

    async def present(*_args):
        return accepted

    async def process(_acceptance):
        return WorkerResult(ProcessStatus.COMMITTED)

    consumer = WorkerConsumer(
        SimpleNamespace(get_acceptance=present),
        queue,
        SimpleNamespace(process=process),
        consumer_id="worker",
    )

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(consumer.process_message(message), timeout=1)

    assert queue.heartbeat_cancelled.is_set()
    assert queue.acked == []


class StreamingConsumerQueue:
    def __init__(self) -> None:
        self.messages: asyncio.Queue[QueueMessage] = asyncio.Queue()
        self.acked: list[QueueMessage] = []
        self.deferred: list[QueueMessage] = []
        self.deferred_consumers: list[str] = []
        self.heartbeat_calls: list[tuple[QueueMessage, str]] = []
        self.heartbeat_stops: list[QueueMessage] = []
        self.consume_counts: list[int] = []
        self.consume_waiting = asyncio.Event()
        self.consume_cancelled = asyncio.Event()
        self.settled = asyncio.Event()
        self.expected_settlements = 0

    async def ensure_group(self) -> None:
        return None

    async def consume(
        self,
        *,
        consumer: str,
        count: int,
        active_stream_ids=(),
    ) -> tuple[QueueMessage, ...]:
        del active_stream_ids
        self.consume_counts.append(count)
        if self.messages.empty():
            self.consume_waiting.set()
        try:
            first = await self.messages.get()
        except asyncio.CancelledError:
            self.consume_cancelled.set()
            raise
        self.consume_waiting.clear()
        values = [first]
        while len(values) < count:
            try:
                values.append(self.messages.get_nowait())
            except asyncio.QueueEmpty:
                break
        return tuple(values)

    async def ack(self, message: QueueMessage) -> None:
        self.acked.append(message)
        self._mark_settled()

    async def defer(
        self,
        message: QueueMessage,
        *,
        consumer_id: str,
        retry_delay_seconds=0.1,
    ) -> bool:
        self.deferred.append(message)
        self.deferred_consumers.append(consumer_id)
        del retry_delay_seconds
        self._mark_settled()
        return False

    async def heartbeat(
        self,
        message: QueueMessage,
        *,
        consumer_id: str,
        stop_event: asyncio.Event,
    ) -> bool:
        self.heartbeat_calls.append((message, consumer_id))
        await stop_event.wait()
        self.heartbeat_stops.append(message)
        return True

    def _mark_settled(self) -> None:
        if len(self.acked) + len(self.deferred) == self.expected_settlements:
            self.settled.set()


def consumer_message(aggregate_id: str) -> QueueMessage:
    return QueueMessage(
        aggregate_id,
        f"outbox-{aggregate_id}",
        "tenant-a",
        "inbound.accepted",
        aggregate_id,
        {},
        {},
    )


@pytest.mark.asyncio
async def test_worker_consumer_continuously_fills_bounded_slots() -> None:
    queue = StreamingConsumerQueue()
    queue.expected_settlements = 4
    messages = {name: consumer_message(name) for name in ("one", "two", "three", "four")}
    await queue.messages.put(messages["one"])

    async def get_acceptance(_tenant_id: str, aggregate_id: str) -> str:
        return aggregate_id

    releases = {name: asyncio.Event() for name in messages}
    starts = {name: asyncio.Event() for name in messages}
    active = 0
    maximum_active = 0

    async def process(acceptance: str) -> WorkerResult:
        nonlocal active, maximum_active
        active += 1
        maximum_active = max(maximum_active, active)
        starts[acceptance].set()
        try:
            await releases[acceptance].wait()
        finally:
            active -= 1
        status = ProcessStatus.BUSY if acceptance == "two" else ProcessStatus.COMMITTED
        return WorkerResult(status=status)

    consumer = WorkerConsumer(
        SimpleNamespace(get_acceptance=get_acceptance),
        queue,
        SimpleNamespace(process=process),
        consumer_id="worker",
        concurrency=3,
    )
    runner = asyncio.create_task(consumer.run())
    await asyncio.wait_for(starts["one"].wait(), timeout=1)
    await queue.messages.put(messages["two"])
    await asyncio.wait_for(starts["two"].wait(), timeout=1)
    await queue.messages.put(messages["three"])
    await asyncio.wait_for(starts["three"].wait(), timeout=1)
    assert queue.consume_counts[:3] == [3, 2, 1]

    await queue.messages.put(messages["four"])
    await asyncio.sleep(0)
    assert not starts["four"].is_set()
    releases["one"].set()
    await asyncio.wait_for(starts["four"].wait(), timeout=1)
    assert maximum_active == 3

    for name in ("two", "three", "four"):
        releases[name].set()
    await asyncio.wait_for(queue.settled.wait(), timeout=1)
    await asyncio.wait_for(queue.consume_waiting.wait(), timeout=1)
    runner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await runner
    assert {message.aggregate_id for message in queue.acked} == {"one", "three", "four"}
    assert [message.aggregate_id for message in queue.deferred] == ["two"]
    assert queue.deferred_consumers == ["worker"]
    assert queue.consume_cancelled.is_set()


@pytest.mark.asyncio
async def test_worker_consumer_propagates_failure_and_awaits_cancelled_tasks() -> None:
    queue = StreamingConsumerQueue()
    messages = {name: consumer_message(name) for name in ("ok", "boom", "blocked")}
    for message in messages.values():
        await queue.messages.put(message)

    async def get_acceptance(_tenant_id: str, aggregate_id: str) -> str:
        return aggregate_id

    blocked_cancelled = asyncio.Event()

    async def process(acceptance: str) -> WorkerResult:
        if acceptance == "ok":
            return WorkerResult(status=ProcessStatus.COMMITTED)
        if acceptance == "boom":
            await queue.consume_waiting.wait()
            raise RuntimeError("worker failed")
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            blocked_cancelled.set()
            raise

    consumer = WorkerConsumer(
        SimpleNamespace(get_acceptance=get_acceptance),
        queue,
        SimpleNamespace(process=process),
        consumer_id="worker",
        concurrency=3,
    )
    with pytest.raises(RuntimeError, match="worker failed"):
        await asyncio.wait_for(consumer.run(), timeout=1)
    assert [message.aggregate_id for message in queue.acked] == ["ok"]
    assert blocked_cancelled.is_set()
    assert queue.consume_cancelled.is_set()


@pytest.mark.parametrize("concurrency", [0, 257])
def test_worker_consumer_rejects_out_of_range_concurrency(concurrency: int) -> None:
    with pytest.raises(ValueError, match="between 1 and 256"):
        WorkerConsumer(
            SimpleNamespace(),
            SimpleNamespace(),
            SimpleNamespace(),
            consumer_id="worker",
            concurrency=concurrency,
        )


class TenantRuntimeForTest:
    @staticmethod
    async def acceptance():
        repo = repository()
        from trpc_service.runtime import TenantRuntime

        return await TenantRuntime(repo, routing_key=b"q" * 32).accept(
            "binding-unpredictable-a", envelope()
        )


def test_prometheus_metrics_are_tenant_label_safe() -> None:
    CALLBACKS.labels(channel="feishu", outcome="accepted").inc()
    QUEUE_DEPTH.labels(queue="inbound").set(1)
