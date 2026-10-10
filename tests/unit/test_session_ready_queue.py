from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest
from redis.exceptions import ResponseError

from trpc_service.queue.session_ready import (
    SESSION_READY_GROUP_V2,
    SESSION_READY_STREAM_V2,
    SessionReady,
    SessionReadyCodec,
    SessionReadyDelivery,
    SessionReadyQueue,
    SessionReadyReclaimer,
)


def notice(**updates: object) -> SessionReady:
    values: dict[str, object] = {
        "event_id": "event-1",
        "tenant_id": "tenant-a",
        "session_id": "session-1",
        "generation": 4,
        "priority": 2,
        "trace_id": "trace-1",
        "created_at": datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
    }
    values.update(updates)
    return SessionReady(**values)


class FakeRedis:
    def __init__(self) -> None:
        self.group_error: ResponseError | None = None
        self.xadd_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.xreadgroup_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.xautoclaim_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.xack_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.xdel_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.read_rows: list[object] = []
        self.reclaim_results: list[object] = []
        self.xack_result = 1
        self.xdel_error: BaseException | None = None

    async def xgroup_create(self, *args: object, **kwargs: object) -> None:
        if self.group_error is not None:
            raise self.group_error

    async def xadd(self, *args: object, **kwargs: object) -> bytes:
        self.xadd_calls.append((args, kwargs))
        return b"1710000000000-0"

    async def xreadgroup(self, *args: object, **kwargs: object) -> list[object]:
        self.xreadgroup_calls.append((args, kwargs))
        return self.read_rows

    async def xack(self, *args: object, **kwargs: object) -> int:
        self.xack_calls.append((args, kwargs))
        return self.xack_result

    async def xdel(self, *args: object, **kwargs: object) -> int:
        self.xdel_calls.append((args, kwargs))
        if self.xdel_error is not None:
            raise self.xdel_error
        return 1

    async def xautoclaim(self, *args: object, **kwargs: object) -> object:
        self.xautoclaim_calls.append((args, kwargs))
        if self.reclaim_results:
            return self.reclaim_results.pop(0)
        return (b"0-0", [], [])


@pytest.mark.asyncio
async def test_session_ready_codec_has_exact_seven_fields() -> None:
    message = notice()
    encoded = SessionReadyCodec.encode(message)
    assert tuple(encoded) == (
        "event_id",
        "tenant_id",
        "session_id",
        "generation",
        "priority",
        "trace_id",
        "created_at",
    )
    assert SessionReadyCodec.decode(encoded) == message
    with pytest.raises(ValueError, match="extra=unexpected"):
        SessionReadyCodec.decode({**encoded, "unexpected": "no"})
    with pytest.raises(ValueError, match="missing=trace_id"):
        SessionReadyCodec.decode(
            {key: value for key, value in encoded.items() if key != "trace_id"}
        )
    with pytest.raises(ValueError, match="invalid session-ready:v2 field value"):
        SessionReadyCodec.decode({**encoded, "generation": "0"})


@pytest.mark.asyncio
async def test_session_ready_receive_is_v2_only_and_ack_is_explicit() -> None:
    redis = FakeRedis()
    with pytest.raises(ValueError, match="xdel_attempts"):
        SessionReadyQueue(redis, xdel_attempts=0)
    with pytest.raises(ValueError, match="xdel_retry_delay_seconds"):
        SessionReadyQueue(redis, xdel_retry_delay_seconds=float("inf"))
    queue = SessionReadyQueue(redis)
    await queue.ensure_group()
    redis.group_error = ResponseError("BUSYGROUP exists")
    await queue.ensure_group()
    redis.group_error = ResponseError("permission denied")
    with pytest.raises(ResponseError):
        await queue.ensure_group()
    redis.group_error = None

    message = notice()
    assert await queue.publish(message) == "1710000000000-0"
    fields = redis.xadd_calls[0][0][1]
    assert set(fields) == {
        "event_id",
        "tenant_id",
        "session_id",
        "generation",
        "priority",
        "trace_id",
        "created_at",
    }
    assert redis.xadd_calls[0][0][0] == SESSION_READY_STREAM_V2
    assert redis.xadd_calls[0][1] == {"id": "*"}

    redis.read_rows = [
        (
            SESSION_READY_STREAM_V2.encode(),
            [(b"1710000000000-0", {key.encode(): value.encode() for key, value in fields.items()})],
        )
    ]
    deliveries = await queue.receive(consumer="worker-a", count=1, block_ms=0)
    assert deliveries == (SessionReadyDelivery("1710000000000-0", message),)
    assert len(redis.xautoclaim_calls) == 0
    assert redis.xreadgroup_calls[0][1] == {
        "count": 1,
        "streams": {SESSION_READY_STREAM_V2: ">"},
    }

    assert await queue.ack(deliveries[0]) is True
    assert redis.xack_calls == [
        ((SESSION_READY_STREAM_V2, SESSION_READY_GROUP_V2, "1710000000000-0"), {})
    ]
    assert redis.xdel_calls == [((SESSION_READY_STREAM_V2, "1710000000000-0"), {})]


@pytest.mark.asyncio
async def test_session_ready_ack_does_not_delete_unacknowledged_pel_entry() -> None:
    redis = FakeRedis()
    redis.xack_result = 0
    queue = SessionReadyQueue(redis)
    item = SessionReadyDelivery("1710000000000-0", notice())

    assert await queue.ack(item) is False

    # XDEL is only safe after this ACK call removed the PEL entry.  A zero
    # XACK result may mean another consumer still owns the PEL entry, so the
    # stream entry must remain untouched.
    assert redis.xdel_calls == []


@pytest.mark.asyncio
async def test_session_ready_ack_retries_exact_delete_without_releasing_pel() -> None:
    redis = FakeRedis()
    redis.xdel_error = RuntimeError("temporary redis outage")
    queue = SessionReadyQueue(redis, xdel_attempts=3, xdel_retry_delay_seconds=0)
    item = SessionReadyDelivery("1710000000000-0", notice())

    # XACK is authoritative for the short hand-off.  A temporary XDEL outage
    # must not turn a successful ACK into a retryable business delivery.
    assert await queue.ack(item) is True
    assert len(redis.xdel_calls) == 3


@pytest.mark.asyncio
async def test_session_ready_reclaimer_preserves_cursor_and_never_acks() -> None:
    redis = FakeRedis()
    seen: list[SessionReadyDelivery] = []

    async def handle(delivery: SessionReadyDelivery) -> None:
        seen.append(delivery)

    message_a = notice(event_id="event-a")
    message_b = notice(event_id="event-b", generation=5)
    fields_a = SessionReadyCodec.encode(message_a)
    fields_b = SessionReadyCodec.encode(message_b)
    redis.reclaim_results = [
        (
            b"9-0",
            [(b"1-0", {key.encode(): value.encode() for key, value in fields_a.items()})],
            [],
        ),
        (
            b"0-0",
            [(b"2-0", {key.encode(): value.encode() for key, value in fields_b.items()})],
            [],
        ),
    ]
    reclaimer = SessionReadyReclaimer(
        redis,
        consumer="reclaimer-a",
        on_delivery=handle,
        min_idle_ms=60_000,
        count=10,
    )

    first = await reclaimer.reclaim_once()
    assert first[0].message == message_a
    assert reclaimer.cursor == "9-0"
    second = await reclaimer.reclaim_once()
    assert second[0].message == message_b
    assert reclaimer.cursor == "0-0"
    assert seen == [*first, *second]
    assert redis.xack_calls == []
    assert [call[1]["start_id"] for call in redis.xautoclaim_calls] == ["0-0", "9-0"]
    assert all(call[1]["min_idle_time"] == 60_000 for call in redis.xautoclaim_calls)


@pytest.mark.asyncio
async def test_session_ready_reclaimer_run_stops_without_busy_loop() -> None:
    redis = FakeRedis()
    stop = asyncio.Event()
    called = 0

    async def handle(_: SessionReadyDelivery) -> None:
        nonlocal called
        called += 1
        stop.set()

    fields = SessionReadyCodec.encode(notice())
    redis.reclaim_results = [
        (b"0-0", [(b"1-0", {key.encode(): value.encode() for key, value in fields.items()})], [])
    ]
    reclaimer = SessionReadyReclaimer(redis, consumer="reclaimer-a", on_delivery=handle)
    await reclaimer.run(stop)
    assert called == 1
