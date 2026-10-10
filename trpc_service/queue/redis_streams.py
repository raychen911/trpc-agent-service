"""Redis Streams transport; PostgreSQL outbox remains the source of truth."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any, Protocol

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
from trpc_service.storage.models import OutboxRecord

_PUBLISH_LUA = """
local dedupe = KEYS[1]
local stream = KEYS[2]
if redis.call('SET', dedupe, '1', 'NX', 'EX', ARGV[1]) then
  return redis.call('XADD', stream, '*',
    'outbox_id', ARGV[2],
    'tenant_id', ARGV[3],
    'event_type', ARGV[4],
    'aggregate_id', ARGV[5],
    'payload', ARGV[6],
    'trace_headers', ARGV[7])
end
return ''
"""

_HEARTBEAT_MIN_DELAY_SECONDS = 0.1
_BUSY_RETRY_MIN_DELAY_SECONDS = 0.1
_BUSY_RETRY_MAX_DELAY_SECONDS = 0.25

_REFRESH_OWNER_LUA = """
local pending = redis.call('XPENDING', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 then
  return 0
end
if pending[1][2] ~= ARGV[3] then
  return 0
end
local claimed = redis.call('XCLAIM', KEYS[1], ARGV[1], ARGV[3], 0, ARGV[2], 'JUSTID')
return (#claimed > 0) and 1 or 0
"""


class RedisStreamsClient(Protocol):
    async def xgroup_create(self, *args: Any, **kwargs: Any) -> Any: ...

    async def eval(self, script: str, key_count: int, *args: Any) -> Any: ...

    async def xreadgroup(self, *args: Any, **kwargs: Any) -> Any: ...

    async def xautoclaim(self, *args: Any, **kwargs: Any) -> Any: ...

    async def xclaim(self, *args: Any, **kwargs: Any) -> Any: ...

    async def xack(self, *args: Any, **kwargs: Any) -> Any: ...

    async def xadd(self, *args: Any, **kwargs: Any) -> Any: ...

    async def aclose(self) -> None: ...


@dataclass(frozen=True, slots=True)
class QueueMessage:
    stream_id: str
    outbox_id: str
    tenant_id: str
    event_type: str
    aggregate_id: str
    payload: dict[str, Any]
    trace_headers: dict[str, str]


class RedisStreamQueue:
    def __init__(
        self,
        redis: RedisStreamsClient,
        *,
        stream: str = "trpc:inbound:v1",
        group: str = "trpc-workers-v1",
        dedupe_seconds: int = 7 * 24 * 3600,
        reclaim_after_ms: int = 60_000,
    ) -> None:
        self._redis = redis
        self.stream = stream
        self.group = group
        self._dedupe_seconds = dedupe_seconds
        self._reclaim_after_ms = reclaim_after_ms
        self._reclaim_cursors: dict[str, str] = {}

    async def ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(self.stream, self.group, id="0", mkstream=True)
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def publish(self, record: OutboxRecord) -> str | None:
        value = await self._redis.eval(
            _PUBLISH_LUA,
            2,
            f"trpc:published:{record.outbox_id}",
            self.stream,
            self._dedupe_seconds,
            record.outbox_id,
            record.tenant_id,
            record.event_type,
            record.aggregate_id,
            json.dumps(record.payload, separators=(",", ":")),
            json.dumps(record.trace_headers, separators=(",", ":")),
        )
        if not value:
            return None
        return _text(value)

    async def consume(
        self,
        *,
        consumer: str,
        count: int = 10,
        block_ms: int = 5_000,
        active_stream_ids: Collection[str] = (),
    ) -> tuple[QueueMessage, ...]:
        reclaim_cursor = self._reclaim_cursors.get(consumer, "0-0")
        claimed = await self._redis.xautoclaim(
            self.stream,
            self.group,
            consumer,
            min_idle_time=self._reclaim_after_ms,
            start_id=reclaim_cursor,
            count=count,
        )
        if claimed:
            self._reclaim_cursors[consumer] = _text(claimed[0])
        else:
            self._reclaim_cursors[consumer] = "0-0"
        claimed_entries = claimed[1] if len(claimed) > 1 else ()
        if claimed_entries:
            claimed_messages = _decode_rows(((self.stream, claimed_entries),))
            active = set(active_stream_ids)
            available = tuple(
                message for message in claimed_messages if message.stream_id not in active
            )
            if available:
                return available

        read_kwargs: dict[str, Any] = {"count": count}
        # Redis interprets BLOCK 0 as an infinite wait.  Treat a non-positive
        # caller value as an explicit non-blocking read instead.
        if block_ms > 0:
            read_kwargs["block"] = block_ms
        rows = await self._redis.xreadgroup(
            self.group,
            consumer,
            streams={self.stream: ">"},
            **read_kwargs,
        )
        return _decode_rows(rows)

    async def ack(self, message: QueueMessage) -> None:
        await self._redis.xack(self.stream, self.group, message.stream_id)

    async def requeue(self, message: QueueMessage) -> None:
        await self._redis.xadd(
            self.stream,
            {
                "outbox_id": message.outbox_id,
                "tenant_id": message.tenant_id,
                "event_type": message.event_type,
                "aggregate_id": message.aggregate_id,
                "payload": json.dumps(message.payload, separators=(",", ":")),
                "trace_headers": json.dumps(message.trace_headers, separators=(",", ":")),
            },
        )
        await self.ack(message)

    async def defer(
        self,
        message: QueueMessage,
        consumer_id: str,
        *,
        retry_delay_seconds: float = _BUSY_RETRY_MIN_DELAY_SECONDS,
    ) -> bool:
        """Refresh a busy delivery only while this consumer still owns it."""

        delay = min(
            max(retry_delay_seconds, _BUSY_RETRY_MIN_DELAY_SECONDS),
            _BUSY_RETRY_MAX_DELAY_SECONDS,
        )
        await asyncio.sleep(delay)
        return await self._refresh_owner(message, consumer_id)

    async def heartbeat(
        self,
        message: QueueMessage,
        consumer_id: str,
        stop_event: asyncio.Event,
    ) -> bool:
        """Keep a long-running delivery owned until its worker turn ends."""

        interval_seconds = max(self._reclaim_after_ms / 3_000, _HEARTBEAT_MIN_DELAY_SECONDS)
        while True:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except TimeoutError:
                if not await self._refresh_owner(message, consumer_id):
                    return False
            else:
                return True

    async def _refresh_owner(self, message: QueueMessage, consumer_id: str) -> bool:
        refreshed = await self._redis.eval(
            _REFRESH_OWNER_LUA,
            1,
            self.stream,
            self.group,
            message.stream_id,
            consumer_id,
        )
        return refreshed in (True, 1, b"1", "1")

    async def close(self) -> None:
        await self._redis.aclose()


def _text(value: str | bytes) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _decode_rows(rows: Any) -> tuple[QueueMessage, ...]:
    if not rows:
        return ()
    return tuple(_decode_entry(entry) for _, entries in rows for entry in entries)


def _decode_entry(entry: Any) -> QueueMessage:
    stream_id, fields = entry
    normalized = {_text(key): _text(value) for key, value in fields.items()}
    return QueueMessage(
        stream_id=_text(stream_id),
        outbox_id=normalized["outbox_id"],
        tenant_id=normalized["tenant_id"],
        event_type=normalized["event_type"],
        aggregate_id=normalized["aggregate_id"],
        payload=json.loads(normalized["payload"]),
        trace_headers=json.loads(normalized["trace_headers"]),
    )


__all__ = [
    "SESSION_READY_GROUP_V2",
    "SESSION_READY_STREAM_V2",
    "QueueMessage",
    "RedisStreamQueue",
    "SessionReady",
    "SessionReadyCodec",
    "SessionReadyDelivery",
    "SessionReadyQueue",
    "SessionReadyReclaimer",
]
