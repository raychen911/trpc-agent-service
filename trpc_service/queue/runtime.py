"""Message queues with at-least-once delivery and dead-letter handling."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from ..tenant import InboundMessage, inbound_from_dict, inbound_to_dict


@dataclass(frozen=True)
class QueueDelivery:
    delivery_id: str
    message: InboundMessage
    attempts: int = 0


class InMemoryMessageQueue:
    def __init__(self, maxsize: int = 1000) -> None:
        self._queue: asyncio.Queue[QueueDelivery] = asyncio.Queue(maxsize=maxsize)
        self.dead_letters: list[QueueDelivery] = []

    async def ensure(self) -> None:
        return None

    async def put(self, message: InboundMessage) -> str:
        delivery = QueueDelivery(uuid4().hex, message)
        self._queue.put_nowait(delivery)
        return delivery.delivery_id

    def put_nowait(self, message: InboundMessage) -> str:
        delivery = QueueDelivery(uuid4().hex, message)
        self._queue.put_nowait(delivery)
        return delivery.delivery_id

    async def get(self, consumer: str = "local", block_ms: int = 250) -> QueueDelivery | None:
        _ = consumer
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=max(block_ms, 1) / 1000)
        except asyncio.TimeoutError:
            return None

    async def ack(self, delivery: QueueDelivery) -> None:
        _ = delivery
        self._queue.task_done()

    async def fail(self, delivery: QueueDelivery, error_type: str, max_attempts: int = 3) -> None:
        _ = error_type
        self._queue.task_done()
        retried = QueueDelivery(delivery.delivery_id, delivery.message, delivery.attempts + 1)
        if retried.attempts >= max_attempts:
            self.dead_letters.append(retried)
        else:
            await self._queue.put(retried)

    async def defer(self, delivery: QueueDelivery) -> None:
        self._queue.task_done()
        await asyncio.sleep(0.05)
        await self._queue.put(delivery)

    async def reclaim(self, consumer: str, count: int = 100) -> list[QueueDelivery]:
        _ = consumer, count
        return []

    async def heartbeat(self, delivery: QueueDelivery, consumer: str, stop: asyncio.Event) -> None:
        _ = delivery, consumer
        await stop.wait()

    async def join(self) -> None:
        await self._queue.join()

    def qsize(self) -> int:
        return self._queue.qsize()

    async def close(self) -> None:
        return None


class RedisStreamQueue:
    """Redis Streams consumer-group queue with pending reclaim and a DLQ."""

    def __init__(
        self,
        redis_url: str,
        *,
        stream: str = "trpc:inbound:v1",
        group: str = "trpc-workers-v1",
        dlq_stream: str = "trpc:inbound:dlq:v1",
        reclaim_after_ms: int = 60_000,
        stream_maxlen: int = 100_000,
        dlq_maxlen: int = 10_000,
        client: Any | None = None,
    ) -> None:
        if stream_maxlen < 1 or dlq_maxlen < 1:
            raise ValueError("Redis Stream max lengths must be positive")
        if client is None:
            from redis.asyncio import Redis

            client = Redis.from_url(redis_url, decode_responses=True)
        self.client = client
        self.stream = stream
        self.group = group
        self.dlq_stream = dlq_stream
        self.reclaim_after_ms = reclaim_after_ms
        self.stream_maxlen = stream_maxlen
        self.dlq_maxlen = dlq_maxlen
        self._group_ready = False
        self._depth = 0

    async def ensure(self) -> None:
        if self._group_ready:
            return
        try:
            await self.client.xgroup_create(self.stream, self.group, id="0-0", mkstream=True)
        except Exception as exc:  # redis-py and simple test doubles expose different error types.
            if "BUSYGROUP" not in str(exc):
                raise
        self._group_ready = True

    async def put(self, message: InboundMessage) -> str:
        await self.ensure()
        identifier = await self.client.xadd(
            self.stream,
            {
                "payload": json.dumps(inbound_to_dict(message), ensure_ascii=False, separators=(",", ":")),
                "attempts": "0",
            },
            maxlen=self.stream_maxlen,
            approximate=True,
        )
        self._depth += 1
        return _text(identifier)

    async def get(self, consumer: str, block_ms: int = 250) -> QueueDelivery | None:
        await self.ensure()
        reclaimed = await self.reclaim(consumer, count=1)
        if reclaimed:
            return reclaimed[0]
        rows = await self.client.xreadgroup(
            self.group,
            consumer,
            streams={self.stream: ">"},
            count=1,
            block=max(block_ms, 1),
        )
        deliveries = _decode_rows(rows)
        return deliveries[0] if deliveries else None

    async def ack(self, delivery: QueueDelivery) -> None:
        if await self._ack_and_delete(delivery.delivery_id):
            self._depth = max(0, self._depth - 1)

    async def fail(self, delivery: QueueDelivery, error_type: str, max_attempts: int = 3) -> None:
        attempts = delivery.attempts + 1
        fields = {
            "payload": json.dumps(inbound_to_dict(delivery.message), ensure_ascii=False, separators=(",", ":")),
            "attempts": str(attempts),
            "last_error": error_type,
            "source_id": delivery.delivery_id,
        }
        target = self.dlq_stream if attempts >= max_attempts else self.stream
        await self.client.xadd(
            target,
            fields,
            maxlen=self.dlq_maxlen if target == self.dlq_stream else self.stream_maxlen,
            approximate=True,
        )
        acknowledged = await self._ack_and_delete(delivery.delivery_id)
        if target == self.dlq_stream and acknowledged:
            self._depth = max(0, self._depth - 1)

    async def defer(self, delivery: QueueDelivery) -> None:
        await asyncio.sleep(0.05)
        await self.client.xadd(
            self.stream,
            {
                "payload": json.dumps(inbound_to_dict(delivery.message), ensure_ascii=False, separators=(",", ":")),
                "attempts": str(delivery.attempts),
                "source_id": delivery.delivery_id,
            },
            maxlen=self.stream_maxlen,
            approximate=True,
        )
        await self._ack_and_delete(delivery.delivery_id)

    async def _ack_and_delete(self, delivery_id: str) -> bool:
        acknowledged = int(await self.client.xack(self.stream, self.group, delivery_id))
        if acknowledged:
            try:
                await self.client.xdel(self.stream, delivery_id)
            except Exception:  # noqa: BLE001 - MAXLEN still bounds entries if best-effort cleanup is unavailable.
                return True
        return bool(acknowledged)

    async def reclaim(self, consumer: str, count: int = 100) -> list[QueueDelivery]:
        await self.ensure()
        result = await self.client.xautoclaim(
            self.stream,
            self.group,
            consumer,
            min_idle_time=self.reclaim_after_ms,
            start_id="0-0",
            count=count,
        )
        entries = result[1] if result and len(result) > 1 else []
        return _decode_entries(entries)

    async def heartbeat(self, delivery: QueueDelivery, consumer: str, stop: asyncio.Event) -> None:
        interval = max(0.1, self.reclaim_after_ms / 3000)
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
            except asyncio.TimeoutError:
                await self.client.xclaim(
                    self.stream,
                    self.group,
                    consumer,
                    min_idle_time=0,
                    message_ids=[delivery.delivery_id],
                    justid=True,
                )

    async def join(self) -> None:
        while True:
            pending = await self.client.xpending(self.stream, self.group)
            count = pending.get("pending", 0) if isinstance(pending, dict) else pending[0]
            if not count:
                return
            await asyncio.sleep(0.05)

    def qsize(self) -> int:
        return self._depth

    async def stats(self) -> dict[str, int]:
        await self.ensure()
        pending = await self.client.xpending(self.stream, self.group)
        pending_count = pending.get("pending", 0) if isinstance(pending, dict) else pending[0]
        return {
            "stream_length": int(await self.client.xlen(self.stream)),
            "pending": int(pending_count),
            "dead_letters": int(await self.client.xlen(self.dlq_stream)),
        }

    async def close(self) -> None:
        await self.client.aclose()


def _text(value: str | bytes) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _decode_rows(rows: Any) -> list[QueueDelivery]:
    return [delivery for _, entries in rows or [] for delivery in _decode_entries(entries)]


def _decode_entries(entries: Any) -> list[QueueDelivery]:
    values: list[QueueDelivery] = []
    for identifier, fields in entries or []:
        normalized = {_text(key): _text(value) for key, value in fields.items()}
        values.append(
            QueueDelivery(
                _text(identifier),
                inbound_from_dict(json.loads(normalized["payload"])),
                int(normalized.get("attempts", "0")),
            )
        )
    return values
