# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Agent task queue implementations for local and node-based deployment."""

from __future__ import annotations

import asyncio
import uuid
from dataclasses import dataclass
from typing import Protocol

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from redis.asyncio import Redis
from redis.asyncio import from_url
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from trpc_service.gateway.models import AgentRequest


class AgentTaskEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")

    task_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    request: AgentRequest
    idempotency_key: str
    attempts: int = 0


@dataclass(frozen=True, slots=True)
class QueueDelivery:
    receipt: str
    envelope: AgentTaskEnvelope


class AgentTaskQueue(Protocol):

    async def enqueue(self, envelope: AgentTaskEnvelope) -> str:
        """Persist a task and return its queue receipt."""

    async def receive(self, consumer: str, *, timeout_seconds: float = 5.0) -> QueueDelivery | None:
        """Receive one task using at-least-once delivery."""

    async def ack(self, delivery: QueueDelivery) -> None:
        """Acknowledge successful processing."""

    async def retry(self, delivery: QueueDelivery) -> None:
        """Schedule a failed delivery again."""

    async def reclaim_stale(self, consumer: str, *, idle_ms: int = 60000, count: int = 100) -> list[QueueDelivery]:
        """Claim and return abandoned pending entries after a Worker crash."""

    async def dead_letter(self, delivery: QueueDelivery, error_code: str) -> None:
        """Persist an exhausted task for inspection, then ACK its source entry."""

    async def unregister_consumer(self, consumer: str) -> bool:
        """Remove an idle consumer after its Worker has stopped claiming tasks."""


class InMemoryAgentTaskQueue:
    """Asyncio queue for one-process development."""

    def __init__(self, maxsize: int = 1024) -> None:
        self._queue: asyncio.Queue[AgentTaskEnvelope] = asyncio.Queue(maxsize=maxsize)
        self.dead_letters: list[tuple[AgentTaskEnvelope, str]] = []

    async def enqueue(self, envelope: AgentTaskEnvelope) -> str:
        await self._queue.put(envelope.model_copy(deep=True))
        return envelope.task_id

    async def receive(self, consumer: str, *, timeout_seconds: float = 5.0) -> QueueDelivery | None:
        del consumer
        try:
            envelope = await asyncio.wait_for(self._queue.get(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            return None
        return QueueDelivery(receipt=envelope.task_id, envelope=envelope)

    async def ack(self, delivery: QueueDelivery) -> None:
        del delivery
        self._queue.task_done()

    async def retry(self, delivery: QueueDelivery) -> None:
        self._queue.task_done()
        envelope = delivery.envelope.model_copy(update={"attempts": delivery.envelope.attempts + 1}, deep=True)
        await self._queue.put(envelope)

    async def reclaim_stale(self, consumer: str, *, idle_ms: int = 60000, count: int = 100) -> list[QueueDelivery]:
        del consumer, idle_ms, count
        return []

    async def dead_letter(self, delivery: QueueDelivery, error_code: str) -> None:
        self.dead_letters.append((delivery.envelope.model_copy(deep=True), error_code))
        self._queue.task_done()

    async def unregister_consumer(self, consumer: str) -> bool:
        del consumer
        return True


class RedisStreamAgentTaskQueue:
    """Redis Streams consumer-group queue shared by Gateway and Worker roles."""

    def __init__(self,
                 redis_url: str,
                 *,
                 stream: str = "trpc-service:agent-tasks",
                 group: str = "agent-workers",
                 dead_stream: str = "trpc-service:agent-tasks:dead",
                 client: Redis | None = None) -> None:
        self._redis = client or from_url(redis_url, decode_responses=True)
        self._owns_client = client is None
        self._stream = stream
        self._group = group
        self._dead_stream = dead_stream
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def _reset_connections(self) -> None:
        """Discard sockets invalidated by a Redis restart."""
        pool = getattr(self._redis, "connection_pool", None)
        disconnect = getattr(pool, "disconnect", None)
        if disconnect is None:
            return
        try:
            await disconnect(inuse_connections=True)
        except TypeError:  # lightweight test clients and older redis-py
            await disconnect()

    async def _with_reconnect(self, operation):
        """Retry one Redis operation after replacing a stale pooled socket."""
        try:
            return await operation()
        except (RedisConnectionError, RedisTimeoutError, ConnectionResetError, OSError):
            await self._reset_connections()
            return await operation()

    async def _initialize(self) -> None:
        if self._initialized:
            return
        async with self._init_lock:
            if self._initialized:
                return
            try:
                await self._redis.xgroup_create(self._stream, self._group, id="0", mkstream=True)
            except Exception as error:
                if "BUSYGROUP" not in str(error):
                    raise
            self._initialized = True

    async def enqueue(self, envelope: AgentTaskEnvelope) -> str:

        async def enqueue_once():
            await self._initialize()
            # A disconnect can happen after Redis committed XADD but before
            # the Gateway received its result. The task-id marker makes the
            # bounded reconnect retry return the original receipt instead of
            # creating a second Stream entry.
            return await self._redis.eval(
                """
                local existing = redis.call('GET', KEYS[2])
                if existing then return existing end
                local receipt = redis.call('XADD', KEYS[1], '*', 'payload', ARGV[1])
                redis.call('SET', KEYS[2], receipt, 'EX', ARGV[2])
                return receipt
                """,
                2,
                self._stream,
                f"{self._stream}:enqueued:{envelope.task_id}",
                envelope.model_dump_json(),
                604800,
            )

        return await self._with_reconnect(enqueue_once)

    async def receive(self, consumer: str, *, timeout_seconds: float = 5.0) -> QueueDelivery | None:
        await self._initialize()
        rows = await self._redis.xreadgroup(
            self._group,
            consumer,
            {self._stream: ">"},
            count=1,
            block=max(1, int(timeout_seconds * 1000)),
        )
        if not rows:
            return None
        _, messages = rows[0]
        receipt, fields = messages[0]
        return QueueDelivery(receipt=receipt, envelope=AgentTaskEnvelope.model_validate_json(fields["payload"]))

    async def ack(self, delivery: QueueDelivery) -> None:
        await self._redis.xack(self._stream, self._group, delivery.receipt)

    async def retry(self, delivery: QueueDelivery) -> None:
        envelope = delivery.envelope.model_copy(update={"attempts": delivery.envelope.attempts + 1}, deep=True)
        await self._initialize()
        # XADD followed by XACK as two client calls can leave both the new
        # retry and the old pending entry alive when Redis disconnects between
        # them. Commit both operations in one server-side transaction so a
        # request has only one queue recovery path.
        await self._redis.eval(
            """
            local retry_id = redis.call('XADD', KEYS[1], '*', 'payload', ARGV[3])
            redis.call('XACK', KEYS[1], ARGV[1], ARGV[2])
            return retry_id
            """,
            1,
            self._stream,
            self._group,
            delivery.receipt,
            envelope.model_dump_json(),
        )

    async def reclaim_stale(self, consumer: str, *, idle_ms: int = 60000, count: int = 100) -> list[QueueDelivery]:
        """Claim abandoned pending entries and return them for real processing."""
        await self._initialize()
        result = await self._redis.xautoclaim(self._stream, self._group, consumer, idle_ms, "0-0", count=count)
        messages = result[1] if len(result) > 1 else []
        deliveries: list[QueueDelivery] = []
        for receipt, fields in messages:
            payload = fields.get("payload")
            if payload:
                deliveries.append(
                    QueueDelivery(receipt=receipt, envelope=AgentTaskEnvelope.model_validate_json(payload)))
        return deliveries

    async def dead_letter(self, delivery: QueueDelivery, error_code: str) -> None:
        await self._redis.xadd(self._dead_stream, {
            "payload": delivery.envelope.model_dump_json(),
            "source_receipt": delivery.receipt,
            "error_code": error_code,
        })
        await self.ack(delivery)

    async def unregister_consumer(self, consumer: str) -> bool:
        """Delete this Worker's Consumer metadata only when it owns no pending entry.

        Redis ``XGROUP DELCONSUMER`` discards that Consumer's pending ownership.
        Calling it while pending entries still exist would make crash recovery less
        reliable, so shutdown deliberately leaves such a Consumer for XAUTOCLAIM.
        """

        async def unregister_once() -> bool:
            await self._initialize()
            consumers = await self._redis.xinfo_consumers(self._stream, self._group)
            current = next((item for item in consumers if item.get("name") == consumer), None)
            if current is None:
                return True
            if int(current.get("pending", 0)) != 0:
                return False
            await self._redis.xgroup_delconsumer(self._stream, self._group, consumer)
            return True

        return bool(await self._with_reconnect(unregister_once))

    async def close(self) -> None:
        if self._owns_client:
            await self._redis.aclose()

    async def ping(self) -> bool:
        return bool(await self._with_reconnect(self._redis.ping))
