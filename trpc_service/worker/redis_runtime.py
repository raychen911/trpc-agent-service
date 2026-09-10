"""Standalone Redis Stream worker for the simplified multi-replica deployment."""

from __future__ import annotations

import asyncio
from typing import Any

from redis.asyncio import Redis
from redis.exceptions import ResponseError
from redis.exceptions import TimeoutError as RedisTimeoutError

from trpc_service.bus.redis_bus import (
    deserialize_command,
    encode_result,
    result_key,
    serialize_reply,
)
from trpc_service.config.models import OutboxStatus
from trpc_service.storage.database import Database
from trpc_service.storage.repositories import ExecutionOutboxRepository
from trpc_service.worker.service import WorkerService


class RedisWorkerRuntime:
    def __init__(
        self,
        database: Database,
        redis_url: str,
        worker: WorkerService,
        *,
        stream: str,
        group: str,
        consumer: str,
        claim_idle_ms: int = 60_000,
        max_attempts: int = 3,
    ) -> None:
        self._outbox = ExecutionOutboxRepository(database)
        self._redis = Redis.from_url(redis_url, decode_responses=True)
        self._worker = worker
        self._stream = stream
        self._group = group
        self._consumer = consumer
        self._claim_idle_ms = claim_idle_ms
        self._max_attempts = max_attempts

    async def run(self) -> None:
        await self._ensure_group()
        while True:
            claimed = await self._claim_stale()
            if claimed:
                for message_id, fields in claimed:
                    await self.process_message(message_id, fields)
                continue
            try:
                batches = await self._redis.xreadgroup(
                    self._group,
                    self._consumer,
                    {self._stream: ">"},
                    count=10,
                    block=5_000,
                )
            except RedisTimeoutError:
                # Some Redis/client combinations surface an empty blocking read as a
                # socket timeout. It is an idle poll, not a worker failure.
                continue
            for _, messages in batches:
                for message_id, fields in messages:
                    await self.process_message(message_id, fields)

    async def process_message(self, message_id: str, fields: dict[str, Any]) -> None:
        outbox_id = str(fields["outbox_id"])
        record = await self._outbox.get(outbox_id)
        if record is None or record.status in {
            OutboxStatus.COMPLETED,
            OutboxStatus.DEAD_LETTER,
        }:
            await self._redis.xack(self._stream, self._group, message_id)
            return

        await self._outbox.mark(
            outbox_id,
            OutboxStatus.PROCESSING,
            increment_attempts=True,
        )
        should_ack = False
        try:
            reply = await self._worker.run(deserialize_command(record.payload))
            await self._outbox.mark(outbox_id, OutboxStatus.COMPLETED)
            await self._publish_result(outbox_id, {"ok": True, "reply": serialize_reply(reply)})
            should_ack = True
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            current = await self._outbox.get(outbox_id)
            attempts = current.attempts if current is not None else self._max_attempts
            if attempts < self._max_attempts:
                await self._outbox.mark(
                    outbox_id,
                    OutboxStatus.RETRY,
                    error_type=type(exc).__name__,
                )
                await self._redis.xadd(self._stream, {"outbox_id": outbox_id})
            else:
                await self._outbox.mark(
                    outbox_id,
                    OutboxStatus.DEAD_LETTER,
                    error_type=type(exc).__name__,
                )
                await self._publish_result(
                    outbox_id,
                    {"ok": False, "error_type": type(exc).__name__},
                )
            should_ack = True
        finally:
            if should_ack:
                await self._redis.xack(self._stream, self._group, message_id)

    async def _publish_result(self, outbox_id: str, payload: dict[str, Any]) -> None:
        key = result_key(outbox_id)
        await self._redis.rpush(key, encode_result(payload))
        await self._redis.expire(key, 300)

    async def _ensure_group(self) -> None:
        try:
            await self._redis.xgroup_create(
                self._stream,
                self._group,
                id="0-0",
                mkstream=True,
            )
        except ResponseError as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    async def _claim_stale(self) -> list[tuple[str, dict[str, Any]]]:
        response = await self._redis.xautoclaim(
            self._stream,
            self._group,
            self._consumer,
            min_idle_time=self._claim_idle_ms,
            start_id="0-0",
            count=10,
        )
        return list(response[1])

    async def close(self) -> None:
        await self._redis.aclose()


__all__ = ["RedisWorkerRuntime"]
