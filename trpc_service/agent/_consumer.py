# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Stream worker consumer: pulls tasks from Redis Streams and runs them.

The consumer owns the full turn (agent run + IM reply) via the shared
:func:`run_and_reply` helper, so it is behaviourally identical to the
in-process gateway dispatch path. Delivery is at-least-once via the consumer
group; combined with the gateway's idempotency key, redelivery is safe.
"""

from __future__ import annotations

import logging
from typing import Any

from trpc_service.web._dispatch import reply_to_channel
from trpc_service.web._dispatch import run_turn
from trpc_service.agent._queue import TaskMessage
from trpc_service.metrics._observability import extracted_trace_context
from ._results import LocalTaskResultStore

logger = logging.getLogger(__name__)


class StreamWorker:
    """Consumes :class:`TaskMessage` entries from a :class:`StreamQueue`."""

    def __init__(self,
                 *,
                 queue: Any,
                 worker: Any,
                 registry: Any,
                 min_idle_ms: int = 60000,
                 max_attempts: int = 3,
                 result_store: Any = None) -> None:
        self._queue = queue
        self._worker = worker
        self._registry = registry
        self._min_idle_ms = min_idle_ms
        self._max_attempts = max_attempts
        self._result_store = result_store or LocalTaskResultStore()

    async def _process(self, message_id: str, payload: str) -> bool:
        try:
            task = TaskMessage.model_validate_json(payload)
            inbound = task.to_inbound()
            with extracted_trace_context(task.trace_headers):
                text = await self._result_store.get(task.idempotency_key)
                if text is None:
                    text = await run_turn(
                        tenant_id=task.tenant_id,
                        channel=task.channel,
                        inbound=inbound,
                        worker=self._worker,
                    )
                    await self._result_store.put(task.idempotency_key, text)
                await reply_to_channel(
                    tenant_id=task.tenant_id,
                    channel=task.channel,
                    inbound=inbound,
                    text=text,
                    worker=self._worker,
                    registry=self._registry,
                )
        except Exception:  # noqa: BLE001 - a bad message must not stop the consumer
            logger.exception("failed to process task %s", message_id)
            attempts = await self._queue.delivery_count(message_id)
            if attempts >= self._max_attempts:
                await self._queue.dead_letter(message_id, payload, "task processing failed")
            return False
        await self._queue.ack(message_id)
        return True

    async def _process_batches(self, messages: list) -> int:
        processed = 0
        for _stream, entries in messages:
            for message_id, fields in entries:
                payload = fields.get("payload") or fields.get(b"payload")
                if isinstance(payload, bytes):
                    payload = payload.decode()
                await self._process(message_id, payload)
                processed += 1
        return processed

    async def run_once(self, count: int = 10, block: int = 0) -> int:
        """Process one batch of messages; returns the number processed."""
        claimed = await self._queue.claim_stale(min_idle_ms=self._min_idle_ms, count=count)
        processed = await self._process_batches(claimed)
        if processed >= count:
            return processed
        messages = await self._queue.read(count=count - processed, block=block)
        return processed + await self._process_batches(messages)

    async def run(self, block: int = 5000) -> None:
        """Blocking consumption loop (production entrypoint)."""
        await self._queue.ensure_group()
        while True:
            await self.run_once(count=10, block=block)
