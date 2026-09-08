# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Stream worker consumer: pulls tasks from Redis Streams and runs them.

The consumer owns the full turn (agent run + IM reply) via the shared dispatch
primitives, so it is behaviourally identical to the in-process gateway path.
Delivery is at-least-once via the consumer group. A heartbeat prevents healthy
long turns from being reclaimed, and processing is cancelled if ownership is
lost; result caching and downstream idempotency keys cover recovery retries.
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import suppress
from typing import Any
from typing import Optional
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError

from trpc_service.web._dispatch import reply_to_channel
from trpc_service.web._dispatch import run_turn
from trpc_service.agent._queue import TaskMessage
from trpc_service.metrics._observability import extracted_trace_context
from trpc_service.metrics import EnterpriseMetrics
from trpc_service.metrics import get_enterprise_metrics
from trpc_service.metrics import operation_span
from ._results import LocalTaskResultStore
from trpc_service.messaging._models import ClaimStatus
from trpc_service.runtime import RuntimeResources

logger = logging.getLogger(__name__)


class StreamWorker:
    """Consumes :class:`TaskMessage` entries from a :class:`StreamQueue`."""

    def __init__(self,
                 *,
                 queue: Any,
                 worker: Any,
                 registry: Any,
                 min_idle_ms: int = 60000,
                 heartbeat_interval_ms: int | None = None,
                 worker_heartbeat_ttl_seconds: float = 30.0,
                 reconnect_delay_seconds: float = 1.0,
                 max_attempts: int = 3,
                 result_store: Any = None,
                 message_store: Any = None,
                 metrics: EnterpriseMetrics | None = None,
                 owned_resources: Optional[list[Any]] = None) -> None:
        self._queue = queue
        self._worker = worker
        self._registry = registry
        self._min_idle_ms = min_idle_ms
        self._heartbeat_interval_ms = (heartbeat_interval_ms if heartbeat_interval_ms is not None else max(
            100, min_idle_ms // 3))
        self._worker_heartbeat_ttl_seconds = max(1.0, worker_heartbeat_ttl_seconds)
        self._reconnect_delay_seconds = max(0.0, reconnect_delay_seconds)
        self._max_attempts = max_attempts
        self._result_store = result_store or LocalTaskResultStore()
        self._message_store = message_store
        self._metrics = metrics or getattr(worker, "metrics", None) or get_enterprise_metrics()
        self._resources = RuntimeResources(*(owned_resources or []), registry, message_store, self._result_store, queue)

    async def _heartbeat(
        self,
        message_id: str,
        owner_task: asyncio.Task,
        ownership_lost: asyncio.Event,
        *,
        task: Optional[TaskMessage] = None,
        receipt_owner: str = "",
        receipt_token: int = 0,
        receipt_lease_seconds: float = 0,
    ) -> None:
        touch = getattr(self._queue, "touch", None)
        renew_receipt = getattr(self._message_store, "renew_inbound", None)
        if (not callable(touch) and not callable(renew_receipt)) or self._heartbeat_interval_ms <= 0:
            return
        interval = self._heartbeat_interval_ms / 1000
        while True:
            await asyncio.sleep(interval)
            try:
                queue_owned = await touch(message_id) if callable(touch) else True
                receipt_owned = (await renew_receipt(
                    task,
                    owner=receipt_owner,
                    fencing_token=receipt_token,
                    lease_seconds=receipt_lease_seconds,
                ) if callable(renew_receipt) and task is not None and receipt_token else True)
                if queue_owned and receipt_owned:
                    continue
                logger.warning("task %s is no longer owned by this worker", message_id)
            except Exception:  # noqa: BLE001 - uncertain ownership must stop side effects
                logger.warning("failed to heartbeat task %s", message_id, exc_info=True)
            ownership_lost.set()
            owner_task.cancel()
            return

    async def _process(self, message_id: str, payload: str) -> bool:
        started = time.perf_counter()
        task = None
        heartbeat_task = None
        ownership_lost = asyncio.Event()
        outcome = "error"
        error_type = None
        receipt_owner = getattr(self._queue, "consumer_name", "stream-worker")
        receipt_token = 0
        receipt_owned = False
        receipt_lease_seconds = max(30.0, self._min_idle_ms / 1000 * 2)
        try:
            task = TaskMessage.model_validate_json(payload)
            inbound = task.to_inbound()
            inbound.metadata = {
                **inbound.metadata,
                "turn_id": task.turn_id,
                "config_revision": task.config_revision,
            }
            if self._message_store is not None:
                claim = await self._message_store.claim_inbound(
                    task,
                    owner=receipt_owner,
                    lease_seconds=receipt_lease_seconds,
                )
                receipt_token = claim.fencing_token
                if claim.status == ClaimStatus.COMPLETED:
                    await self._queue.ack(message_id)
                    outcome = "duplicate"
                    return True
                if claim.status == ClaimStatus.BUSY:
                    outcome = "busy"
                    return False
                receipt_owned = True
            owner_task = asyncio.current_task()
            if owner_task is None:  # pragma: no cover - an async function always has a current task
                raise RuntimeError("stream task is not running inside an asyncio task")
            heartbeat_task = asyncio.create_task(
                self._heartbeat(
                    message_id,
                    owner_task,
                    ownership_lost,
                    task=task,
                    receipt_owner=receipt_owner,
                    receipt_token=receipt_token,
                    receipt_lease_seconds=receipt_lease_seconds,
                ))
            with extracted_trace_context(task.trace_headers):
                with operation_span(
                        "worker.process_task",
                        **{
                            "tenant.id": task.tenant_id,
                            "channel": task.channel,
                        },
                ):
                    with operation_span("result_cache.get", **{"tenant.id": task.tenant_id}):
                        text = await self._result_store.get(task.idempotency_key)
                    cache_outcome = "hit" if text is not None else "miss"
                    self._metrics.increment(
                        "agent_result_cache_total",
                        tenant_id=task.tenant_id,
                        channel=task.channel,
                        outcome=cache_outcome,
                    )
                    if text is None:
                        text = await run_turn(
                            tenant_id=task.tenant_id,
                            channel=task.channel,
                            inbound=inbound,
                            worker=self._worker,
                        )
                        with operation_span("result_cache.put", **{"tenant.id": task.tenant_id}):
                            await self._result_store.put(task.idempotency_key, text)
                    if self._message_store is not None:
                        await self._message_store.complete_with_outbox(
                            task,
                            inbound,
                            text,
                            owner=receipt_owner,
                            fencing_token=receipt_token,
                        )
                        receipt_owned = False
                    else:
                        await reply_to_channel(
                            tenant_id=task.tenant_id,
                            channel=task.channel,
                            inbound=inbound,
                            text=text,
                            worker=self._worker,
                            registry=self._registry,
                        )
                    await self._queue.ack(message_id)
            outcome = "success"
            return True
        except asyncio.CancelledError:
            if not ownership_lost.is_set():
                raise
            error_type = "TaskOwnershipLost"
            outcome = "ownership_lost"
            logger.warning("stopped processing task %s after ownership was lost", message_id)
            return False
        except Exception as exc:  # noqa: BLE001 - a bad message must not stop the consumer
            error_type = type(exc).__name__
            logger.exception("failed to process task %s", message_id)
            if receipt_owned and task is not None:
                with suppress(Exception):
                    await self._message_store.abandon_inbound(
                        task,
                        owner=receipt_owner,
                        fencing_token=receipt_token,
                        error=str(exc),
                    )
            attempts = await self._queue.delivery_count(message_id)
            if attempts >= self._max_attempts:
                await self._queue.dead_letter(message_id, payload, "task processing failed")
                outcome = "dead_letter"
                self._metrics.increment(
                    "agent_queue_dlq_total",
                    tenant_id=task.tenant_id if task is not None else None,
                    channel=task.channel if task is not None else None,
                )
            else:
                outcome = "retry"
                self._metrics.increment(
                    "agent_worker_retry_total",
                    tenant_id=task.tenant_id if task is not None else None,
                    channel=task.channel if task is not None else None,
                    error_type=error_type,
                )
            return False
        finally:
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                with suppress(asyncio.CancelledError):
                    await heartbeat_task
            attributes = {
                "tenant_id": task.tenant_id if task is not None else None,
                "channel": task.channel if task is not None else None,
                "outcome": outcome,
                "error_type": error_type,
            }
            self._metrics.increment("agent_worker_task_total", **attributes)
            self._metrics.observe(
                "agent_worker_task_duration_ms",
                (time.perf_counter() - started) * 1000,
                **attributes,
            )

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
        """Consume forever, recovering from Redis timeout and connection loss."""
        group_ready = False
        effective_block = min(block, max(1, int(self._worker_heartbeat_ttl_seconds * 1000 / 3)))
        try:
            while True:
                try:
                    if not group_ready:
                        await self._queue.ensure_group()
                        group_ready = True
                    publish_heartbeat = getattr(self._queue, "publish_worker_heartbeat", None)
                    if callable(publish_heartbeat):
                        await publish_heartbeat(self._worker_heartbeat_ttl_seconds)
                    await self.run_once(count=10, block=effective_block)
                except asyncio.CancelledError:
                    raise
                except (RedisTimeoutError, RedisConnectionError, TimeoutError, ConnectionError) as exc:
                    group_ready = False
                    self._metrics.increment(
                        "agent_worker_reconnect_total",
                        error_type=type(exc).__name__,
                    )
                    logger.warning("worker queue connection failed; retrying", exc_info=True)
                    await asyncio.sleep(self._reconnect_delay_seconds)
        finally:
            remove_heartbeat = getattr(self._queue, "remove_worker_heartbeat", None)
            if callable(remove_heartbeat):
                with suppress(Exception):
                    await remove_heartbeat()

    async def close(self) -> None:
        """Close queue, result, message, channel and process-owned resources."""
        await self._resources.close()
