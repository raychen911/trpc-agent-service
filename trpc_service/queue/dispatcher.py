"""Transactional-outbox to Redis Streams dispatcher."""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from typing import Protocol

from trpc_service.metrics.privacy import extract_trace_context, inject_trace_headers
from trpc_service.metrics.prometheus import QUEUE_DEPTH
from trpc_service.metrics.telemetry import get_tracer, mark_span_error, queue_label
from trpc_service.storage.models import OutboxRecord
from trpc_service.storage.protocols import RuntimeRepository

logger = logging.getLogger(__name__)


class OutboxPublisher(Protocol):
    """Minimal queue contract required by the transactional-outbox loop."""

    async def ensure_group(self) -> None: ...

    async def publish(self, record: OutboxRecord) -> object: ...


class OutboxDispatcher:
    def __init__(
        self,
        repository: RuntimeRepository,
        queue: OutboxPublisher,
        *,
        owner_id: str,
        event_type: str = "inbound.accepted",
        batch_size: int = 100,
    ) -> None:
        self._repository = repository
        self._queue = queue
        self._owner_id = owner_id
        self._event_type = event_type
        self._batch_size = batch_size

    async def dispatch_once(self, stop_event: asyncio.Event | None = None) -> int:
        records = await self._repository.claim_outbox(
            event_type=self._event_type,
            owner_id=self._owner_id,
            limit=self._batch_size,
            lease_for=timedelta(seconds=30),
        )
        published = 0
        queue = queue_label(self._event_type)
        QUEUE_DEPTH.labels(queue=queue).set(len(records))
        try:
            for index, record in enumerate(records):
                if stop_event is not None and stop_event.is_set():
                    for pending in records[index:]:
                        await self._repository.release_outbox(
                            pending.tenant_id,
                            pending.outbox_id,
                            owner_id=self._owner_id,
                            delay=timedelta(0),
                            error_type="dispatcher_draining",
                        )
                    break
                parent_context = extract_trace_context(record.trace_headers)
                outcome = "error"
                with get_tracer().start_as_current_span(
                    "queue.publish",
                    context=parent_context,
                    attributes={"queue": queue},
                ) as span:
                    try:
                        trace_headers = dict(record.trace_headers)
                        inject_trace_headers(trace_headers)
                        queued = record.model_copy(update={"trace_headers": trace_headers})
                        await self._queue.publish(queued)
                        await self._repository.mark_outbox_published(
                            record.tenant_id, record.outbox_id, owner_id=self._owner_id
                        )
                        published += 1
                        outcome = "published"
                    except Exception as exc:
                        outcome = "deferred"
                        mark_span_error(span, type(exc).__name__)
                        logger.warning(
                            "outbox publish deferred",
                            extra={
                                "outbox_id": record.outbox_id,
                                "error_type": type(exc).__name__,
                            },
                        )
                        await self._repository.release_outbox(
                            record.tenant_id,
                            record.outbox_id,
                            owner_id=self._owner_id,
                            delay=timedelta(seconds=min(2 ** min(record.attempts, 6), 60)),
                            error_type=type(exc).__name__,
                        )
                    finally:
                        span.set_attribute("outcome", outcome)
        finally:
            QUEUE_DEPTH.labels(queue=queue).set(0)
        return published

    async def run(
        self,
        *,
        poll_seconds: float = 0.5,
        stop_event: asyncio.Event | None = None,
    ) -> None:
        await self._queue.ensure_group()
        event = stop_event or asyncio.Event()
        retry_seconds = poll_seconds
        while not event.is_set():
            try:
                count = await self.dispatch_once(event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "outbox dispatcher cycle failed",
                    extra={"error_type": type(exc).__name__},
                )
                await _wait_or_stop(event, retry_seconds)
                retry_seconds = min(max(retry_seconds * 2, 0.1), 30.0)
                continue
            retry_seconds = poll_seconds
            if count == 0:
                await _wait_or_stop(event, poll_seconds)


async def _wait_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    if seconds <= 0:
        await asyncio.sleep(0)
        return
    try:
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
    except TimeoutError:
        pass


__all__ = ["OutboxDispatcher", "OutboxPublisher"]
