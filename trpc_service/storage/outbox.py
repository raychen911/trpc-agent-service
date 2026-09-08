import logging
import time
from collections.abc import Mapping

from opentelemetry import trace
from opentelemetry.propagate import extract

from trpc_service.metrics import PlatformMetrics, tracer
from trpc_service.storage.contracts import OutboxHandler, OutboxRecord, OutboxStore
from trpc_service.storage.vector import SemanticStore

logger = logging.getLogger(__name__)


class OutboxWorker:
    def __init__(
        self,
        store: OutboxStore,
        handlers: Mapping[str, OutboxHandler],
        worker_id: str,
        metrics: PlatformMetrics | None = None,
        max_attempts: int = 8,
    ) -> None:
        self._store = store
        self._handlers = dict(handlers)
        self._worker_id = worker_id
        self._metrics = metrics
        self._max_attempts = max_attempts

    async def poll_once(self, limit: int = 100) -> int:
        records = await self._store.claim_batch(self._worker_id, limit)
        for record in records:
            started = time.perf_counter()
            im_channel = (
                record.topic.removeprefix("im.reply.")
                if record.topic.startswith("im.reply.")
                else None
            )
            handler = self._handlers.get(record.topic)
            if handler is None:
                await self._store.mark_failed(record.id, f"no handler for topic {record.topic}")
                if self._metrics is not None:
                    self._metrics.outbox_messages.labels(record.topic, "unhandled").inc()
                    if im_channel:
                        self._metrics.im_deliveries.labels(im_channel, "unhandled").inc()
                        self._metrics.im_delivery_latency.labels(im_channel, "unhandled").observe(
                            time.perf_counter() - started
                        )
                continue
            try:
                metadata = record.payload.get("metadata", {})
                carrier = metadata.get("_trace_context", {}) if isinstance(metadata, dict) else {}
                context = extract(carrier) if isinstance(carrier, dict) else None
                with tracer.start_as_current_span(
                    "outbox.deliver", context=context, kind=trace.SpanKind.PRODUCER
                ) as span:
                    span.set_attribute("messaging.destination.name", record.topic)
                    span.set_attribute("trpc.tenant_id", record.tenant_id)
                    await handler(record)
            except Exception as error:
                logger.exception(
                    "outbox_delivery_failed",
                    extra={"outbox_id": record.id, "topic": record.topic},
                )
                dead_letter = getattr(self._store, "mark_dead_letter", None)
                if record.attempts >= self._max_attempts and dead_letter is not None:
                    await dead_letter(record.id, str(error))
                    metric_status = "dead_letter"
                else:
                    await self._store.mark_failed(record.id, str(error))
                    metric_status = "failed"
                if self._metrics is not None:
                    self._metrics.outbox_messages.labels(record.topic, metric_status).inc()
                    if im_channel:
                        self._metrics.im_deliveries.labels(im_channel, metric_status).inc()
                        self._metrics.im_delivery_latency.labels(im_channel, metric_status).observe(
                            time.perf_counter() - started
                        )
            else:
                await self._store.mark_processed(record.id)
                if self._metrics is not None:
                    self._metrics.outbox_messages.labels(record.topic, "processed").inc()
                    if im_channel:
                        self._metrics.im_deliveries.labels(im_channel, "processed").inc()
                        self._metrics.im_delivery_latency.labels(im_channel, "processed").observe(
                            time.perf_counter() - started
                        )
        return len(records)


class VectorOutboxHandlers:
    """Handlers for durable Memory and Knowledge vector synchronization."""

    def __init__(self, semantic_store: SemanticStore) -> None:
        self._semantic_store = semantic_store

    def handlers(self) -> Mapping[str, OutboxHandler]:
        return {
            "memory.upsert": self.memory_upsert,
            "knowledge.upsert": self.knowledge_upsert,
        }

    async def memory_upsert(self, record: OutboxRecord) -> None:
        payload = record.payload
        with tracer.start_as_current_span("storage.memory.upsert") as span:
            span.set_attribute("trpc.tenant_id", record.tenant_id)
            await self._semantic_store.upsert_memory(
                tenant_id=record.tenant_id,
                agent_app_id=str(payload["agent_app_id"]),
                user_id=str(payload["user_id"]),
                memory_id=str(payload["memory_id"]),
                text=str(payload["text"]),
                metadata=payload.get("metadata", {}),
            )

    async def knowledge_upsert(self, record: OutboxRecord) -> None:
        payload = record.payload
        with tracer.start_as_current_span("storage.knowledge.upsert") as span:
            span.set_attribute("trpc.tenant_id", record.tenant_id)
            await self._semantic_store.upsert_knowledge(
                tenant_id=record.tenant_id,
                agent_app_id=str(payload["agent_app_id"]),
                document_id=str(payload["document_id"]),
                text=str(payload["text"]),
                metadata=payload.get("metadata", {}),
            )
