from __future__ import annotations

from opentelemetry.context import Context

from trpc_service.metrics.telemetry import queue_label, stable_tenant_label
from trpc_service.queue import worker_consumer
from trpc_service.queue.redis_streams import QueueMessage


class _Span:
    def __init__(self, name: str, context: Context | None) -> None:
        self.name = name
        self.context = context
        self.attributes = {}

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def set_attribute(self, key, value):
        self.attributes[key] = value

    def set_status(self, status):
        return None


class _Tracer:
    def __init__(self) -> None:
        self.spans = []

    def start_as_current_span(self, name, *, context=None, attributes=None):
        span = _Span(name, context)
        if attributes:
            span.attributes.update(attributes)
        self.spans.append(span)
        return span


class _Queue:
    async def ack(self, message):
        return None


class _Repository:
    async def get_acceptance(self, tenant_id, aggregate_id):
        return None


async def _unused_worker(_acceptance):
    raise AssertionError("worker must not run for a missing acceptance")


def test_telemetry_labels_are_bounded_or_opaque() -> None:
    assert queue_label("inbound.accepted") == "inbound"
    assert queue_label("outbound.feishu.ready") == "outbound"
    assert queue_label("untrusted.event") == "other"
    assert stable_tenant_label("tenant-a") == stable_tenant_label("tenant-a")
    assert stable_tenant_label("tenant-a") != "tenant-a"


async def test_worker_consumer_extracts_queue_trace_context(monkeypatch) -> None:
    carrier = {"traceparent": "00-" + "1" * 32 + "2" * 16 + "01"}
    seen = []
    tracer = _Tracer()

    def extract(headers):
        seen.append(dict(headers))
        return Context()

    monkeypatch.setattr(worker_consumer, "extract_trace_context", extract)
    monkeypatch.setattr(worker_consumer, "get_tracer", lambda: tracer)
    message = QueueMessage("1-0", "outbox", "tenant-a", "inbound.accepted", "inbound", {}, carrier)
    consumer = worker_consumer.WorkerConsumer(
        _Repository(), _Queue(), _unused_worker, consumer_id="worker"
    )

    await consumer.process_message(message)

    assert seen == [carrier]
    assert tracer.spans[0].name == "queue.consume"
    assert tracer.spans[0].attributes["queue"] == "agent"
