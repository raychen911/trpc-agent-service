"""In-memory OpenTelemetry propagation test (no OTLP network connection)."""

import pytest

from opentelemetry import trace

from trpc_service.metrics import current_trace_context
from trpc_service.metrics import platform_span

sdk_trace = pytest.importorskip("opentelemetry.sdk.trace")
export_module = pytest.importorskip("opentelemetry.sdk.trace.export")
memory_export = pytest.importorskip("opentelemetry.sdk.trace.export.in_memory_span_exporter")

pytestmark = pytest.mark.component


def test_queue_serialized_context_keeps_one_trace():
    exporter = memory_export.InMemorySpanExporter()
    provider = sdk_trace.TracerProvider()
    provider.add_span_processor(export_module.SimpleSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    with platform_span("channel.inbound"):
        queue_context = current_trace_context()
    with platform_span("worker.execute", queue_context):
        outbox_context = current_trace_context()
    with platform_span("delivery.send", outbox_context):
        pass
    spans = exporter.get_finished_spans()
    assert {span.context.trace_id for span in spans}.__len__() == 1
    assert {span.name for span in spans} == {"channel.inbound", "worker.execute", "delivery.send"}
    provider.shutdown()
