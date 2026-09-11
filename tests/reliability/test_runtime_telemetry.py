"""Exported spans connect across durable hops and never include error bodies."""

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.metrics.export import InMemoryMetricReader

from trpc_service.telemetry import runtime


def test_exported_parent_context_latency_and_redaction(monkeypatch):
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    exporter, reader = InMemorySpanExporter(), InMemoryMetricReader()
    runtime.configure("test-worker", span_exporter=exporter, metric_reader=reader)
    try:
        with runtime.operation("gateway.inbox") as gateway:
            persisted = runtime.carrier()
        with pytest.raises(ValueError):
            with runtime.operation("worker.execute", parent=persisted):
                raise ValueError("password=must-not-appear")
        runtime._providers[0].force_flush()
        spans = exporter.get_finished_spans()
        child = next(span for span in spans if span.name == "worker.execute")
        assert child.context.trace_id == gateway.get_span_context().trace_id
        assert child.parent.span_id == gateway.get_span_context().span_id
        assert child.attributes["error.type"] == "ValueError"
        assert "must-not-appear" not in str(spans)
        assert not child.events
        data = reader.get_metrics_data()
        metrics = data.resource_metrics[0].scope_metrics[0].metrics
        assert {metric.name for metric in metrics} >= {"trpc.operation.calls", "trpc.operation.duration_ms"}
    finally:
        runtime.shutdown()
