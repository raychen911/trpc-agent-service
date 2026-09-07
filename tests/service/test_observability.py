# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Metrics and cross-queue OpenTelemetry context tests."""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from trpc_service import EnterpriseMetrics
from trpc_service import callback_span
from trpc_service import configure_telemetry
from trpc_service import extracted_trace_context
from trpc_service import inject_trace_headers
from trpc_service import storage_span
import trpc_service.metrics._observability as observability_module


def test_metrics_snapshot_is_tenant_scoped_and_has_aggregates():
    metrics = EnterpriseMetrics(meter=False)
    metrics.increment("agent_requests_total", tenant_id="tenant_a", outcome="success")
    metrics.increment("agent_requests_total", tenant_id="tenant_b", outcome="error")
    metrics.observe("agent_runner_latency_ms", 10, tenant_id="tenant_a")
    metrics.observe("agent_runner_latency_ms", 20, tenant_id="tenant_a")

    snapshot = metrics.snapshot("tenant_a")
    assert len(snapshot["counters"]) == 1
    assert snapshot["counters"][0]["attributes"]["tenant_id"] == "tenant_a"
    assert snapshot["histograms"][0]["count"] == 2
    assert snapshot["histograms"][0]["sum"] == 30
    assert snapshot["histograms"][0]["max"] == 20


def test_trace_context_survives_queue_carrier():
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    original_provider = trace._TRACER_PROVIDER  # type: ignore[attr-defined]
    trace._TRACER_PROVIDER = provider  # type: ignore[attr-defined]  # isolated test provider
    try:
        tracer = trace.get_tracer("test")
        with tracer.start_as_current_span("gateway"):
            headers = inject_trace_headers()
        with extracted_trace_context(headers):
            with callback_span("tenant_a", "feishu"):
                pass
        spans = exporter.get_finished_spans()
        assert {span.name for span in spans} == {"gateway", "im_callback"}
        assert len({span.context.trace_id for span in spans}) == 1
        worker_span = next(span for span in spans if span.name == "im_callback")
        assert worker_span.attributes["tenant.id"] == "tenant_a"
    finally:
        trace._TRACER_PROVIDER = original_provider  # type: ignore[attr-defined]


def test_storage_span_is_part_of_callback_trace():
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    original_provider = trace._TRACER_PROVIDER  # type: ignore[attr-defined]
    trace._TRACER_PROVIDER = provider  # type: ignore[attr-defined]
    try:
        with callback_span("tenant_a", "wecom"):
            with storage_span("tenant_a", "session", "get"):
                pass
        spans = exporter.get_finished_spans()
        assert {span.name for span in spans} == {"im_callback", "session.get"}
        assert len({span.context.trace_id for span in spans}) == 1
        storage = next(span for span in spans if span.name == "session.get")
        assert storage.attributes["tenant.id"] == "tenant_a"
        assert storage.attributes["storage.operation"] == "get"
    finally:
        trace._TRACER_PROVIDER = original_provider  # type: ignore[attr-defined]


def test_configure_telemetry_requires_endpoint(monkeypatch):
    monkeypatch.setattr(observability_module, "_TELEMETRY_CONFIGURED", False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    assert configure_telemetry("test-service") is False


def test_configure_telemetry_builds_otlp_provider(monkeypatch):
    from opentelemetry.exporter.otlp.proto.http import trace_exporter
    from opentelemetry.sdk.trace.export import SpanExportResult
    from opentelemetry.sdk.trace.export import SpanExporter

    class FakeExporter(SpanExporter):

        def export(self, spans):
            return SpanExportResult.SUCCESS

    captured = {}
    monkeypatch.setattr(observability_module, "_TELEMETRY_CONFIGURED", False)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
    monkeypatch.setattr(trace_exporter, "OTLPSpanExporter", FakeExporter)
    monkeypatch.setattr(trace, "set_tracer_provider", lambda provider: captured.setdefault("provider", provider))

    assert configure_telemetry("test-service") is True
    assert configure_telemetry("ignored-second-service") is True
    assert captured["provider"].resource.attributes["service.name"] == "test-service"
    captured["provider"].shutdown()
