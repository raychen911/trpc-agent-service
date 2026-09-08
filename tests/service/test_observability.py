# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Metrics and cross-queue OpenTelemetry context tests."""

from __future__ import annotations

import pytest
from opentelemetry import metrics as otel_metrics
from opentelemetry import trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from trpc_service import EnterpriseMetrics
from trpc_service import callback_span
from trpc_service import configure_telemetry
from trpc_service import extracted_trace_context
from trpc_service import inject_trace_headers
from trpc_service import operation_span
from trpc_service import shutdown_telemetry
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
    assert snapshot["histograms"][0]["unit"] == "ms"
    assert snapshot["histograms"][0]["buckets"][:2] == [
        {
            "lower": None,
            "upper": 10.0,
            "count": 1
        },
        {
            "lower": 10.0,
            "upper": 50.0,
            "count": 1
        },
    ]
    assert sum(bucket["count"] for bucket in snapshot["histograms"][0]["buckets"]) == 2


def test_metrics_reject_high_cardinality_attributes_and_wrong_kind():
    metrics = EnterpriseMetrics(meter=False)

    with pytest.raises(ValueError, match="message_id"):
        metrics.increment("agent_requests_total", message_id="m1")
    with pytest.raises(ValueError, match="defined as histogram"):
        metrics.increment("agent_runner_latency_ms")
    with pytest.raises(ValueError, match="defined as counter"):
        metrics.observe("agent_requests_total", 1)


def test_metrics_are_recorded_by_otel_with_normalized_tenant_attribute():
    reader = InMemoryMetricReader()
    provider = MeterProvider(metric_readers=[reader])
    metrics = EnterpriseMetrics(meter=provider.get_meter("test"))
    try:
        metrics.increment("agent_requests_total", tenant_id="tenant_a", outcome="success")
        metrics.observe("agent_runner_latency_ms", 12, tenant_id="tenant_a", outcome="success")
        metrics.set_gauge("agent_budget_tokens_used", 120, tenant_id="tenant_a")

        exported = reader.get_metrics_data()
        instruments = {
            metric.name: metric
            for resource_metrics in exported.resource_metrics
            for scope_metrics in resource_metrics.scope_metrics
            for metric in scope_metrics.metrics
        }
        assert set(instruments) == {
            "agent_requests_total",
            "agent_runner_latency_ms",
            "agent_budget_tokens_used",
        }
        request_point = instruments["agent_requests_total"].data.data_points[0]
        latency_point = instruments["agent_runner_latency_ms"].data.data_points[0]
        budget_point = instruments["agent_budget_tokens_used"].data.data_points[0]
        assert request_point.attributes == {"tenant.id": "tenant_a", "outcome": "success"}
        assert latency_point.attributes["tenant.id"] == "tenant_a"
        assert latency_point.count == 1
        assert latency_point.sum == 12
        assert budget_point.value == 120
        assert metrics.snapshot("tenant_a")["gauges"][0]["value"] == 120
    finally:
        provider.shutdown()


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
            with operation_span("queue.enqueue", **{"messaging.system": "redis"}):
                with storage_span("tenant_a", "session", "get"):
                    pass
        spans = exporter.get_finished_spans()
        assert {span.name for span in spans} == {"im_callback", "queue.enqueue", "session.get"}
        assert len({span.context.trace_id for span in spans}) == 1
        storage = next(span for span in spans if span.name == "session.get")
        assert storage.attributes["tenant.id"] == "tenant_a"
        assert storage.attributes["storage.operation"] == "get"
    finally:
        trace._TRACER_PROVIDER = original_provider  # type: ignore[attr-defined]


def test_configure_telemetry_requires_endpoint(monkeypatch):
    monkeypatch.setattr(observability_module, "_TELEMETRY_CONFIGURED", False)
    monkeypatch.setattr(observability_module, "_TRACE_PROVIDER", None)
    monkeypatch.setattr(observability_module, "_METER_PROVIDER", None)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", raising=False)
    assert configure_telemetry("test-service") is False


def test_configure_telemetry_builds_otlp_provider(monkeypatch):
    from opentelemetry.exporter.otlp.proto.http import metric_exporter
    from opentelemetry.exporter.otlp.proto.http import trace_exporter
    from opentelemetry.sdk.metrics.export import MetricExporter
    from opentelemetry.sdk.metrics.export import MetricExportResult
    from opentelemetry.sdk.trace.export import SpanExportResult
    from opentelemetry.sdk.trace.export import SpanExporter

    class FakeSpanExporter(SpanExporter):

        def export(self, spans):
            return SpanExportResult.SUCCESS

    class FakeMetricExporter(MetricExporter):

        def export(self, metrics_data, timeout_millis=10000, **kwargs):
            return MetricExportResult.SUCCESS

        def force_flush(self, timeout_millis=10000):
            return True

        def shutdown(self, timeout_millis=30000, **kwargs):
            return None

    captured = {}
    monkeypatch.setattr(observability_module, "_TELEMETRY_CONFIGURED", False)
    monkeypatch.setattr(observability_module, "_TRACE_PROVIDER", None)
    monkeypatch.setattr(observability_module, "_METER_PROVIDER", None)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "test-service")
    monkeypatch.setenv("OTEL_SERVICE_INSTANCE_ID", "pod-1")
    monkeypatch.setenv("OTEL_METRIC_EXPORT_INTERVAL", "invalid")
    monkeypatch.setattr(trace_exporter, "OTLPSpanExporter", FakeSpanExporter)
    monkeypatch.setattr(metric_exporter, "OTLPMetricExporter", FakeMetricExporter)
    monkeypatch.setattr(trace, "set_tracer_provider", lambda provider: captured.setdefault("trace", provider))
    monkeypatch.setattr(otel_metrics, "set_meter_provider", lambda provider: captured.setdefault("meter", provider))

    assert configure_telemetry("test-service") is True
    assert configure_telemetry("ignored-second-service") is True
    assert captured["trace"].resource.attributes["service.name"] == "test-service"
    assert captured["meter"]._sdk_config.resource.attributes["service.instance.id"] == "pod-1"
    shutdown_telemetry()
    assert observability_module._TELEMETRY_CONFIGURED is False


def test_configure_telemetry_can_enable_metrics_without_traces(monkeypatch):
    from opentelemetry.exporter.otlp.proto.http import metric_exporter
    from opentelemetry.sdk.metrics.export import MetricExporter
    from opentelemetry.sdk.metrics.export import MetricExportResult

    class FakeMetricExporter(MetricExporter):

        def export(self, metrics_data, timeout_millis=10000, **kwargs):
            return MetricExportResult.SUCCESS

        def force_flush(self, timeout_millis=10000):
            return True

        def shutdown(self, timeout_millis=30000, **kwargs):
            return None

    captured = {}
    monkeypatch.setattr(observability_module, "_TELEMETRY_CONFIGURED", False)
    monkeypatch.setattr(observability_module, "_TRACE_PROVIDER", None)
    monkeypatch.setattr(observability_module, "_METER_PROVIDER", None)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", raising=False)
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "http://collector:4318/v1/metrics")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "metrics-only")
    monkeypatch.setattr(metric_exporter, "OTLPMetricExporter", FakeMetricExporter)
    monkeypatch.setattr(otel_metrics, "set_meter_provider", lambda provider: captured.setdefault("meter", provider))

    assert configure_telemetry("metrics-only") is True
    assert observability_module._TRACE_PROVIDER is None
    assert captured["meter"]._sdk_config.resource.attributes["service.name"] == "metrics-only"
    shutdown_telemetry()
