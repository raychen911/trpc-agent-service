"""OpenTelemetry 初始化。"""
from __future__ import annotations

import os


def setup_telemetry(service_name: str = "trpc-agent-service") -> bool:
    """初始化 OTel；未启用时返回 False（no-op，不影响运行）。"""
    if os.getenv("OTEL_ENABLED", "").lower() not in ("1", "true", "yes"):
        return False
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:  # pragma: no cover
        return False

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4318")
    resource = Resource.create(
        {
            "service.name": service_name,
            "service.namespace": "trpc-agent",
        }
    )
    provider = TracerProvider(resource=resource)
    exporter = OTLPSpanExporter(endpoint=f"{endpoint.rstrip('/')}/v1/traces")
    provider.add_span_processor(BatchSpanProcessor(exporter))
    # 替换全局 provider，使框架 tracer（trpc.python.agent）随之生效
    trace.set_tracer_provider(provider)
    _setup_metrics(resource, endpoint)
    return True


def _setup_metrics(resource, endpoint: str) -> None:
    """安装 MeterProvider（失败不阻断服务，指标仍留在进程内聚合）。"""
    try:
        from opentelemetry import metrics
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
    except ImportError:  # pragma: no cover
        return

    exporter = OTLPMetricExporter(endpoint=f"{endpoint.rstrip('/')}/v1/metrics")
    reader = PeriodicExportingMetricReader(exporter)
    metrics.set_meter_provider(MeterProvider(resource=resource, metric_readers=[reader]))
