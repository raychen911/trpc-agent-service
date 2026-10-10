"""OpenTelemetry setup shared by every role."""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

from trpc_service.metrics.privacy import PrivacySpanProcessor


def configure_tracing(
    *, service_name: str, endpoint: str | None, capture_content: bool = False
) -> TracerProvider:
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    if endpoint:
        exporter = OTLPSpanExporter(
            endpoint=endpoint,
            insecure=endpoint.startswith("http://"),
        )
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(
            processor if capture_content else PrivacySpanProcessor(processor)
        )
    trace.set_tracer_provider(provider)
    return provider


__all__ = ["configure_tracing"]
