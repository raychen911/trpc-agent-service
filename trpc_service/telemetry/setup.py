"""Process-level OpenTelemetry bootstrap."""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

from trpc_service.config import Settings
from trpc_service.telemetry.sanitizer import SanitizingSpanExporter
from trpc_service.version import __version__


def configure_telemetry(settings: Settings) -> TracerProvider | None:
    """Install one provider whose terminal exporter enforces the privacy allowlist."""

    if not settings.otel_enabled:
        return None
    provider = TracerProvider(
        resource=Resource.create(
            {
                "service.name": settings.service_name,
                "service.version": __version__,
                "deployment.environment.name": settings.env.value,
            }
        ),
        sampler=ParentBased(TraceIdRatioBased(settings.otel_sample_ratio)),
    )
    raw_exporter = OTLPSpanExporter(
        endpoint=settings.otel_endpoint,
        insecure=settings.otel_endpoint.startswith("http://"),
    )
    provider.add_span_processor(BatchSpanProcessor(SanitizingSpanExporter(raw_exporter)))
    trace.set_tracer_provider(provider)
    return provider
