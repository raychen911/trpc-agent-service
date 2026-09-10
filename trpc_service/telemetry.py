"""Small OpenTelemetry setup and tracer shared by service components."""

from __future__ import annotations

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter


def configure_telemetry(service_name: str, console_exporter: bool = False) -> None:
    """Install an SDK provider once; external auto-instrumentation may install one first."""

    current = trace.get_tracer_provider()
    if isinstance(current, TracerProvider):
        return
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    if console_exporter:
        provider.add_span_processor(BatchSpanProcessor(ConsoleSpanExporter()))
    trace.set_tracer_provider(provider)


tracer = trace.get_tracer("trpc-agent-service")


__all__ = ["configure_telemetry", "tracer"]
