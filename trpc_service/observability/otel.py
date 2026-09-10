"""Process-level OpenTelemetry bootstrap (T027, DEC-001).

The official ``trpc_agent_sdk`` tracer is a ``ProxyTracer`` that resolves
the process-global TracerProvider lazily at first use.  ``TelemetryBootstrap``
therefore installs the platform providers exactly once per process — before
any Runner instance creates spans — so official ``trpc.python.agent`` spans
join the platform trace without touching SDK internals.  Subsequent
``configure()`` calls (including on new bootstrap instances) adopt the
already-installed providers instead of replacing them.
"""

from __future__ import annotations

from typing import Any

from opentelemetry import metrics as otel_metrics
from opentelemetry import trace as otel_trace
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider

# Process-level singleton state: the platform installs providers once.
_SHARED: dict[str, Any] = {}


class TelemetryBootstrap:
    """Idempotent provider installation for one service process."""

    def __init__(self, *, service_name: str = "trpc-agent-service") -> None:
        self.service_name = service_name

    def configure(
        self,
        *,
        span_processor: Any | None = None,
        metric_reader: Any | None = None,
    ) -> TracerProvider:
        """Install providers once; later calls adopt the existing ones."""
        existing = _SHARED.get("tracer_provider")
        if existing is not None:
            if span_processor is not None:
                try:
                    existing.add_span_processor(span_processor)
                except Exception:  # pragma: no cover - defensive
                    pass
            return existing

        resource = Resource.create({"service.name": self.service_name})
        tracer_provider = TracerProvider(resource=resource)
        if span_processor is not None:
            tracer_provider.add_span_processor(span_processor)
        try:
            otel_trace.set_tracer_provider(tracer_provider)
        except Exception:  # pragma: no cover - already set in this process
            pass

        readers = [metric_reader] if metric_reader is not None else []
        meter_provider = MeterProvider(metric_readers=readers, resource=resource)
        try:
            otel_metrics.set_meter_provider(meter_provider)
        except Exception:  # pragma: no cover - already set in this process
            pass

        _SHARED["tracer_provider"] = tracer_provider
        _SHARED["meter_provider"] = meter_provider
        return tracer_provider

    @property
    def tracer_provider(self) -> TracerProvider | None:
        return _SHARED.get("tracer_provider")

    @property
    def meter_provider(self) -> MeterProvider | None:
        return _SHARED.get("meter_provider")

    def tracer(self) -> Any:
        provider = self.tracer_provider
        if provider is None:
            provider = self.configure()
        return provider.get_tracer(self.service_name)

    def meter(self) -> Any:
        provider = self.meter_provider
        if provider is None:
            self.configure()
            provider = self.meter_provider
        return provider.get_meter(self.service_name)

    def shutdown(self) -> None:
        tracer_provider = _SHARED.pop("tracer_provider", None)
        meter_provider = _SHARED.pop("meter_provider", None)
        for provider in (tracer_provider, meter_provider):
            if provider is None:
                continue
            try:
                provider.shutdown()
            except Exception:  # pragma: no cover - best-effort
                pass
