# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Observability helpers for the enterprise layer.

The framework's ``report_call_llm`` / ``report_execute_tool`` /
``report_invoke_agent`` already accept ``extra_attributes``; these helpers
produce the tenant attribute mapping so every metric and span can be sliced by
tenant.
"""

from __future__ import annotations

import os
import socket
from contextlib import contextmanager
from typing import Any
from typing import Iterator
from typing import Mapping
from typing import Optional

TENANT_ID_ATTRIBUTE = "tenant.id"
"""OpenTelemetry attribute key used to tag spans and metrics with the tenant."""

_TELEMETRY_CONFIGURED = False
_TRACE_PROVIDER: Any = None
_METER_PROVIDER: Any = None


def configure_telemetry(service_name: str) -> bool:
    """Configure OTLP tracing and metrics when an exporter endpoint is present.

    Local development stays dependency-free and uses the no-op provider. In a
    deployed gateway/worker, ``OTEL_EXPORTER_OTLP_ENDPOINT`` activates batch
    trace export and periodic metric export to the configured collector. The
    process is configured once.
    """
    global _METER_PROVIDER
    global _TELEMETRY_CONFIGURED
    global _TRACE_PROVIDER
    if _TELEMETRY_CONFIGURED:
        return True
    generic_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    trace_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
    metric_endpoint = os.environ.get("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
    if not any((generic_endpoint, trace_endpoint, metric_endpoint)):
        return False
    try:
        from opentelemetry import metrics
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.metric_exporter import OTLPMetricExporter
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.metrics import MeterProvider
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader
        from opentelemetry.sdk.metrics.view import ExplicitBucketHistogramAggregation
        from opentelemetry.sdk.metrics.view import View
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:  # pragma: no cover - deployment dependency is optional
        return False

    configured_name = os.environ.get("OTEL_SERVICE_NAME", service_name)
    resource_attributes = {"service.name": configured_name}
    instance_id = os.environ.get("OTEL_SERVICE_INSTANCE_ID") or os.environ.get("HOSTNAME") or socket.gethostname()
    if instance_id:
        resource_attributes["service.instance.id"] = instance_id
    resource = Resource.create(resource_attributes)

    trace_provider = None
    if generic_endpoint or trace_endpoint:
        trace_provider = TracerProvider(resource=resource)
        trace_provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
        trace.set_tracer_provider(trace_provider)

    meter_provider = None
    if generic_endpoint or metric_endpoint:
        from ._metrics import METRIC_DEFINITIONS

        try:
            interval_ms = int(os.environ.get("OTEL_METRIC_EXPORT_INTERVAL", "60000"))
        except ValueError:
            interval_ms = 60000
        metric_reader = PeriodicExportingMetricReader(
            OTLPMetricExporter(),
            export_interval_millis=max(1000, interval_ms),
        )
        views = [
            View(
                instrument_name=name,
                aggregation=ExplicitBucketHistogramAggregation(definition.boundaries),
            ) for name, definition in METRIC_DEFINITIONS.items()
            if definition.kind == "histogram" and definition.boundaries
        ]
        meter_provider = MeterProvider(resource=resource, metric_readers=[metric_reader], views=views)
        metrics.set_meter_provider(meter_provider)

    _TRACE_PROVIDER = trace_provider
    _METER_PROVIDER = meter_provider
    _TELEMETRY_CONFIGURED = True
    return True


def shutdown_telemetry() -> None:
    """Flush and close process-owned telemetry providers."""
    global _METER_PROVIDER
    global _TELEMETRY_CONFIGURED
    global _TRACE_PROVIDER
    providers = (_METER_PROVIDER, _TRACE_PROVIDER)
    _METER_PROVIDER = None
    _TRACE_PROVIDER = None
    _TELEMETRY_CONFIGURED = False
    for provider in providers:
        if provider is not None:
            provider.shutdown()


def tenant_attributes(tenant_id: str) -> Mapping[str, str]:
    """Return the attribute mapping to pass as ``extra_attributes``."""
    return {TENANT_ID_ATTRIBUTE: tenant_id}


def attach_tenant_to_span(tenant_id: str) -> None:
    """Tag the currently active span with the tenant id (no-op outside a span)."""
    try:
        from opentelemetry import trace
    except ImportError:  # pragma: no cover - OTel optional at runtime
        return
    span = trace.get_current_span()
    if span is not None and span.is_recording():
        span.set_attribute(TENANT_ID_ATTRIBUTE, tenant_id)


def inject_trace_headers() -> dict[str, str]:
    """Serialize the active OpenTelemetry context for an async queue message."""
    try:
        from opentelemetry import propagate
    except ImportError:  # pragma: no cover - OTel optional at runtime
        return {}
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


def current_trace_id() -> Optional[str]:
    """Return the active 32-character trace id, or ``None`` outside a trace."""
    try:
        from opentelemetry import trace
    except ImportError:  # pragma: no cover - OTel optional at runtime
        return None
    span = trace.get_current_span()
    if span is None:
        return None
    trace_id = span.get_span_context().trace_id
    return f"{trace_id:032x}" if trace_id else None


@contextmanager
def extracted_trace_context(headers: dict[str, str]) -> Iterator[None]:
    """Attach queue-carried trace context for the duration of worker dispatch."""
    if not headers:
        yield
        return
    try:
        from opentelemetry import context
        from opentelemetry import propagate
    except ImportError:  # pragma: no cover - OTel optional at runtime
        yield
        return
    token: Any = context.attach(propagate.extract(headers))
    try:
        yield
    finally:
        context.detach(token)


@contextmanager
def callback_span(tenant_id: str, channel: str) -> Iterator[None]:
    """Open an ``im_callback`` span tagged with tenant and channel.

    The span wraps the whole gateway dispatch so the trace links the inbound
    IM callback to the Runner / Tool / Session spans created downstream.
    """
    try:
        from opentelemetry import trace
    except ImportError:  # pragma: no cover - OTel optional at runtime
        yield
        return
    tracer = trace.get_tracer("trpc.python.agent")
    with tracer.start_as_current_span("im_callback") as span:
        span.set_attribute(TENANT_ID_ATTRIBUTE, tenant_id)
        span.set_attribute("channel", channel)
        yield


@contextmanager
def operation_span(name: str, **attributes: Any) -> Iterator[None]:
    """Open a platform operation span with low-cardinality attributes."""
    try:
        from opentelemetry import trace
    except ImportError:  # pragma: no cover - OTel optional at runtime
        yield
        return
    tracer = trace.get_tracer("trpc.python.agent.enterprise")
    with tracer.start_as_current_span(name) as span:
        for key, value in attributes.items():
            if value is not None:
                span.set_attribute(key, value)
        yield


@contextmanager
def storage_span(tenant_id: str, data_type: str, operation: str) -> Iterator[None]:
    """Trace one tenant-scoped Session or Memory backend operation."""
    try:
        from opentelemetry import trace
    except ImportError:  # pragma: no cover - OTel optional at runtime
        yield
        return
    tracer = trace.get_tracer("trpc.python.agent.enterprise.storage")
    with tracer.start_as_current_span(f"{data_type}.{operation}") as span:
        span.set_attribute(TENANT_ID_ATTRIBUTE, tenant_id)
        span.set_attribute("storage.data_type", data_type)
        span.set_attribute("storage.operation", operation)
        yield
