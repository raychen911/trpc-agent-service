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
from contextlib import contextmanager
from typing import Any
from typing import Iterator
from typing import Mapping
from typing import Optional

TENANT_ID_ATTRIBUTE = "tenant.id"
"""OpenTelemetry attribute key used to tag spans and metrics with the tenant."""

_TELEMETRY_CONFIGURED = False


def configure_telemetry(service_name: str) -> bool:
    """Configure OTLP tracing when an exporter endpoint is present.

    Local development stays dependency-free and uses the no-op provider. In a
    deployed gateway/worker, ``OTEL_EXPORTER_OTLP_ENDPOINT`` activates a batch
    exporter to the configured collector. The process is configured once.
    """
    global _TELEMETRY_CONFIGURED
    if _TELEMETRY_CONFIGURED:
        return True
    if not os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        return False
    try:
        from opentelemetry import trace
        from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError:  # pragma: no cover - deployment dependency is optional
        return False

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(provider)
    _TELEMETRY_CONFIGURED = True
    return True


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
