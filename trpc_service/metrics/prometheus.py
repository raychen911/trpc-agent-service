"""Prometheus exposition and OpenTelemetry bootstrap."""

from __future__ import annotations

import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

from .privacy import tenant_label

INBOUND_TOTAL = Counter("trpc_inbound_messages_total", "Inbound messages", ("tenant", "channel", "result"))
OUTBOUND_TOTAL = Counter("trpc_outbound_messages_total", "Outbound messages", ("tenant", "channel", "result"))
QUEUE_DEPTH = Gauge("trpc_queue_depth", "Current queue depth", ("queue",))
TOOL_TOTAL = Counter("trpc_tool_calls_total", "Tool calls", ("tenant", "tool", "result"))
SESSION_FENCING_CONFLICTS = Counter("trpc_session_fencing_conflicts_total", "Stale session commits", ("tenant",))
OPERATION_DURATION = Histogram(
    "trpc_operation_duration_seconds",
    "Latency of bounded service operations",
    ("component", "tenant", "operation", "result"),
)
TOKEN_USAGE = Counter("trpc_model_tokens_total", "Estimated model tokens", ("tenant", "kind"))
MODEL_COST = Counter("trpc_model_cost_total", "Estimated model cost", ("tenant", "currency"))
def record_inbound(tenant_id: str, channel: str, result: str) -> None:
    INBOUND_TOTAL.labels(tenant_label(tenant_id), channel, result).inc()


def record_outbound(tenant_id: str, channel: str, result: str) -> None:
    OUTBOUND_TOTAL.labels(tenant_label(tenant_id), channel, result).inc()


def record_queue_depth(queue: str, depth: int) -> None:
    QUEUE_DEPTH.labels(queue).set(max(0, depth))


def record_tool(tenant_id: str, tool: str, result: str) -> None:
    TOOL_TOTAL.labels(tenant_label(tenant_id), tool, result).inc()


def record_fencing_conflict(tenant_id: str) -> None:
    SESSION_FENCING_CONFLICTS.labels(tenant_label(tenant_id)).inc()


def record_model_usage(tenant_id: str, input_tokens: int, output_tokens: int, cost: float) -> None:
    tenant = tenant_label(tenant_id)
    TOKEN_USAGE.labels(tenant, "input").inc(max(0, input_tokens))
    TOKEN_USAGE.labels(tenant, "output").inc(max(0, output_tokens))
    MODEL_COST.labels(tenant, "configured").inc(max(0.0, cost))


@contextmanager
def observe_operation(component: str, tenant_id: str, operation: str) -> Iterator[None]:
    start = time.perf_counter()
    result = "ok"
    try:
        yield
    except BaseException:
        result = "error"
        raise
    finally:
        OPERATION_DURATION.labels(component, tenant_label(tenant_id), operation, result).observe(
            time.perf_counter() - start
        )


def exposition() -> tuple[bytes, str]:
    return generate_latest(), CONTENT_TYPE_LATEST


def configure_telemetry(service_name: str, otlp_endpoint: str | None = None) -> Any:
    """Configure an OTLP span exporter when explicitly requested."""
    if not otlp_endpoint:
        return None
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint.rstrip("/") + "/v1/traces")))
    trace.set_tracer_provider(provider)
    return provider


def configure_fastapi_instrumentation(app: Any) -> None:
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(app)
    except (ImportError, RuntimeError):
        return
