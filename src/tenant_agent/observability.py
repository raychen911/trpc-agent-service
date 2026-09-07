"""Low-cardinality metrics and end-to-end OpenTelemetry helpers."""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import contextmanager
from typing import Any

from opentelemetry import context, metrics, propagate, trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, SpanExporter, SpanExportResult
from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased
from opentelemetry.trace import Link, Status
from prometheus_client import Counter, Gauge, Histogram

from tenant_agent.security import Redactor
from tenant_agent.settings import Settings

logger = logging.getLogger(__name__)

REQUESTS = Counter(
    "tenant_agent_requests_total",
    "Accepted inbound messages",
    ("tenant_id", "channel", "result"),
)
MODEL_LATENCY = Histogram(
    "tenant_agent_model_duration_seconds",
    "End-to-end model/agent execution latency",
    ("tenant_id", "model", "result"),
)
TOOL_LATENCY = Histogram(
    "tenant_agent_tool_duration_seconds",
    "Tool execution latency",
    ("tenant_id", "tool", "result"),
)
DELIVERIES = Counter(
    "tenant_agent_im_delivery_total",
    "IM delivery attempts",
    ("tenant_id", "channel", "result"),
)
ERRORS = Counter(
    "tenant_agent_errors_total",
    "Platform errors by safe type",
    ("tenant_id", "component", "error_type"),
)
TOKENS = Counter(
    "tenant_agent_tokens_total",
    "Model tokens",
    ("tenant_id", "direction"),
)
COST = Counter(
    "tenant_agent_cost_usd_total",
    "Estimated model cost in USD",
    ("tenant_id",),
)
BACKEND_LATENCY = Histogram(
    "tenant_agent_backend_duration_seconds",
    "Session and memory backend latency",
    ("tenant_id", "resource", "operation", "backend", "result"),
)
ACTIVE_SESSIONS = Gauge(
    "tenant_agent_active_sessions",
    "Sessions currently executing on this node",
    ("tenant_id",),
)
QUEUE_DEPTH = Gauge(
    "tenant_agent_queue_depth",
    "Pending broker jobs",
    ("stream",),
)
AUXILIARY_REPAIRS = Counter(
    "tenant_agent_auxiliary_repair_total",
    "Summary and Memory repair attempts",
    ("tenant_id", "resource", "result"),
)

_tracer = trace.get_tracer("tenant-agent-platform")


class RedactingSpanExporter(SpanExporter):
    """Sanitize both tRPC-Agent and platform spans immediately before export."""

    def __init__(self, delegate: SpanExporter, redactor: Redactor) -> None:
        self.delegate = delegate
        self.redactor = redactor

    def _attributes(self, attributes: Mapping[str, Any] | None) -> dict[str, Any]:
        safe: dict[str, Any] = {}
        for key, value in (attributes or {}).items():
            if key.endswith(
                (
                    ".runner.input",
                    ".runner.output",
                    ".agent.input",
                    ".agent.output",
                    ".llm_request",
                    ".llm_response",
                    ".tool_call_args",
                    ".tool_response",
                    ".stream_function_calls.raw",
                    ".stream_function_calls.post_planner",
                    ".state.begin",
                    ".state.end",
                    ".state.partial",
                )
            ) or key in {
                "exception.message",
                "exception.stacktrace",
                "gen_ai.prompt",
                "gen_ai.completion",
                "gen_ai.input.messages",
                "gen_ai.output.messages",
            }:
                safe[key] = "[REDACTED]"
                continue
            if key in {"url.query"}:
                safe[key] = "[REDACTED]"
                continue
            if key in {"http.url", "url.full", "http.target"} and isinstance(value, str):
                safe[key] = value.partition("?")[0] + ("?[REDACTED]" if "?" in value else "")
                continue
            redacted = self.redactor.value(value, key=key)
            if isinstance(redacted, (str, bool, int, float)):
                safe[key] = redacted
            elif isinstance(redacted, list) and all(
                isinstance(item, (str, bool, int, float)) for item in redacted
            ):
                safe[key] = redacted
            else:
                safe[key] = json.dumps(redacted, ensure_ascii=False, default=str)
        return safe

    def _span(self, span: ReadableSpan) -> ReadableSpan:
        events = tuple(
            Event(
                name=self.redactor.text(item.name),
                attributes=self._attributes(item.attributes),
                timestamp=item.timestamp,
            )
            for item in span.events
        )
        links = tuple(Link(item.context, attributes=self._attributes(item.attributes)) for item in span.links)
        description = span.status.description
        return ReadableSpan(
            name=self.redactor.text(span.name),
            context=span.context,
            parent=span.parent,
            resource=span.resource,
            attributes=self._attributes(span.attributes),
            events=events,
            links=links,
            kind=span.kind,
            status=Status(
                span.status.status_code,
                self.redactor.text(description) if description else None,
            ),
            start_time=span.start_time,
            end_time=span.end_time,
            instrumentation_scope=span.instrumentation_scope,
        )

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        return self.delegate.export(tuple(self._span(span) for span in spans))

    def shutdown(self) -> None:
        self.delegate.shutdown()

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        return self.delegate.force_flush(timeout_millis)


def setup_telemetry(settings: Settings, redactor: Redactor | None = None) -> None:
    """Configure OTLP once; tRPC-Agent spans inherit this provider."""

    global _tracer
    if isinstance(trace.get_tracer_provider(), TracerProvider):
        _tracer = trace.get_tracer("tenant-agent-platform")
        return
    resource = Resource.create(
        {
            "service.name": settings.service_name,
            "service.instance.id": settings.node_id,
            "deployment.environment.name": settings.environment,
            "service.role": settings.service_role.value,
        }
    )
    provider = TracerProvider(
        resource=resource,
        sampler=ParentBased(TraceIdRatioBased(settings.trace_sample_ratio)),
    )
    if settings.otlp_endpoint:
        headers: dict[str, str] = {}
        if settings.otlp_headers:
            for pair in settings.otlp_headers.get_secret_value().split(","):
                name, separator, value = pair.partition("=")
                if separator:
                    headers[name.strip()] = value.strip()
                    if redactor is not None:
                        redactor.registry.register(value.strip())
        exporter: SpanExporter = OTLPSpanExporter(endpoint=settings.otlp_endpoint, headers=headers or None)
        if redactor is not None:
            exporter = RedactingSpanExporter(exporter, redactor)
        provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    metrics.get_meter("tenant-agent-platform")
    _tracer = trace.get_tracer("tenant-agent-platform")


def trace_id() -> str:
    span_context = trace.get_current_span().get_span_context()
    if not span_context.is_valid:
        return "0" * 32
    return f"{span_context.trace_id:032x}"


@contextmanager
def backend_timing(
    tenant_id: str,
    resource: str,
    operation: str,
    repository: Any,
) -> Iterator[None]:
    backend = str(getattr(repository, "backend_name", repository.__class__.__name__))
    started = time.perf_counter()
    result = "success"
    try:
        yield
    except Exception:
        result = "error"
        raise
    finally:
        BACKEND_LATENCY.labels(
            tenant_id,
            resource,
            operation,
            backend,
            result,
        ).observe(time.perf_counter() - started)


@contextmanager
def traced(
    name: str,
    attributes: Mapping[str, Any] | None = None,
    *,
    redactor: Redactor | None = None,
) -> Iterator[trace.Span]:
    safe_attributes = dict(attributes or {})
    if redactor:
        safe_attributes = redactor.value(safe_attributes)
    with _tracer.start_as_current_span(name, attributes=safe_attributes) as span:
        try:
            yield span
        except Exception as exc:
            span.record_exception(exc, attributes={"exception.message": exc.__class__.__name__})
            span.set_status(trace.Status(trace.StatusCode.ERROR, exc.__class__.__name__))
            raise


def inject_trace_context() -> dict[str, str]:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return carrier


@contextmanager
def extracted_trace_context(carrier: Mapping[str, str]) -> Iterator[None]:
    token = context.attach(propagate.extract(dict(carrier)))
    try:
        yield
    finally:
        context.detach(token)
