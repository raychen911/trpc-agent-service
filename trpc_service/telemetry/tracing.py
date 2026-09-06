"""Minimal trace recorder with an OpenTelemetry-compatible span shape.

增强功能：
- 支持更细粒度的 span（Tool 执行、Storage 操作、IM 投递）
- 记录关键中间状态
- 支持 span 属性扩展
"""

from __future__ import annotations

import hashlib
import logging
import os
from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from time import monotonic

from trpc_service.security.secrets import redact_secret_text
from trpc_service.telemetry.metrics import observe_session_backend_latency
from trpc_service.tenant.models import TenantContext

logger = logging.getLogger("trpc_service.telemetry")
_current_span: ContextVar[object | None] = ContextVar("trpc_current_span", default=None)


@dataclass(slots=True)
class SpanRecord:
    name: str
    trace_id: str
    tenant_id: str
    attributes: dict[str, str]
    span_id: str = ""
    duration_ms: int = 0
    error: str | None = None


class TraceRecorder:
    def __init__(self) -> None:
        try:
            self._max_retained_spans = max(1, int(os.getenv("TRACE_MAX_RETAINED_SPANS", "1000")))
        except ValueError:
            self._max_retained_spans = 1000
        self.spans: list[SpanRecord] = []
        self._tracer = None
        self._otel_trace = None
        self._provider = None
        try:
            from opentelemetry import trace
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import BatchSpanProcessor
            from opentelemetry.trace.status import Status, StatusCode

            if os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT"):
                provider = TracerProvider(
                    resource=Resource.create({"service.name": os.getenv("OTEL_SERVICE_NAME", "trpc-agent-service")})
                )
                provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
                trace.set_tracer_provider(provider)
                self._provider = provider
            self._tracer = trace.get_tracer("trpc_service")
            self._otel_trace = trace
            self._otel_status = Status
            self._otel_status_code = StatusCode
        except ImportError:
            pass

    @contextmanager
    def span(
        self,
        name: str,
        context: TenantContext,
        attributes: dict[str, str] | None = None,
    ) -> Iterator[SpanRecord]:
        span = SpanRecord(
            name=name,
            trace_id=context.trace_id,
            tenant_id=context.tenant_id,
            attributes={
                "tenant.id": context.tenant_id,
                "agent.app.id": context.agent_app_id,
                "config.version": str(context.config_version),
                "session.id": _trace_identifier(context.session_id),
                "channel": context.channel or "",
                "user.id": _trace_identifier(context.user_id),
            },
        )
        if attributes:
            for key, value in attributes.items():
                span.attributes[str(key)] = redact_secret_text(str(value))
        started = monotonic()
        parent = _current_span.get()
        otel_span = None
        if self._tracer:
            if parent and self._otel_trace:
                otel_context = self._otel_trace.set_span_in_context(parent)
                otel_span = self._tracer.start_span(name, context=otel_context)
            elif context.traceparent and self._otel_trace:
                try:
                    from opentelemetry.propagate import extract

                    remote_context = extract({"traceparent": context.traceparent})
                    otel_span = self._tracer.start_span(name, context=remote_context)
                except Exception:
                    otel_span = self._tracer.start_span(name)
            else:
                otel_span = self._tracer.start_span(name)
        if otel_span:
            for key, value in span.attributes.items():
                otel_span.set_attribute(key, value)
            otel_span.set_attribute("trace.id", context.trace_id)
            span_context = otel_span.get_span_context()
            if span_context.is_valid:
                span.span_id = format(span_context.span_id, "016x")
                span.attributes["otel.trace_id"] = format(span_context.trace_id, "032x")
        otel_scope = None
        if otel_span and self._otel_trace:
            otel_scope = self._otel_trace.use_span(otel_span, end_on_exit=False)
            otel_scope.__enter__()
        token = _current_span.set(otel_span or parent)
        try:
            yield span
        except Exception as exc:
            span.error = redact_secret_text(f"{type(exc).__name__}: {exc}")
            if otel_span:
                otel_span.record_exception(RuntimeError(redact_secret_text(str(exc))))
                status = getattr(self, "_otel_status", None)
                status_code = getattr(self, "_otel_status_code", None)
                if status and status_code:
                    otel_span.set_status(status(status_code.ERROR, redact_secret_text(str(exc))))
            raise
        finally:
            span.duration_ms = int((monotonic() - started) * 1000)
            if otel_span:
                otel_span.end()
            if otel_scope:
                otel_scope.__exit__(None, None, None)
            _current_span.reset(token)
            self.spans.append(span)
            if len(self.spans) > self._max_retained_spans:
                del self.spans[: len(self.spans) - self._max_retained_spans]
            if span.name.startswith("storage."):
                observe_session_backend_latency(
                    context.tenant_id,
                    span.name.removeprefix("storage."),
                    span.duration_ms / 1000,
                    "error" if span.error else "ok",
                )
            logger.info(
                "trace span=%s trace_id=%s tenant_id=%s duration_ms=%s error=%s",
                span.name,
                span.trace_id,
                span.tenant_id,
                span.duration_ms,
                span.error or "",
            )

    def inject_traceparent(self) -> str | None:
        """Serialize the active OTel context for a cross-process queue hop."""
        if not self._otel_trace:
            return None
        try:
            from opentelemetry.propagate import inject

            carrier: dict[str, str] = {}
            inject(carrier)
            return carrier.get("traceparent")
        except Exception:
            return None

    def close(self) -> None:
        """Flush an OTLP provider when the process is shutting down."""
        shutdown = getattr(self._provider, "shutdown", None)
        if shutdown:
            shutdown()


def _trace_identifier(value: str | None) -> str:
    """Use stable non-reversible identifiers for user and session attributes."""
    if not value:
        return ""
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()[:16]
