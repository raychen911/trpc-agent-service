# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""W3C trace-context helpers for platform spans around SDK spans."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
import importlib
from typing import Any

from opentelemetry import propagate
from opentelemetry import trace
from trpc_service.log.audit import mask_sensitive_text

from trpc_service.gateway.models import TraceContext

_TRACER = trace.get_tracer("trpc-agent-service", "0.1.0")


class _AsyncGeneratorSafeTracer:
    """Create SDK spans without attaching Context across generator yields."""

    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    @contextmanager
    def start_as_current_span(self,
                              name: str,
                              context: Any = None,
                              kind: Any = trace.SpanKind.INTERNAL,
                              attributes: Any = None,
                              links: Any = None,
                              start_time: Any = None,
                              record_exception: bool = True,
                              set_status_on_exception: bool = True,
                              end_on_exit: bool = True,
                              **kwargs: Any) -> Iterator[trace.Span]:
        del kwargs
        span = self._delegate.start_span(name,
                                         context=context,
                                         kind=kind,
                                         attributes=attributes,
                                         links=links,
                                         start_time=start_time)
        try:
            yield span
        except Exception as error:
            if record_exception:
                span.record_exception(error)
            if set_status_on_exception:
                span.set_status(trace.StatusCode.ERROR, type(error).__name__)
            raise
        finally:
            if end_on_exit:
                span.end()


_SDK_TRACING_PATCHED = False


def install_sdk_async_generator_safe_tracing() -> None:
    """Apply a process-local SDK 1.1.19 tracing compatibility adapter.

    No SDK source file is changed. SDK spans are still recorded under the
    current platform span, but they are not installed as current Context over
    an async-generator yield boundary.
    """
    global _SDK_TRACING_PATCHED
    if _SDK_TRACING_PATCHED:
        return
    module_names = (
        "trpc_agent_sdk.telemetry",
        "trpc_agent_sdk.telemetry._trace",
        "trpc_agent_sdk.runners",
        "trpc_agent_sdk.agents._base_agent",
        "trpc_agent_sdk.agents.core._llm_processor",
        "trpc_agent_sdk.agents.core._tools_processor",
    )
    for module_name in module_names:
        module = importlib.import_module(module_name)
        sdk_tracer = getattr(module, "tracer", None)
        if sdk_tracer is not None and not isinstance(sdk_tracer, _AsyncGeneratorSafeTracer):
            setattr(module, "tracer", _AsyncGeneratorSafeTracer(sdk_tracer))
    _SDK_TRACING_PATCHED = True


@contextmanager
def platform_span(name: str,
                  trace_context: TraceContext | None = None,
                  attributes: dict[str, Any] | None = None) -> Iterator[trace.Span]:
    carrier = {}
    if trace_context:
        carrier = {
            "traceparent": trace_context.traceparent,
            "tracestate": trace_context.tracestate,
        }
    parent = propagate.extract(carrier={
        key: value
        for key, value in carrier.items() if value
    }) if carrier.get("traceparent") else None
    allowed = {
        "trpc_service.tenant_id", "trpc_service.app_id", "trpc_service.config_version", "trpc_service.channel",
        "trpc_service.backend", "trpc_service.operation"
    }
    safe = {
        key: mask_sensitive_text(value) if isinstance(value, str) else value
        for key, value in (attributes or {}).items() if key in allowed
    }
    with _TRACER.start_as_current_span(name,
                                       context=parent,
                                       attributes=safe,
                                       record_exception=False,
                                       set_status_on_exception=False) as span:
        try:
            yield span
        except BaseException as error:
            span.set_attribute("error.type", type(error).__name__)
            span.set_status(trace.StatusCode.ERROR)
            raise


def current_trace_context() -> TraceContext:
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return TraceContext(
        traceparent=carrier.get("traceparent", ""),
        tracestate=carrier.get("tracestate", ""),
        baggage="",
    )


def configure_otlp_tracing(endpoint: str, service_name: str = "trpc-agent-service") -> Any:
    """Install an OTLP SDK provider on explicit production configuration only."""
    if not endpoint:
        return None
    try:
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
    except ImportError as error:
        raise RuntimeError("OTLP endpoint configured but OpenTelemetry SDK/exporter is missing") from error
    provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
    provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter(endpoint=endpoint)))
    trace.set_tracer_provider(provider)
    return provider
