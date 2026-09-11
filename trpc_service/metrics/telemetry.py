"""Privacy-safe OpenTelemetry span helper."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from secrets import randbits
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, StatusCode, TraceFlags, TraceState

from .privacy import sanitize_attributes, tenant_label


@contextmanager
def span(name: str, *, tenant_id: str | None = None, **attributes: object) -> Iterator[Any]:
    values = sanitize_attributes(attributes)
    if tenant_id:
        values["tenant"] = tenant_label(tenant_id)
    parent = None
    propagated = values.get("trace_id")
    if not trace.get_current_span().get_span_context().is_valid and isinstance(propagated, str):
        try:
            trace_id = int(propagated, 16)
        except ValueError:
            trace_id = 0
        if len(propagated) == 32 and trace_id:
            span_id = randbits(64) or 1
            parent_span = NonRecordingSpan(
                SpanContext(
                    trace_id=trace_id,
                    span_id=span_id,
                    is_remote=True,
                    trace_flags=TraceFlags(TraceFlags.SAMPLED),
                    trace_state=TraceState(),
                )
            )
            parent = trace.set_span_in_context(parent_span)
    with trace.get_tracer("trpc_service").start_as_current_span(
        name,
        context=parent,
        attributes=values,
        record_exception=False,
        set_status_on_exception=False,
    ) as current:
        try:
            yield current
        except BaseException as exc:
            current.set_attribute("error.type", type(exc).__name__)
            current.set_status(StatusCode.ERROR)
            raise
