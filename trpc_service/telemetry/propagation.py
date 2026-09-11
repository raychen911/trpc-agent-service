"""W3C TraceContext propagation helpers.

Only ``traceparent`` is ever injected or extracted: a dedicated
:class:`TraceContextTextMapPropagator` instance is used (never the global
propagator), and baggage is deliberately excluded.  Invalid inbound values
yield an invalid span context, so callers simply start a new trace.
"""

from __future__ import annotations

from typing import MutableMapping

from opentelemetry import context as otel_context
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator

_PROPAGATOR: TraceContextTextMapPropagator = TraceContextTextMapPropagator()

TRACEPARENT_HEADER = "traceparent"


def inject_traceparent(headers: MutableMapping[str, str]) -> None:
    """Write ``traceparent`` for the currently active span into ``headers``.

    No-op (headers untouched) when there is no recording span context.
    Must never raise: propagation is an observability side channel.
    """
    try:
        _PROPAGATOR.inject(headers)
    except Exception:
        # A propagation failure must not break an outbound request.
        return None


def extract_traceparent(headers):
    """Return a Context carrying the parent span extracted from ``headers``.

    Invalid/missing values return a context without a valid span — callers
    pass it straight to ``start_span(context=...)`` and naturally start a
    fresh trace.
    """
    try:
        carrier = dict(headers or {})
        # Never inherit an ambient task context for a missing/invalid remote
        # header.  ASGI servers and IM callbacks may reuse event-loop tasks;
        # an explicit empty base guarantees that such requests start a root.
        return _PROPAGATOR.extract(carrier=carrier, context=otel_context.Context())
    except Exception:
        return otel_context.Context()


__all__ = ["TRACEPARENT_HEADER", "extract_traceparent", "inject_traceparent"]
