"""Span sanitizing at the process boundary (DEC-001, FR-031, NFR-003).

``SanitizingSpanProcessor`` maps official/platform ``ReadableSpan`` objects
to ``SafeSpanEnvelope`` values through their PUBLIC read-only surface only:

1. identity (trace/span/parent ids), timing, status and instrumentation
   scope are preserved;
2. only centrally allowlisted attribute keys with scalar values survive;
3. events and links contribute counts only — no payloads ever leave;
4. the input span is never mutated and never handed to a network exporter;
5. unsupported SDK shapes fail closed as ``telemetry_adapter_incompatible``.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from opentelemetry.sdk.trace import SpanProcessor
from opentelemetry.trace import StatusCode

from trpc_service.operations.operations_errors import TelemetryAdapterIncompatible

ATTRIBUTE_ALLOWLIST: frozenset[str] = frozenset(
    {
        "component",
        "stage",
        "outcome",
        "channel",
        "role",
        "generation",
        "adapter_generation",
        "attempt_no",
        "configuration_version",
        "error_type",
        "retryable",
        "tool_class",
        "backend_type",
        "operation",
        "kind",
        "signal_type",
        "priority",
        "reason_code",
        "actor_role",
    }
)

_SCALAR_TYPES = (str, int, float, bool)

_STATUS_NAMES: dict[int, str] = {
    StatusCode.UNSET: "unset",
    StatusCode.OK: "ok",
    StatusCode.ERROR: "error",
}


@dataclass(frozen=True, slots=True)
class SafeSpanEnvelope:
    """Sanitized, export-safe view of one span."""

    trace_id: str
    span_id: str
    parent_span_id: str | None
    name: str
    scope_name: str
    status: str
    start_ns: int
    end_ns: int
    attributes: dict[str, Any]
    event_count: int
    links_count: int


def _require_attribute(span: Any, name: str) -> Any:
    try:
        return getattr(span, name)
    except AttributeError:
        raise TelemetryAdapterIncompatible(f"span shape lacks {name!r}") from None


def build_safe_span_envelope(span: Any) -> SafeSpanEnvelope:
    """Map one ReadableSpan-like object to a SafeSpanEnvelope.

    Raises ``TelemetryAdapterIncompatible`` when the shape is unsupported so
    callers fail closed instead of exporting an unsafe or partial span.
    """
    context = _require_attribute(span, "get_span_context")()
    try:
        trace_id = context.trace_id
        span_id = context.span_id
    except AttributeError:
        raise TelemetryAdapterIncompatible("span context is incomplete") from None
    if not trace_id or not span_id:
        raise TelemetryAdapterIncompatible("span context ids are invalid")
    # ``ReadableSpan.parent`` is the public read-only parent SpanContext.
    parent = getattr(span, "parent", None)

    name = _require_attribute(span, "name")
    start_ns = _require_attribute(span, "start_time")
    end_ns = _require_attribute(span, "end_time")
    if not isinstance(start_ns, int) or not isinstance(end_ns, int):
        raise TelemetryAdapterIncompatible("span timing is not nanosecond based")

    status_code = getattr(getattr(span, "status", None), "status_code", None)
    status = _STATUS_NAMES.get(status_code, "unset")

    scope = getattr(span, "instrumentation_scope", None)
    scope_name = getattr(scope, "name", "") or ""

    raw_attributes = getattr(span, "attributes", None) or {}
    attributes: dict[str, Any] = {}
    for key, value in raw_attributes.items():
        if str(key) not in ATTRIBUTE_ALLOWLIST:
            continue
        if isinstance(value, _SCALAR_TYPES):
            attributes[str(key)] = value

    events = getattr(span, "events", None) or ()
    links = getattr(span, "links", None) or ()

    return SafeSpanEnvelope(
        trace_id=format(trace_id, "032x"),
        span_id=format(span_id, "016x"),
        parent_span_id=format(parent.span_id, "016x") if parent is not None else None,
        name=str(name),
        scope_name=str(scope_name),
        status=status,
        start_ns=int(start_ns),
        end_ns=int(end_ns),
        attributes=attributes,
        event_count=len(events),
        links_count=len(links),
    )


class SanitizingSpanProcessor(SpanProcessor):
    """Forwards only sanitized envelopes to the sink; never raw spans."""

    def __init__(self, sink: Callable[[SafeSpanEnvelope], None]) -> None:
        self._sink = sink

    def on_start(self, span: Any, parent_context: Any = None) -> None:  # pragma: no cover - pass-through
        del parent_context, span

    def on_end(self, readable_span: Any) -> None:
        envelope = build_safe_span_envelope(readable_span)
        self._sink(envelope)

    def force_flush(self, timeout_millis: int = 30000) -> bool:
        del timeout_millis
        return True

    def shutdown(self) -> None:
        return None
