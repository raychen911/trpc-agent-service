"""OpenTelemetry processor that removes content before export."""

from __future__ import annotations

from collections.abc import Mapping
from types import MappingProxyType
from typing import Any

from opentelemetry.context import Context
from opentelemetry.sdk.trace import Event, ReadableSpan, Span, SpanProcessor
from opentelemetry.trace import Status

from trpc_service.log.redaction import REDACTED, is_sensitive_key


def sanitize_attributes(attributes: Mapping[str, Any] | None) -> Mapping[str, Any]:
    if not attributes:
        return MappingProxyType({})
    sanitized = {
        key: REDACTED if is_sensitive_key(key) else value for key, value in attributes.items()
    }
    return MappingProxyType(sanitized)


class _SanitizedSpan:
    """Read-only proxy preserving span metadata but replacing attributes."""

    def __init__(self, span: ReadableSpan) -> None:
        self._span = span
        self.attributes = sanitize_attributes(span.attributes)
        self.status = Status(span.status.status_code)
        self.events = tuple(
            Event(
                event.name,
                sanitize_attributes(event.attributes),
                event.timestamp,
            )
            for event in span.events
        )

    def __getattr__(self, name: str) -> Any:
        return getattr(self._span, name)


class PrivacySpanProcessor(SpanProcessor):
    """Wrap another processor and sanitize completed spans.

    SDK 1.1.x writes prompts, tool arguments, state, and results into attributes.
    The wrapped exporter never receives those raw attribute values unless this
    processor is deliberately omitted in local development.
    """

    def __init__(self, delegate: SpanProcessor) -> None:
        self._delegate = delegate

    def on_start(self, span: Span, parent_context: Context | None = None) -> None:
        self._delegate.on_start(span, parent_context=parent_context)

    def on_end(self, span: ReadableSpan) -> None:
        self._delegate.on_end(_SanitizedSpan(span))  # type: ignore[arg-type]

    def shutdown(self) -> None:
        self._delegate.shutdown()

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        return self._delegate.force_flush(timeout_millis)


def inject_trace_headers(carrier: dict[str, str]) -> None:
    """Inject current W3C trace context into an outbox/queue carrier."""

    from opentelemetry.propagate import inject

    inject(carrier)


def extract_trace_context(carrier: Mapping[str, str]) -> Context:
    """Extract W3C trace context at a process boundary."""

    from opentelemetry.propagate import extract

    return extract(carrier)


__all__ = [
    "PrivacySpanProcessor",
    "extract_trace_context",
    "inject_trace_headers",
    "sanitize_attributes",
]
