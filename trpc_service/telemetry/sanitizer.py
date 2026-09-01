"""Allowlist span data before it reaches an OTLP exporter.

tRPC-Agent may create rich in-process span attributes. A collector-side processor is
too late for a strict "sensitive content never leaves the process" guarantee, so this
module creates sanitized ``ReadableSpan`` copies at the terminal exporter boundary.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import Event, ReadableSpan
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import Link
from opentelemetry.trace.status import Status

type AttributeValue = (
    str | bool | int | float | Sequence[str] | Sequence[bool] | Sequence[int] | Sequence[float]
)

_ALLOWED_EXACT = frozenset(
    {
        "db.system",
        "error.type",
        "exception.type",
        "gen_ai.operation.name",
        "gen_ai.request.max_tokens",
        "gen_ai.request.model",
        "gen_ai.response.finish_reasons",
        "gen_ai.response.model",
        "gen_ai.system",
        "gen_ai.usage.input_tokens",
        "gen_ai.usage.output_tokens",
        "http.request.method",
        "http.response.status_code",
        "http.route",
        "network.protocol.version",
        "platform.app",
        "platform.channel",
        "platform.operation",
        "platform.outcome",
        "platform.request_id",
        "platform.session_key",
        "platform.tenant_key",
        "server.address",
        "server.port",
        "service.name",
        "service.version",
    }
)
_ALLOWED_PREFIXES = ("telemetry.sdk.", "deployment.environment.")


def _sanitize_attributes(
    attributes: Mapping[str, AttributeValue] | None,
) -> dict[str, AttributeValue]:
    if not attributes:
        return {}
    return {
        key: value
        for key, value in attributes.items()
        if key in _ALLOWED_EXACT or key.startswith(_ALLOWED_PREFIXES)
    }


def _sanitize_span(span: ReadableSpan) -> ReadableSpan:
    events = tuple(
        Event(
            event.name,
            attributes=_sanitize_attributes(event.attributes),
            timestamp=event.timestamp,
        )
        for event in span.events
    )
    links = tuple(
        Link(link.context, attributes=_sanitize_attributes(link.attributes)) for link in span.links
    )
    resource = Resource(
        _sanitize_attributes(span.resource.attributes),
        schema_url=span.resource.schema_url,
    )
    return ReadableSpan(
        name=span.name[:128],
        context=span.context,
        parent=span.parent,
        resource=resource,
        attributes=_sanitize_attributes(span.attributes),
        events=events,
        links=links,
        kind=span.kind,
        status=Status(span.status.status_code),
        start_time=span.start_time,
        end_time=span.end_time,
        instrumentation_scope=span.instrumentation_scope,
    )


class SanitizingSpanExporter(SpanExporter):
    """Wrap an exporter and pass it only allowlisted span copies."""

    def __init__(self, delegate: SpanExporter) -> None:
        self._delegate = delegate

    def export(self, spans: Sequence[ReadableSpan]) -> SpanExportResult:
        return self._delegate.export(tuple(_sanitize_span(span) for span in spans))

    def shutdown(self) -> None:
        self._delegate.shutdown()

    def force_flush(self, timeout_millis: int = 30_000) -> bool:
        return self._delegate.force_flush(timeout_millis)
