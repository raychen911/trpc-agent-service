"""T020 RED: SanitizingSpanProcessor contract (FR-002, FR-031, NFR-003).

Real OpenTelemetry SDK spans are created offline and fed through the
processor; the mapping must preserve identity/timing/status while dropping
every non-allowlisted attribute and all raw event/link payloads.
"""

from __future__ import annotations

import importlib
import json
from types import SimpleNamespace

from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import Status, StatusCode

from trpc_service.operations import operations_errors


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


sanitizing = _load("trpc_service.observability.sanitizing")


class _CaptureExporter:
    """Captures raw ReadableSpan objects handed to export()."""

    def __init__(self) -> None:
        self.captured: list = []

    def export(self, spans) -> int:
        self.captured.extend(spans)
        return 0  # SpanExportResult.SUCCESS

    def shutdown(self) -> None:  # pragma: no cover - trivial
        pass


class _Sink:
    def __init__(self) -> None:
        self.envelopes: list = []
        self.errors: list = []

    def __call__(self, envelope) -> None:
        self.envelopes.append(envelope)


def _build_span(parent: bool = False):
    provider = TracerProvider()
    capture = _CaptureExporter()
    provider.add_span_processor(SimpleSpanProcessor(capture))
    tracer = provider.get_tracer("phase8-contract")

    with tracer.start_as_current_span("platform.parent") as parent_span:
        parent_span.set_attribute("component", "gateway")
        with tracer.start_as_current_span("stage.child") as span:
            span.set_attribute("component", "worker")
            span.set_attribute("stage", "runner.invoke")
            span.set_attribute("outcome", "success")
            span.set_attribute("channel", "local_http")
            span.set_attribute("url", "https://sentinel.invalid/wecom/callback?token=x")
            span.set_attribute("secret", "sk-sentinel-a1b2c3d4e5f6g7h8i9j0")
            span.set_attribute("state.payload", "SENTINEL 正文")
            span.set_attribute("user_id", "sentinel-user")
            span.set_attribute("llm.input", "raw prompt SENTINEL")
            span.add_event("llm.call", {"llm.output": "raw completion", "url": "https://sentinel.invalid"})
            span.set_status(Status(StatusCode.ERROR))
    # The child span ends (and exports) first; the parent exports last.
    child = capture.captured[0]
    root = capture.captured[-1]
    return child, root


def test_sanitizing_module_exists() -> None:
    assert sanitizing is not None, (
        "trpc_service.observability.sanitizing is not implemented yet"
    )


def test_processor_maps_identity_timing_status_and_scope() -> None:
    assert sanitizing is not None
    child, parent = _build_span()
    sink = _Sink()
    processor = sanitizing.SanitizingSpanProcessor(sink)
    processor.on_end(child)
    assert len(sink.envelopes) == 1
    envelope = sink.envelopes[0]
    assert envelope.trace_id == format(child.get_span_context().trace_id, "032x")
    assert envelope.span_id == format(child.get_span_context().span_id, "016x")
    assert envelope.parent_span_id == format(parent.get_span_context().span_id, "016x")
    assert envelope.start_ns == child.start_time
    assert envelope.end_ns == child.end_time
    assert envelope.status == "error"
    assert envelope.scope_name == "phase8-contract"
    assert envelope.name == "stage.child"


def test_processor_keeps_only_allowlisted_attributes() -> None:
    assert sanitizing is not None
    child, _parent = _build_span()
    sink = _Sink()
    sanitizing.SanitizingSpanProcessor(sink).on_end(child)
    attributes = dict(sink.envelopes[0].attributes)
    assert attributes.get("component") == "worker"
    assert attributes.get("stage") == "runner.invoke"
    serialized = json.dumps({str(k): str(v) for k, v in attributes.items()})
    for forbidden in ("url", "secret", "state.payload", "user_id", "llm.input", "sentinel", "SENTINEL"):
        assert forbidden not in serialized.lower(), (
            f"sanitized attributes still contain {forbidden!r}"
        )


def test_processor_drops_event_and_link_payloads() -> None:
    assert sanitizing is not None
    child, _parent = _build_span()
    sink = _Sink()
    sanitizing.SanitizingSpanProcessor(sink).on_end(child)
    envelope = sink.envelopes[0]
    serialized = envelope.__dict__ if hasattr(envelope, "__dict__") else repr(envelope)
    text = json.dumps(str(serialized))
    assert "sentinel.invalid" not in text.lower()
    assert "llm.call" not in text  # event payloads never survive sanitizing
    assert envelope.event_count == 1  # only the count is retained


def test_processor_does_not_mutate_readable_span() -> None:
    assert sanitizing is not None
    child, _parent = _build_span()
    before = dict(child.attributes)
    sink = _Sink()
    sanitizing.SanitizingSpanProcessor(sink).on_end(child)
    assert dict(child.attributes) == before
    assert child.status.status_code.name == "ERROR"


def test_incompatible_sdk_shape_fails_closed() -> None:
    assert sanitizing is not None
    sink = _Sink()
    processor = sanitizing.SanitizingSpanProcessor(sink)
    bogus = SimpleNamespace(name="rogue")  # no span context / attributes shape
    try:
        processor.on_end(bogus)
    except operations_errors.TelemetryAdapterIncompatible:
        pass
    else:
        raise AssertionError("incompatible SDK shapes must fail closed")
    assert sink.envelopes == [], "no envelope may leave the process on failure"
