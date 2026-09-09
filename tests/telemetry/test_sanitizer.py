"""Export-boundary telemetry privacy regression tests."""

from __future__ import annotations

from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from trpc_service.telemetry import SanitizingSpanExporter


def test_sanitizer_exports_only_allowlisted_attributes() -> None:
    captured = InMemorySpanExporter()
    provider = TracerProvider(
        resource=Resource.create(
            {"service.name": "test-service", "process.command_args": ["--secret", "value"]}
        )
    )
    provider.add_span_processor(SimpleSpanProcessor(SanitizingSpanExporter(captured)))
    tracer = provider.get_tracer("privacy-test")

    with tracer.start_as_current_span("invocation") as span:
        span.set_attribute("gen_ai.operation.name", "run_runner")
        span.set_attribute("runner.input", "private prompt")
        span.set_attribute("tool_call_args", "api_key=private")
        span.set_attribute("platform.request_id", "req-1")
        span.add_event(
            "model.complete",
            {"gen_ai.usage.output_tokens": 12, "llm_response": "private output"},
        )

    provider.shutdown()
    exported = captured.get_finished_spans()
    assert len(exported) == 1
    assert exported[0].attributes == {
        "gen_ai.operation.name": "run_runner",
        "platform.request_id": "req-1",
    }
    assert exported[0].events[0].attributes == {"gen_ai.usage.output_tokens": 12}
    assert exported[0].resource.attributes["service.name"] == "test-service"
    assert "process.command_args" not in exported[0].resource.attributes
