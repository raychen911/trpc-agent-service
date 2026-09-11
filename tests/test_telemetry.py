from __future__ import annotations

import asyncio
from dataclasses import replace

import pytest
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from tests.test_reliability import message, service
from trpc_service.log import redact
from trpc_service.metrics import telemetry
from trpc_service.metrics.privacy import sanitize_attributes


@pytest.fixture
def spans(monkeypatch):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(telemetry.trace, "get_tracer", provider.get_tracer)
    yield exporter
    provider.shutdown()


def test_queue_and_storage_spans_keep_message_trace(spans):
    async def run():
        runtime, _ = service()
        inbound = replace(message(), trace_id="1234567890abcdef1234567890abcdef")
        await runtime.enqueue(inbound)
        await runtime.process_one()
        return inbound

    inbound = asyncio.run(run())
    recorded = spans.get_finished_spans()
    assert {"queue.publish", "queue.consume", "agent.turn", "runner", "session.read",
            "memory.read", "session.write", "memory.write", "im.reply"} <= {s.name for s in recorded}
    assert {s.context.trace_id for s in recorded} == {int(inbound.trace_id, 16)}


def test_span_does_not_export_exception_credentials(spans):
    with pytest.raises(RuntimeError), telemetry.span("failure"):
        raise RuntimeError("postgresql://admin:very-private@db/main password=also-private")
    recorded = spans.get_finished_spans()[0]
    assert recorded.attributes["error.type"] == "RuntimeError"
    assert recorded.status.is_ok is False
    assert not recorded.events
    assert recorded.status.description is None


def test_recursive_redaction_covers_trace_attributes_and_database_urls():
    values = {
        "model_api_key": "very-private",
        "detail": {"Authorization": "Basic private", "dsn": "postgresql://admin:db-pass@db/main"},
        "messages": ("token=private", "Bearer private"),
    }
    cleaned = sanitize_attributes(values)
    assert "very-private" not in repr(cleaned)
    assert "db-pass" not in repr(cleaned)
    assert "Basic private" not in repr(cleaned)
    assert cleaned["messages"] == ("token=[REDACTED]", "Bearer [REDACTED]")
    assert redact({"Authorization": "Basic private"})["Authorization"] == "[REDACTED]"
