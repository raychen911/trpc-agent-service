"""SDK public logger and platform operations share safe trace-bearing JSON."""

import asyncio
import json
import logging

import pytest
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from trpc_agent_sdk import log as sdk_log

from trpc_service.telemetry import runtime
from trpc_service.telemetry.logging import SafeJsonFormatter, context
from trpc_service.telemetry.sdk_logging import configure_sdk_logging


def test_sdk_and_exception_logs_keep_trace_without_bodies(caplog):
    configure_sdk_logging()
    exporter = InMemorySpanExporter()
    runtime.configure("logs-test", span_exporter=exporter)
    values = []

    class Capture(logging.Handler):

        def emit(self, record):
            values.append(json.loads(SafeJsonFormatter().format(record)))

    handler = Capture()
    logging.getLogger().addHandler(handler)
    try:
        with caplog.at_level(logging.INFO), pytest.raises(ValueError):
            with runtime.operation("worker.execute", attributes={"tenant.id": "tenant_a", "request_id": "req_1"}):
                sdk_log.warning("api_key=private", extra={"prompt": "private"})
                logging.getLogger("dependency").error("private", exc_info=True)
                raise ValueError("private")
        sdk = next(value for value in values if value["event"] == "sdk.warning")
        failure = next(value for value in values if value["event"] == "operation.error")
        assert sdk["trace_id"] == failure["trace_id"]
        assert failure["tenant_id"] == "tenant_a" and failure["error_type"] == "ValueError"
        assert failure["latency_ms"] >= 0
        assert "private" not in json.dumps(values)
        record = logging.LogRecord("test", 20, "", 1, "private", (), None)
        assert "tenant_id" not in json.loads(SafeJsonFormatter().format(record))
    finally:
        logging.getLogger().removeHandler(handler)
        runtime.shutdown()


@pytest.mark.asyncio
async def test_logging_context_does_not_cross_concurrent_tenants():

    async def task(tenant):
        with context(tenant_id=tenant, prompt="private", request_id="email@example.com"):
            await asyncio.sleep(0)
            return json.loads(SafeJsonFormatter().format(logging.LogRecord("test", 20, "", 1, "private", (), None)))

    left, right = await asyncio.gather(task("tenant_a"), task("tenant_b"))
    assert left["tenant_id"] == "tenant_a" and right["tenant_id"] == "tenant_b"
    assert "prompt" not in left and "request_id" not in right


def test_native_sdk_span_export_removes_content_but_preserves_parent():
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.trace import Status, StatusCode
    from trpc_service.telemetry.sdk_tracing import SanitizedSDKExporter
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(SanitizedSDKExporter(exporter)))
    sdk = provider.get_tracer("trpc.python.agent")
    try:
        with sdk.start_as_current_span("private-runner") as parent:
            with sdk.start_as_current_span("private-prompt") as child:
                child.set_attribute("gen_ai.operation.name", "call_llm")
                child.set_attribute("gen_ai.usage.input_tokens", 12)
                child.set_attribute("trpc.python.agent.llm_request", "private-body")
                child.add_event("private-body", {"secret": "private-body"})
                child.set_status(Status(StatusCode.ERROR, "private-error"))
        result = exporter.get_finished_spans()[0]
        assert result.name == "sdk.call_llm" and result.parent.span_id == parent.get_span_context().span_id
        assert result.attributes["gen_ai.usage.input_tokens"] == 12
        assert not result.events and not result.status.description
        assert "private" not in result.to_json()
    finally:
        provider.shutdown()
