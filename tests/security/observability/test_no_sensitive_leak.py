"""T035 RED: zero sensitive leakage through every ordinary telemetry surface.

Preset canary secrets travel through success and failure paths; afterwards
they must not appear (unmasked) in ordinary logs, metrics, traces, error
details, run events, alert bodies or candidate files. The fail-closed audit
boundary stays intact and telemetry failures never change business outcomes.
"""

from __future__ import annotations

import asyncio
import dataclasses
import importlib
import json
import logging

import pytest

from tests.observability_support import (
    SENSITIVE_SENTINELS,
    sentinel_values,
)
from tests.support import FIXED_UTC, inbound_message_data


def _metrics_module():
    try:
        return importlib.import_module("trpc_service.observability.metrics")
    except ModuleNotFoundError:
        return None


def _buffer_module():
    try:
        return importlib.import_module("trpc_service.observability.buffer")
    except ModuleNotFoundError:
        return None


def _assert_no_sentinels(blob: str, surface: str) -> None:
    for value in sentinel_values():
        assert value not in blob, f"sentinel leaked via {surface}: {value[:24]!r}"


def _recorder_with_sentinel_attributes():
    from trpc_service.observability.context import build_correlation, bind_tenant_scope
    from trpc_service.observability.service import TelemetryRecorder

    correlation = bind_tenant_scope(
        build_correlation(
            channel="local_http",
            external_message_digest="sha256:" + "b" * 16,
            trace_id=None,
            role="gateway",
            node_id="worker-a",
        ),
        "tenant-alpha",
    )
    recorder = TelemetryRecorder()
    recorder.record_stage_now(
        correlation,
        "gateway.accept",
        "success",
        attributes={
            "url": SENSITIVE_SENTINELS["response_url"],
            "secret": SENSITIVE_SENTINELS["api_key"],
            "state.payload": SENSITIVE_SENTINELS["message_body"],
            "user_id": SENSITIVE_SENTINELS["phone"],
            "stage": "gateway.accept",
        },
    )
    return recorder, correlation


def test_stage_records_and_query_results_contain_no_sentinels() -> None:
    import dataclasses

    recorder, correlation = _recorder_with_sentinel_attributes()
    spans = recorder.spans_for(correlation.tenant_scope, correlation.trace_digest)
    assert spans, "the stage must be recorded"
    blob = json.dumps([dataclasses.asdict(span) for span in spans], default=str)
    _assert_no_sentinels(blob, "diagnostic stage records")
    from trpc_service.observability.service import DiagnosticQueryService

    result = asyncio.run(
        DiagnosticQueryService(store=recorder).query(
            correlation.tenant_scope, correlation.trace_digest
        )
    )
    _assert_no_sentinels(json.dumps(result, default=str), "diagnostic query")


def test_sanitized_span_envelopes_contain_no_sentinels() -> None:
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.trace import Status, StatusCode

    from trpc_service.observability.sanitizing import SanitizingSpanProcessor

    captured: list = []
    provider = TracerProvider()
    sanitizer = SanitizingSpanProcessor(sink=captured.append)
    provider.add_span_processor(sanitizer)
    tracer = provider.get_tracer("phase8-security")
    with tracer.start_as_current_span("platform.security") as span:
        span.set_attribute("stage", "gateway.accept")
        span.set_attribute("url", SENSITIVE_SENTINELS["response_url"])
        span.set_attribute("secret", SENSITIVE_SENTINELS["api_key"])
        span.set_attribute("body", SENSITIVE_SENTINELS["message_body"])
        span.set_attribute("email", SENSITIVE_SENTINELS["email"])
        span.add_event("llm.call", {"llm.output": SENSITIVE_SENTINELS["message_body"]})
        span.set_status(Status(StatusCode.ERROR))
    blob = json.dumps([dataclasses.asdict(envelope) for envelope in captured], default=str)
    _assert_no_sentinels(blob, "safe span envelopes")


def test_buffer_and_exporter_payloads_contain_no_sentinels() -> None:
    buffer_mod = _buffer_module()
    assert buffer_mod is not None, "trpc_service.observability.buffer is not implemented"
    recorder, correlation = _recorder_with_sentinel_attributes()
    exporter_mod = importlib.import_module("trpc_service.observability.exporter")
    sent: list = []

    async def transport(envelopes, *, timeout_seconds):
        sent.extend(envelopes)
        return True

    adapter = exporter_mod.OtlpHttpExporterAdapter(
        endpoint="https://otel.invalid/v1/traces", transport=transport, retry_limit=1,
    )
    from trpc_service.observability.models import TelemetryEnvelope

    envelope = TelemetryEnvelope(
        envelope_id="env-security",
        signal_type="log",
        priority="normal",
        scope_digest=correlation.tenant_scope,
        payload={"stage": "gateway.accept", "outcome": "success"},
        trace_digest=correlation.trace_digest,
    )
    buffer = buffer_mod.PriorityTelemetryBuffer(capacity=10)
    buffer.offer(envelope)
    drained = buffer.drain_normal()
    asyncio.run(adapter.export(drained))
    import dataclasses

    blob = json.dumps(
        [dataclasses.asdict(e) for e in drained] + [dataclasses.asdict(e) for e in sent],
        default=str,
    )
    _assert_no_sentinels(blob, "buffer/exporter payloads")


def test_operational_logs_contain_no_sentinels(caplog) -> None:
    from trpc_service.log import log_operational

    # Bounded envelopes are emitted; raw sentinel values never appear.
    with caplog.at_level(logging.DEBUG, logger="trpc_service"):
        log_operational(
            component="gateway",
            operation="handle_verified_message",
            error_type="audit_unavailable",
            retryable=False,
            trace_digest="sha256:" + "c" * 16,
        )
    blob = "\n".join(record.getMessage() for record in caplog.records)
    _assert_no_sentinels(blob, "operational logs")
    # Sentinel-shaped error types are rejected at the logging boundary.
    with pytest.raises(ValueError):
        log_operational(
            component="gateway",
            operation="handle_verified_message",
            error_type=SENSITIVE_SENTINELS["api_key"],
            retryable=False,
            trace_digest="sha256:" + "c" * 16,
        )


async def test_local_flow_success_and_failure_paths_leak_nothing(
    runtime_secret_env, caplog
) -> None:
    from trpc_service.web.app import build_runtime

    env = dict(runtime_secret_env)
    # Canary values deliberately enter through configuration and message body.
    env["TRPC_DEMO_ALPHA_SECRET"] = SENSITIVE_SENTINELS["api_key"]
    env["TRPC_DEMO_BETA_SECRET"] = SENSITIVE_SENTINELS["database_password"]
    runtime = build_runtime(env, now=lambda: FIXED_UTC)
    try:
        from trpc_service.channels.contracts import InboundMessage

        body = dict(inbound_message_data())
        body["text"] = SENSITIVE_SENTINELS["message_body"]
        message = InboundMessage(**body)
        reply = await runtime.gateway.handle_verified_message_for_test(message)
        assert reply.status.value in {"succeeded", "failed"}
        blob = json.dumps(reply.model_dump(), default=str)
        _assert_no_sentinels(blob, "local flow reply")
        # Every recorded stage payload also stays clean.
        spans = runtime.telemetry.spans_for(runtime.telemetry.scope_digest("tenant-alpha"))
        import dataclasses

        stage_blob = json.dumps([dataclasses.asdict(span) for span in spans], default=str)
        _assert_no_sentinels(stage_blob, "recorded stages of local flow")
    finally:
        await runtime.close()


async def test_audit_unavailable_keeps_fail_closed_boundary(runtime_secret_env) -> None:
    from trpc_service.channels.contracts import InboundMessage
    from trpc_service.storage.contracts import AuditUnavailable
    from trpc_service.web.app import build_runtime

    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    try:
        class FailingAudit:
            async def append(self, *args, **kwargs):
                raise AuditUnavailable("audit backend SENTINEL down")

        runtime.adapters.audit = FailingAudit()
        message = InboundMessage(**inbound_message_data())
        reply = await runtime.gateway.handle_verified_message_for_test(message)
        # Fail-closed: the message is NOT processed to success.
        assert reply.status.value != "succeeded"
        blob = json.dumps(reply.model_dump(), default=str)
        _assert_no_sentinels(blob, "fail-closed reply")
    finally:
        await runtime.close()


async def test_telemetry_failure_does_not_change_business_outcome(runtime_secret_env) -> None:
    from trpc_service.channels.contracts import InboundMessage
    from trpc_service.web.app import build_runtime

    runtime = build_runtime(runtime_secret_env, now=lambda: FIXED_UTC)
    try:
        class ExplodingTelemetry:
            def record_stage_now(self, *args, **kwargs):
                raise RuntimeError("telemetry SENTINEL exploded")

            def __getattr__(self, name):
                def explode(*args, **kwargs):
                    raise RuntimeError("telemetry SENTINEL exploded")

                return explode

        runtime.gateway.telemetry = ExplodingTelemetry()
        message = InboundMessage(**inbound_message_data())
        reply = await runtime.gateway.handle_verified_message_for_test(message)
        assert reply.status.value == "succeeded"
        blob = json.dumps(reply.model_dump(), default=str)
        _assert_no_sentinels(blob, "fail-open reply")
    finally:
        await runtime.close()


def test_metric_registry_labels_reject_sentinel_values() -> None:
    metrics = _metrics_module()
    assert metrics is not None, "trpc_service.observability.metrics is not implemented"
    registry = metrics.MetricRegistry.default()
    definition = registry.get("trpc.requests")
    label_key = definition.allowed_label_keys[0]
    for value in sentinel_values():
        try:
            registry.validate_labels("trpc.requests", {label_key: value})
        except ValueError:
            continue
        raise AssertionError("sentinel must never pass the metric label boundary")
