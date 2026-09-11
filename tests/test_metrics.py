"""Stage 6C Task 2: low-cardinality metrics contract.

The metric label space is structurally bounded to service/operation/result/
error_code; unbounded identifiers (tenant/user/session/message/model profile)
can never be attached, and a source scan proves no call site even attempts
to pass them.  Exporter/collector failures never reach business code.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from tests.tenant_helpers import FakeLLMModel, make_default_test_configs, make_in_memory_state_backend
from trpc_service.agent.runtime import TenantAgentRuntime
from trpc_service.telemetry import instrument_memory_service, instrument_session_service, traced_tool_function
from trpc_service.telemetry.metrics import (
    METRIC_DURATION_MS,
    MetricsAttributeError,
    MetricsRecorder,
    NoopMetricsRecorder,
)
from trpc_service.telemetry.runtime import SafeMetricExporter, TelemetryRuntime
from trpc_service.telemetry.settings import TelemetrySettings
from trpc_service.tenant.context import TenantContext

FORBIDDEN_ATTR_TOKENS = ("tenant", "user", "session", "message", "model_profile", "request_id")


class _RecordingMeter:

    def __init__(self):
        self.counters = {}
        self.histograms = {}

    def create_counter(self, name):
        instrument = _Instrument()
        self.counters[name] = instrument
        return instrument

    def create_histogram(self, name):
        instrument = _Instrument()
        self.histograms[name] = instrument
        return instrument


class _Instrument:

    def __init__(self):
        self.added = []
        self.recorded = []

    def add(self, value, attrs):
        self.added.append((value, dict(attrs)))

    def record(self, value, attrs):
        self.recorded.append((value, dict(attrs)))


def _recorder():
    meter = _RecordingMeter()
    return MetricsRecorder(meter, "worker"), meter


class TestMetricsRecorderContract:

    def test_allowed_labels_only(self):
        recorder, meter = _recorder()
        recorder.record_counter("trpc.requests", 1, operation="chat", result="error", error_code="session_busy")
        value, attrs = meter.counters["trpc.requests"].added[0]
        assert attrs == {"service": "worker", "operation": "chat", "result": "error", "error_code": "session_busy"}

    def test_signature_cannot_receive_forbidden_keys(self):
        recorder, _meter = _recorder()
        with pytest.raises(TypeError):
            recorder.record_counter("x", tenant_id="secret-tenant", operation="chat")  # type: ignore[call-arg]

    def test_values_must_be_short_fixed_strings(self):
        recorder, meter = _recorder()
        recorder.record_counter("x", 1, operation="a" * 100)  # swallowed, nothing recorded
        assert "x" not in meter.counters or meter.counters["x"].added == []

    def test_recording_never_raises(self):

        class _ExplodingMeter:

            def create_counter(self, name):
                raise RuntimeError("collector exploded")

        recorder = MetricsRecorder(_ExplodingMeter(), "svc")
        recorder.record_counter("x", 1, operation="op")  # must swallow
        recorder.observe("h", 1, operation="op")

    def test_observe_histogram_attrs_are_whitelisted(self):
        recorder, meter = _recorder()
        recorder.observe("trpc.operation.duration.ms", 12, operation="agent_turn", result="ok")
        value, attrs = meter.histograms["trpc.operation.duration.ms"].recorded[0]
        assert attrs == {"service": "worker", "operation": "agent_turn", "result": "ok"}

    def test_validate_helper_rejects_bad_values(self):
        from trpc_service.telemetry.metrics import _attrs

        with pytest.raises(MetricsAttributeError):
            _attrs("svc", "op", None, "")
        with pytest.raises(MetricsAttributeError):
            _attrs("svc", "op", "r" * 200, None)


class TestNoop:

    def test_noop_is_silent(self):
        noop = NoopMetricsRecorder()
        assert noop.record_counter("x", 5, operation="op") is None
        assert noop.observe("x", 1.0, operation="op") is None


class TestRuntimeLifecycle:

    def test_disabled_runtime_hands_noop_and_builds_nothing(self):
        settings = TelemetrySettings(enabled=False,
                                     service_name="worker",
                                     otlp_endpoint=None,
                                     sample_ratio=0.0,
                                     export_timeout_seconds=1.0)
        runtime = TelemetryRuntime(settings)
        assert isinstance(runtime.metrics_recorder(), NoopMetricsRecorder)

    def test_closed_runtime_degrades_to_noop(self):
        import asyncio

        class _InnerExporter:

            def export(self, data):
                from opentelemetry.sdk.metrics.export import MetricExportResult

                return MetricExportResult.SUCCESS

            def shutdown(self):
                return True

        settings = TelemetrySettings(
            enabled=True,
            service_name="worker",
            otlp_endpoint="http://127.0.0.1:4319/v1/traces",
            sample_ratio=1.0,
            export_timeout_seconds=1.0,
        )
        runtime = TelemetryRuntime(settings, exporter=_TraceOk(), metric_exporter=_InnerExporter())

        async def _scenario():
            await runtime.start()
            assert not isinstance(runtime.metrics_recorder(), NoopMetricsRecorder)
            await runtime.close()
            assert isinstance(runtime.metrics_recorder(), NoopMetricsRecorder)

        asyncio.run(_scenario())


class _TraceOk:

    def export(self, spans):
        from opentelemetry.sdk.trace.export import SpanExportResult

        return SpanExportResult.SUCCESS

    def shutdown(self):
        return True


class _MetricCapture:

    def __init__(self):
        self.exports = []

    def export(self, metrics_data, timeout_millis=10_000, **kwargs):
        from opentelemetry.sdk.metrics.export import MetricExportResult

        self.exports.append(metrics_data)
        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis=10_000):
        return True

    def shutdown(self, timeout_millis=30_000):
        return True


def _duration_points(capture: _MetricCapture):
    points = []
    for export in capture.exports:
        for resource_metrics in export.resource_metrics:
            for scope_metrics in resource_metrics.scope_metrics:
                for metric in scope_metrics.metrics:
                    if metric.name == METRIC_DURATION_MS:
                        points.extend(metric.data.data_points)
    return points


class TestProductDurationMetrics:

    @pytest.mark.asyncio
    async def test_real_turn_tool_and_state_paths_observe_safe_durations(self):
        capture = _MetricCapture()
        settings = TelemetrySettings.from_env(
            "worker",
            {
                "TRPC_TRACE_ENABLED": "true",
                "TRPC_TRACE_OTLP_ENDPOINT": "http://127.0.0.1:4318/v1/traces",
            },
        )
        telemetry = TelemetryRuntime(settings, exporter=_TraceOk(), metric_exporter=capture)
        await telemetry.start()

        config = make_default_test_configs()["tenant_default"]
        agent = TenantAgentRuntime(
            config=config,
            model=FakeLLMModel(),
            tools=[],
            state_backend=make_in_memory_state_backend(),
            telemetry=telemetry,
        )
        context = TenantContext(
            tenant_id="tenant_default",
            app_id="app_demo",
            user_id="SENTINEL_TENANT_USER",
            channel="web",
        )
        assert [event async for event in agent.run(context, "SENTINEL_SESSION", "SENTINEL_BODY")]

        async def secret_tool(secret_argument: str):
            return secret_argument

        wrapped = traced_tool_function(telemetry.tracer("metrics-test"), secret_tool)
        assert await wrapped("SENTINEL_SECRET") == "SENTINEL_SECRET"

        class _Session:

            async def get_session(self, **kwargs):
                return kwargs["session_id"]

        class _Memory:

            async def store_session(self, session):
                return session

        session = instrument_session_service(_Session(), telemetry.tracer("metrics-test"))
        memory = instrument_memory_service(_Memory(), telemetry.tracer("metrics-test"))
        assert await session.get_session(app_name="secret-app", user_id="secret-user", session_id="secret-session")
        assert await memory.store_session("secret-memory") == "secret-memory"

        await agent.close()
        await telemetry.close()

        points = _duration_points(capture)
        operations = {point.attributes["operation"] for point in points}
        assert {"agent_turn", "tool_execute", "session_backend", "memory_backend"} <= operations
        assert all(set(point.attributes) == {"service", "operation", "result"} for point in points)
        blob = " ".join(str(dict(point.attributes)) for point in points)
        assert not any(token in blob for token in ("SENTINEL", "secret", "tenant_default"))


class TestSafeMetricExporter:

    def test_exception_and_failure_results_become_success(self):
        from opentelemetry.sdk.metrics.export import MetricExportResult

        class _Boom:

            def export(self, data):
                raise RuntimeError(f"collector exploded at {data}")

        safe = SafeMetricExporter(_Boom())
        assert safe.export(object()) is MetricExportResult.SUCCESS

        class _Fail:

            def export(self, data):
                return MetricExportResult.FAILURE

        assert SafeMetricExporter(_Fail()).export(object()) is MetricExportResult.SUCCESS

    def test_shutdown_never_raises(self):

        class _Boom:

            def shutdown(self):
                raise RuntimeError("boom")

            def export(self, data):
                return None

        assert SafeMetricExporter(_Boom()).shutdown() is True


class TestSourceLabelHygiene:
    """No metric call site in product code may pass unbounded identifiers."""

    def test_metric_call_sites_use_whitelisted_labels_only(self):
        import ast

        root = Path("trpc_service")
        offenders = []
        for path in root.rglob("*.py"):
            if "metrics" in path.name:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call):
                    continue
                fname = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
                if fname not in {"record_counter", "observe"}:
                    continue
                for kw in node.keywords:
                    if any(token in kw.arg for token in FORBIDDEN_ATTR_TOKENS):
                        offenders.append(f"{path}:{kw.arg}")
        assert offenders == []

    def test_metric_names_are_a_fixed_catalog(self):
        root = Path("trpc_service")
        names = set()
        for path in root.rglob("*.py"):
            names.update(re.findall(r'record_counter\(\s*"([a-z_.]+)"', path.read_text(encoding="utf-8")))
        assert names <= {
            "trpc.requests",
            "trpc.tokens",
            "trpc.cost.microunits",
            "trpc.budget.rejections",
            "trpc.rate_limit.rejections",
            "trpc.delivery",
        }
