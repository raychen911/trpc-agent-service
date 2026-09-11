"""Stage 6B1 telemetry core unit tests (RED first).

Covers: strict environment settings, app-scoped provider lifecycle without
touching the global provider, W3C traceparent inject/extract (no baggage),
minimal ASGI middleware (/health excluded), transparent SDK Session/Memory
proxies, the shared tool.execute boundary for normal and approved execution,
sampling, cancellation-safe span ending, and exporter failure safety.
"""

from __future__ import annotations

import asyncio
import inspect
import logging
import threading
import time

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import NoOpTracer, SpanKind, StatusCode

from trpc_service.agent.errors import TenantAgentConfigurationError
from trpc_service.governance.approved_execution import ApprovedToolExecutor
from trpc_service.telemetry import (
    ATTR_ERROR_CODE,
    ATTR_EXCEPTION_TYPE,
    ATTR_OPERATION,
    ATTR_STATUS,
    SPAN_STATE_MEMORY,
    SPAN_STATE_SESSION,
    SPAN_TOOL_EXECUTE,
    TelemetryConfigurationError,
    TelemetryRuntime,
    TelemetrySettings,
    TraceRequestMiddleware,
    extract_traceparent,
    inject_traceparent,
    instrument_memory_service,
    instrument_session_service,
    traced_tool_execution,
    traced_tool_function,
)

VALID_TRACEPARENT = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
VALID_TRACE_ID = 0x0AF7651916CD43DD8448EB211C80319C
VALID_SPAN_ID = 0xB7AD6B7169203331


def _env(**overrides: str | None) -> dict[str, str]:
    env = {
        "TRPC_TRACE_ENABLED": "true",
        "TRPC_TRACE_OTLP_ENDPOINT": "http://127.0.0.1:4318/v1/traces",
    }
    for key, value in overrides.items():
        if value is None:
            env.pop(key, None)
        else:
            env[key] = value
    return env


def _runtime(
    exporter: InMemorySpanExporter | object | None = None,
    **overrides: str | None,
) -> tuple[TelemetryRuntime, object]:
    settings = TelemetrySettings.from_env("test-service", _env(**overrides))
    mem = InMemorySpanExporter() if exporter is None else exporter
    runtime = TelemetryRuntime(settings, exporter=mem)  # type: ignore[arg-type]
    return runtime, mem


def _by_name(spans, name: str):
    return [s for s in spans if s.name == name]


# ---------------------------------------------------------------------------
# Settings: strict parsing, safe defaults
# ---------------------------------------------------------------------------


class TestTelemetrySettings:

    def test_defaults_are_disabled(self) -> None:
        settings = TelemetrySettings.from_env("gateway", {})
        assert settings.enabled is False
        assert settings.service_name == "gateway"
        assert settings.otlp_endpoint is None
        assert settings.sample_ratio == 1.0
        assert settings.export_timeout_seconds == 5.0

    def test_frozen_slots(self) -> None:
        settings = TelemetrySettings.from_env("gateway", {})
        assert dataclass_frozen(settings)
        with pytest.raises(Exception):
            settings.enabled = True  # type: ignore[misc]

    def test_enabled_requires_endpoint(self) -> None:
        with pytest.raises(TelemetryConfigurationError):
            TelemetrySettings.from_env("gateway", {"TRPC_TRACE_ENABLED": "true"})

    @pytest.mark.parametrize("raw", ["true", "True", "TRUE", "false", "FALSE"])
    def test_enabled_accepts_case_insensitive_booleans(self, raw: str) -> None:
        env = {"TRPC_TRACE_ENABLED": raw, "TRPC_TRACE_OTLP_ENDPOINT": "http://localhost:4318/v1/traces"}
        settings = TelemetrySettings.from_env("gw", env)
        assert settings.enabled is (raw.lower() == "true")

    @pytest.mark.parametrize("raw", ["1", "yes", "on", "", "maybe"])
    def test_enabled_rejects_non_booleans(self, raw: str) -> None:
        with pytest.raises(TelemetryConfigurationError):
            TelemetrySettings.from_env("gw", {"TRPC_TRACE_ENABLED": raw})

    @pytest.mark.parametrize(
        "url",
        [
            "ftp://localhost:4318",
            "http://user:pass@localhost:4318/v1/traces",
            "http://localhost:4318/v1/traces?token=abc",
            "http://localhost:4318/v1/traces#frag",
            "http:///v1/traces",
            "not a url",
        ],
    )
    def test_endpoint_rejects_unsafe_urls(self, url: str) -> None:
        env = {"TRPC_TRACE_ENABLED": "true", "TRPC_TRACE_OTLP_ENDPOINT": url}
        with pytest.raises(TelemetryConfigurationError):
            TelemetrySettings.from_env("gw", env)

    @pytest.mark.parametrize("url", ["http://localhost:4318/v1/traces", "https://collector.internal:4318"])
    def test_endpoint_accepts_plain_http_urls(self, url: str) -> None:
        env = {"TRPC_TRACE_ENABLED": "true", "TRPC_TRACE_OTLP_ENDPOINT": url}
        assert TelemetrySettings.from_env("gw", env).otlp_endpoint == url

    @pytest.mark.parametrize("raw", ["-0.5", "1.5", "abc", ""])
    def test_sample_ratio_strict(self, raw: str) -> None:
        env = _env(TRPC_TRACE_SAMPLE_RATIO=raw)
        with pytest.raises(TelemetryConfigurationError):
            TelemetrySettings.from_env("gw", env)

    @pytest.mark.parametrize("raw", ["0", "0.5", "1", "1.0", "0.25"])
    def test_sample_ratio_accepts(self, raw: str) -> None:
        env = _env(TRPC_TRACE_SAMPLE_RATIO=raw)
        assert TelemetrySettings.from_env("gw", env).sample_ratio == float(raw)

    @pytest.mark.parametrize("raw", ["0", "-2", "abc", ""])
    def test_export_timeout_strict(self, raw: str) -> None:
        env = _env(TRPC_TRACE_EXPORT_TIMEOUT_SECONDS=raw)
        with pytest.raises(TelemetryConfigurationError):
            TelemetrySettings.from_env("gw", env)

    def test_export_timeout_accepts(self) -> None:
        env = _env(TRPC_TRACE_EXPORT_TIMEOUT_SECONDS="0.5")
        assert TelemetrySettings.from_env("gw", env).export_timeout_seconds == 0.5

    def test_empty_service_name_rejected(self) -> None:
        with pytest.raises(TelemetryConfigurationError):
            TelemetrySettings.from_env("", {})


def dataclass_frozen(obj) -> bool:
    return isinstance(obj, type) or obj.__dataclass_params__.frozen


# ---------------------------------------------------------------------------
# Runtime: app-scoped provider, never global; disabled never touches network
# ---------------------------------------------------------------------------


class TestTelemetryRuntime:

    @pytest.mark.asyncio
    async def test_tracer_before_start_does_not_build_provider_or_exporter(self, monkeypatch) -> None:
        calls: list[str] = []

        class _UnexpectedExporter:

            def __init__(self, **kwargs):
                calls.append("constructed")

            def export(self, spans):
                raise AssertionError("exporter must not run before start")

            def shutdown(self):
                return None

        import trpc_service.telemetry.runtime as runtime_module
        monkeypatch.setattr(runtime_module, "OTLPSpanExporter", _UnexpectedExporter)
        runtime = TelemetryRuntime(TelemetrySettings.from_env("gw", _env()))

        try:
            tracer = runtime.tracer("pre-start")

            assert tracer is not None
            assert calls == []
            assert runtime._provider is None
        finally:
            # If this regresses, reap the prematurely constructed provider so
            # the failing test itself does not leak its worker thread.
            if runtime._provider is not None:
                runtime._provider.shutdown()

    @pytest.mark.asyncio
    async def test_close_does_not_create_default_executor_work(self, monkeypatch) -> None:
        runtime, _ = _runtime()
        await runtime.start()
        calls: list[str] = []

        async def _unexpected_to_thread(*args, **kwargs):
            calls.append("to_thread")
            return True

        monkeypatch.setattr(asyncio, "to_thread", _unexpected_to_thread)
        await runtime.close()
        assert calls == []

    @pytest.mark.asyncio
    async def test_disabled_runtime_uses_noop_tracer_and_no_exporter(self, monkeypatch) -> None:

        def _boom(*args, **kwargs):
            raise AssertionError("disabled telemetry must not construct an OTLP exporter")

        import trpc_service.telemetry.runtime as runtime_module
        monkeypatch.setattr(runtime_module, "OTLPSpanExporter", _boom, raising=False)
        settings = TelemetrySettings.from_env("gw", {})
        runtime = TelemetryRuntime(settings)
        await runtime.start()
        assert isinstance(runtime.tracer("any"), NoOpTracer)
        await runtime.close()
        await runtime.close()  # idempotent

    @pytest.mark.asyncio
    async def test_enabled_builds_otlp_exporter_from_settings(self, monkeypatch) -> None:
        captured: dict = {}

        class _FakeOtlp:

            def __init__(self, *, endpoint=None, timeout=None, **kwargs):
                captured["endpoint"] = endpoint
                captured["timeout"] = timeout

            def shutdown(self):
                return None

        import trpc_service.telemetry.runtime as runtime_module
        monkeypatch.setattr(runtime_module, "OTLPSpanExporter", _FakeOtlp)
        settings = TelemetrySettings.from_env("gw", _env(TRPC_TRACE_EXPORT_TIMEOUT_SECONDS="2.5"))
        runtime = TelemetryRuntime(settings)
        await runtime.start()
        assert captured["endpoint"] == "http://127.0.0.1:4318/v1/traces"
        assert captured["timeout"] == 2.5
        await runtime.close()

    @pytest.mark.asyncio
    async def test_global_provider_is_never_replaced(self) -> None:
        before = trace.get_tracer_provider()
        runtime, _exporter = _runtime()
        await runtime.start()
        with runtime.tracer("x").start_as_current_span("s"):
            pass
        assert trace.get_tracer_provider() is before
        await runtime.close()

    @pytest.mark.asyncio
    async def test_spans_exported_and_close_is_idempotent(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        with runtime.tracer("x").start_as_current_span("unit.span") as span:
            span.set_attribute(ATTR_STATUS, "ok")
        await runtime.close()
        assert [s.name for s in exporter.get_finished_spans()] == ["unit.span"]
        await runtime.close()  # second close must not raise

    @pytest.mark.asyncio
    async def test_sample_ratio_zero_drops_local_traces(self) -> None:
        runtime, exporter = _runtime(TRPC_TRACE_SAMPLE_RATIO="0")
        await runtime.start()
        with runtime.tracer("x").start_as_current_span("dropped"):
            pass
        await runtime.close()
        assert exporter.get_finished_spans() == ()

    @pytest.mark.asyncio
    async def test_no_logs_or_metrics_exporters_started(self) -> None:
        # The runtime must only carry a tracer; provider type stays trace-only.
        runtime, _ = _runtime()
        await runtime.start()
        assert not hasattr(runtime, "logger_provider")
        assert not hasattr(runtime, "meter_provider")
        await runtime.close()


class TestExporterFailureSafety:

    @pytest.mark.asyncio
    async def test_export_exception_produces_fixed_log_only(self, caplog) -> None:

        class _RaisingExporter:

            def export(self, spans):
                raise RuntimeError("collector exploded with secret body")

            def shutdown(self):
                return None

        runtime = TelemetryRuntime(TelemetrySettings.from_env("gw", _env()), exporter=_RaisingExporter())
        await runtime.start()
        with runtime.tracer("x").start_as_current_span("unit.span"):
            pass
        with caplog.at_level(logging.WARNING, logger="trpc_service.telemetry"):
            await runtime.close()
        events = [r.getMessage() for r in caplog.records]
        assert any(e.startswith("telemetry.") for e in events), events
        assert not any("exploded" in e or "secret" in e for e in events)
        await asyncio.sleep(0.05)

    @pytest.mark.asyncio
    async def test_export_failure_result_produces_fixed_log_only(self, caplog) -> None:
        from opentelemetry.sdk.trace.export import SpanExportResult

        class _FailureExporter:

            def export(self, spans):
                return SpanExportResult.FAILURE

            def shutdown(self):
                return None

        runtime = TelemetryRuntime(TelemetrySettings.from_env("gw", _env()), exporter=_FailureExporter())
        await runtime.start()
        with runtime.tracer("x").start_as_current_span("unit.span"):
            pass
        with caplog.at_level(logging.WARNING, logger="trpc_service.telemetry"):
            await runtime.close()
        assert any(r.getMessage().startswith("telemetry.") for r in caplog.records)
        assert not any("Failed to export" in r.getMessage() for r in caplog.records)

    @pytest.mark.asyncio
    async def test_shutdown_hang_is_bounded_and_logged(self, caplog) -> None:
        finished = threading.Event()

        class _StallingExporter:

            def export(self, spans):
                return None

            def shutdown(self):
                # ignore any timeout hint; hang deterministically
                time.sleep(1.5)
                finished.set()
                return None

        runtime = TelemetryRuntime(
            TelemetrySettings.from_env("gw", _env(TRPC_TRACE_EXPORT_TIMEOUT_SECONDS="0.3")),
            exporter=_StallingExporter(),
        )
        await runtime.start()
        with runtime.tracer("x").start_as_current_span("unit.span"):
            pass
        start = time.monotonic()
        await runtime.close()
        elapsed = time.monotonic() - start
        assert elapsed < 1.0, f"close() must be bounded by configured timeout, took {elapsed:.2f}s"
        # the background shutdown thread must be reclaimed once the stall ends
        deadline = time.monotonic() + 4.0
        while time.monotonic() < deadline and not finished.is_set():
            await asyncio.sleep(0.05)
        assert finished.is_set()
        await asyncio.sleep(0.1)

    @pytest.mark.asyncio
    async def test_flush_exception_never_propagates(self) -> None:

        class _RaisingFlush:

            def export(self, spans):
                return None

            def force_flush(self, timeout_millis=None):
                raise RuntimeError("flush failed")

            def shutdown(self):
                raise RuntimeError("shutdown failed")

        runtime = TelemetryRuntime(TelemetrySettings.from_env("gw", _env()), exporter=_RaisingFlush())
        await runtime.start()
        with runtime.tracer("x").start_as_current_span("unit.span"):
            pass
        await runtime.close()  # must not raise


# ---------------------------------------------------------------------------
# Propagation: W3C traceparent only, invalid values ignored
# ---------------------------------------------------------------------------


class TestPropagation:

    def test_extract_valid_traceparent(self) -> None:
        context = extract_traceparent({"traceparent": VALID_TRACEPARENT})
        span_context = trace.get_current_span(context).get_span_context()
        assert span_context.trace_id == VALID_TRACE_ID
        assert span_context.span_id == VALID_SPAN_ID
        assert span_context.is_remote

    @pytest.mark.parametrize(
        "bad",
        [
            "garbage",
            "00-x-y-01",
            "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331",  # truncated
            "ff-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01",  # unsupported version
            "00-00000000000000000000000000000000-b7ad6b7169203331-01",  # all-zero trace id
            "",
        ],
    )
    def test_invalid_traceparent_is_ignored(self, bad: str) -> None:
        context = extract_traceparent({"traceparent": bad})
        assert not trace.get_current_span(context).get_span_context().is_valid

    @pytest.mark.asyncio
    async def test_invalid_traceparent_does_not_inherit_ambient_span(self) -> None:
        runtime, _ = _runtime()
        await runtime.start()
        with runtime.tracer("ambient").start_as_current_span("ambient"):
            context = extract_traceparent({"traceparent": "garbage"})
            assert not trace.get_current_span(context).get_span_context().is_valid
        await runtime.close()

    def test_inject_outside_span_is_noop(self) -> None:
        headers: dict[str, str] = {}
        inject_traceparent(headers)
        assert headers == {}

    @pytest.mark.asyncio
    async def test_inject_writes_only_traceparent(self) -> None:
        runtime, _ = _runtime()
        await runtime.start()
        with runtime.tracer("x").start_as_current_span("root") as span:
            headers: dict[str, str] = {}
            inject_traceparent(headers)
        await runtime.close()
        assert list(headers) == ["traceparent"]
        trace_id_hex = format(span.get_span_context().trace_id, "032x")
        assert headers["traceparent"].startswith(f"00-{trace_id_hex}-")

    @pytest.mark.asyncio
    async def test_baggage_is_never_injected(self) -> None:
        # Even if a baggage header is supplied inbound, our inject must not
        # carry it forward.
        from opentelemetry import context as otel_context
        context = extract_traceparent({"traceparent": VALID_TRACEPARENT, "baggage": "secret=value"})
        runtime, _ = _runtime()
        await runtime.start()
        token = otel_context.attach(context)
        try:
            headers: dict[str, str] = {}
            inject_traceparent(headers)
        finally:
            otel_context.detach(token)
        await runtime.close()
        assert "baggage" not in headers
        assert headers.get("traceparent", "").startswith("00-0af7651916cd43dd8448eb211c80319c-")


# ---------------------------------------------------------------------------
# ASGI middleware
# ---------------------------------------------------------------------------


async def _asgi_app(sink: dict):

    async def app(scope, receive, send):
        sink["called"] = sink.get("called", 0) + 1
        sink["current_trace"] = trace.get_current_span().get_span_context().trace_id
        await send({
            "type": "http.response.start",
            "status": 200,
            "headers": [(b"content-type", b"application/json")],
        })
        await send({"type": "http.response.body", "body": b"{}"})

    return app


def _scope(method: str = "POST", path: str = "/api/console/messages", headers: list | None = None) -> dict:
    return {
        "type": "http",
        "asgi": {
            "version": "3.0",
            "spec_version": "2.3"
        },
        "http_version": "1.1",
        "method": method,
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "scheme": "http",
        "server": ("test", 80),
        "client": ("test", 1),
        "headers": headers or [],
    }


async def _call(app, scope: dict) -> list[dict]:
    messages: list[dict] = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        messages.append(message)

    await app(scope, receive, send)
    return messages


class TestAsgiMiddleware:

    @pytest.mark.asyncio
    async def test_health_path_creates_no_span(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        sink: dict = {}
        app = TraceRequestMiddleware(await _asgi_app(sink), runtime.tracer("t"), "gateway.request")
        await _call(app, _scope(method="GET", path="/health"))
        await runtime.close()
        assert sink["called"] == 1
        assert exporter.get_finished_spans() == ()

    @pytest.mark.asyncio
    async def test_creates_server_span_with_extracted_parent(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        sink: dict = {}
        app = TraceRequestMiddleware(await _asgi_app(sink), runtime.tracer("t"), "gateway.request")
        messages = await _call(app, _scope(headers=[(b"traceparent", VALID_TRACEPARENT.encode())]))
        await runtime.close()
        assert messages[0]["status"] == 200
        spans = _by_name(exporter.get_finished_spans(), "gateway.request")
        assert len(spans) == 1
        span = spans[0]
        assert span.kind is SpanKind.SERVER
        assert span.context.trace_id == VALID_TRACE_ID
        assert span.parent is not None and span.parent.span_id == VALID_SPAN_ID
        assert sink["current_trace"] == VALID_TRACE_ID

    @pytest.mark.asyncio
    async def test_invalid_traceparent_starts_new_trace(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        app = TraceRequestMiddleware(await _asgi_app({}), runtime.tracer("t"), "worker.request")
        await _call(app, _scope(headers=[(b"traceparent", b"garbage")]))
        await runtime.close()
        spans = _by_name(exporter.get_finished_spans(), "worker.request")
        assert len(spans) == 1
        assert spans[0].parent is None
        assert spans[0].context.trace_id not in (0, VALID_TRACE_ID)

    @pytest.mark.asyncio
    async def test_app_exception_recorded_without_exception_event(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()

        async def boom(scope, receive, send):
            raise ValueError("secret traceback body")

        app = TraceRequestMiddleware(boom, runtime.tracer("t"), "gateway.request")
        with pytest.raises(ValueError):
            await _call(app, _scope())
        await runtime.close()
        span = _by_name(exporter.get_finished_spans(), "gateway.request")[0]
        assert span.attributes[ATTR_STATUS] == "error"
        assert span.attributes[ATTR_EXCEPTION_TYPE] == "ValueError"
        assert span.events == ()
        assert span.status.status_code is StatusCode.ERROR
        joined = " ".join(f"{k}:{v}" for k, v in span.attributes.items())
        assert "secret" not in joined

    @pytest.mark.asyncio
    async def test_non_http_scope_passes_through(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        called = []

        async def app(scope, receive, send):
            called.append(scope["type"])

        mw = TraceRequestMiddleware(app, runtime.tracer("t"), "gateway.request")
        await mw({"type": "lifespan"}, None, None)
        await runtime.close()
        assert called == ["lifespan"]
        assert exporter.get_finished_spans() == ()

    @pytest.mark.asyncio
    async def test_concurrent_requests_keep_isolated_contexts(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()

        other = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
        other_trace = 0x4BF92F3577B34DA6A3CE929D0E0E4736

        async def slow_app(scope, receive, send):
            await asyncio.sleep(0.02)
            trace_id = trace.get_current_span().get_span_context().trace_id
            (scope["sink"]).append(trace_id)
            await send({"type": "http.response.start", "status": 200, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        app = TraceRequestMiddleware(slow_app, runtime.tracer("t"), "worker.request")
        scope_a = _scope(headers=[(b"traceparent", VALID_TRACEPARENT.encode())])
        scope_b = _scope(headers=[(b"traceparent", other.encode())])
        scope_a["sink"] = []
        scope_b["sink"] = []
        await asyncio.gather(_call(app, scope_a), _call(app, scope_b))
        await runtime.close()
        assert scope_a["sink"] == [VALID_TRACE_ID]
        assert scope_b["sink"] == [other_trace]

    @pytest.mark.asyncio
    async def test_attributes_are_limited_to_safe_keys(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        app = TraceRequestMiddleware(await _asgi_app({}), runtime.tracer("t"), "gateway.request")
        await _call(app, _scope())
        await runtime.close()
        span = _by_name(exporter.get_finished_spans(), "gateway.request")[0]
        assert set(span.attributes) <= {ATTR_STATUS, ATTR_ERROR_CODE, ATTR_EXCEPTION_TYPE}


# ---------------------------------------------------------------------------
# Session / Memory service proxies
# ---------------------------------------------------------------------------


class _FakeSessionService:

    def __init__(self) -> None:
        self.calls: list[tuple] = []

    async def get_session(self, *, app_name: str, user_id: str, session_id: str):
        self.calls.append(("get_session", app_name, user_id, session_id))
        return {"session": session_id}

    async def create_session(self, *, app_name: str, user_id: str, state: dict | None = None):
        self.calls.append(("create_session", app_name, user_id))
        return {"created": True}

    async def append_event(self, session, event, **kwargs):
        self.calls.append(("append_event", session, event))
        return event

    async def close(self) -> None:
        self.calls.append(("close", ))


class _FakeMemoryService:

    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.enabled = True

    async def store_session(self, session) -> None:
        self.calls.append(("store_session", session))


class TestSdkServiceProxies:

    @pytest.mark.asyncio
    async def test_session_proxy_delegates_transparently(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        target = _FakeSessionService()
        proxy = instrument_session_service(target, runtime.tracer("t"))

        result = await proxy.get_session(app_name="a", user_id="u", session_id="s")
        assert result == {"session": "s"}
        assert target.calls == [("get_session", "a", "u", "s")]

        await proxy.append_event("SESSION", "EVENT")
        assert target.calls[1] == ("append_event", "SESSION", "EVENT")
        await runtime.close()

        spans = _by_name(exporter.get_finished_spans(), SPAN_STATE_SESSION)
        assert [s.attributes[ATTR_OPERATION] for s in spans] == ["get_session", "append_event"]
        # create_session/close never called -> no spans for them
        assert all(s.attributes[ATTR_STATUS] == "ok" for s in spans)

    @pytest.mark.asyncio
    async def test_memory_proxy_span_name(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        proxy = instrument_memory_service(_FakeMemoryService(), runtime.tracer("t"))
        assert proxy.enabled is True  # plain attribute passthrough
        await proxy.store_session("S")
        await runtime.close()
        spans = _by_name(exporter.get_finished_spans(), SPAN_STATE_MEMORY)
        assert len(spans) == 1
        assert spans[0].attributes[ATTR_OPERATION] == "store_session"

    @pytest.mark.asyncio
    async def test_proxy_preserves_method_introspection(self) -> None:
        runtime, _ = _runtime()
        await runtime.start()
        proxy = instrument_session_service(_FakeSessionService(), runtime.tracer("t"))
        assert inspect.iscoroutinefunction(proxy.get_session)
        assert callable(proxy.create_session)
        assert hasattr(proxy, "append_event")
        await runtime.close()

    @pytest.mark.asyncio
    async def test_proxy_error_span_is_safe(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()

        class _Boom:

            async def get_session(self, **kwargs):
                raise ValueError("payload secret leak")

        proxy = instrument_session_service(_Boom(), runtime.tracer("t"))
        with pytest.raises(ValueError):
            await proxy.get_session(app_name="a", user_id="u", session_id="s")
        await runtime.close()
        span = _by_name(exporter.get_finished_spans(), SPAN_STATE_SESSION)[0]
        assert span.attributes[ATTR_STATUS] == "error"
        assert span.attributes[ATTR_EXCEPTION_TYPE] == "ValueError"
        assert span.events == ()
        joined = " ".join(f"{k}:{v}" for k, v in span.attributes.items())
        assert "secret" not in joined

    @pytest.mark.asyncio
    async def test_proxy_spans_nest_under_current_context(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()
        target = _FakeSessionService()
        proxy = instrument_session_service(target, runtime.tracer("t"))
        with runtime.tracer("t").start_as_current_span("agent.turn") as parent:
            await proxy.get_session(app_name="a", user_id="u", session_id="s")
        await runtime.close()
        span = _by_name(exporter.get_finished_spans(), SPAN_STATE_SESSION)[0]
        assert span.parent is not None
        assert span.parent.span_id == parent.get_span_context().span_id


# ---------------------------------------------------------------------------
# Shared tool.execute boundary
# ---------------------------------------------------------------------------


class TestToolBoundary:

    def test_none_tracer_returns_original(self) -> None:

        def tool_ok() -> str:
            return "fine"

        assert traced_tool_function(None, tool_ok) is tool_ok

    def test_wrapper_preserves_sdk_metadata(self) -> None:
        """The wrapper must be FunctionTool-compatible and must not change
        the function's sync/async flavor (SDK picks thread vs await on it)."""

        def sync_tool(text: str, count: int = 1) -> str:
            """Sample doc."""
            return text * count

        async def async_tool(query: str) -> str:
            """Async doc."""
            return query

        runtime, _ = _runtime()
        wrapped_sync = traced_tool_function(runtime.tracer("t"), sync_tool)
        assert wrapped_sync.__name__ == "sync_tool"
        assert wrapped_sync.__doc__ == "Sample doc."
        assert inspect.signature(wrapped_sync) == inspect.signature(sync_tool)
        assert inspect.iscoroutinefunction(wrapped_sync) is False

        wrapped_async = traced_tool_function(runtime.tracer("t"), async_tool)
        assert inspect.iscoroutinefunction(wrapped_async) is True
        assert inspect.signature(wrapped_async) == inspect.signature(async_tool)

        from trpc_agent_sdk.tools import FunctionTool
        tool = FunctionTool(wrapped_sync)
        assert tool.name == "sync_tool"
        declaration = tool._get_declaration()  # noqa: SLF001
        assert set(declaration.parameters.properties) == {"text", "count"}
        assert declaration.description == "Sample doc."

    @pytest.mark.asyncio
    async def test_sync_wrapper_executes_inline(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()

        def sync_tool(text: str) -> str:
            return text

        wrapped = traced_tool_function(runtime.tracer("t"), sync_tool)
        assert wrapped(text="inline") == "inline"
        await runtime.close()
        spans = _by_name(exporter.get_finished_spans(), SPAN_TOOL_EXECUTE)
        assert len(spans) == 1
        assert spans[0].attributes[ATTR_STATUS] == "ok"

    @pytest.mark.asyncio
    async def test_normal_tool_creates_sanitized_span(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()

        async def secret_tool(query: str) -> dict:
            return {"result": query}

        wrapped = traced_tool_function(runtime.tracer("t"), secret_tool)
        result = await wrapped(query="SENTINEL_ARGS")
        assert result == {"result": "SENTINEL_ARGS"}
        await runtime.close()

        span = _by_name(exporter.get_finished_spans(), SPAN_TOOL_EXECUTE)[0]
        assert span.attributes[ATTR_STATUS] == "ok"
        assert set(span.attributes) <= {ATTR_STATUS, ATTR_ERROR_CODE, ATTR_EXCEPTION_TYPE}
        joined = " ".join(f"{k}:{v}" for k, v in span.attributes.items())
        for sentinel in ("SENTINEL_ARGS", "secret_tool", "query"):
            assert sentinel not in joined

    @pytest.mark.asyncio
    async def test_tool_exception_records_type_not_message(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()

        async def failing_tool() -> None:
            raise RuntimeError("tool blew up with credentials=xyz")

        wrapped = traced_tool_function(runtime.tracer("t"), failing_tool)
        with pytest.raises(RuntimeError):
            await wrapped()
        await runtime.close()
        span = _by_name(exporter.get_finished_spans(), SPAN_TOOL_EXECUTE)[0]
        assert span.attributes[ATTR_STATUS] == "error"
        assert span.attributes[ATTR_EXCEPTION_TYPE] == "RuntimeError"
        assert span.events == ()
        assert "credentials" not in str(dict(span.attributes))

    @pytest.mark.asyncio
    async def test_domain_error_maps_to_fixed_error_code(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()

        def reject() -> None:
            raise TenantAgentConfigurationError()

        with pytest.raises(TenantAgentConfigurationError):

            async def _call():
                await traced_tool_execution(runtime.tracer("t"), reject)

            await _call()
        await runtime.close()
        span = _by_name(exporter.get_finished_spans(), SPAN_TOOL_EXECUTE)[0]
        assert span.attributes[ATTR_ERROR_CODE] == "tenant_agent_configuration"
        assert span.attributes[ATTR_EXCEPTION_TYPE] == "TenantAgentConfigurationError"

    @pytest.mark.asyncio
    async def test_executor_uses_same_boundary(self) -> None:

        def add(a: int, b: int) -> int:
            return a + b

        runtime, exporter = _runtime()
        await runtime.start()
        executor = ApprovedToolExecutor({"add": add}, tracer=runtime.tracer("t"))
        assert await executor.execute("add", {"a": 1, "b": 2}) == 3

        with pytest.raises(TenantAgentConfigurationError):
            await executor.execute("nope", {})
        await runtime.close()

        spans = _by_name(exporter.get_finished_spans(), SPAN_TOOL_EXECUTE)
        assert len(spans) == 2  # successful + rejected execution
        assert spans[0].attributes[ATTR_STATUS] == "ok"
        assert spans[1].attributes[ATTR_ERROR_CODE] == "tenant_agent_configuration"
        assert "add" not in str(dict(spans[0].attributes))

    @pytest.mark.asyncio
    async def test_executor_without_tracer_is_unchanged(self) -> None:

        def add(a: int, b: int) -> int:
            return a + b

        executor = ApprovedToolExecutor({"add": add})
        assert await executor.execute("add", {"a": 2, "b": 2}) == 4

    @pytest.mark.asyncio
    async def test_traced_tool_execution_awaits_coroutines(self) -> None:
        runtime, exporter = _runtime()
        await runtime.start()

        async def work() -> str:

            async def _inner():
                await asyncio.sleep(0)
                return "done"

            return await _inner()

        result = await traced_tool_execution(runtime.tracer("t"), work)
        assert result == "done"
        await runtime.close()
        assert _by_name(exporter.get_finished_spans(), SPAN_TOOL_EXECUTE)[0].attributes[ATTR_STATUS] == "ok"
