"""T021 RED: TelemetryBootstrap installs providers once and nests official spans.

The official ``trpc_agent_sdk`` tracer resolves the process-global provider
lazily; the platform bootstrap must install its providers BEFORE any Runner
instance creates spans, so official ``trpc.python.agent`` spans join the
platform trace as children without touching SDK internals.
"""

from __future__ import annotations

import importlib

from opentelemetry import context as otel_context
from opentelemetry import trace as otel_trace


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


otel_mod = _load("trpc_service.observability.otel")


def test_otel_module_exists() -> None:
    assert otel_mod is not None, "trpc_service.observability.otel is not implemented yet"


def test_bootstrap_configures_idempotently() -> None:
    assert otel_mod is not None
    bootstrap = otel_mod.TelemetryBootstrap(service_name="phase8-nesting")
    first = bootstrap.configure()
    second = bootstrap.configure()
    assert first is not None
    assert second is first, "configure() must be idempotent within one process"


def test_official_runner_span_nests_under_platform_root() -> None:
    assert otel_mod is not None
    from trpc_agent_sdk.telemetry import (
        get_trpc_agent_span_name,
        tracer as official_tracer,
    )

    bootstrap = otel_mod.TelemetryBootstrap(service_name="phase8-nesting")
    bootstrap.configure()

    root = bootstrap.tracer().start_span("platform.root")
    token = otel_context.attach(otel_trace.set_span_in_context(root))
    try:
        official = official_tracer.start_span(get_trpc_agent_span_name())
        official.set_attribute("component", "worker")
        official.end()
    finally:
        otel_context.detach(token)
    root.end()

    assert official.get_span_context().trace_id == root.get_span_context().trace_id
    assert official.parent is not None
    assert official.parent.span_id == root.get_span_context().span_id
    assert root.get_span_context().is_valid


def test_gen_ai_metrics_reuse_the_same_meter_provider() -> None:
    assert otel_mod is not None
    bootstrap = otel_mod.TelemetryBootstrap(service_name="phase8-nesting")
    bootstrap.configure()
    meter_one = bootstrap.meter()
    meter_two = bootstrap.meter()
    assert meter_one is meter_two, "one process must reuse a single meter provider"
    counter = meter_one.create_counter("gen_ai.client.token.usage")
    histogram = meter_one.create_histogram("gen_ai.server.request.duration")
    assert counter is not None and histogram is not None
    assert bootstrap.meter_provider is not None
