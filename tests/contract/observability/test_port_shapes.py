"""T009a RED: observability port shapes must exist and be async."""

from __future__ import annotations

import importlib
import inspect

OBSERVABILITY_PORTS: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("CorrelationContextPort", ("start_root", "bind_tenant", "link_attempt", "inject", "extract")),
    ("TelemetryRecorderPort", ("start_stage", "finish_stage", "record_metric", "record_operational", "flush", "shutdown")),
    ("TelemetryExporterPort", ("export",)),
    ("SamplingPolicyPort", ("decide",)),
    ("TelemetryBufferPort", ("offer", "take_batch", "ack", "retry", "drop", "snapshot")),
    ("HealthProbePort", ("probe_liveness", "probe_dependency", "evaluate_role", "aggregate")),
    ("AlertRepository", ("observe",)),
    ("AlertNotifierPort", ("notify",)),
    ("DiagnosticQueryPort", ("query",)),
)

ASYNC_METHODS: dict[str, tuple[str, ...]] = {
    "CorrelationContextPort": ("start_root", "bind_tenant", "link_attempt", "inject", "extract"),
    "TelemetryRecorderPort": ("start_stage", "finish_stage", "record_metric", "record_operational", "flush", "shutdown"),
    "TelemetryExporterPort": ("export",),
    "SamplingPolicyPort": ("decide",),
    "TelemetryBufferPort": ("take_batch", "snapshot"),
    "HealthProbePort": ("probe_liveness", "probe_dependency", "evaluate_role", "aggregate"),
    "AlertRepository": ("observe",),
    "AlertNotifierPort": ("notify",),
    "DiagnosticQueryPort": ("query",),
}


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


contracts = _load("trpc_service.observability.contracts")


def test_observability_contracts_module_exists() -> None:
    assert contracts is not None, (
        "trpc_service.observability.contracts is not implemented yet"
    )


def test_all_nine_observability_ports_declared() -> None:
    assert contracts is not None
    for port_name, methods in OBSERVABILITY_PORTS:
        port = getattr(contracts, port_name, None)
        assert port is not None, f"missing port {port_name}"
        for method in methods:
            assert callable(getattr(port, method, None)), (
                f"{port_name}.{method} is missing or not callable"
            )


def test_declared_async_methods_are_coroutine_functions() -> None:
    assert contracts is not None
    for port_name, methods in ASYNC_METHODS.items():
        port = getattr(contracts, port_name, None)
        assert port is not None, f"missing port {port_name}"
        for method in methods:
            func = getattr(port, method, None)
            assert func is not None and inspect.iscoroutinefunction(func), (
                f"{port_name}.{method} must be an async method"
            )
