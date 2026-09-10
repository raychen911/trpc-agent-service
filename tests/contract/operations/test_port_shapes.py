"""T009b RED: operations port shapes must exist and be async."""

from __future__ import annotations

import importlib
import inspect

OPERATIONS_PORTS: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "ConfigurationSnapshotRepository",
        ("create", "get", "verify_compatible"),
    ),
    (
        "TenantConfigRouteRepository",
        ("resolve_for_new_execution", "get_route", "compare_and_route"),
    ),
    (
        "ReleaseRepository",
        ("create_release", "validate", "start_canary", "advance", "pause", "resume", "rollback", "repair"),
    ),
    (
        "ReleaseCoordinator",
        ("execute",),
    ),
    (
        "GateEvaluationPort",
        ("evaluate",),
    ),
    (
        "CapacityHarnessPort",
        ("prepare", "run", "compare"),
    ),
    (
        "DrainControllerPort",
        ("begin", "snapshot", "complete_or_handoff", "expire"),
    ),
)

ASYNC_METHODS: dict[str, tuple[str, ...]] = {
    "ConfigurationSnapshotRepository": ("create", "get", "verify_compatible"),
    "TenantConfigRouteRepository": ("resolve_for_new_execution", "get_route", "compare_and_route"),
    "ReleaseRepository": (
        "create_release", "validate", "start_canary", "advance",
        "pause", "resume", "rollback", "repair",
    ),
    "ReleaseCoordinator": ("execute",),
    "GateEvaluationPort": ("evaluate",),
    "CapacityHarnessPort": ("prepare", "run", "compare"),
    "DrainControllerPort": ("begin", "snapshot", "complete_or_handoff", "expire"),
}


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


contracts = _load("trpc_service.operations.contracts")


def test_operations_contracts_module_exists() -> None:
    assert contracts is not None, "trpc_service.operations.contracts is not implemented yet"


def test_all_seven_operations_ports_declared() -> None:
    assert contracts is not None
    for port_name, methods in OPERATIONS_PORTS:
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
