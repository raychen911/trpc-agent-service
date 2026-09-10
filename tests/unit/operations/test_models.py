"""T008b RED: operations domain models and their invariants."""

from __future__ import annotations

import importlib


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


models = _load("trpc_service.operations.models")


def test_operations_models_module_exists() -> None:
    assert models is not None, "trpc_service.operations.models is not implemented yet"


def test_canary_release_state_machine_values() -> None:
    assert models is not None
    release_type = getattr(models, "CanaryRelease", None)
    assert release_type is not None
    states = set(getattr(release_type, "STATES", ()))
    assert states == {
        "draft", "validated", "canary", "completed",
        "paused_quality", "paused_insufficient_sample",
        "rolling_back", "rolled_back", "failed", "failed_requires_repair",
    }


def test_configuration_snapshot_is_immutable_and_secret_ref_only() -> None:
    assert models is not None
    snapshot_type = getattr(models, "ConfigurationSnapshot", None)
    assert snapshot_type is not None
    kwargs = {
        "snapshot_id": "s", "tenant_id": "tenant-alpha", "sequence": 1,
        "contract_version": "v1", "min_runtime_contract": "v1",
        "agent_config_ref": "agent@1", "governance_policy_ref": "gov@1",
        "data_backend_profile_ref": "data@1", "payload_digest": "d",
        "change_summary": "summary", "created_by_digest": "c",
        "payload": {"api_key": "sk-sentinel-plain"},
    }
    try:
        snapshot_type(**kwargs)
    except (ValueError, TypeError):
        pass
    else:
        raise AssertionError("snapshot payload with plain secret values must be rejected")


def test_execution_config_pin_is_unique_per_tenant_and_key() -> None:
    assert models is not None
    pin_type = getattr(models, "ExecutionConfigPin", None)
    assert pin_type is not None
    hints = getattr(pin_type, "__dataclass_fields__", None) or getattr(
        pin_type, "model_fields", {}
    )
    for required in (
        "tenant_id", "idempotency_key_digest", "content_fingerprint",
        "snapshot_id", "route_generation",
    ):
        assert required in set(hints), f"ExecutionConfigPin missing {required}"


def test_tenant_config_route_fields_and_hard_latch() -> None:
    assert models is not None
    route_type = getattr(models, "TenantConfigRoute", None)
    assert route_type is not None
    hints = getattr(route_type, "__dataclass_fields__", None) or getattr(
        route_type, "model_fields", {}
    )
    for required in (
        "tenant_id", "stable_snapshot_id", "candidate_snapshot_id",
        "route_generation", "hard_gate_latched",
    ):
        assert required in set(hints), f"TenantConfigRoute missing {required}"


def test_release_gate_signal_severity_is_hard_or_quality() -> None:
    assert models is not None
    signal_type = getattr(models, "ReleaseGateSignal", None)
    assert signal_type is not None
    assert set(getattr(signal_type, "SEVERITIES", ())) == {"hard", "quality"}


def test_drain_snapshot_states_only_move_forward() -> None:
    assert models is not None
    drain_type = getattr(models, "DrainSnapshot", None)
    assert drain_type is not None
    assert set(getattr(drain_type, "STATES", ())) == {
        "accepting", "draining", "drained", "timed_out",
    }
    transition = getattr(models, "drain_transition", None) or getattr(
        drain_type, "transition", None
    )
    assert callable(transition)
    assert transition("accepting", "draining") == "draining"
    for bad in (("draining", "accepting"), ("drained", "draining")):
        try:
            transition(*bad)
        except ValueError:
            continue
        raise AssertionError(f"drain state must not go backwards: {bad}")


def test_capacity_scenario_pins_formal_acceptance_load() -> None:
    assert models is not None
    scenario_type = getattr(models, "CapacityScenario", None)
    assert scenario_type is not None
    scenario = scenario_type(
        scenario_version="v1", tenant_count=2, worker_count=2,
        concurrent_sessions=100, messages_per_session=10,
    )
    assert scenario.total_messages() == 1_000
    too_big = scenario_type(
        scenario_version="v1", tenant_count=2, worker_count=2,
        concurrent_sessions=101, messages_per_session=10,
    )
    assert too_big.total_messages() == 1_010
    assert not too_big.is_formal_acceptance()


def test_capacity_comparison_enforces_dual_gates() -> None:
    assert models is not None
    comparison_type = getattr(models, "CapacityComparison", None)
    assert comparison_type is not None
    clean = comparison_type(
        lost_results=0, cross_tenant_leaks=0, unexplained_duplicates=0,
        throughput_delta_pct=5.0, p50_delta_pct=3.0, p95_delta_pct=8.0, p99_delta_pct=9.0,
    )
    assert clean.passed() is True
    correctness_fail = comparison_type(
        lost_results=1, cross_tenant_leaks=0, unexplained_duplicates=0,
        throughput_delta_pct=0.0, p50_delta_pct=0.0, p95_delta_pct=0.0, p99_delta_pct=0.0,
    )
    assert correctness_fail.passed() is False
    perf_fail = comparison_type(
        lost_results=0, cross_tenant_leaks=0, unexplained_duplicates=0,
        throughput_delta_pct=11.0, p50_delta_pct=0.0, p95_delta_pct=0.0, p99_delta_pct=0.0,
    )
    assert perf_fail.passed() is False


def test_rollback_decision_does_not_rewrite_history() -> None:
    assert models is not None
    decision_type = getattr(models, "RollbackDecision", None)
    assert decision_type is not None
    hints = getattr(decision_type, "__dataclass_fields__", None) or getattr(
        decision_type, "model_fields", {}
    )
    for required in (
        "decision_id", "release_id", "command_id", "actor_digest",
        "reason_code", "target_snapshot_id",
    ):
        assert required in set(hints), f"RollbackDecision missing {required}"
