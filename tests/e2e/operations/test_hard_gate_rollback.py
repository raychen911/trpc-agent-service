"""T056 RED (e2e): a latched hard gate stops candidate traffic and rolls back.

The first hard-gate hit from the persistent enforcement point latches the
tenant route (candidate traffic stops immediately), the supervisor triggers
the automatic rollback (DEC-004), new requests uniformly resolve the known
good snapshot, in-flight executions keep their original pin, and the append
only journal/audit history is never rewritten (FR-018, FR-019, FR-020,
FR-021, FR-034, SC-006).
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone

_NOW = datetime(2026, 9, 11, 0, 0, 0, tzinfo=timezone.utc)


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


release_mod = _load("trpc_service.operations.release")
memory_store = _load("trpc_service.operations.memory_store")
routing = _load("trpc_service.operations.routing")
gates_mod = _load("trpc_service.operations.gates")
governance_gate = _load("trpc_service.governance.hard_gate")
models = _load("trpc_service.operations.models")


async def _canary_platform():
    store = memory_store.InMemoryOperationsStore()
    await store.create_snapshot(
        models.ConfigurationSnapshot(
            snapshot_id="stab-0001",
            tenant_id="tenant-alpha",
            sequence=1,
            contract_version="v1",
            min_runtime_contract="v1",
            agent_config_ref="agent://a",
            governance_policy_ref="policy://a",
            data_backend_profile_ref="profile://a",
            payload_digest=memory_store.canonical_payload_digest({"stable": True}),
            change_summary="stable",
            created_by_digest="8888",
            created_at=_NOW,
            payload={"stable": True},
        )
    )
    await store.create_snapshot(
        models.ConfigurationSnapshot(
            snapshot_id="cand-0001",
            tenant_id="tenant-alpha",
            sequence=2,
            contract_version="v2",
            min_runtime_contract="v2",
            agent_config_ref="agent://a",
            governance_policy_ref="policy://a",
            data_backend_profile_ref="profile://a",
            payload_digest=memory_store.canonical_payload_digest({"candidate": True}),
            change_summary="candidate",
            created_by_digest="8888",
            created_at=_NOW,
            payload={"candidate": True},
        )
    )
    await store.create_release(
        models.CanaryRelease(
            release_id="rel-0001",
            candidate_snapshot_id="cand-0001",
            rollback_snapshot_id="stab-0001",
            created_by_digest="8888",
            created_at=_NOW,
            cohorts=("tenant-alpha",),
        )
    )
    await store.set_route(
        models.TenantConfigRoute(
            tenant_id="tenant-alpha",
            stable_snapshot_id="stab-0001",
            route_generation=1,
        )
    )
    coordinator = release_mod.ReleaseCoordinator(store)
    await coordinator.execute("rel-0001", "cmd-1", "validate", 1, "op")
    await coordinator.execute("rel-0001", "cmd-2", "start_canary", 2, "op")
    await store.compare_and_route(
        "tenant-alpha", 1, "cand-0001", release_id="rel-0001"
    )
    resolver = routing.RouteResolver(store, node_contract="v2")
    enforcement = governance_gate.HardGateEnforcementPoint(store)
    supervisor = release_mod.ReleaseSupervisor(store, coordinator)
    return store, coordinator, resolver, enforcement, supervisor


async def test_hard_gate_hit_latches_routes_and_rolls_back_automatically() -> None:
    store, _coordinator, resolver, enforcement, supervisor = await _canary_platform()
    inflight = await resolver.resolve_for_new_execution(
        "tenant-alpha", "alpha-key-1", "alpha-fp-1"
    )
    assert inflight.snapshot_id == "cand-0001"
    # First hard-gate hit: latch is immediate, before any coordinator command.
    signal = await enforcement.report_violation(
        tenant_id="tenant-alpha",
        release_id="rel-0001",
        gate_type="cross_tenant_leak",
        evidence_digest="e" * 64,
    )
    assert signal.severity == "hard"
    blocked = await resolver.resolve_for_new_execution(
        "tenant-alpha", "alpha-key-2", "alpha-fp-2"
    )
    assert blocked.snapshot_id == "stab-0001", (
        "candidate new requests stop at the first hard-gate hit"
    )
    # The supervisor sees the latched signal and rolls back automatically.
    release = await supervisor.apply_hard_gate(
        "rel-0001", "cmd-rollback", actor_digest="system", fence_generation=0
    )
    assert release.state == "rolled_back"
    route = await store.get_route("tenant-alpha")
    assert route.candidate_snapshot_id is None and route.hard_gate_latched
    after = await resolver.resolve_for_new_execution(
        "tenant-alpha", "alpha-key-3", "alpha-fp-3"
    )
    assert after.snapshot_id == "stab-0001", (
        "after rollback every new request uses the known good version"
    )
    still = await resolver.resolve_for_new_execution(
        "tenant-alpha", "alpha-key-1", "alpha-fp-1"
    )
    assert still == inflight, "the in-flight execution keeps its original pin"


async def test_rollback_never_rewrites_history() -> None:
    store, _coordinator, _resolver, enforcement, supervisor = await _canary_platform()
    await enforcement.report_violation(
        tenant_id="tenant-alpha",
        release_id="rel-0001",
        gate_type="data_consistency",
        evidence_digest="f" * 64,
    )
    await supervisor.apply_hard_gate(
        "rel-0001", "cmd-rollback", actor_digest="system", fence_generation=0
    )
    events = await store.transition_events("rel-0001")
    states = [(e.from_state, e.to_state) for e in events]
    assert ("draft", "validated") in states
    assert ("validated", "canary") in states
    assert ("canary", "rolling_back") in states
    assert ("rolling_back", "rolled_back") in states
    assert all(e.from_revision < e.to_revision for e in events), "forward-only journal"
    decisions = await store.rollback_decisions("rel-0001")
    assert len(decisions) == 1
    assert decisions[0].reason_code == "hard_gate_triggered"
    audit_actions = [entry["action"] for entry in store.audit_records]
    assert "rollback" in audit_actions, "the rollback is formally audited"


async def test_enforcement_report_is_idempotent_and_evidence_bound() -> None:
    store, _coordinator, _resolver, enforcement, _supervisor = await _canary_platform()
    first = await enforcement.report_violation(
        tenant_id="tenant-alpha",
        release_id="rel-0001",
        gate_type="cross_tenant_leak",
        evidence_digest="e" * 64,
    )
    second = await enforcement.report_violation(
        tenant_id="tenant-alpha",
        release_id="rel-0001",
        gate_type="cross_tenant_leak",
        evidence_digest="e" * 64,
    )
    assert second.signal_id == first.signal_id, "duplicate reports dedup by digest"
    route = await store.get_route("tenant-alpha")
    assert route.route_generation == 3, "latch applied exactly once (route+candidate)"
    rejected = None
    try:
        await enforcement.report_violation(
            tenant_id="tenant-alpha",
            release_id="rel-0001",
            gate_type="not_a_hard_gate",
            evidence_digest="e" * 64,
        )
    except ValueError:
        rejected = "rejected"
    assert rejected == "rejected", "unknown gate types must be rejected"
    no_evidence = None
    try:
        await enforcement.report_violation(
            tenant_id="tenant-alpha",
            release_id="rel-0001",
            gate_type="cross_tenant_leak",
            evidence_digest=None,
        )
    except ValueError:
        no_evidence = "rejected"
    assert no_evidence == "rejected", (
        "hard signals must be bound to persistent enforcement evidence"
    )


async def test_gate_evaluator_sees_only_enforcement_hard_signals() -> None:
    store, _coordinator, _resolver, enforcement, _supervisor = await _canary_platform()
    await enforcement.report_violation(
        tenant_id="tenant-alpha",
        release_id="rel-0001",
        gate_type="unauthorized_side_effect",
        evidence_digest="9" * 64,
    )
    signals = await store.gate_signals("rel-0001")
    evaluator = gates_mod.GateEvaluator(
        observation_window=300, minimum_sample=100, quality_gates=()
    )
    verdict = evaluator.evaluate(
        hard_signals=[s for s in signals if s.severity == "hard"],
        quality_signals=[],
        window_elapsed=False,
        sample_count=0,
    )
    assert verdict == "hard_stop"
