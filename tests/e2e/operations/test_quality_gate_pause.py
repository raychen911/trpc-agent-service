"""T056 RED (e2e): quality gates pause for humans, never advance silently.

An insufficient sample at window end forbids silent advance; a threshold
breach with the minimum sample reached pauses and waits for explicit
authorization; a mixed-version node that cannot serve the candidate config
drops out of readiness (FR-018..FR-022, FR-034, SC-006, DEC-004).
"""

from __future__ import annotations

import importlib
from datetime import datetime, timedelta, timezone

from trpc_service.operations.operations_errors import ReleaseConflict

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
models = _load("trpc_service.operations.models")


async def _canary_platform():
    store = memory_store.InMemoryOperationsStore()
    for snapshot_id, sequence, contract in (
        ("stab-0001", 1, "v1"),
        ("cand-0001", 2, "v2"),
    ):
        await store.create_snapshot(
            models.ConfigurationSnapshot(
                snapshot_id=snapshot_id,
                tenant_id="tenant-alpha",
                sequence=sequence,
                contract_version=contract,
                min_runtime_contract=contract,
                agent_config_ref="agent://a",
                governance_policy_ref="policy://a",
                data_backend_profile_ref="profile://a",
                payload_digest=memory_store.canonical_payload_digest(
                    {"snapshot": snapshot_id}
                ),
                change_summary=snapshot_id,
                created_by_digest="9999",
                created_at=_NOW,
                payload={"snapshot": snapshot_id},
            )
        )
    await store.create_release(
        models.CanaryRelease(
            release_id="rel-0001",
            candidate_snapshot_id="cand-0001",
            rollback_snapshot_id="stab-0001",
            created_by_digest="9999",
            created_at=_NOW,
            cohorts=("tenant-alpha",),
            observation_window=300,
            minimum_sample=100,
            quality_gates=(
                {"gate_type": "error_rate", "threshold": 0.05, "direction": "max"},
            ),
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
    supervisor = release_mod.ReleaseSupervisor(store, coordinator)
    return store, coordinator, supervisor


async def test_insufficient_sample_forbids_silent_advance() -> None:
    store, coordinator, supervisor = await _canary_platform()
    release = await store.get_release("rel-0001")
    # Window elapsed but only 40 of 100 required samples observed.
    await store.record_gate_signal(
        models.ReleaseGateSignal(
            signal_id="sig-low",
            tenant_id="tenant-alpha",
            release_id="rel-0001",
            signal_digest="low".ljust(64, "0"),
            gate_type="error_rate",
            severity="quality",
            observation_window=300,
            sample_count=40,
            observed_value=0.01,
            observed_at=_NOW + timedelta(seconds=301),
        )
    )
    verdict = supervisor.evaluate_release(release)
    assert verdict == "insufficient_sample"
    paused = await supervisor.apply_quality_verdict(
        "rel-0001", "cmd-3", verdict, actor_digest="system"
    )
    assert paused.state == "paused_insufficient_sample"
    blocked = None
    try:
        await coordinator.execute("rel-0001", "cmd-4", "advance", paused.revision, "op")
    except ReleaseConflict:
        blocked = "blocked"
    assert blocked == "blocked", "a paused release cannot advance"


async def test_threshold_breach_pauses_and_waits_for_authorization() -> None:
    store, coordinator, supervisor = await _canary_platform()
    await store.record_gate_signal(
        models.ReleaseGateSignal(
            signal_id="sig-breach",
            tenant_id="tenant-alpha",
            release_id="rel-0001",
            signal_digest="breach".ljust(64, "0"),
            gate_type="error_rate",
            severity="quality",
            observation_window=300,
            sample_count=150,
            observed_value=0.2,
            observed_at=_NOW + timedelta(seconds=301),
        )
    )
    release = await store.get_release("rel-0001")
    verdict = supervisor.evaluate_release(release)
    assert verdict == "quality_pause"
    paused = await supervisor.apply_quality_verdict(
        "rel-0001", "cmd-3", verdict, actor_digest="system"
    )
    assert paused.state == "paused_quality"
    # Still paused: advancing is rejected until a human resumes.
    blocked = None
    try:
        await coordinator.execute("rel-0001", "cmd-4", "advance", paused.revision, "op")
    except ReleaseConflict:
        blocked = "blocked"
    assert blocked == "blocked"
    resumed = await coordinator.execute(
        "rel-0001", "cmd-5", "resume", paused.revision, "authorized_operator"
    )
    assert resumed.state == "canary"
    # The route still serves the candidate while resumed.
    resolver = routing.RouteResolver(store, node_contract="v2")
    pin = await resolver.resolve_for_new_execution(
        "tenant-alpha", "alpha-key-1", "alpha-fp-1"
    )
    assert pin.snapshot_id == "cand-0001"


async def test_clean_window_passes_and_advances() -> None:
    store, coordinator, supervisor = await _canary_platform()
    await store.record_gate_signal(
        models.ReleaseGateSignal(
            signal_id="sig-clean",
            tenant_id="tenant-alpha",
            release_id="rel-0001",
            signal_digest="clean".ljust(64, "0"),
            gate_type="error_rate",
            severity="quality",
            observation_window=300,
            sample_count=200,
            observed_value=0.01,
            observed_at=_NOW + timedelta(seconds=301),
        )
    )
    release = await store.get_release("rel-0001")
    verdict = supervisor.evaluate_release(release)
    assert verdict == "pass"
    completed = await supervisor.apply_quality_verdict(
        "rel-0001", "cmd-3", verdict, actor_digest="system"
    )
    assert completed.state == "completed"


async def test_mixed_version_node_drops_out_of_readiness() -> None:
    store, _coordinator, _supervisor = await _canary_platform()
    # Candidate requires contract v2; an old v1 worker cannot serve it.
    old_node_contract = "v1"
    snapshot = await store.get_snapshot("tenant-alpha", "cand-0001")
    readiness = routing.node_config_readiness(old_node_contract, snapshot)
    assert readiness is False, "a node below min_runtime_contract exits readiness"
    new_node_contract = "v2"
    snapshot_v2 = await store.get_snapshot("tenant-alpha", "cand-0001")
    assert routing.node_config_readiness(new_node_contract, snapshot_v2) is True
