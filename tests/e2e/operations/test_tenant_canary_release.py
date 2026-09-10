"""T056 RED (e2e): tenant-scoped canary rollout keeps outsiders on stable.

Out-of-cohort tenants always resolve the stable snapshot; the cohort tenant
moves to the candidate only while the release is in canary; an in-flight
execution keeps its original pin across route changes; advance completes the
release (FR-018..FR-022, FR-034, SC-006, DEC-004).
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
models = _load("trpc_service.operations.models")


async def _platform():
    """Two tenants, one cohort canary release, stable + candidate snapshots."""
    store = memory_store.InMemoryOperationsStore()
    for tenant, snapshot_id in (
        ("tenant-alpha", "stab-0001"),
        ("tenant-beta", "stab-0002"),
    ):
        await store.create_snapshot(
            models.ConfigurationSnapshot(
                snapshot_id=snapshot_id,
                tenant_id=tenant,
                sequence=1,
                contract_version="v1",
                min_runtime_contract="v1",
                agent_config_ref="agent://a",
                governance_policy_ref="policy://a",
                data_backend_profile_ref="profile://a",
                payload_digest=memory_store.canonical_payload_digest(
                    {"stable": snapshot_id}
                ),
                change_summary="stable",
                created_by_digest="7777",
                created_at=_NOW,
                payload={"stable": snapshot_id},
            )
        )
        await store.set_route(
            models.TenantConfigRoute(
                tenant_id=tenant, stable_snapshot_id=snapshot_id, route_generation=1
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
            created_by_digest="7777",
            created_at=_NOW,
            payload={"candidate": True},
        )
    )
    await store.create_release(
        models.CanaryRelease(
            release_id="rel-0001",
            candidate_snapshot_id="cand-0001",
            rollback_snapshot_id="stab-0001",
            created_by_digest="7777",
            created_at=_NOW,
            cohorts=("tenant-alpha",),
        )
    )
    coordinator = release_mod.ReleaseCoordinator(store)
    resolver = routing.RouteResolver(store, node_contract="v2")
    return store, coordinator, resolver


async def test_out_of_cohort_tenant_stays_on_stable_throughout() -> None:
    store, coordinator, resolver = await _platform()
    await coordinator.execute("rel-0001", "cmd-1", "validate", 1, "op")
    await coordinator.execute("rel-0001", "cmd-2", "start_canary", 2, "op")
    await store.compare_and_route(
        "tenant-alpha", 1, "cand-0001", release_id="rel-0001"
    )
    outside = await resolver.resolve_for_new_execution(
        "tenant-beta", "beta-key-1", "beta-fp-1"
    )
    assert outside.snapshot_id == "stab-0002"
    inside = await resolver.resolve_for_new_execution(
        "tenant-alpha", "alpha-key-1", "alpha-fp-1"
    )
    assert inside.snapshot_id == "cand-0001"
    await coordinator.execute("rel-0001", "cmd-3", "advance", 3, "op")
    # After completion the route generation is bumped; outsiders still stable.
    outside_later = await resolver.resolve_for_new_execution(
        "tenant-beta", "beta-key-2", "beta-fp-2"
    )
    assert outside_later.snapshot_id == "stab-0002"


async def test_inflight_execution_keeps_its_original_pin() -> None:
    store, coordinator, resolver = await _platform()
    await coordinator.execute("rel-0001", "cmd-1", "validate", 1, "op")
    await coordinator.execute("rel-0001", "cmd-2", "start_canary", 2, "op")
    await store.compare_and_route(
        "tenant-alpha", 1, "cand-0001", release_id="rel-0001"
    )
    pinned = await resolver.resolve_for_new_execution(
        "tenant-alpha", "alpha-key-1", "alpha-fp-1"
    )
    # Route changes afterwards (advance completes, generation bumps).
    await coordinator.execute("rel-0001", "cmd-3", "advance", 3, "op")
    again = await resolver.resolve_for_new_execution(
        "tenant-alpha", "alpha-key-1", "alpha-fp-1"
    )
    assert again == pinned, "an in-flight execution keeps its original pin"


async def test_canary_lifecycle_records_append_only_journal() -> None:
    store, coordinator, _resolver = await _platform()
    await coordinator.execute("rel-0001", "cmd-1", "validate", 1, "op")
    await coordinator.execute("rel-0001", "cmd-2", "start_canary", 2, "op")
    await coordinator.execute("rel-0001", "cmd-3", "advance", 3, "op")
    events = await store.transition_events("rel-0001")
    assert [(e.from_state, e.to_state) for e in events] == [
        ("draft", "validated"),
        ("validated", "canary"),
        ("canary", "completed"),
    ]
    assert all(e.command_id for e in events)
    transition_audits = [
        entry
        for entry in store.audit_records
        if entry["action"] in {"validate", "start_canary", "advance"}
    ]
    assert len(transition_audits) == 3
