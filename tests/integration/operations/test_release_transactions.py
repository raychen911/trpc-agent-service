"""T055 RED: release commands are atomic, idempotent and fenced.

Release state, tenant routes, transition events, rollback decisions and the
formal audit trail commit inside ONE transaction — an audit failure rolls
everything back; a crash before commit leaves no change; a crash after commit
(response lost) is answered by replaying the same ``command_id``; fenced-out
writers are rejected and a new node takes over from the last committed
revision; Redis cache failures never touch the authoritative state
(FR-019, FR-020, FR-022, FR-034, DEC-004).
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone

import pytest

from trpc_service.operations.operations_errors import (
    ReleaseConflict,
    StaleReleaseFence,
)

_NOW = datetime(2026, 9, 11, 0, 0, 0, tzinfo=timezone.utc)


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


release_mod = _load("trpc_service.operations.release")
memory_store = _load("trpc_service.operations.memory_store")
models = _load("trpc_service.operations.models")


async def _store_with_release():
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
            created_by_digest="6666",
            created_at=_NOW,
            payload={"stable": True},
        )
    )
    await store.create_release(
        models.CanaryRelease(
            release_id="rel-0001",
            candidate_snapshot_id="cand-0001",
            rollback_snapshot_id="stab-0001",
            created_by_digest="6666",
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
    return store


async def test_audit_failure_rolls_back_the_whole_command() -> None:
    store = await _store_with_release()
    coordinator = release_mod.ReleaseCoordinator(store)
    await coordinator.execute("rel-0001", "cmd-1", "validate", 1, "op")
    # The next audit write fails: the entire rollback command must abort.
    store.audit_failure_countdown = 1
    failed = None
    try:
        await coordinator.execute(
            "rel-0001", "cmd-2", "start_canary", 2, "op"
        )
    except Exception:
        failed = "failed"
    assert failed == "failed"
    release = await store.get_release("rel-0001")
    assert release.state == "validated" and release.revision == 2, "state unchanged"
    assert len(await store.transition_events("rel-0001")) == 1, "no new event"
    surviving = [
        entry for entry in store.audit_records if entry["action"] == "start_canary"
    ]
    assert surviving == [], "no audit row survived the abort"


async def test_rollback_reverts_routes_and_records_decision_atomically() -> None:
    store = await _store_with_release()
    coordinator = release_mod.ReleaseCoordinator(store)
    await coordinator.execute("rel-0001", "cmd-1", "validate", 1, "op")
    await coordinator.execute("rel-0001", "cmd-2", "start_canary", 2, "op")
    await store.compare_and_route(
        "tenant-alpha", 1, "cand-0001", release_id="rel-0001"
    )
    rolled = await coordinator.execute(
        "rel-0001", "cmd-3", "rollback", 3, "op", reason_code="hard_gate_triggered"
    )
    assert rolled.state == "rolled_back"
    route = await store.get_route("tenant-alpha")
    assert route.candidate_snapshot_id is None, "rollback reverts candidate routing"
    assert route.hard_gate_latched, "rollback latches the route against re-entry"
    assert route.route_generation == 3, "route generation advanced twice (route + latch)"
    decisions = await store.rollback_decisions("rel-0001")
    assert len(decisions) == 1
    assert decisions[0].target_snapshot_id == "stab-0001"
    assert decisions[0].affected_tenant_count == 1


async def test_crash_before_commit_leaves_no_change() -> None:
    store = await _store_with_release()
    coordinator = release_mod.ReleaseCoordinator(store)
    await coordinator.execute("rel-0001", "cmd-1", "validate", 1, "op")
    store.crash_mode = "before_commit"
    crashed = None
    try:
        await coordinator.execute("rel-0001", "cmd-2", "start_canary", 2, "op")
    except memory_store.SimulatedCrash:
        crashed = "crashed"
    assert crashed == "crashed"
    store.crash_mode = None
    release = await store.get_release("rel-0001")
    assert release.state == "validated" and release.revision == 2
    assert len(await store.transition_events("rel-0001")) == 1
    surviving = [
        entry for entry in store.audit_records if entry["action"] == "start_canary"
    ]
    assert surviving == [], "the crashed command left no audit row"


async def test_crash_after_commit_replay_returns_original_result() -> None:
    store = await _store_with_release()
    coordinator = release_mod.ReleaseCoordinator(store)
    await coordinator.execute("rel-0001", "cmd-1", "validate", 1, "op")
    store.crash_mode = "after_commit"
    crashed = None
    try:
        await coordinator.execute("rel-0001", "cmd-2", "start_canary", 2, "op")
    except memory_store.SimulatedCrash:
        crashed = "crashed"
    assert crashed == "crashed", "response lost after commit"
    store.crash_mode = None
    replayed = await coordinator.execute("rel-0001", "cmd-2", "start_canary", 2, "op")
    assert replayed.state == "canary" and replayed.revision == 3
    release = await store.get_release("rel-0001")
    assert release.revision == 3, "replay did not apply the command twice"
    assert len(await store.transition_events("rel-0001")) == 2


async def test_stale_fence_rejected_and_new_node_takes_over() -> None:
    store = await _store_with_release()
    node_a = release_mod.ReleaseCoordinator(store)
    await node_a.execute("rel-0001", "cmd-1", "validate", 1, "op-a", fence_generation=5)
    node_b = release_mod.ReleaseCoordinator(store)
    rejected = None
    try:
        await node_b.execute("rel-0001", "cmd-2", "start_canary", 2, "op-b", fence_generation=3)
    except StaleReleaseFence:
        rejected = "rejected"
    assert rejected == "rejected", "old fence writes are rejected"
    release = await store.get_release("rel-0001")
    assert release.revision == 2, "rejected write left no change"
    takeover = await node_b.execute(
        "rel-0001", "cmd-3", "start_canary", 2, "op-b", fence_generation=6
    )
    assert takeover.state == "canary"
    assert takeover.owner_fence_generation == 6, "new node took over from last committed"


async def test_redis_cache_failure_does_not_touch_authority() -> None:
    store = await _store_with_release()
    coordinator = release_mod.ReleaseCoordinator(store)

    class ExplodingCache:
        calls = 0

        async def refresh(self, route) -> None:
            self.calls += 1
            raise RuntimeError("redis unavailable")

    cache = ExplodingCache()
    result = await coordinator.execute(
        "rel-0001", "cmd-1", "validate", 1, "op", cache=cache
    )
    assert result.state == "validated"
    assert cache.calls >= 1, "post-commit refresh was attempted"
    release = await store.get_release("rel-0001")
    assert release.state == "validated", "authority committed regardless of cache"


@pytest.mark.shared_backend
def test_release_transactions_shared_backend(
    ops_namespace: str,
    shared_database_url: str,
) -> None:
    """The same command/fence/idempotency contract over real PostgreSQL."""

    async def scenario() -> tuple:
        from trpc_service.storage.postgres.database import PostgresDatabase
        from trpc_service.storage.postgres.operations_repositories import (
            PostgresReleaseRepository,
        )

        run_id = ops_namespace.rsplit("-", 1)[-1][:12]
        release_id = f"rel-{run_id}"
        command_id = f"cmd-{run_id}"
        database = PostgresDatabase(shared_database_url)
        repo = PostgresReleaseRepository(database)
        try:
            await database.initialize_schema()
            release = models.CanaryRelease(
                release_id=release_id,
                candidate_snapshot_id=f"cand-{run_id}",
                rollback_snapshot_id=f"stab-{run_id}",
                created_by_digest="6666",
                created_at=_NOW,
                cohorts=("tenant-alpha",),
            )
            created = await repo.create_release(release)
            assert created.state == "draft"
            validated = await repo.apply_command(
                release_id=release_id,
                command_id=command_id,
                action="validate",
                expected_revision=1,
                fence_generation=0,
                actor_digest="op",
            )
            assert validated.state == "validated"
            replay = await repo.apply_command(
                release_id=release_id,
                command_id=command_id,
                action="validate",
                expected_revision=1,
                fence_generation=0,
                actor_digest="op",
            )
            assert replay == validated
            conflicted = None
            try:
                await repo.apply_command(
                    release_id=release_id,
                    command_id=f"{command_id}-conflict",
                    action="start_canary",
                    expected_revision=1,
                    fence_generation=0,
                    actor_digest="op",
                )
            except ReleaseConflict:
                conflicted = "conflict"
            assert conflicted == "conflict"
            fenced = None
            try:
                await repo.apply_command(
                    release_id=release_id,
                    command_id=f"{command_id}-fence",
                    action="start_canary",
                    expected_revision=2,
                    fence_generation=-1,
                    actor_digest="op",
                )
            except StaleReleaseFence:
                fenced = "fenced"
            assert fenced == "fenced", "negative/stale fence must be rejected"
            return (validated.state, replay.revision)
        finally:
            await database.close()

    import asyncio

    state, revision = asyncio.run(scenario())
    assert state == "validated"
    assert revision == 2
