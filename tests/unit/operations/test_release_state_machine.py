"""T052 RED: canary release bounded state machine with CAS and fences.

Full happy path DRAFT -> VALIDATED -> CANARY -> COMPLETED plus the
PAUSED_QUALITY / PAUSED_INSUFFICIENT_SAMPLE / ROLLING_BACK -> ROLLED_BACK /
FAILED / FAILED_REQUIRES_REPAIR branches; every transition requires
``expected_revision`` and a fence not below the highest seen generation;
illegal transitions have no side effects; the same ``command_id`` retried
returns the first result (FR-018, FR-019, FR-022, DEC-004).
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone

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


def _release(**overrides):
    base = dict(
        release_id="rel-0001",
        candidate_snapshot_id="cand-0001",
        rollback_snapshot_id="stab-0001",
        created_by_digest="4444",
        created_at=_NOW,
        cohorts=("tenant-alpha",),
        observation_window=300,
        minimum_sample=100,
        hard_gate_types=("cross_tenant_leak", "data_consistency"),
    )
    base.update(overrides)
    return models.CanaryRelease(**base)


async def _coordinator():
    store = memory_store.InMemoryOperationsStore()
    await store.create_release(_release())
    coordinator = release_mod.ReleaseCoordinator(store)
    return store, coordinator


async def test_full_happy_path_advances_with_revision_cas() -> None:
    store, coordinator = await _coordinator()
    validated = await coordinator.execute(
        "rel-0001", "cmd-1", "validate", expected_revision=1, actor_digest="op"
    )
    assert validated.state == "validated"
    assert validated.revision == 2
    canary = await coordinator.execute(
        "rel-0001", "cmd-2", "start_canary", expected_revision=2, actor_digest="op"
    )
    assert canary.state == "canary"
    assert canary.revision == 3
    completed = await coordinator.execute(
        "rel-0001", "cmd-3", "advance", expected_revision=3, actor_digest="op"
    )
    assert completed.state == "completed"
    assert completed.revision == 4
    events = await store.transition_events("rel-0001")
    assert [event.to_state for event in events] == [
        "validated", "canary", "completed",
    ], "each transition must append exactly one journal event"


async def test_pause_resume_and_rollback_branches() -> None:
    store, coordinator = await _coordinator()
    await coordinator.execute("rel-0001", "cmd-1", "validate", 1, "op")
    await coordinator.execute("rel-0001", "cmd-2", "start_canary", 2, "op")
    paused = await coordinator.execute(
        "rel-0001", "cmd-3", "pause", 3, "op", reason_code="quality_threshold_breached"
    )
    assert paused.state == "paused_quality"
    insufficient = await coordinator.execute(
        "rel-0001", "cmd-4", "resume", 4, "op"
    )
    assert insufficient.state == "canary"
    paused_low = await coordinator.execute(
        "rel-0001", "cmd-5", "pause", 5, "op", reason_code="insufficient_sample"
    )
    assert paused_low.state == "paused_insufficient_sample"
    resumed = await coordinator.execute("rel-0001", "cmd-6", "resume", 6, "op")
    assert resumed.state == "canary"
    rolled = await coordinator.execute(
        "rel-0001", "cmd-7", "rollback", 7, "op", reason_code="operator_requested"
    )
    assert rolled.state == "rolled_back"
    events = await store.transition_events("rel-0001")
    assert ("canary", "rolling_back") in [(e.from_state, e.to_state) for e in events]
    assert ("rolling_back", "rolled_back") in [(e.from_state, e.to_state) for e in events]
    decisions = await store.rollback_decisions("rel-0001")
    assert len(decisions) == 1
    assert decisions[0].target_snapshot_id == "stab-0001"


async def test_failed_and_failed_requires_repair_branches() -> None:
    store, coordinator = await _coordinator()
    await coordinator.execute("rel-0001", "cmd-1", "validate", 1, "op")
    await coordinator.execute("rel-0001", "cmd-2", "start_canary", 2, "op")
    failed = await coordinator.execute(
        "rel-0001", "cmd-3", "fail", 3, "op", reason_code="irrecoverable_error"
    )
    assert failed.state == "failed"
    repair_needed = await coordinator.execute(
        "rel-0001", "cmd-4", "require_repair", 4, "op"
    )
    assert repair_needed.state == "failed_requires_repair"
    redrafted = await coordinator.execute(
        "rel-0001", "cmd-5", "repair", 5, "op"
    )
    assert redrafted.state == "draft"


async def test_wrong_expected_revision_conflicts_without_side_effects() -> None:
    store, coordinator = await _coordinator()
    await coordinator.execute("rel-0001", "cmd-1", "validate", 1, "op")
    conflicted = None
    try:
        await coordinator.execute("rel-0001", "cmd-2", "start_canary", 1, "op")
    except ReleaseConflict:
        conflicted = "conflict"
    assert conflicted == "conflict", "stale revision must conflict loudly"
    release = await store.get_release("rel-0001")
    assert release.state == "validated" and release.revision == 2
    assert len(await store.transition_events("rel-0001")) == 1


async def test_stale_fence_is_rejected_without_side_effects() -> None:
    store, coordinator = await _coordinator()
    advanced = await coordinator.execute(
        "rel-0001", "cmd-1", "validate", 1, "op", fence_generation=5
    )
    assert advanced.owner_fence_generation == 5, "transition adopts the fence it saw"
    rejected = None
    try:
        await coordinator.execute(
            "rel-0001", "cmd-2", "start_canary", 2, "op", fence_generation=3
        )
    except StaleReleaseFence:
        rejected = "rejected"
    assert rejected == "rejected", "a fence below the highest seen generation must be rejected"
    release = await store.get_release("rel-0001")
    assert release.state == "validated", "rejected fence must leave state untouched"
    accepted = await coordinator.execute(
        "rel-0001", "cmd-3", "start_canary", 2, "op", fence_generation=6
    )
    assert accepted.state == "canary"
    assert accepted.owner_fence_generation == 6


async def test_illegal_transition_has_no_side_effects() -> None:
    store, coordinator = await _coordinator()
    rejected = None
    try:
        await coordinator.execute("rel-0001", "cmd-1", "advance", 1, "op")
    except ReleaseConflict:
        rejected = "rejected"
    assert rejected == "rejected", "draft cannot advance directly"
    release = await store.get_release("rel-0001")
    assert release.state == "draft" and release.revision == 1
    assert await store.transition_events("rel-0001") == []


async def test_same_command_id_returns_first_result() -> None:
    store, coordinator = await _coordinator()
    first = await coordinator.execute(
        "rel-0001", "cmd-1", "validate", 1, "op"
    )
    retried = await coordinator.execute(
        "rel-0001", "cmd-1", "validate", 1, "op"
    )
    assert retried == first, "command retries are idempotent"
    release = await store.get_release("rel-0001")
    assert release.revision == 2, "a retried command must not advance the revision again"
    assert len(await store.transition_events("rel-0001")) == 1
    mismatch = None
    try:
        await coordinator.execute("rel-0001", "cmd-1", "rollback", 2, "op")
    except ReleaseConflict:
        mismatch = "mismatch"
    assert mismatch == "mismatch", (
        "reusing a command_id for a different action must be rejected"
    )


async def test_every_transition_writes_formal_audit() -> None:
    store, coordinator = await _coordinator()
    await coordinator.execute("rel-0001", "cmd-1", "validate", 1, "op")
    transition_audits = [
        entry for entry in store.audit_records if entry["action"] == "validate"
    ]
    assert len(transition_audits) == 1
    entry = transition_audits[0]
    assert entry["action"] == "validate"
    assert entry["release_id"] == "rel-0001"
    assert entry["command_id"] == "cmd-1"
    assert "op" == entry["actor_digest"]
