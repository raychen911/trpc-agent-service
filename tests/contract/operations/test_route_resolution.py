"""T053 RED: fail-closed route resolution for new executions.

``resolve_for_new_execution`` reads the authoritative route inside one tenant
scope and creates/reads an immutable pin; a repeated key with the same
fingerprint returns the same pin while a different fingerprint conflicts;
PostgreSQL unavailability, missing route, digest mismatch or contract
incompatibility fail closed with no fallback to process defaults or Redis;
tenants outside the release cohort never see the candidate snapshot
(FR-020, FR-021, DEC-004).
"""

from __future__ import annotations

import dataclasses
import importlib
from datetime import datetime, timezone

from trpc_service.operations.operations_errors import (
    ConfigurationIncompatible,
    ReleaseStateUnavailable,
    SnapshotDigestMismatch,
)

_NOW = datetime(2026, 9, 11, 0, 0, 0, tzinfo=timezone.utc)


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


memory_store = _load("trpc_service.operations.memory_store")
routing = _load("trpc_service.operations.routing")
models = _load("trpc_service.operations.models")


async def _seeded_store():
    store = memory_store.InMemoryOperationsStore()
    for tenant, stable, candidate in (
        ("tenant-alpha", "stab-0001", "cand-0001"),
        ("tenant-beta", "stab-0002", None),
    ):
        await store.create_snapshot(
            models.ConfigurationSnapshot(
                snapshot_id=stable,
                tenant_id=tenant,
                sequence=1,
                contract_version="v1",
                min_runtime_contract="v1",
                agent_config_ref="agent://a",
                governance_policy_ref="policy://a",
                data_backend_profile_ref="profile://a",
                payload_digest=memory_store.canonical_payload_digest({"stable": tenant}),
                change_summary="stable",
                created_by_digest="5555",
                created_at=_NOW,
                payload={"stable": tenant},
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
            created_by_digest="5555",
            created_at=_NOW,
            payload={"candidate": True},
        )
    )
    await store.set_route(
        models.TenantConfigRoute(
            tenant_id="tenant-alpha",
            stable_snapshot_id="stab-0001",
            route_generation=4,
            candidate_snapshot_id="cand-0001",
            release_id="rel-0001",
        )
    )
    await store.set_route(
        models.TenantConfigRoute(
            tenant_id="tenant-beta",
            stable_snapshot_id="stab-0002",
            route_generation=1,
        )
    )
    return store


def _resolver(store, **kwargs):
    return routing.RouteResolver(store, **kwargs)


async def test_cohort_tenant_pins_candidate_with_route_generation() -> None:
    store = await _seeded_store()
    pin = await _resolver(store).resolve_for_new_execution(
        "tenant-alpha", "key-digest-1", "fingerprint-1"
    )
    assert pin.snapshot_id == "cand-0001"
    assert pin.route_generation == 4
    assert pin.release_id == "rel-0001"
    assert pin.tenant_id == "tenant-alpha"


async def test_out_of_cohort_tenant_never_sees_candidate() -> None:
    store = await _seeded_store()
    pin = await _resolver(store).resolve_for_new_execution(
        "tenant-beta", "key-digest-2", "fingerprint-2"
    )
    assert pin.snapshot_id == "stab-0002", (
        "tenants outside the release cohort must stay on the stable snapshot"
    )


async def test_repeat_key_same_fingerprint_returns_same_pin() -> None:
    store = await _seeded_store()
    resolver = _resolver(store)
    first = await resolver.resolve_for_new_execution(
        "tenant-alpha", "key-digest-1", "fingerprint-1"
    )
    second = await resolver.resolve_for_new_execution(
        "tenant-alpha", "key-digest-1", "fingerprint-1"
    )
    assert second == first, "pins are immutable per idempotency key"


async def test_repeat_key_different_fingerprint_conflicts() -> None:
    store = await _seeded_store()
    resolver = _resolver(store)
    await resolver.resolve_for_new_execution(
        "tenant-alpha", "key-digest-1", "fingerprint-1"
    )
    conflicted = None
    try:
        await resolver.resolve_for_new_execution(
            "tenant-alpha", "key-digest-1", "fingerprint-other"
        )
    except SnapshotDigestMismatch:
        conflicted = "conflict"
    assert conflicted == "conflict", "same key with a new fingerprint is a conflict"
    pin = await store.get_pin("tenant-alpha", "key-digest-1")
    assert pin.content_fingerprint == "fingerprint-1", "the original pin stands"


async def test_authority_outage_fails_closed_without_fallback() -> None:
    store = await _seeded_store()
    store.set_available(False)
    denied = None
    try:
        await _resolver(store).resolve_for_new_execution(
            "tenant-alpha", "key-digest-1", "fingerprint-1"
        )
    except ReleaseStateUnavailable:
        denied = "denied"
    assert denied == "denied", "authority outage must fail closed"
    pin = None
    try:
        pin = await store.get_pin("tenant-alpha", "key-digest-1")
    except ReleaseStateUnavailable:
        pin = None
    assert pin is None, "no pin may exist after a fail-closed resolution"


async def test_missing_route_fails_closed() -> None:
    store = await _seeded_store()
    denied = None
    try:
        await _resolver(store).resolve_for_new_execution(
            "tenant-gamma", "key-digest-3", "fingerprint-3"
        )
    except ReleaseStateUnavailable:
        denied = "denied"
    assert denied == "denied", "an unrouted tenant must never fall back to defaults"
    assert await store.get_pin("tenant-gamma", "key-digest-3") is None


async def test_digest_mismatch_fails_closed() -> None:
    store = await _seeded_store()
    # Corrupt the stored snapshot so its declared digest no longer matches.
    store.snapshots[("tenant-alpha", "cand-0001")] = dataclasses.replace(
        store.snapshots[("tenant-alpha", "cand-0001")], payload_digest="f" * 64
    )
    denied = None
    try:
        await _resolver(store).resolve_for_new_execution(
            "tenant-alpha", "key-digest-1", "fingerprint-1"
        )
    except SnapshotDigestMismatch:
        denied = "denied"
    assert denied == "denied", "a digest mismatch must fail closed"


async def test_contract_incompatibility_fails_closed() -> None:
    store = await _seeded_store()
    denied = None
    try:
        await _resolver(store, node_contract="v1").resolve_for_new_execution(
            "tenant-alpha", "key-digest-1", "fingerprint-1"
        )
    except ConfigurationIncompatible:
        denied = "denied"
    assert denied == "denied", "a node below min_runtime_contract must not serve"


async def test_latched_route_pins_last_good_snapshot() -> None:
    store = await _seeded_store()
    await store.latch_hard_gate(
        "tenant-alpha",
        signal=models.ReleaseGateSignal(
            signal_id="sig-0001",
            tenant_id="tenant-alpha",
            release_id="rel-0001",
            signal_digest="a" * 64,
            gate_type="cross_tenant_leak",
            severity="hard",
            evidence_digest="b" * 64,
            observed_at=_NOW,
        ),
        actor_digest="enforcement",
    )
    pin = await _resolver(store).resolve_for_new_execution(
        "tenant-alpha", "key-digest-1", "fingerprint-1"
    )
    assert pin.snapshot_id == "stab-0001", "a latched route pins last-good"
    assert pin.release_id is None


async def test_redis_cache_failure_does_not_affect_authority() -> None:
    store = await _seeded_store()

    class ExplodingCache:
        refreshed = 0

        async def refresh(self, route) -> None:
            self.refreshed += 1
            raise RuntimeError("redis down")

    cache = ExplodingCache()
    pin = await _resolver(store, cache=cache).resolve_for_new_execution(
        "tenant-alpha", "key-digest-1", "fingerprint-1"
    )
    assert pin.snapshot_id == "cand-0001", "cache failure must not break resolution"
    assert cache.refreshed >= 1, "resolver still attempts the post-read refresh"
