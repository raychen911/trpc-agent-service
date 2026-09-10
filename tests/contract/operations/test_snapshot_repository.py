"""T051 RED: ConfigurationSnapshotRepository immutable insert semantics.

Snapshots are insert-only: the same tenant/id/digest returns the original
value, a different digest is a conflict; payloads may only reference secrets
indirectly; compatibility follows contract versions; and when scope, digest,
compatibility or the formal audit cannot be verified nothing is written
(FR-017, FR-021, DEC-004).
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone

from trpc_service.operations.operations_errors import (
    SnapshotDigestMismatch,
    SnapshotInvalid,
)

_NOW = datetime(2026, 9, 11, 0, 0, 0, tzinfo=timezone.utc)


def _load(name: str):
    try:
        return importlib.import_module(name)
    except ImportError:
        return None


memory_store = _load("trpc_service.operations.memory_store")
models = _load("trpc_service.operations.models")


def _snapshot(
    tenant_id: str = "tenant-alpha",
    snapshot_id: str = "11111111-1111-1111-1111-111111111111",
    sequence: int = 1,
    payload_digest: str = "2222",
    min_runtime_contract: str = "v1",
    payload: dict | None = None,
):
    if payload is None:
        payload = {"model": "default", "temperature": 0.2}
    canonical = memory_store.canonical_payload_digest(payload)
    if payload_digest == "2222":
        payload_digest = canonical
    return models.ConfigurationSnapshot(
        snapshot_id=snapshot_id,
        tenant_id=tenant_id,
        sequence=sequence,
        contract_version="v1",
        min_runtime_contract=min_runtime_contract,
        agent_config_ref="agent-config://alpha",
        governance_policy_ref="policy://alpha",
        data_backend_profile_ref="profile://alpha",
        payload_digest=payload_digest,
        change_summary="initial snapshot",
        created_by_digest="3333",
        created_at=_NOW,
        payload=payload,
    )


async def test_create_is_immutable_insert_and_returns_original() -> None:
    store = memory_store.InMemoryOperationsStore()
    snapshot = _snapshot()
    first = await store.create_snapshot(snapshot)
    second = await store.create_snapshot(_snapshot())
    assert first == second, "same tenant/id/digest must return the original value"
    stored = await store.get_snapshot("tenant-alpha", snapshot.snapshot_id)
    assert stored == snapshot, "round trip must preserve the snapshot"


async def test_create_with_different_digest_is_a_conflict() -> None:
    store = memory_store.InMemoryOperationsStore()
    await store.create_snapshot(_snapshot())
    conflict = None
    try:
        await store.create_snapshot(
            _snapshot(payload_digest="dead" * 16, payload={"model": "changed"})
        )
    except SnapshotDigestMismatch:
        conflict = "conflict"
    assert conflict == "conflict", "different digest for the same id must conflict"
    stored = await store.get_snapshot("tenant-alpha", "11111111-1111-1111-1111-111111111111")
    assert stored.payload == {"model": "default", "temperature": 0.2}, (
        "conflicting create must not overwrite the stored snapshot"
    )


async def test_create_rejects_plain_secret_payloads() -> None:
    store = memory_store.InMemoryOperationsStore()
    rejected = None
    try:
        await store.create_snapshot(
            _snapshot(payload={"api_key": "sk-plain-secret-value"})
        )
    except (SnapshotInvalid, ValueError):
        rejected = "rejected"
    assert rejected == "rejected", "plain secrets must never enter the store"


async def test_create_recomputes_digest_and_rejects_mismatch() -> None:
    store = memory_store.InMemoryOperationsStore()
    rejected = None
    try:
        await store.create_snapshot(
            _snapshot(payload={"model": "default", "temperature": 0.2}, payload_digest="0" * 64)
        )
    except SnapshotDigestMismatch:
        rejected = "rejected"
    assert rejected == "rejected", "declared digest must match the canonical payload digest"
    assert await store.get_snapshot("tenant-alpha", "11111111-1111-1111-1111-111111111111") is None


async def test_verify_compatible_follows_contract_versions() -> None:
    store = memory_store.InMemoryOperationsStore()
    await store.create_snapshot(_snapshot(min_runtime_contract="v2"))
    assert await store.verify_compatible(
        "tenant-alpha", "11111111-1111-1111-1111-111111111111", "v3"
    ), "newer runtime must serve the snapshot"
    assert not await store.verify_compatible(
        "tenant-alpha", "11111111-1111-1111-1111-111111111111", "v1"
    ), "older runtime must be rejected"


async def test_audit_failure_leaves_no_write() -> None:
    store = memory_store.InMemoryOperationsStore()
    store.audit_failure_countdown = 1
    failed = None
    try:
        await store.create_snapshot(_snapshot())
    except Exception:
        failed = "failed"
    assert failed == "failed", "audit failure must fail the create loudly"
    assert await store.get_snapshot("tenant-alpha", "11111111-1111-1111-1111-111111111111") is None, (
        "nothing may be written when the audit trail is unavailable"
    )
    assert store.audit_records == [], "no audit row may survive a rolled-back create"


async def test_authority_outage_fails_closed() -> None:
    store = memory_store.InMemoryOperationsStore()
    await store.create_snapshot(_snapshot())
    store.set_available(False)
    from trpc_service.operations.operations_errors import ReleaseStateUnavailable

    denied = None
    try:
        await store.get_snapshot("tenant-alpha", "11111111-1111-1111-1111-111111111111")
    except ReleaseStateUnavailable:
        denied = "denied"
    assert denied == "denied", "authority outage must fail closed, not return stale data"
