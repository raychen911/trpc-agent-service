from __future__ import annotations

import json
import os
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.migrate_data import (
    _endpoint_fingerprint,
    _independent_endpoints,
    _load_production_control,
    _production_candidate,
    _report,
    _require_release_binding,
    _require_runtime_target_role,
    _runtime_role_contract_error,
)
from scripts.migration_full_acceptance import (
    _require_runtime_target_role as _full_acceptance_runtime_target_role,
)
from scripts.release_gate import _validate_migration_candidate_semantics
from scripts.release_manifest import report_image_digest
from trpc_service.storage.migration import (
    MigrationPhase,
    MigrationRecord,
    MigrationScopeManifest,
    _rolling_checksum,
    canonical_migration_kinds,
)


def test_live_migration_report_has_current_candidate_lineage_without_promoting_gate(
    tmp_path: Path,
) -> None:
    report = _report(
        tmp_path / "migration-live.json",
        gate="pass",
        rejection_reasons=[],
        case_deltas={"phase": "shadow-read"},
    )
    rendered = json.loads((tmp_path / "migration-live.json").read_text(encoding="utf-8"))
    assert report["production_gate"] == "not_run"
    assert rendered["run_id"] == rendered["evidence"]["run_id"]
    assert rendered["evidence"]["kind"] == "current_candidate"
    assert rendered["evidence"]["producer"] == "scripts.migrate_data"


def test_migration_endpoint_fingerprint_uses_default_ports_without_secrets() -> None:
    assert _endpoint_fingerprint("redis://source.example/0")
    assert _endpoint_fingerprint("postgresql://target.example/db")
    assert _independent_endpoints("redis://source.example/0", "postgresql://target.example/db")
    assert not _independent_endpoints(
        "redis://same.example:6379/0", "postgresql://same.example:6379/db"
    )


def test_live_migration_checksum_uses_canonical_kind_order() -> None:
    records = (
        MigrationRecord(kind="session", resource_id="session-00", payload={"value": 0}),
        MigrationRecord(kind="session", resource_id="session-01", payload={"value": 1}),
        MigrationRecord(kind="memory", resource_id="memory-00", payload={"value": 0}),
        MigrationRecord(kind="memory", resource_id="memory-01", payload={"value": 1}),
    )

    def checksum(kinds: tuple[str, ...]) -> str:
        value = "0" * 64
        for kind in kinds:
            for record in sorted(
                (item for item in records if item.kind == kind),
                key=lambda item: item.resource_id,
            ):
                value = _rolling_checksum(value, record.checksum)
        return value

    source_checksum = checksum(canonical_migration_kinds(("memory", "session")))
    target_checksum = checksum(("session", "memory"))

    assert source_checksum == target_checksum
    assert checksum(("memory", "session")) != target_checksum


def test_live_target_requires_explicit_runtime_role() -> None:
    assert _require_runtime_target_role("postgresql://trpc_runtime@target.example/db")
    with pytest.raises(ValueError, match="explicit runtime role"):
        _require_runtime_target_role("postgresql://target.example/db")
    with pytest.raises(ValueError, match="runtime role"):
        _require_runtime_target_role("postgresql://trpc_migration@target.example/db")
    with pytest.raises(ValueError, match="runtime role"):
        _require_runtime_target_role("postgresql://trpc_worker@target.example/db")
    with pytest.raises(ValueError, match="runtime role"):
        _require_runtime_target_role("postgresql://superuser@target.example/db")
    with pytest.raises(ValueError, match="runtime role"):
        _full_acceptance_runtime_target_role("postgresql://trpc_worker@target.example/db")


def test_live_target_rejects_privileged_or_owner_runtime_identity() -> None:
    base = {
        "session_user": "trpc_runtime",
        "current_user": "trpc_runtime",
        "rolname": "trpc_runtime",
        "rolcanlogin": True,
        "rolsuper": False,
        "rolbypassrls": False,
        "rolcreaterole": False,
        "rolcreatedb": False,
        "rolreplication": False,
        "owns_public_objects": False,
    }
    assert _runtime_role_contract_error(base, "trpc_runtime") is None
    assert _runtime_role_contract_error({**base, "rolsuper": True}, "trpc_runtime")
    assert _runtime_role_contract_error({**base, "rolbypassrls": True}, "trpc_runtime")
    assert _runtime_role_contract_error({**base, "owns_public_objects": True}, "trpc_runtime")


@pytest.mark.asyncio
async def test_documented_production_control_alias_resolves_to_builtin() -> None:
    control = await _load_production_control(
        "production_migration_control.create",
        pool=SimpleNamespace(acquire=lambda: None),
        tenant_id="tenant-a",
        migration_id="migration-a",
    )

    assert type(control).__name__ == "PostgresMigrationControl"


def test_production_migration_requires_release_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TRPC_RELEASE_ID", raising=False)
    monkeypatch.delenv("TRPC_RELEASE_NONCE", raising=False)

    with pytest.raises(ValueError, match="TRPC_RELEASE_ID and TRPC_RELEASE_NONCE"):
        _require_release_binding()

    monkeypatch.setenv("TRPC_RELEASE_ID", "release-20260824-candidate")
    monkeypatch.setenv("TRPC_RELEASE_NONCE", "n" * 32)
    _require_release_binding()


def test_production_candidate_lineage_supplies_release_manifest_image(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TRPC_RELEASE_ID", "release-20260824-candidate")
    monkeypatch.setenv("TRPC_RELEASE_NONCE", "n" * 32)
    image_digest = "sha256:" + "a" * 64
    report = _report(
        tmp_path / "migration-live.json",
        gate="pass",
        rejection_reasons=[],
        production_gate="pass",
        production_rejection_reasons=[],
        production_candidate={
            "mode": "real_redis_to_postgresql",
            "scope": "production",
            "lineage": {"image_digest": image_digest},
        },
    )

    assert isinstance(report["candidate"], dict)
    assert report["candidate"]["lineage"]["image_digest"] == image_digest
    assert report_image_digest("migration-live.json", report) == image_digest
    assert report["evidence"]["release_binding"]["release_id"] == os.environ["TRPC_RELEASE_ID"]


def test_live_migration_candidate_satisfies_complete_candidate_contract(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("TRPC_RELEASE_ID", "release-20260824-candidate")
    monkeypatch.setenv("TRPC_RELEASE_NONCE", "n" * 32)
    manifest = MigrationScopeManifest(
        tenant_id="acme-prod-42",
        migration_id="cutover-42",
        source_kind="redis",
        kinds=("session", "memory"),
        source_snapshot_id="snapshot-42",
        source_count=101,
        source_checksum="b" * 64,
        app_id="app-42",
        app_revision=1,
        config_version=1,
        binding_id="binding-42",
        binding_revision=1,
    )
    phase_order = [
        *(
            phase.value
            for phase in (
                MigrationPhase.PREPARE,
                MigrationPhase.BACKFILL,
                MigrationPhase.SHADOW_READ,
                MigrationPhase.DUAL_WRITE,
                MigrationPhase.CUTOVER,
                MigrationPhase.VERIFY,
                MigrationPhase.CLEANUP,
            )
        ),
        MigrationPhase.ROLLBACK.value,
    ]
    phase_evidence = {
        phase: {
            "gate": "pass",
            "tenant_id": manifest.tenant_id,
            "migration_id": manifest.migration_id,
            "control_state": {"dual_write": phase == MigrationPhase.DUAL_WRITE.value},
        }
        for phase in phase_order
    }
    image_digest = "sha256:" + "a" * 64
    candidate = _production_candidate(
        manifest=manifest,
        phase_evidence=phase_evidence,
        source_endpoint_sha256="c" * 64,
        target_endpoint_sha256="d" * 64,
        target_count=101,
        target_checksum="b" * 64,
        cleanup_state={"atomic_cutover": True, "cleaned": True},
        rollback_state={"rollback_verified": True},
        operator_confirmation={
            "operator_id_sha256": "e" * 64,
            "confirmed_at": "2026-08-24T17:00:00Z",
        },
        image_digest=image_digest,
    )
    runtime = {
        "algorithm": "sha256",
        "status": "available",
        "value": "f" * 64,
        "mode": "real_migration",
        "worker_count": 1,
        "worker_identity_summary_sha256": "1" * 64,
        "stream_group_sha256": "2" * 64,
        "parameters_sha256": "3" * 64,
    }
    report = _report(
        tmp_path / "migration-live.json",
        gate="pass",
        rejection_reasons=[],
        production_gate="pass",
        production_rejection_reasons=[],
        production_candidate=candidate,
        runtime=runtime,
    )

    assert _validate_migration_candidate_semantics(report, report["evidence"]) == (None, None)
    assert report_image_digest("migration-live.json", report) == image_digest
