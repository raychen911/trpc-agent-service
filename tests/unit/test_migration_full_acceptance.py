from __future__ import annotations

import asyncio
import json
from argparse import Namespace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

import scripts.migration_full_acceptance as full_acceptance
import trpc_service.storage.migration as migration_module
from scripts.migration_full_acceptance import (
    _bounded_positive_int,
    _bounded_timeout,
    _FailOnceAfterBatchTarget,
    _live_failure,
    _LiveProgress,
    _phase_evidence,
    _PhaseAwareTarget,
    _postgres_connection_initializer,
    _run,
    _run_branch,
    execute_full_acceptance,
)
from trpc_service.storage.migration import (
    InMemoryMigrationCheckpointStore,
    MigrationCheckpoint,
    MigrationLease,
    MigrationPhase,
    MigrationRecord,
    MigrationResult,
    MigrationScopeManifest,
    MigrationSourceSnapshot,
)


@pytest.fixture(autouse=True)
def _acceptance_identity_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TRPC_MIGRATION_APP_ID", "migration-acceptance-app-test")
    monkeypatch.setenv("TRPC_MIGRATION_APP_REVISION", "1")
    monkeypatch.setenv("TRPC_MIGRATION_CONFIG_VERSION", "1")
    monkeypatch.setenv("TRPC_MIGRATION_BINDING_ID", "migration-acceptance-binding-test")
    monkeypatch.setenv("TRPC_MIGRATION_BINDING_REVISION", "1")


class _Source:
    def __init__(self) -> None:
        self.records = tuple(
            MigrationRecord(
                kind="session" if index % 2 == 0 else "memory",
                resource_id=f"record-{index}",
                payload={"value": index, "secret": "api-token-not-for-reports"},
            )
            for index in range(5)
        )

    async def fetch(self, tenant_id: str, *, cursor: str | None, limit: int):
        del tenant_id
        start = int(cursor or 0)
        values = self.records[start : start + limit]
        end = start + len(values)
        return values, str(end) if end < len(self.records) else None


class _Target:
    def __init__(self, control: _Control) -> None:
        self.control = control
        self.records: dict[tuple[str, str, str], MigrationRecord] = {}
        self.actions: list[tuple[str, str, bool | None]] = []

    async def prepare(self, tenant_id: str) -> None:
        self.actions.append(("prepare", tenant_id, None))

    async def upsert(self, tenant_id: str, record: MigrationRecord) -> None:
        self.records[(tenant_id, record.kind, record.resource_id)] = record

    async def read(self, tenant_id: str, kind: str, resource_id: str):
        return self.records.get((tenant_id, kind, resource_id))

    async def set_dual_write(self, tenant_id: str, enabled: bool) -> None:
        self.actions.append(("dual-write", tenant_id, enabled))
        await self.control.set_dual_write(tenant_id, enabled)

    async def cutover(self, tenant_id: str) -> None:
        self.actions.append(("cutover", tenant_id, None))
        await self.control.cutover(tenant_id)

    async def cleanup(self, tenant_id: str) -> None:
        self.actions.append(("cleanup", tenant_id, None))
        await self.control.cleanup(tenant_id)

    async def rollback(self, tenant_id: str) -> None:
        self.actions.append(("rollback", tenant_id, None))
        await self.control.rollback(tenant_id)


class _Control:
    def __init__(self) -> None:
        self.state: dict[str, Any] = {
            "dual_write": False,
            "active_profile": "source",
            "cleaned": False,
            "rolled_back": False,
            "mailbox_v2": "ready",
        }

    async def set_dual_write(self, tenant_id: str, enabled: bool) -> None:
        del tenant_id
        self.state["dual_write"] = enabled
        self.state["mailbox_v2"] = (
            "dual-write"
            if enabled
            else "source"
            if self.state["active_profile"] == "source"
            else "target"
        )

    async def cutover(self, tenant_id: str) -> None:
        del tenant_id
        self.state["active_profile"] = "target"
        self.state["mailbox_v2"] = "target"

    async def cleanup(self, tenant_id: str) -> None:
        del tenant_id
        self.state["dual_write"] = False
        self.state["cleaned"] = True
        self.state["mailbox_v2"] = "target"

    async def rollback(self, tenant_id: str) -> None:
        del tenant_id
        self.state["dual_write"] = False
        self.state["active_profile"] = "source"
        self.state["rolled_back"] = True
        self.state["mailbox_v2"] = "source"

    async def read_state(self, tenant_id: str, migration_id: str) -> dict[str, Any]:
        del tenant_id, migration_id
        return dict(self.state)


class _NoOpControl(_Control):
    async def set_dual_write(self, tenant_id: str, enabled: bool) -> None:
        del tenant_id, enabled

    async def cutover(self, tenant_id: str) -> None:
        del tenant_id

    async def cleanup(self, tenant_id: str) -> None:
        del tenant_id

    async def rollback(self, tenant_id: str) -> None:
        del tenant_id


def _source_snapshot(source: _Source, *, snapshot_id: str = "snapshot") -> MigrationSourceSnapshot:
    checksum = "0" * 64
    for record in source.records:
        checksum = migration_module._rolling_checksum(checksum, record.checksum)
    return MigrationSourceSnapshot(
        source_snapshot_id=snapshot_id,
        source_count=len(source.records),
        source_checksum=checksum,
    )


def test_resume_probe_target_delegates_lease_binding() -> None:
    class Target:
        def __init__(self) -> None:
            self.bound: MigrationLease | None = None

        def bind_migration_lease(self, lease: MigrationLease) -> None:
            self.bound = lease

    lease = MigrationLease(
        tenant_id="migration-acceptance-test",
        migration_id="migration-acceptance-test",
        owner_id="worker",
        owner_instance="instance",
        lease_epoch=1,
        expires_at=datetime.now(UTC) + timedelta(minutes=1),
    )
    target = Target()
    wrapped = _FailOnceAfterBatchTarget(target, fail_after=1)
    wrapped.bind_migration_lease(lease)
    assert target.bound == lease


@pytest.mark.asyncio
async def test_phase_aware_target_defers_enumeration_until_shadow_read() -> None:
    class EnumeratingTarget(_Target):
        async def list_records_page(
            self,
            tenant_id: str,
            kind: str,
            *,
            cursor: str | None,
            limit: int,
        ) -> tuple[tuple[MigrationRecord, ...], str | None]:
            del tenant_id, kind, cursor, limit
            return (), None

    target = EnumeratingTarget(_Control())
    wrapped = _PhaseAwareTarget(target)
    with pytest.raises(ValueError, match="deferred until SHADOW_READ"):
        await wrapped.list_records_page(
            "migration-acceptance-test", "session", cursor=None, limit=2
        )

    wrapped.set_enumeration_enabled(True)
    assert await wrapped.list_records_page(
        "migration-acceptance-test", "session", cursor=None, limit=2
    ) == ((), None)


@pytest.mark.asyncio
async def test_manifest_bound_branch_uses_coordinator_snapshot_validation_once_per_phase() -> None:
    source = _Source()
    expected = _source_snapshot(source)

    class EnumeratingTarget(_Target):
        async def list_records_page(
            self,
            tenant_id: str,
            kind: str,
            *,
            cursor: str | None,
            limit: int,
        ) -> tuple[tuple[MigrationRecord, ...], str | None]:
            records = sorted(
                (
                    record
                    for (record_tenant, record_kind, _), record in self.records.items()
                    if record_tenant == tenant_id and record_kind == kind
                ),
                key=lambda record: record.resource_id,
            )
            start = int(cursor or 0)
            page = tuple(records[start : start + limit])
            end = start + len(page)
            return page, str(end) if end < len(records) else None

    class SnapshotSource(_Source):
        def __init__(self) -> None:
            super().__init__()
            self.snapshot_calls = 0

        async def snapshot(self, _tenant_id: str) -> MigrationSourceSnapshot:
            self.snapshot_calls += 1
            return expected

    manifest = MigrationScopeManifest(
        tenant_id="migration-acceptance-test",
        migration_id="migration-acceptance-branch",
        source_kind="redis",
        kinds=("session", "memory"),
        source_snapshot_id=expected.source_snapshot_id,
        source_count=expected.source_count,
        source_checksum=expected.source_checksum,
        app_id="migration-acceptance-app-test",
        app_revision=1,
        config_version=1,
        binding_id="migration-acceptance-binding-test",
        binding_revision=1,
    )
    snapshot_source = SnapshotSource()
    control = _Control()
    target = _PhaseAwareTarget(EnumeratingTarget(control))
    result = await _run_branch(
        snapshot_source,
        target,
        InMemoryMigrationCheckpointStore(),
        control=control,
        tenant_id=manifest.tenant_id,
        migration_id=manifest.migration_id,
        batch_size=2,
        expected_records=5,
        terminal=MigrationPhase.ROLLBACK,
        resume_probe=False,
        expected_source_snapshot=expected,
        guard=None,
        lease=None,
        manifest=manifest,
    )

    assert result["phase_evidence"][MigrationPhase.VERIFY.value]["gate"] == "pass"
    phase_evidence = result["phase_evidence"]
    assert phase_evidence[MigrationPhase.PREPARE.value]["target_enumeration"] == (
        "deferred_until_shadow_read"
    )
    assert phase_evidence[MigrationPhase.SHADOW_READ.value]["target_enumeration"] == ("independent")
    assert phase_evidence[MigrationPhase.VERIFY.value]["target_enumeration"] == "independent"
    assert phase_evidence[MigrationPhase.ROLLBACK.value]["target_enumeration"] == (
        "deferred_until_shadow_read"
    )
    # The coordinator performs the two guarded checks around each of the
    # seven phases.  The acceptance wrapper must not add another two checks.
    assert snapshot_source.snapshot_calls == 14


@pytest.mark.asyncio
async def test_full_acceptance_proves_resume_counts_checksum_cutover_cleanup_and_rollback() -> None:
    rollback_control = _Control()
    cleanup_control = _Control()
    rollback_target = _Target(rollback_control)
    cleanup_target = _Target(cleanup_control)
    report = await execute_full_acceptance(
        _Source(),
        rollback_target,
        InMemoryMigrationCheckpointStore(),
        tenant_id="migration-acceptance-test",
        migration_id="migration-acceptance-test",
        rollback_control=rollback_control,
        cleanup_target=cleanup_target,
        cleanup_control=cleanup_control,
        batch_size=2,
        expected_records=5,
    )

    assert report["gate"] == "pass"
    assert report["production_gate"] == "not_run"
    assert report["candidate"]["mailbox_v2_control"]["required"] is True
    assert "mailbox_v2" in report["candidate"]["mailbox_v2_control"]["state_field"]
    deltas = report["case_deltas"]
    assert deltas["checkpoint_persistence"] == "pass"
    assert deltas["checkpoint_resume"] == "pass"
    assert deltas["branch_migration_ids"] == {
        "rollback_before_cleanup": "migration-acceptance-test-rollback",
        "final_cleanup": "migration-acceptance-test-cleanup",
    }
    assert deltas["cutover"] == "pass"
    assert deltas["cleanup"] == "pass"
    assert deltas["rollback"] == "pass"
    assert deltas["mailbox_v2"] == "pass"
    assert deltas["target_extra_records"] == "not_verified"
    assert "not_verified" in report["caveats"][0]
    assert "api-token-not-for-reports" not in json.dumps(report)
    assert deltas["source_count"] == deltas["target_count"] == 5
    assert len(deltas["checksum"]) == 64
    assert deltas["differences"] == []
    assert (
        deltas["phase_evidence"]["rollback_branch"]["phases"]["backfill"]["checkpoint"]["completed"]
        is True
    )
    assert (
        deltas["phase_evidence"]["rollback_branch"]["phases"]["rollback"]["checkpoint"]["phase"]
        == "rollback"
    )
    assert deltas["phase_evidence"]["rollback_branch"]["control_state"]["after_rollback"] == {
        "dual_write": False,
        "active_profile": "source",
        "cleaned": False,
        "rolled_back": True,
        "mailbox_v2": "source",
    }
    assert (
        deltas["phase_evidence"]["rollback_branch"]["control_state"]["after_prepare"]["mailbox_v2"]
        == "ready"
    )
    assert (
        deltas["phase_evidence"]["rollback_branch"]["control_state"]["after_dual_write"][
            "mailbox_v2"
        ]
        == "dual-write"
    )
    assert (
        deltas["phase_evidence"]["rollback_branch"]["control_state"]["after_backfill"]["mailbox_v2"]
        == "ready"
    )
    assert (
        deltas["phase_evidence"]["rollback_branch"]["control_state"]["after_shadow_read"][
            "mailbox_v2"
        ]
        == "ready"
    )
    assert (
        deltas["phase_evidence"]["rollback_branch"]["control_state"]["after_cutover"]["mailbox_v2"]
        == "target"
    )
    assert (
        deltas["phase_evidence"]["rollback_branch"]["control_state"]["after_verify"]["mailbox_v2"]
        == "target"
    )
    assert deltas["phase_evidence"]["cleanup_branch"]["control_state"]["after_cleanup"] == {
        "dual_write": False,
        "active_profile": "target",
        "cleaned": True,
        "rolled_back": False,
        "mailbox_v2": "target",
    }
    assert [item[0] for item in rollback_target.actions] == [
        "prepare",
        "dual-write",
        "cutover",
        "rollback",
        "dual-write",
    ]
    assert [item[0] for item in cleanup_target.actions] == [
        "prepare",
        "dual-write",
        "cutover",
        "cleanup",
        "dual-write",
    ]


@pytest.mark.asyncio
async def test_live_branch_factory_binds_each_manifest_and_lease_scope(monkeypatch) -> None:
    base_manifest = MigrationScopeManifest(
        tenant_id="migration-acceptance-test",
        migration_id="migration-acceptance-run",
        source_kind="redis",
        kinds=("memory", "session"),
        source_snapshot_id="snapshot",
        source_count=5,
        source_checksum="a" * 64,
        app_id="migration-acceptance-app-test",
        app_revision=1,
        config_version=1,
        binding_id="migration-acceptance-binding-test",
        binding_revision=1,
    )
    scopes: list[tuple[str, str, str]] = []
    released: list[str] = []

    async def branch_scope(suffix: str):
        migration_id = f"{base_manifest.migration_id}-{suffix}"
        manifest = base_manifest.model_copy(update={"migration_id": migration_id})
        lease = MigrationLease(
            tenant_id=manifest.tenant_id,
            migration_id=manifest.migration_id,
            owner_id="acceptance-worker",
            owner_instance=f"instance-{suffix}",
            lease_epoch=1,
            expires_at=datetime.now(UTC) + timedelta(minutes=1),
        )
        scopes.append((suffix, manifest.migration_id, lease.migration_id))
        return _Target(_Control()), _Control(), manifest, lease

    async def release(lease: MigrationLease) -> None:
        released.append(lease.migration_id)

    async def fake_run_branch(*args: Any, **kwargs: Any) -> dict[str, Any]:
        del args
        scopes.append(
            (
                "run",
                kwargs["migration_id"],
                kwargs["manifest"].migration_id,
            )
        )
        return {
            "checkpoint_resume": "pass",
            "phase_evidence": {
                "verify": {
                    "source_count": 5,
                    "target_count": 5,
                    "checksum": "b" * 64,
                    "differences": [],
                }
            },
            "control_state": {},
        }

    monkeypatch.setattr(full_acceptance, "_run_branch", fake_run_branch)
    report = await execute_full_acceptance(
        _Source(),
        None,
        InMemoryMigrationCheckpointStore(),
        tenant_id=base_manifest.tenant_id,
        migration_id=base_manifest.migration_id,
        rollback_control=None,
        batch_size=2,
        expected_records=5,
        guard=object(),  # type: ignore[arg-type]
        manifest=base_manifest,
        branch_scope_factory=branch_scope,
        release_branch=release,
    )
    assert scopes == [
        ("rollback", "migration-acceptance-run-rollback", "migration-acceptance-run-rollback"),
        ("run", "migration-acceptance-run-rollback", "migration-acceptance-run-rollback"),
        ("cleanup", "migration-acceptance-run-cleanup", "migration-acceptance-run-cleanup"),
        ("run", "migration-acceptance-run-cleanup", "migration-acceptance-run-cleanup"),
    ]
    assert released == [
        "migration-acceptance-run-rollback",
        "migration-acceptance-run-cleanup",
    ]
    assert report["gate"] == "pass"


@pytest.mark.asyncio
async def test_full_acceptance_binds_snapshot_checksum_and_identity() -> None:
    source = _Source()
    expected = _source_snapshot(source)
    rollback_control = _Control()
    cleanup_control = _Control()

    class SnapshotSource(_Source):
        async def snapshot(self, _tenant_id: str) -> MigrationSourceSnapshot:
            return expected

    report = await execute_full_acceptance(
        SnapshotSource(),
        _Target(rollback_control),
        InMemoryMigrationCheckpointStore(),
        tenant_id="migration-acceptance-test",
        migration_id="migration-acceptance-snapshot",
        rollback_control=rollback_control,
        cleanup_target=_Target(cleanup_control),
        cleanup_control=cleanup_control,
        batch_size=2,
        expected_records=5,
        require_source_snapshot=True,
    )
    assert report["baseline"]["source_snapshot"] == expected.model_dump(mode="json")


@pytest.mark.asyncio
async def test_live_full_acceptance_requires_callable_source_snapshot() -> None:
    class InvalidSnapshotSource(_Source):
        snapshot = "not-callable"

    with pytest.raises(AssertionError, match="snapshot must be callable"):
        await execute_full_acceptance(
            InvalidSnapshotSource(),
            _Target(_Control()),
            InMemoryMigrationCheckpointStore(),
            tenant_id="migration-acceptance-test",
            migration_id="migration-acceptance-invalid-snapshot",
            rollback_control=_Control(),
            batch_size=2,
            expected_records=5,
            require_source_snapshot=True,
        )


@pytest.mark.asyncio
async def test_full_acceptance_requires_resume_fixture_size() -> None:
    with pytest.raises(ValueError, match="resume probe"):
        await execute_full_acceptance(
            _Source(),
            _Target(_Control()),
            InMemoryMigrationCheckpointStore(),
            tenant_id="migration-acceptance-test",
            migration_id="migration-acceptance-test",
            rollback_control=_Control(),
            batch_size=5,
            expected_records=5,
        )


def test_phase_evidence_rejects_incomplete_checkpoint() -> None:
    result = MigrationResult(
        baseline="source",
        candidate="target",
        case_deltas={
            "phase": MigrationPhase.BACKFILL.value,
            "source_count": 1,
            "target_count": 1,
            "checksum": "a" * 64,
            "differences": [],
        },
        gate="pass",
    )
    checkpoint = MigrationCheckpoint(
        tenant_id="migration-acceptance-test",
        migration_id="migration-acceptance-test",
        phase=MigrationPhase.BACKFILL,
        checksum="a" * 64,
        source_count=1,
        target_count=1,
        completed=False,
    )
    with pytest.raises(AssertionError, match="not completed"):
        _phase_evidence(result, checkpoint)


@pytest.mark.asyncio
async def test_full_acceptance_rejects_noop_control_with_stale_observable_state() -> None:
    control = _NoOpControl()
    with pytest.raises(AssertionError, match="dual-write state"):
        await execute_full_acceptance(
            _Source(),
            _Target(control),
            InMemoryMigrationCheckpointStore(),
            tenant_id="migration-acceptance-test",
            migration_id="migration-acceptance-test",
            rollback_control=control,
            batch_size=2,
            expected_records=5,
        )


@pytest.mark.asyncio
async def test_full_acceptance_requires_mailbox_v2_control_state() -> None:
    control = _Control()
    del control.state["mailbox_v2"]
    with pytest.raises(AssertionError, match="mailbox_v2"):
        await execute_full_acceptance(
            _Source(),
            _Target(control),
            InMemoryMigrationCheckpointStore(),
            tenant_id="migration-acceptance-test",
            migration_id="migration-acceptance-test",
            rollback_control=control,
            batch_size=2,
            expected_records=5,
        )


@pytest.mark.asyncio
async def test_full_acceptance_default_is_not_run_without_both_opt_ins(
    tmp_path: Path, monkeypatch
) -> None:
    output = tmp_path / "migration-full.json"
    monkeypatch.delenv("TRPC_RUN_REAL_MIGRATION", raising=False)
    monkeypatch.delenv("TRPC_MIGRATION_FULL_ACCEPTANCE", raising=False)
    report = await _run(Namespace(output=output, batch_size=2, db_pool_size=2))
    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
    rendered = json.loads(output.read_text(encoding="utf-8"))
    assert rendered["gate"] == "not_run"
    assert rendered["run_id"] == rendered["evidence"]["run_id"]
    assert rendered["evidence"]["producer"] == full_acceptance.PRODUCER


@pytest.mark.asyncio
async def test_full_acceptance_single_missing_opt_in_does_not_touch_connections(
    tmp_path: Path, monkeypatch
) -> None:
    output = tmp_path / "migration-full.json"
    monkeypatch.setenv("TRPC_RUN_REAL_MIGRATION", "1")
    monkeypatch.delenv("TRPC_MIGRATION_FULL_ACCEPTANCE", raising=False)

    def fail_connection(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AssertionError("connection sentinel was touched")

    monkeypatch.setattr(full_acceptance.redis_async, "from_url", fail_connection)
    monkeypatch.setattr(full_acceptance.asyncpg, "create_pool", fail_connection)
    report = await _run(Namespace(output=output, batch_size=2, db_pool_size=2))
    assert report["gate"] == "not_run"
    assert "FULL_ACCEPTANCE=1" in report["rejection_reasons"][0]


@pytest.mark.asyncio
async def test_full_acceptance_missing_run_opt_in_does_not_touch_connections(
    tmp_path: Path, monkeypatch
) -> None:
    output = tmp_path / "migration-full.json"
    monkeypatch.delenv("TRPC_RUN_REAL_MIGRATION", raising=False)
    monkeypatch.setenv("TRPC_MIGRATION_FULL_ACCEPTANCE", "1")

    def fail_connection(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AssertionError("connection sentinel was touched")

    monkeypatch.setattr(full_acceptance.redis_async, "from_url", fail_connection)
    monkeypatch.setattr(full_acceptance.asyncpg, "create_pool", fail_connection)
    report = await _run(Namespace(output=output, batch_size=2, db_pool_size=2))
    assert report["gate"] == "not_run"
    assert "RUN_REAL_MIGRATION=1" in report["rejection_reasons"][0]


@pytest.mark.asyncio
async def test_full_acceptance_rejects_non_dedicated_tenant_before_connecting(
    tmp_path: Path, monkeypatch
) -> None:
    output = tmp_path / "migration-full.json"
    monkeypatch.setenv("TRPC_RUN_REAL_MIGRATION", "1")
    monkeypatch.setenv("TRPC_MIGRATION_FULL_ACCEPTANCE", "1")
    monkeypatch.setenv("TRPC_MIGRATION_TENANT_ID", "production-tenant")
    monkeypatch.setenv("TRPC_MIGRATION_SOURCE_REDIS_URL", "redis://source:6379/0")
    monkeypatch.setenv("TRPC_MIGRATION_TARGET_DATABASE_DSN", "postgresql://target/db")
    monkeypatch.setenv("TRPC_MIGRATION_ID", "migration-test")
    monkeypatch.setenv("TRPC_MIGRATION_EXPECTED_RECORDS", "5")
    monkeypatch.setenv(
        "TRPC_MIGRATION_CONTROL_FACTORY",
        "tests.unit.test_migration_full_acceptance:factory",
    )
    report = await _run(Namespace(output=output, batch_size=2, db_pool_size=2))
    assert report["gate"] == "not_run"
    assert "dedicated migration-acceptance- prefix" in report["rejection_reasons"][0]


@pytest.mark.asyncio
async def test_full_acceptance_rejects_empty_tenant_or_migration_suffix(
    tmp_path: Path, monkeypatch
) -> None:
    output = tmp_path / "migration-full.json"
    monkeypatch.setenv("TRPC_RUN_REAL_MIGRATION", "1")
    monkeypatch.setenv("TRPC_MIGRATION_FULL_ACCEPTANCE", "1")
    monkeypatch.setenv("TRPC_MIGRATION_SOURCE_REDIS_URL", "redis://source:6379/0")
    monkeypatch.setenv("TRPC_MIGRATION_TARGET_DATABASE_DSN", "postgresql://target:5432/db")
    monkeypatch.setenv("TRPC_MIGRATION_EXPECTED_RECORDS", "5")
    monkeypatch.setenv("TRPC_MIGRATION_CONTROL_FACTORY", "tests.fake:factory")
    monkeypatch.setenv("TRPC_MIGRATION_TENANT_ID", "migration-acceptance-")
    monkeypatch.setenv("TRPC_MIGRATION_ID", "migration-acceptance-valid")
    report = await _run(Namespace(output=output, batch_size=2, db_pool_size=2))
    assert report["gate"] == "not_run"
    assert "non-empty suffix" in report["rejection_reasons"][0]

    monkeypatch.setenv("TRPC_MIGRATION_TENANT_ID", "migration-acceptance-valid")
    monkeypatch.setenv("TRPC_MIGRATION_ID", "migration-acceptance-")
    report = await _run(Namespace(output=output, batch_size=2, db_pool_size=2))
    assert report["gate"] == "not_run"
    assert "TRPC_MIGRATION_ID" in report["rejection_reasons"][0]


@pytest.mark.asyncio
async def test_full_acceptance_rejects_same_service_endpoint_before_connections(
    tmp_path: Path, monkeypatch
) -> None:
    output = tmp_path / "migration-full.json"
    monkeypatch.setenv("TRPC_RUN_REAL_MIGRATION", "1")
    monkeypatch.setenv("TRPC_MIGRATION_FULL_ACCEPTANCE", "1")
    monkeypatch.setenv("TRPC_MIGRATION_SOURCE_REDIS_URL", "redis://same-host:6379/0")
    monkeypatch.setenv("TRPC_MIGRATION_TARGET_DATABASE_DSN", "postgresql://same-host:6379/db")
    monkeypatch.setenv("TRPC_MIGRATION_TENANT_ID", "migration-acceptance-valid")
    monkeypatch.setenv("TRPC_MIGRATION_ID", "migration-acceptance-valid")
    monkeypatch.setenv("TRPC_MIGRATION_EXPECTED_RECORDS", "5")
    monkeypatch.setenv("TRPC_MIGRATION_CONTROL_FACTORY", "tests.fake:factory")

    def fail_connection(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AssertionError("connection sentinel was touched")

    monkeypatch.setattr(full_acceptance.redis_async, "from_url", fail_connection)
    monkeypatch.setattr(full_acceptance.asyncpg, "create_pool", fail_connection)
    report = await _run(Namespace(output=output, batch_size=2, db_pool_size=2))
    assert report["gate"] == "not_run"
    assert "independent backends" in report["rejection_reasons"][0]


@pytest.mark.asyncio
async def test_full_acceptance_invalid_endpoint_port_writes_not_run_report(
    tmp_path: Path, monkeypatch
) -> None:
    output = tmp_path / "migration-full.json"
    monkeypatch.setenv("TRPC_RUN_REAL_MIGRATION", "1")
    monkeypatch.setenv("TRPC_MIGRATION_FULL_ACCEPTANCE", "1")
    monkeypatch.setenv("TRPC_MIGRATION_SOURCE_REDIS_URL", "redis://source:6379/0")
    monkeypatch.setenv("TRPC_MIGRATION_TARGET_DATABASE_DSN", "postgresql://target:not-a-port/db")
    monkeypatch.setenv("TRPC_MIGRATION_TENANT_ID", "migration-acceptance-valid")
    monkeypatch.setenv("TRPC_MIGRATION_ID", "migration-acceptance-valid")
    monkeypatch.setenv("TRPC_MIGRATION_EXPECTED_RECORDS", "5")
    monkeypatch.setenv("TRPC_MIGRATION_CONTROL_FACTORY", "tests.fake:factory")

    def fail_connection(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AssertionError("connection sentinel was touched")

    monkeypatch.setattr(full_acceptance.redis_async, "from_url", fail_connection)
    monkeypatch.setattr(full_acceptance.asyncpg, "create_pool", fail_connection)
    report = await _run(Namespace(output=output, batch_size=2, db_pool_size=2))
    assert report["gate"] == "not_run"
    assert report["production_gate"] == "not_run"
    assert "invalid URL or port" in report["rejection_reasons"][0]
    assert json.loads(output.read_text(encoding="utf-8"))["gate"] == "not_run"


@pytest.mark.asyncio
async def test_full_acceptance_requires_current_release_binding_before_connecting(
    tmp_path: Path, monkeypatch
) -> None:
    output = tmp_path / "migration-full.json"
    monkeypatch.setenv("TRPC_RUN_REAL_MIGRATION", "1")
    monkeypatch.setenv("TRPC_MIGRATION_FULL_ACCEPTANCE", "1")
    values = {
        "TRPC_MIGRATION_SOURCE_REDIS_URL": "redis://source:6379/0",
        "TRPC_MIGRATION_TARGET_DATABASE_DSN": "postgresql://trpc_runtime@target:5432/db",
        "TRPC_MIGRATION_TENANT_ID": "migration-acceptance-valid",
        "TRPC_MIGRATION_ID": "migration-acceptance-valid",
        "TRPC_MIGRATION_EXPECTED_RECORDS": "5",
        "TRPC_MIGRATION_CONTROL_FACTORY": "tests.unit.test_migration_full_acceptance:factory",
        "TRPC_MIGRATION_APP_ID": "migration-acceptance-app-valid",
        "TRPC_MIGRATION_APP_REVISION": "1",
        "TRPC_MIGRATION_CONFIG_VERSION": "1",
        "TRPC_MIGRATION_BINDING_ID": "migration-acceptance-binding-valid",
        "TRPC_MIGRATION_BINDING_REVISION": "1",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("TRPC_RELEASE_ID", raising=False)
    monkeypatch.delenv("TRPC_RELEASE_NONCE", raising=False)

    def fail_connection(*args: Any, **kwargs: Any) -> None:
        del args, kwargs
        raise AssertionError("connection sentinel was touched")

    monkeypatch.setattr(full_acceptance.redis_async, "from_url", fail_connection)
    monkeypatch.setattr(full_acceptance.asyncpg, "create_pool", fail_connection)
    report = await _run(Namespace(output=output, batch_size=2, db_pool_size=2))

    assert report["gate"] == "not_run"
    assert "TRPC_RELEASE_ID and TRPC_RELEASE_NONCE" in report["rejection_reasons"][0]


def test_endpoint_identity_rejects_same_host_and_port_even_with_different_schemes() -> None:
    assert not full_acceptance._independent_endpoints(
        "redis://same-host:6379/0", "postgresql://same-host:6379/db"
    )
    assert not full_acceptance._independent_endpoints(
        "redis://same-host:6379/0", "redis://same-host:6379/1"
    )
    assert full_acceptance._independent_endpoints(
        "redis://source-host:6379/0", "postgresql://target-host:5432/db"
    )


def test_live_acceptance_limits_reject_non_finite_or_excessive_values() -> None:
    with pytest.raises(ValueError, match="positive integer"):
        _bounded_positive_int("NaN", name="batch", maximum=10)
    with pytest.raises(ValueError, match="between 1 and 10"):
        _bounded_positive_int("11", name="batch", maximum=10)
    with pytest.raises(ValueError, match="positive integer"):
        _bounded_positive_int(1.5, name="batch", maximum=10)


def test_live_acceptance_timeout_limits_reject_non_finite_or_excessive_values() -> None:
    with pytest.raises(ValueError, match="finite positive"):
        _bounded_timeout("NaN", name="timeout", maximum=10.0)
    with pytest.raises(ValueError, match="between 0 and 10"):
        _bounded_timeout("11", name="timeout", maximum=10.0)
    with pytest.raises(ValueError, match="between 0 and 10"):
        _bounded_timeout(0, name="timeout", maximum=10.0)


def test_live_failure_report_contains_only_secret_free_progress_labels(tmp_path: Path) -> None:
    progress = _LiveProgress()
    progress.started("verify", "target_enumeration")
    progress.completed_operation("verify", "target_enumeration")
    report = _live_failure(
        tmp_path / "migration-failure.json",
        "live migration acceptance timed out after 300 seconds",
        progress=progress,
    )

    assert report["case_deltas"] == {
        "last_phase": "verify",
        "last_operation": "target_enumeration",
        "last_completed_phase": "not_started",
        "last_completed_operation": "target_enumeration",
    }
    assert "secret" not in json.dumps(report)


@pytest.mark.asyncio
async def test_postgres_initializer_sets_statement_and_lock_timeouts() -> None:
    class Connection:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...]]] = []

        async def execute(self, query: str, *args: Any) -> None:
            self.calls.append((query, args))

    connection = Connection()
    await _postgres_connection_initializer(30.0)(connection)
    assert len(connection.calls) == 1
    query, args = connection.calls[0]
    assert "statement_timeout" in query and "lock_timeout" in query
    assert args == ("30000ms", "5000ms")


@pytest.mark.asyncio
async def test_full_acceptance_timeout_is_fail_closed_and_terminates_pool(
    tmp_path: Path, monkeypatch
) -> None:
    values = {
        "TRPC_RUN_REAL_MIGRATION": "1",
        "TRPC_MIGRATION_FULL_ACCEPTANCE": "1",
        "TRPC_MIGRATION_SOURCE_REDIS_URL": "redis://source:6379/0",
        "TRPC_MIGRATION_TARGET_DATABASE_DSN": "postgresql://trpc_runtime@target:5432/db",
        "TRPC_MIGRATION_TENANT_ID": "migration-acceptance-timeout",
        "TRPC_MIGRATION_ID": "migration-acceptance-timeout",
        "TRPC_MIGRATION_EXPECTED_RECORDS": "2",
        "TRPC_MIGRATION_CONTROL_FACTORY": "tests.fake:factory",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    monkeypatch.setenv("TRPC_RELEASE_ID", "release-timeout-test")
    monkeypatch.setenv("TRPC_RELEASE_NONCE", "n" * 32)

    class Redis:
        closed = False

        async def aclose(self) -> None:
            self.closed = True

    class Pool:
        closed = False
        terminated = False

        async def close(self) -> None:
            self.closed = True

        def terminate(self) -> None:
            self.terminated = True

    redis = Redis()
    pool = Pool()

    class Source:
        async def snapshot(self, tenant_id: str) -> MigrationSourceSnapshot:
            del tenant_id
            return MigrationSourceSnapshot(
                source_snapshot_id="snapshot-timeout",
                source_count=2,
                source_checksum="a" * 64,
            )

    async def fake_create_pool(*args: Any, **kwargs: Any) -> Pool:
        del args, kwargs
        return pool

    async def fake_acceptance(*args: Any, **kwargs: Any) -> dict[str, Any]:
        del args, kwargs
        await asyncio.sleep(10)
        return {}

    monkeypatch.setattr(full_acceptance.redis_async, "from_url", lambda *args, **kwargs: redis)
    monkeypatch.setattr(full_acceptance.asyncpg, "create_pool", fake_create_pool)
    monkeypatch.setattr(full_acceptance, "RedisMigrationSource", lambda *args, **kwargs: Source())
    monkeypatch.setattr(full_acceptance, "execute_full_acceptance", fake_acceptance)
    monkeypatch.setattr(full_acceptance, "_load_control_factory", lambda _spec: object())

    report = await _run(
        Namespace(
            output=tmp_path / "migration-timeout.json",
            batch_size=1,
            db_pool_size=2,
            timeout_seconds=0.01,
            db_command_timeout_seconds=1.0,
        )
    )
    assert report["gate"] == "fail"
    assert "timed out after 0.01 seconds" in report["rejection_reasons"][0]
    assert report["case_deltas"] == {
        "last_phase": "acceptance",
        "last_operation": "execute_full_acceptance",
        "last_completed_phase": "not_started",
        "last_completed_operation": "manifest_source_snapshot",
    }
    assert redis.closed is True
    assert pool.terminated is True
    assert pool.closed is False


def test_live_acceptance_report_replaces_atomically_and_rejects_nan(tmp_path: Path) -> None:
    output = tmp_path / "acceptance.json"
    report = full_acceptance._write_report(
        output,
        {
            "baseline": "source",
            "candidate": "target",
            "case_deltas": {},
            "gate": "not_run",
            "rejection_reasons": ["offline"],
            "production_gate": "not_run",
            "production_rejection_reasons": ["offline"],
        },
    )
    assert json.loads(output.read_text(encoding="utf-8"))["gate"] == "not_run"
    assert report["run_id"] == report["evidence"]["run_id"]
