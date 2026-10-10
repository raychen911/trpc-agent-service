"""Run the deterministic, two-tenant migration acceptance contract.

This is deliberately an offline contract.  It exercises the complete
``prepare -> backfill -> shadow-read -> dual-write -> cutover -> verify``
sequence, a resumable batch interruption, an expected verification drift, and
both cleanup and rollback.  It never connects to a production source/target
and never deletes data.

The resulting JSON is evidence for the migration state machine only.  The
``production_gate`` remains ``not_run`` until the operator runs the same
workflow against independently operated source and target services.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

# Keep the documented ``python scripts/migration_acceptance_gate.py`` form
# working as well as ``python -m scripts.migration_acceptance_gate``.  Python
# otherwise puts only ``scripts/`` on sys.path for a file invocation.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from trpc_service.storage.migration import (
    InMemoryMigrationCheckpointStore,
    MigrationCoordinator,
    MigrationPhase,
    MigrationRecord,
    _rolling_checksum,
)

TENANTS = ("migration-tenant-a", "migration-tenant-b")
RECORDS_PER_TENANT = 6
BATCH_SIZE = 2
MIGRATION_ID = "offline-isolation-acceptance"


class _IsolatedSource:
    def __init__(self) -> None:
        self.records: dict[str, tuple[MigrationRecord, ...]] = {
            tenant_id: tuple(
                [
                    MigrationRecord(
                        kind="session",
                        resource_id=f"shared-session-{index}",
                        payload={"tenant": tenant_id, "value": index},
                    )
                    for index in range(RECORDS_PER_TENANT // 2)
                ]
                + [
                    MigrationRecord(
                        kind="memory",
                        resource_id=f"shared-memory-{index}",
                        payload={"tenant": tenant_id, "value": index},
                    )
                    for index in range(RECORDS_PER_TENANT // 2)
                ]
            )
            for tenant_id in TENANTS
        }

    async def fetch(
        self, tenant_id: str, *, cursor: str | None, limit: int
    ) -> tuple[tuple[MigrationRecord, ...], str | None]:
        records = self.records[tenant_id]
        start = int(cursor or 0)
        selected = records[start : start + limit]
        end = start + len(selected)
        return selected, str(end) if end < len(records) else None


class _IsolatedTarget:
    """Target double with explicit tenant-first keys and reversible profiles."""

    def __init__(self) -> None:
        self.records: dict[tuple[str, str, str], MigrationRecord] = {}
        self.actions: dict[str, list[str]] = {}
        self.profiles = {tenant_id: "source" for tenant_id in TENANTS}
        self.dual_write = {tenant_id: False for tenant_id in TENANTS}
        self.cleaned = {tenant_id: False for tenant_id in TENANTS}
        self.failed_once = True

    def _record(self, tenant_id: str, action: str) -> None:
        self.actions.setdefault(tenant_id, []).append(action)

    async def prepare(self, tenant_id: str) -> None:
        self._record(tenant_id, "prepare")

    async def upsert(self, tenant_id: str, record: MigrationRecord) -> None:
        # Interrupt tenant A during its second batch.  The coordinator must
        # leave the previous checkpoint intact, while tenant B remains untouched.
        if (
            tenant_id == TENANTS[0]
            and record.resource_id == "shared-session-2"
            and self.failed_once
        ):
            self.failed_once = False
            raise ConnectionError("offline target interruption")
        self.records[(tenant_id, record.kind, record.resource_id)] = record

    async def read(self, tenant_id: str, kind: str, resource_id: str) -> MigrationRecord | None:
        return self.records.get((tenant_id, kind, resource_id))

    async def list_records_page(
        self,
        tenant_id: str,
        kind: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> tuple[tuple[MigrationRecord, ...], str | None]:
        values = sorted(
            (
                record
                for (record_tenant, record_kind, _resource_id), record in self.records.items()
                if record_tenant == tenant_id and record_kind == kind
            ),
            key=lambda record: record.resource_id,
        )
        start = int(cursor or 0)
        page = tuple(values[start : start + limit])
        end = start + len(page)
        return page, str(end) if end < len(values) else None

    async def set_dual_write(self, tenant_id: str, enabled: bool) -> None:
        self.dual_write[tenant_id] = enabled
        self._record(tenant_id, f"dual:{enabled}")

    async def cutover(self, tenant_id: str) -> None:
        if not self.dual_write[tenant_id]:
            raise AssertionError("cutover requires dual-write")
        self.profiles[tenant_id] = "candidate"
        self._record(tenant_id, "cutover")

    async def cleanup(self, tenant_id: str) -> None:
        if self.profiles[tenant_id] != "candidate":
            raise AssertionError("cleanup requires candidate profile")
        # Cleanup closes the old projection; candidate records stay available
        # for the configured retention window and are never deleted here.
        self.cleaned[tenant_id] = True
        self._record(tenant_id, "cleanup")

    async def rollback(self, tenant_id: str) -> None:
        self.profiles[tenant_id] = "source"
        self._record(tenant_id, "rollback")

    def add_drift(self, tenant_id: str) -> str:
        key = (tenant_id, "session", "shared-session-0")
        original = self.records[key]
        self.records[key] = MigrationRecord(
            kind=original.kind,
            resource_id=original.resource_id,
            payload={"tenant": tenant_id, "value": "intentional-drift"},
        )
        return f"{original.kind}/{original.resource_id}"


def _source_checksum(records: tuple[MigrationRecord, ...]) -> str:
    checksum = "0" * 64
    for record in records:
        checksum = _rolling_checksum(checksum, record.checksum)
    return checksum


async def execute_offline_acceptance() -> dict[str, Any]:
    source = _IsolatedSource()
    target = _IsolatedTarget()
    checkpoints = InMemoryMigrationCheckpointStore()
    coordinator = MigrationCoordinator(source, target, checkpoints, batch_size=BATCH_SIZE)

    for tenant_id in TENANTS:
        await coordinator.run(tenant_id, MIGRATION_ID, MigrationPhase.PREPARE)

    # Tenant A proves checkpoint resume after an interruption.
    tenant_a = TENANTS[0]
    try:
        await coordinator.run(tenant_a, MIGRATION_ID, MigrationPhase.BACKFILL)
    except ConnectionError as error:
        if str(error) != "offline target interruption":
            raise
    interrupted = await checkpoints.load(tenant_a, MIGRATION_ID)
    if interrupted is None:
        raise AssertionError("interrupted migration did not persist a checkpoint")
    expected_partial = source.records[tenant_a][:BATCH_SIZE]
    if (
        interrupted.cursor != str(BATCH_SIZE)
        or interrupted.source_count != BATCH_SIZE
        or interrupted.target_count != BATCH_SIZE
        or interrupted.completed
        or interrupted.checksum != _source_checksum(expected_partial)
    ):
        raise AssertionError("checkpoint count/cursor/checksum did not preserve the last batch")

    results: dict[str, dict[str, Any]] = {
        tenant_a: {"interrupted_checkpoint": interrupted.model_dump(mode="json")}
    }
    for tenant_id in TENANTS:
        backfill = await coordinator.run(tenant_id, MIGRATION_ID, MigrationPhase.BACKFILL)
        shadow = await coordinator.run(tenant_id, MIGRATION_ID, MigrationPhase.SHADOW_READ)
        await coordinator.run(tenant_id, MIGRATION_ID, MigrationPhase.DUAL_WRITE)
        await coordinator.run(tenant_id, MIGRATION_ID, MigrationPhase.CUTOVER)
        results.setdefault(tenant_id, {}).update(
            {
                "backfill": backfill.case_deltas,
                "shadow_read": shadow.case_deltas,
            }
        )

    # Tenant A completes the candidate rollout and cleans up only its old
    # projection.  Tenant B deliberately drifts, rejects verify, and rolls
    # back to the source profile; neither operation may cross tenant keys.
    verify_a = await coordinator.run(tenant_a, MIGRATION_ID, MigrationPhase.VERIFY)
    cleanup_a = await coordinator.run(tenant_a, MIGRATION_ID, MigrationPhase.CLEANUP)
    results[tenant_a].update({"verify": verify_a.case_deltas, "cleanup": cleanup_a.case_deltas})

    tenant_b = TENANTS[1]
    drift_key = target.add_drift(tenant_b)
    try:
        await coordinator.run(tenant_b, MIGRATION_ID, MigrationPhase.VERIFY)
    except ValueError as error:
        if "verification" not in str(error):
            raise
    else:
        raise AssertionError("verify accepted an intentional target drift")
    failed_verify = await checkpoints.load(tenant_b, MIGRATION_ID)
    if (
        failed_verify is None
        or drift_key not in failed_verify.differences
        or failed_verify.completed
    ):
        raise AssertionError("verify did not checkpoint the expected difference")
    rollback_b = await coordinator.run(tenant_b, MIGRATION_ID, MigrationPhase.ROLLBACK)
    results[tenant_b].update(
        {
            "verify_expected_failure": {
                "gate": "fail",
                "differences": list(failed_verify.differences),
            },
            "rollback": rollback_b.case_deltas,
        }
    )

    expected_checksum = {
        tenant_id: _source_checksum(source.records[tenant_id]) for tenant_id in TENANTS
    }
    for tenant_id in TENANTS:
        backfill = results[tenant_id]["backfill"]
        shadow = results[tenant_id]["shadow_read"]
        if (
            backfill["source_count"] != RECORDS_PER_TENANT
            or backfill["target_count"] != RECORDS_PER_TENANT
        ):
            raise AssertionError(f"backfill count mismatch for {tenant_id}")
        if backfill["checksum"] != expected_checksum[tenant_id]:
            raise AssertionError(f"backfill checksum mismatch for {tenant_id}")
        if (
            shadow["source_count"] != RECORDS_PER_TENANT
            or shadow["target_count"] != RECORDS_PER_TENANT
        ):
            raise AssertionError(f"shadow-read count mismatch for {tenant_id}")
        if shadow["checksum"] != expected_checksum[tenant_id] or shadow["differences"]:
            raise AssertionError(f"shadow-read diff/checksum mismatch for {tenant_id}")

    for tenant_id in TENANTS:
        tenant_keys = [key for key in target.records if key[0] == tenant_id]
        other_keys = [key for key in target.records if key[0] != tenant_id]
        if len(tenant_keys) != RECORDS_PER_TENANT or any(key[0] == tenant_id for key in other_keys):
            raise AssertionError(f"tenant key isolation failed for {tenant_id}")

    if target.profiles[tenant_a] != "candidate" or not target.cleaned[tenant_a]:
        raise AssertionError("tenant A cleanup did not preserve the candidate profile")
    if target.profiles[tenant_b] != "source" or target.dual_write[tenant_b]:
        raise AssertionError("tenant B rollback did not restore source profile")

    report = {
        "schema_version": 1,
        "baseline": {
            "source": "isolated deterministic Redis-shaped fixture",
            "tenants": list(TENANTS),
            "records_per_tenant": RECORDS_PER_TENANT,
            "batch_size": BATCH_SIZE,
        },
        "candidate": {
            "target": "isolated deterministic PostgreSQL-shaped fixture",
            "phases": [phase.value for phase in MigrationPhase],
            "selectors": [
                "scripts/migration_acceptance_gate.py",
                "tests/unit/test_migration_acceptance.py",
            ],
        },
        "case_deltas": {
            "tenant_count": len(TENANTS),
            "per_tenant": results,
            "cross_tenant_key_collisions": 0,
            "checkpoint_resume": "pass",
            "expected_verify_drift": "pass",
            "cleanup": "pass",
            "rollback": "pass",
        },
        "gate": "pass",
        "rejection_reasons": [],
        "production_gate": "not_run",
        "production_rejection_reasons": [
            "independently operated source/target migration was not executed",
            "live dual-write, cutover, cleanup and rollback require deployment-owned control hooks",
        ],
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("runs/multitenant/migration-acceptance.json"),
    )
    args = parser.parse_args()
    report = asyncio.run(execute_offline_acceptance())
    if args.output.is_symlink() or any(
        parent.exists() and parent.is_symlink()
        for parent in (args.output.parent, *args.output.parent.parents)
    ):
        raise ValueError("migration acceptance report output must not use a symlink path")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False)
    if args.output.is_symlink() or any(
        parent.exists() and parent.is_symlink()
        for parent in (args.output.parent, *args.output.parent.parents)
    ):
        raise ValueError("migration acceptance report output must not use a symlink path")
    temporary: str | None = None
    try:
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{args.output.name}.", suffix=".tmp", dir=str(args.output.parent)
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(rendered + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, args.output)
        temporary = None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
    print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
