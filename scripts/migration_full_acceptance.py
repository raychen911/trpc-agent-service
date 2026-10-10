"""Run the complete, test-tenant Redis-to-PostgreSQL migration acceptance.

This command is deliberately more restrictive than ``migrate_data.py``.  It
is an operator-facing acceptance harness for an isolated migration tenant, not
the normal production cutover command.  It connects to source/target services
only when both ``TRPC_RUN_REAL_MIGRATION=1`` and
``TRPC_MIGRATION_FULL_ACCEPTANCE=1`` are set, the tenant is explicitly named
with the ``migration-acceptance-`` prefix, and a deployment-owned migration
control factory is supplied.

The default path writes a machine-readable ``not_run`` report and never opens
a Redis or PostgreSQL connection.  The live path runs every state-machine
phase, injects one resumable batch interruption, records checkpoint evidence,
and executes the control hook's cutover, cleanup, and rollback operations.  A
live run also binds both branches to one immutable source snapshot and refuses
to reuse a target tenant that already contains guarded rows.
Because this is a test-tenant acceptance (and rollback is intentionally part
of the exercise), its ``production_gate`` is always ``not_run``.  A separate
release decision must promote evidence only after the independent production
source/target and operational controls have been reviewed.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import inspect
import json
import logging
import math
import os
import re
import sys
import tempfile
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from types import ModuleType
from typing import Any, Protocol, cast
from urllib.parse import unquote, urlsplit

import asyncpg
import redis.asyncio as redis_async

# Keep the documented ``python scripts/migration_full_acceptance.py`` form
# working as well as ``python -m scripts.migration_full_acceptance``.
if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.evidence_lineage import (
    build_evidence,
    current_release_binding,
    new_run_id,
    validate_release_binding,
)
from trpc_service.storage.migration import (
    MAX_MIGRATION_BATCH_SIZE,
    MAX_MIGRATION_DB_POOL_SIZE,
    MAX_MIGRATION_EXPECTED_RECORDS,
    MigrationCheckpoint,
    MigrationCheckpointStore,
    MigrationControl,
    MigrationCoordinator,
    MigrationLease,
    MigrationPhase,
    MigrationRecord,
    MigrationResult,
    MigrationScopeManifest,
    MigrationSource,
    MigrationSourceKind,
    MigrationSourceSnapshot,
    MigrationTarget,
    PostgresMigrationCheckpointStore,
    PostgresMigrationGuard,
    PostgresMigrationTarget,
    RedisMigrationSource,
    canonical_migration_kinds,
)

FULL_PHASES: tuple[MigrationPhase, ...] = (
    MigrationPhase.PREPARE,
    MigrationPhase.BACKFILL,
    MigrationPhase.SHADOW_READ,
    MigrationPhase.DUAL_WRITE,
    MigrationPhase.CUTOVER,
    MigrationPhase.VERIFY,
    MigrationPhase.CLEANUP,
)

REQUIRED_ENV = (
    "TRPC_MIGRATION_SOURCE_REDIS_URL",
    "TRPC_MIGRATION_TARGET_DATABASE_DSN",
    "TRPC_MIGRATION_TENANT_ID",
    "TRPC_MIGRATION_ID",
    "TRPC_MIGRATION_EXPECTED_RECORDS",
    "TRPC_MIGRATION_CONTROL_FACTORY",
    "TRPC_MIGRATION_APP_ID",
    "TRPC_MIGRATION_APP_REVISION",
    "TRPC_MIGRATION_CONFIG_VERSION",
    "TRPC_MIGRATION_BINDING_ID",
    "TRPC_MIGRATION_BINDING_REVISION",
)
ROOT = Path(__file__).resolve().parents[1]
PRODUCER = "scripts.migration_full_acceptance"
_LOGGER = logging.getLogger(__name__)

# Live acceptance talks through operator-managed Redis/PostgreSQL endpoints.
# Neither client is allowed to wait forever when a port-forward or lock stops
# making progress.  The overall deadline is deliberately generous enough for
# the 200-record fixture while still making an interrupted run fail closed.
DEFAULT_LIVE_TIMEOUT_SECONDS = 300.0
MAX_LIVE_TIMEOUT_SECONDS = 3600.0
DEFAULT_DB_COMMAND_TIMEOUT_SECONDS = 30.0
MAX_DB_COMMAND_TIMEOUT_SECONDS = 300.0
DEFAULT_CLOSE_TIMEOUT_SECONDS = 5.0
_DB_LOCK_TIMEOUT_SECONDS = 5.0


@dataclass(slots=True)
class _LiveProgress:
    """In-memory, secret-free progress for diagnosing a bounded live run."""

    last_phase: str = "not_started"
    last_operation: str = "not_started"
    last_completed_phase: str = "not_started"
    last_completed_operation: str = "not_started"

    def started(self, phase: str, operation: str) -> None:
        self.last_phase = phase
        self.last_operation = operation

    def completed_operation(self, phase: str, operation: str) -> None:
        del phase
        self.last_completed_operation = operation

    def completed_phase(self, phase: str, operation: str = "phase_complete") -> None:
        self.last_completed_phase = phase
        self.last_completed_operation = operation

    def case_deltas(self) -> dict[str, str]:
        """Return only fixed labels suitable for a sanitized failure report."""

        return {
            "last_phase": self.last_phase,
            "last_operation": self.last_operation,
            "last_completed_phase": self.last_completed_phase,
            "last_completed_operation": self.last_completed_operation,
        }


class ObservableMigrationControl(MigrationControl, Protocol):
    """Control hook contract required by the full acceptance harness.

    The state is deliberately small and content-free.  A hook that only
    returns successfully but cannot expose these state transitions is not
    sufficient evidence for cutover, cleanup, or rollback.  The
    ``mailbox_v2`` field is also mandatory: this harness does not claim that
    the session mailbox tables or their scheduling state followed the
    migration unless the deployment-owned control plane reports it.
    """

    async def read_state(self, tenant_id: str, migration_id: str) -> Mapping[str, Any]: ...

    async def set_dual_write_fenced(
        self, tenant_id: str, enabled: bool, *, lease: MigrationLease
    ) -> None: ...

    async def cutover_fenced(self, tenant_id: str, *, lease: MigrationLease) -> None: ...

    async def cleanup_fenced(self, tenant_id: str, *, lease: MigrationLease) -> None: ...

    async def rollback_fenced(self, tenant_id: str, *, lease: MigrationLease) -> None: ...


ControlFactory = Callable[..., ObservableMigrationControl | Awaitable[ObservableMigrationControl]]
BranchScope = tuple[
    MigrationTarget,
    ObservableMigrationControl,
    MigrationScopeManifest,
    MigrationLease,
]
BranchScopeFactory = Callable[[str], Awaitable[BranchScope]]
BranchScopeRelease = Callable[[MigrationLease], Awaitable[None]]


class _FailOnceAfterBatchTarget:
    """Inject one post-checkpoint failure without changing target semantics."""

    def __init__(self, inner: MigrationTarget, *, fail_after: int) -> None:
        self._inner = inner
        self._fail_after = fail_after
        self._writes = 0
        self._failed = False

    async def prepare(self, tenant_id: str) -> None:
        await self._inner.prepare(tenant_id)

    def bind_migration_lease(self, lease: MigrationLease) -> None:
        """Keep the resumable failure wrapper transparent to lease fencing."""

        bind_lease = getattr(self._inner, "bind_migration_lease", None)
        if callable(bind_lease):
            bind_lease(lease)

    async def upsert(self, tenant_id: str, record: MigrationRecord) -> None:
        if not self._failed and self._writes >= self._fail_after:
            self._failed = True
            raise ConnectionError("migration acceptance resume probe interruption")
        await self._inner.upsert(tenant_id, record)
        self._writes += 1

    async def read(self, tenant_id: str, kind: str, resource_id: str) -> MigrationRecord | None:
        return await self._inner.read(tenant_id, kind, resource_id)

    async def list_records(
        self, tenant_id: str, kind: str, *, limit: int = 10000
    ) -> tuple[MigrationRecord, ...]:
        list_records = getattr(self._inner, "list_records", None)
        if list_records is None:
            raise ValueError("migration target does not support target enumeration")
        return cast(
            tuple[MigrationRecord, ...],
            await list_records(tenant_id, kind, limit=limit),
        )

    async def list_records_page(
        self,
        tenant_id: str,
        kind: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> tuple[tuple[MigrationRecord, ...], str | None]:
        list_page = getattr(self._inner, "list_records_page", None)
        if callable(list_page):
            return cast(
                tuple[tuple[MigrationRecord, ...], str | None],
                await list_page(tenant_id, kind, cursor=cursor, limit=limit),
            )
        raise ValueError("migration target does not support target pagination")

    async def set_dual_write(self, tenant_id: str, enabled: bool) -> None:
        await self._inner.set_dual_write(tenant_id, enabled)

    async def cutover(self, tenant_id: str) -> None:
        await self._inner.cutover(tenant_id)

    async def cleanup(self, tenant_id: str) -> None:
        await self._inner.cleanup(tenant_id)

    async def rollback(self, tenant_id: str) -> None:
        await self._inner.rollback(tenant_id)


class _PhaseAwareTarget:
    """Defer expensive target enumeration until shadow-read verification.

    ``MigrationCoordinator`` asks a target for a complete snapshot after every
    phase.  The PostgreSQL adapter implements that snapshot as a paginated
    query followed by one read transaction per row.  Through a port-forward,
    repeating that independent enumeration for control-only phases can spend
    the whole acceptance deadline in serial round trips.  Independent
    enumeration remains enabled for ``SHADOW_READ`` and ``VERIFY`` (and
    ``_compare_all`` still verifies every source record when it is disabled),
    so this wrapper only removes redundant intermediate snapshots from
    control-only phases.
    """

    def __init__(self, inner: MigrationTarget) -> None:
        self._inner = inner
        self._enumeration_enabled = False

    def set_enumeration_enabled(self, enabled: bool) -> None:
        self._enumeration_enabled = enabled

    def bind_migration_lease(self, lease: MigrationLease) -> None:
        bind_lease = getattr(self._inner, "bind_migration_lease", None)
        if callable(bind_lease):
            bind_lease(lease)

    async def prepare(self, tenant_id: str) -> None:
        await self._inner.prepare(tenant_id)

    async def upsert(self, tenant_id: str, record: MigrationRecord) -> None:
        await self._inner.upsert(tenant_id, record)

    async def read(self, tenant_id: str, kind: str, resource_id: str) -> MigrationRecord | None:
        return await self._inner.read(tenant_id, kind, resource_id)

    async def list_records(
        self, tenant_id: str, kind: str, *, limit: int = 10000
    ) -> tuple[MigrationRecord, ...]:
        self._require_enumeration()
        list_records = getattr(self._inner, "list_records", None)
        if not callable(list_records):
            raise ValueError("migration target does not support target enumeration")
        return cast(
            tuple[MigrationRecord, ...],
            await list_records(tenant_id, kind, limit=limit),
        )

    async def list_records_page(
        self,
        tenant_id: str,
        kind: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> tuple[tuple[MigrationRecord, ...], str | None]:
        self._require_enumeration()
        list_page = getattr(self._inner, "list_records_page", None)
        if not callable(list_page):
            raise ValueError("migration target does not support target pagination")
        return cast(
            tuple[tuple[MigrationRecord, ...], str | None],
            await list_page(tenant_id, kind, cursor=cursor, limit=limit),
        )

    async def set_dual_write(self, tenant_id: str, enabled: bool) -> None:
        await self._inner.set_dual_write(tenant_id, enabled)

    async def cutover(self, tenant_id: str) -> None:
        await self._inner.cutover(tenant_id)

    async def cleanup(self, tenant_id: str) -> None:
        await self._inner.cleanup(tenant_id)

    async def rollback(self, tenant_id: str) -> None:
        await self._inner.rollback(tenant_id)

    def _require_enumeration(self) -> None:
        if not self._enumeration_enabled:
            raise ValueError("target enumeration is deferred until SHADOW_READ")


def _phase_evidence(
    result: MigrationResult, checkpoint: MigrationCheckpoint | None
) -> dict[str, Any]:
    if checkpoint is None:
        raise AssertionError(f"missing checkpoint for {result.case_deltas['phase']}")
    if checkpoint.phase.value != result.case_deltas["phase"]:
        raise AssertionError("checkpoint phase does not match migration result")
    if not checkpoint.completed:
        raise AssertionError("migration phase checkpoint is not completed")
    if len(checkpoint.checksum) != 64:
        raise AssertionError("migration checkpoint checksum is not SHA-256")
    return {
        "gate": result.gate,
        "checkpoint": checkpoint.model_dump(mode="json"),
        "source_count": result.case_deltas["source_count"],
        "target_count": result.case_deltas["target_count"],
        "checksum": result.case_deltas["checksum"],
        "target_checksum": result.case_deltas.get(
            "target_checksum", result.case_deltas["checksum"]
        ),
        "differences": list(result.case_deltas["differences"]),
    }


async def execute_full_acceptance(
    source: MigrationSource,
    rollback_target: MigrationTarget | None,
    checkpoints: MigrationCheckpointStore,
    *,
    tenant_id: str,
    migration_id: str,
    rollback_control: ObservableMigrationControl | None,
    cleanup_target: MigrationTarget | None = None,
    cleanup_control: ObservableMigrationControl | None = None,
    batch_size: int,
    expected_records: int,
    resume_probe: bool = True,
    require_source_snapshot: bool = False,
    guard: PostgresMigrationGuard | None = None,
    lease: MigrationLease | None = None,
    manifest: MigrationScopeManifest | None = None,
    branch_scope_factory: BranchScopeFactory | None = None,
    release_branch: BranchScopeRelease | None = None,
    progress: _LiveProgress | None = None,
) -> dict[str, Any]:
    """Execute and validate all phases against injected source/target services.

    This function is dependency-injected so unit tests can remain fully
    offline.  The CLI supplies ``RedisMigrationSource`` and
    ``PostgresMigrationTarget`` only after all live safety gates pass.
    """

    if (
        isinstance(expected_records, bool)
        or not isinstance(expected_records, int)
        or expected_records < 1
        or expected_records > MAX_MIGRATION_EXPECTED_RECORDS
    ):
        raise ValueError(
            f"expected migration records must be between 1 and {MAX_MIGRATION_EXPECTED_RECORDS}"
        )
    if (
        isinstance(batch_size, bool)
        or not isinstance(batch_size, int)
        or batch_size < 1
        or batch_size > MAX_MIGRATION_BATCH_SIZE
    ):
        raise ValueError("migration batch size must be positive")
    if resume_probe and expected_records <= batch_size:
        raise ValueError("resume probe requires expected records greater than batch size")
    if branch_scope_factory is not None:
        if guard is None or lease is not None or manifest is None:
            raise ValueError("branch scopes require a guard and base manifest without a lease")
        if release_branch is None:
            raise ValueError("branch scopes require a lease release callback")
    elif (guard is None) != (lease is None):
        raise ValueError("migration guard and lease must be supplied together")

    source_snapshot: MigrationSourceSnapshot | None = None
    snapshot_method = getattr(source, "snapshot", None)
    if snapshot_method is not None and not callable(snapshot_method):
        raise AssertionError("migration source snapshot must be callable")
    if callable(snapshot_method):
        if progress is not None:
            progress.started("acceptance", "initial_source_snapshot")
        source_snapshot = await snapshot_method(tenant_id)
        if progress is not None:
            progress.completed_operation("acceptance", "initial_source_snapshot")
        if source_snapshot.source_count != expected_records:
            raise AssertionError("source snapshot count differs from expected fixture count")
    elif require_source_snapshot:
        raise AssertionError("live migration source must provide an immutable snapshot")

    async def run_branch(
        suffix: str,
        *,
        target: MigrationTarget,
        control: ObservableMigrationControl,
        terminal: MigrationPhase,
        resume: bool,
        branch_manifest: MigrationScopeManifest | None,
        branch_lease: MigrationLease | None,
    ) -> dict[str, Any]:
        result = await _run_branch(
            source,
            target,
            checkpoints,
            control=control,
            tenant_id=tenant_id,
            migration_id=f"{migration_id}-{suffix}",
            batch_size=batch_size,
            expected_records=expected_records,
            terminal=terminal,
            resume_probe=resume,
            expected_source_snapshot=source_snapshot,
            guard=guard,
            lease=branch_lease,
            manifest=branch_manifest,
            progress=progress,
        )
        if branch_scope_factory is not None:
            if release_branch is None or branch_lease is None:
                raise AssertionError("live branch did not return a releasable lease")
            if progress is not None:
                progress.started("branch_release", "release_branch")
            await release_branch(branch_lease)
            if progress is not None:
                progress.completed_operation("branch_release", "release_branch")
        return result

    if branch_scope_factory is not None:
        (
            rollback_target,
            rollback_control,
            rollback_manifest,
            rollback_lease,
        ) = await branch_scope_factory("rollback")
        rollback_branch = await run_branch(
            "rollback",
            target=rollback_target,
            control=rollback_control,
            terminal=MigrationPhase.ROLLBACK,
            resume=resume_probe,
            branch_manifest=rollback_manifest,
            branch_lease=rollback_lease,
        )
        (
            cleanup_target,
            cleanup_control,
            cleanup_manifest,
            cleanup_lease,
        ) = await branch_scope_factory("cleanup")
        cleanup_branch = await run_branch(
            "cleanup",
            target=cleanup_target,
            control=cleanup_control,
            terminal=MigrationPhase.CLEANUP,
            resume=False,
            branch_manifest=cleanup_manifest,
            branch_lease=cleanup_lease,
        )
    else:
        if rollback_target is None or rollback_control is None:
            raise ValueError("offline acceptance requires a rollback target and control")
        cleanup_target = cleanup_target or rollback_target
        cleanup_control = cleanup_control or rollback_control
        rollback_branch = await run_branch(
            "rollback",
            target=rollback_target,
            control=rollback_control,
            terminal=MigrationPhase.ROLLBACK,
            resume=resume_probe,
            branch_manifest=manifest,
            branch_lease=lease,
        )
        cleanup_branch = await run_branch(
            "cleanup",
            target=cleanup_target,
            control=cleanup_control,
            terminal=MigrationPhase.CLEANUP,
            resume=False,
            branch_manifest=manifest,
            branch_lease=lease,
        )

    verify = rollback_branch["phase_evidence"][MigrationPhase.VERIFY.value]
    if verify["source_count"] != expected_records:
        raise AssertionError("live source count differs from expected fixture count")
    if verify["source_count"] != verify["target_count"]:
        raise AssertionError("live migration matched count differs from source count")
    if verify["differences"]:
        raise AssertionError("live migration verification returned differences")
    if verify["checksum"] == "0" * 64:
        raise AssertionError("live migration verification checksum is empty")

    if source_snapshot is not None:
        if verify["checksum"] != source_snapshot.source_checksum:
            raise AssertionError("migration checksum differs from the immutable source snapshot")
        if not callable(snapshot_method):
            raise AssertionError("migration source snapshot disappeared during acceptance")
        if progress is not None:
            progress.started("acceptance", "final_source_snapshot")
        final_snapshot = await snapshot_method(tenant_id)
        if progress is not None:
            progress.completed_operation("acceptance", "final_source_snapshot")
        if final_snapshot != source_snapshot:
            raise AssertionError("migration source changed during full acceptance")

    return {
        "baseline": {
            "source": "independent Redis migration source",
            "tenant_id": tenant_id,
            "migration_id": migration_id,
            "expected_records": expected_records,
            "batch_size": batch_size,
            "source_snapshot": source_snapshot.model_dump(mode="json")
            if source_snapshot is not None
            else "not_run",
        },
        "candidate": {
            "target": "independent PostgreSQL migration target",
            "phases": [phase.value for phase in (*FULL_PHASES, MigrationPhase.ROLLBACK)],
            "control_hook": "deployment-owned MigrationControl",
            "mailbox_v2_control": {
                "required": True,
                "state_field": "mailbox_v2",
                "states": ["ready", "dual-write", "target", "source"],
            },
        },
        "case_deltas": {
            "checkpoint_persistence": "pass",
            "checkpoint_resume": rollback_branch["checkpoint_resume"],
            "branch_migration_ids": {
                "rollback_before_cleanup": f"{migration_id}-rollback",
                "final_cleanup": f"{migration_id}-cleanup",
            },
            "cutover": "pass",
            "cleanup": "pass",
            "rollback": "pass",
            "mailbox_v2": "pass",
            "source_count": verify["source_count"],
            "target_count": verify["target_count"],
            "checksum": verify["checksum"],
            "differences": verify["differences"],
            "target_extra_records": (
                "verified" if hasattr(rollback_target, "list_records") else "not_verified"
            ),
            "phase_evidence": {
                "rollback_branch": {
                    "phases": rollback_branch["phase_evidence"],
                    "control_state": rollback_branch["control_state"],
                },
                "cleanup_branch": {
                    "phases": cleanup_branch["phase_evidence"],
                    "control_state": cleanup_branch["control_state"],
                },
            },
        },
        "gate": "pass",
        "rejection_reasons": [],
        "production_gate": "not_run",
        "production_rejection_reasons": [
            "full acceptance is scoped to a dedicated test tenant and includes rollback",
            "production promotion requires separately reviewed independent source/target evidence",
            "production promotion requires independent evidence for Mailbox v2 "
            "table/state transitions",
        ],
        "caveats": []
        if hasattr(rollback_target, "list_records")
        else [
            (
                "target_extra_records=not_verified: the injected test target has no "
                "enumeration contract"
            )
        ],
    }


async def _run_branch(
    source: MigrationSource,
    target: MigrationTarget,
    checkpoints: MigrationCheckpointStore,
    *,
    control: ObservableMigrationControl,
    tenant_id: str,
    migration_id: str,
    batch_size: int,
    expected_records: int,
    terminal: MigrationPhase,
    resume_probe: bool,
    expected_source_snapshot: MigrationSourceSnapshot | None,
    guard: PostgresMigrationGuard | None,
    lease: MigrationLease | None,
    manifest: MigrationScopeManifest | None,
    progress: _LiveProgress | None = None,
) -> dict[str, Any]:
    tracker = progress or _LiveProgress()
    migration_target: MigrationTarget = target
    if resume_probe:
        migration_target = _FailOnceAfterBatchTarget(target, fail_after=batch_size)
    coordinator = MigrationCoordinator(
        source,
        migration_target,
        checkpoints,
        batch_size=batch_size,
        guard=guard,
        lease=lease,
        manifest=manifest,
        lease_for=timedelta(minutes=5) if guard is not None else timedelta(minutes=1),
    )
    evidence: dict[str, dict[str, Any]] = {}
    control_evidence: dict[str, dict[str, Any]] = {}
    checkpoint_resume = "not_run"

    async def assert_source_snapshot(phase: MigrationPhase) -> None:
        # A manifest-bound coordinator already validates this exact immutable
        # snapshot before and after every phase.  Repeating the same full
        # Redis discovery/read here doubles the port-forward round trips; keep
        # the wrapper check for unguarded/offline adapters only.
        if expected_source_snapshot is None or manifest is not None:
            return
        tracker.started(phase.value, "source_snapshot")
        snapshot_method = getattr(source, "snapshot", None)
        if not callable(snapshot_method):
            raise AssertionError("migration source snapshot disappeared during acceptance")
        observed = await snapshot_method(tenant_id)
        tracker.completed_operation(phase.value, "source_snapshot")
        if observed != expected_source_snapshot:
            raise AssertionError("migration source changed during full acceptance")

    async def read_control_state(phase: MigrationPhase) -> Mapping[str, Any]:
        tracker.started(phase.value, "control_read_state")
        state = await control.read_state(tenant_id, migration_id)
        tracker.completed_operation(phase.value, "control_read_state")
        return state

    async def run_phase(phase: MigrationPhase) -> MigrationResult:
        set_enumeration_enabled = getattr(target, "set_enumeration_enabled", None)
        target_enumeration_enabled = phase in {
            MigrationPhase.SHADOW_READ,
            MigrationPhase.VERIFY,
        }
        if callable(set_enumeration_enabled):
            set_enumeration_enabled(target_enumeration_enabled)
        await assert_source_snapshot(phase)
        tracker.started(phase.value, "coordinator_run")
        result = await coordinator.run(tenant_id, migration_id, phase)
        tracker.completed_operation(phase.value, "coordinator_run")
        await assert_source_snapshot(phase)
        tracker.started(phase.value, "checkpoint_load")
        checkpoint = await checkpoints.load(tenant_id, migration_id)
        tracker.completed_operation(phase.value, "checkpoint_load")
        phase_evidence = _phase_evidence(result, checkpoint)
        if callable(set_enumeration_enabled):
            phase_evidence["target_enumeration"] = (
                "independent" if target_enumeration_enabled else "deferred_until_shadow_read"
            )
        evidence[phase.value] = phase_evidence
        _require_phase_pass(result)
        tracker.completed_phase(phase.value)
        return result

    await run_phase(MigrationPhase.PREPARE)
    control_evidence["after_prepare"] = _checked_control_state(
        await read_control_state(MigrationPhase.PREPARE),
        dual_write=False,
        active_profile="source",
        cleaned=False,
        rolled_back=False,
        mailbox_v2="ready",
    )
    if resume_probe:
        await assert_source_snapshot(MigrationPhase.BACKFILL)
        tracker.started(MigrationPhase.BACKFILL.value, "resume_probe")
        try:
            await coordinator.run(tenant_id, migration_id, MigrationPhase.BACKFILL)
        except ConnectionError as error:
            if str(error) != "migration acceptance resume probe interruption":
                raise
        tracker.completed_operation(MigrationPhase.BACKFILL.value, "resume_probe")
        tracker.started(MigrationPhase.BACKFILL.value, "resume_checkpoint_load")
        interrupted = await checkpoints.load(tenant_id, migration_id)
        tracker.completed_operation(MigrationPhase.BACKFILL.value, "resume_checkpoint_load")
        if interrupted is None or interrupted.completed:
            raise AssertionError("resume probe did not preserve an incomplete checkpoint")
        if interrupted.cursor is None:
            raise AssertionError("resume probe checkpoint lost its source cursor")
        if interrupted.source_count != interrupted.target_count:
            raise AssertionError("resume probe checkpoint count mismatch")
        if len(interrupted.checksum) != 64:
            raise AssertionError("resume probe checkpoint checksum is invalid")
        await assert_source_snapshot(MigrationPhase.BACKFILL)
        checkpoint_resume = "pass"
    await run_phase(MigrationPhase.BACKFILL)
    control_evidence["after_backfill"] = _checked_control_state(
        await read_control_state(MigrationPhase.BACKFILL),
        dual_write=False,
        active_profile="source",
        cleaned=False,
        rolled_back=False,
        mailbox_v2="ready",
    )
    await run_phase(MigrationPhase.SHADOW_READ)
    control_evidence["after_shadow_read"] = _checked_control_state(
        await read_control_state(MigrationPhase.SHADOW_READ),
        dual_write=False,
        active_profile="source",
        cleaned=False,
        rolled_back=False,
        mailbox_v2="ready",
    )
    await run_phase(MigrationPhase.DUAL_WRITE)
    control_evidence["after_dual_write"] = _checked_control_state(
        await read_control_state(MigrationPhase.DUAL_WRITE),
        dual_write=True,
        active_profile="source",
        cleaned=False,
        rolled_back=False,
        mailbox_v2="dual-write",
    )
    await run_phase(MigrationPhase.CUTOVER)
    control_evidence["after_cutover"] = _checked_control_state(
        await read_control_state(MigrationPhase.CUTOVER),
        dual_write=True,
        active_profile="target",
        cleaned=False,
        rolled_back=False,
        mailbox_v2="target",
    )
    await run_phase(MigrationPhase.VERIFY)
    verify = evidence[MigrationPhase.VERIFY.value]
    if verify["source_count"] != expected_records:
        raise AssertionError("live source count differs from expected fixture count")
    if verify["source_count"] != verify["target_count"] or verify["differences"]:
        raise AssertionError("live verification count or differences failed")
    control_evidence["after_verify"] = _checked_control_state(
        await read_control_state(MigrationPhase.VERIFY),
        dual_write=True,
        active_profile="target",
        cleaned=False,
        rolled_back=False,
        mailbox_v2="target",
    )
    await run_phase(terminal)
    if terminal == MigrationPhase.ROLLBACK:
        control_evidence["after_rollback"] = _checked_control_state(
            await read_control_state(MigrationPhase.ROLLBACK),
            dual_write=False,
            active_profile="source",
            cleaned=False,
            rolled_back=True,
            mailbox_v2="source",
        )
    else:
        control_evidence["after_cleanup"] = _checked_control_state(
            await read_control_state(MigrationPhase.CLEANUP),
            dual_write=False,
            active_profile="target",
            cleaned=True,
            rolled_back=False,
            mailbox_v2="target",
        )
    return {
        "checkpoint_resume": checkpoint_resume,
        "phase_evidence": evidence,
        "control_state": control_evidence,
    }


def _require_control_state(
    state: Mapping[str, Any],
    *,
    dual_write: bool,
    active_profile: str,
    cleaned: bool,
    rolled_back: bool,
    mailbox_v2: str,
) -> None:
    required = {"dual_write", "active_profile", "cleaned", "rolled_back", "mailbox_v2"}
    if not required.issubset(state):
        if "mailbox_v2" not in state:
            raise AssertionError("migration control does not expose mailbox_v2 state")
        raise AssertionError("migration control state is not observable")
    if any(
        not isinstance(state[key], bool)
        for key in required
        if key not in {"active_profile", "mailbox_v2"}
    ):
        raise AssertionError("migration control state contains invalid boolean fields")
    if state["dual_write"] is not dual_write:
        raise AssertionError("migration control did not expose the expected dual-write state")
    if state["active_profile"] != active_profile:
        raise AssertionError("migration control did not expose the expected active profile")
    if state["cleaned"] is not cleaned:
        raise AssertionError("migration control did not expose the expected cleanup state")
    if state["rolled_back"] is not rolled_back:
        raise AssertionError("migration control did not expose the expected rollback state")
    if state["mailbox_v2"] != mailbox_v2:
        raise AssertionError("migration control did not expose the expected mailbox_v2 state")


def _checked_control_state(
    state: Mapping[str, Any],
    *,
    dual_write: bool,
    active_profile: str,
    cleaned: bool,
    rolled_back: bool,
    mailbox_v2: str,
) -> dict[str, Any]:
    _require_control_state(
        state,
        dual_write=dual_write,
        active_profile=active_profile,
        cleaned=cleaned,
        rolled_back=rolled_back,
        mailbox_v2=mailbox_v2,
    )
    return {
        "dual_write": state["dual_write"],
        "active_profile": state["active_profile"],
        "cleaned": state["cleaned"],
        "rolled_back": state["rolled_back"],
        "mailbox_v2": state["mailbox_v2"],
    }


def _require_phase_pass(result: MigrationResult) -> None:
    if result.gate != "pass":
        raise ValueError(f"migration phase {result.case_deltas['phase']} did not pass")


def _safe_endpoint(value: str) -> tuple[str, str, str, int | None]:
    parsed = urlsplit(value)
    default_port = {
        "redis": 6379,
        "rediss": 6379,
        "postgres": 5432,
        "postgresql": 5432,
        "postgresql+asyncpg": 5432,
    }.get(parsed.scheme.lower())
    return (
        parsed.scheme.lower(),
        (parsed.hostname or "").lower(),
        "",
        parsed.port or default_port,
    )


def _bounded_positive_int(value: Any, *, name: str, maximum: int) -> int:
    """Validate a finite, integral live-acceptance control value."""

    if isinstance(value, bool):
        raise ValueError(f"{name} must be a positive integer")
    if isinstance(value, str):
        if not re.fullmatch(r"[0-9]+", value):
            raise ValueError(f"{name} must be a positive integer")
        number = int(value, 10)
    elif isinstance(value, int):
        number = value
    else:
        raise ValueError(f"{name} must be a positive integer")
    if number < 1 or number > maximum:
        raise ValueError(f"{name} must be between 1 and {maximum}")
    return number


def _bounded_timeout(value: Any, *, name: str, maximum: float) -> float:
    """Validate a finite live-I/O timeout and reject an unbounded setting."""

    try:
        timeout = float(value)
    except (TypeError, ValueError) as error:
        raise ValueError(f"{name} must be a finite positive number") from error
    if not math.isfinite(timeout):
        raise ValueError(f"{name} must be a finite positive number")
    if timeout <= 0 or timeout > maximum:
        raise ValueError(f"{name} must be between 0 and {maximum:g} seconds")
    return timeout


def _postgres_connection_initializer(
    command_timeout_seconds: float,
) -> Callable[[Any], Awaitable[None]]:
    """Configure fail-closed server-side timeouts on every pooled connection."""

    command_ms = str(max(1, round(command_timeout_seconds * 1000)))
    lock_ms = str(max(1, round(min(command_timeout_seconds, _DB_LOCK_TIMEOUT_SECONDS) * 1000)))

    async def initialize(connection: Any) -> None:
        await connection.execute(
            "SELECT set_config('statement_timeout', $1, false), "
            "set_config('lock_timeout', $2, false)",
            f"{command_ms}ms",
            f"{lock_ms}ms",
        )

    return initialize


async def _close_live_resources(
    redis: Any | None,
    pool: asyncpg.Pool | None,
    *,
    timeout_seconds: float = DEFAULT_CLOSE_TIMEOUT_SECONDS,
    terminate_pool: bool = False,
) -> None:
    """Close live clients without allowing cleanup to mask the gate result.

    ``asyncpg.Pool.close`` waits for checked-out connections.  That is useful
    during normal shutdown, but unsafe after cancellation of a hung acceptance
    operation: the connection may still be in an open transaction.  In that
    path terminate the pool so PostgreSQL rolls back the transaction and the
    process can emit a deterministic fail-closed report.
    """

    close_timeout = _bounded_timeout(
        timeout_seconds,
        name="live resource close timeout",
        maximum=MAX_DB_COMMAND_TIMEOUT_SECONDS,
    )
    if redis is not None:
        try:
            await asyncio.wait_for(redis.aclose(), timeout=close_timeout)
        except BaseException as error:
            _LOGGER.warning("live Redis client close did not complete: %s", type(error).__name__)
            connection_pool = getattr(redis, "connection_pool", None)
            disconnect = getattr(connection_pool, "disconnect", None)
            if callable(disconnect):
                try:
                    await asyncio.wait_for(
                        disconnect(inuse_connections=True), timeout=close_timeout
                    )
                except BaseException:
                    _LOGGER.warning("live Redis connection pool termination failed")
    if pool is None:
        return
    if terminate_pool:
        pool.terminate()
        return
    try:
        await asyncio.wait_for(pool.close(), timeout=close_timeout)
    except BaseException as error:
        _LOGGER.warning("live PostgreSQL pool close did not complete: %s", type(error).__name__)
        pool.terminate()


def _endpoint_fingerprint(value: str) -> str:
    """Hash only the non-secret identity of a migration endpoint."""

    scheme, hostname, _path, port = _safe_endpoint(value)
    if not scheme or not hostname or port is None:
        raise ValueError("migration endpoint must include a scheme, host, and port")
    identity = "|".join((scheme, hostname, str(port)))
    return hashlib.sha256(identity.encode("utf-8")).hexdigest()


def _independent_endpoints(source_url: str, target_dsn: str) -> bool:
    source = _safe_endpoint(source_url)
    target = _safe_endpoint(target_dsn)
    if not source[1] or not target[1] or source[3] is None or target[3] is None:
        return False
    return (source[1], source[3]) != (target[1], target[3])


def _has_non_empty_suffix(value: str, prefix: str) -> bool:
    if not value.startswith(prefix):
        return False
    suffix = value.removeprefix(prefix)
    return bool(
        suffix
        and suffix[0].isalnum()
        and suffix[-1].isalnum()
        and all(character.isalnum() or character in "-_." for character in suffix)
    )


def _require_runtime_target_role(target_dsn: str) -> str:
    """Require an explicit non-owner role for live target writes.

    Schema changes and migration guard ownership belong to ``trpc_migration``.
    The acceptance target is opened with the application/runtime role so a
    tenant-scoped run cannot accidentally acquire schema-owner privileges.
    """

    parsed = urlsplit(target_dsn)
    role = unquote(parsed.username or "").strip()
    if not role:
        raise ValueError("target database DSN must include an explicit runtime role")
    if role.casefold() in {"trpc_migration", "trpc_worker", "postgres", "root", "trpc"}:
        raise ValueError(
            "target database DSN must use a runtime role, not a schema/migration owner"
        )
    return role


def _acceptance_identity(
    *, app_id: str, binding_id: str, tenant_id: str, migration_id: str
) -> None:
    values = {
        "TRPC_MIGRATION_TENANT_ID": tenant_id,
        "TRPC_MIGRATION_ID": migration_id,
        "TRPC_MIGRATION_APP_ID": app_id,
        "TRPC_MIGRATION_BINDING_ID": binding_id,
    }
    for name, value in values.items():
        if not _has_non_empty_suffix(value, "migration-acceptance-"):
            raise ValueError(
                f"{name} must use the dedicated migration-acceptance- prefix with a "
                "non-empty suffix"
            )


def _require_observable_control(value: Any) -> ObservableMigrationControl:
    if not callable(getattr(value, "read_state", None)):
        raise TypeError("migration control must expose read_state for observable acceptance")
    return cast(ObservableMigrationControl, value)


def _load_control_factory(spec: str) -> ControlFactory:
    module_name, separator, attribute = spec.partition(":")
    if not separator or not module_name or not attribute:
        raise ValueError("control factory must use module:callable syntax")
    module: ModuleType = importlib.import_module(module_name)
    factory = getattr(module, attribute, None)
    if not callable(factory):
        raise TypeError("migration control factory is not callable")
    return cast(ControlFactory, factory)


async def _restage_acceptance_candidate(
    pool: asyncpg.Pool, *, tenant_id: str, app_id: str, source_version: int
) -> int:
    """Re-stage the fixture candidate between independent acceptance branches.

    The rollback branch intentionally returns the app to its source pointer,
    which clears its candidate revision.  Re-staging that already-created
    revision is a test-fixture transition; it does not change the active
    pointer, create a new revision, or clear any target data.
    """

    async with pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
        row = await connection.fetchrow(
            """
            SELECT active_config_version,candidate_config_version,control_version
              FROM agent_apps
             WHERE tenant_id=$1 AND app_id=$2
             FOR UPDATE
            """,
            tenant_id,
            app_id,
        )
        if row is None or int(row["active_config_version"]) != source_version:
            raise ValueError("acceptance cleanup branch source revision is not active")
        if row["candidate_config_version"] is not None:
            raise ValueError("acceptance cleanup branch candidate revision is already staged")
        target_version = await connection.fetchval(
            """
            SELECT max(version)
              FROM config_revisions
             WHERE tenant_id=$1 AND app_id=$2 AND version>$3
            """,
            tenant_id,
            app_id,
            source_version,
        )
        if target_version is None:
            raise ValueError("acceptance cleanup branch candidate revision is missing")
        updated = await connection.fetchrow(
            """
            UPDATE agent_apps
               SET candidate_config_version=$3,candidate_percent=0,updated_at=clock_timestamp()
             WHERE tenant_id=$1 AND app_id=$2 AND active_config_version=$4
               AND candidate_config_version IS NULL
             RETURNING control_version
            """,
            tenant_id,
            app_id,
            int(target_version),
            source_version,
        )
        if updated is None:
            raise ValueError("acceptance cleanup branch candidate staging lost its compare-and-set")
        return int(updated["control_version"])


def _write_report(output: Path, report: dict[str, Any]) -> dict[str, Any]:
    evidence = build_evidence(
        root=ROOT,
        producer=PRODUCER,
        run_id=new_run_id(PRODUCER),
    )

    if (
        os.getenv("TRPC_RUN_REAL_MIGRATION") == "1"
        and os.getenv("TRPC_MIGRATION_FULL_ACCEPTANCE") == "1"
    ):
        try:
            expected_binding = current_release_binding(required=True)
        except ValueError as error:
            binding_reasons = [str(error)]
        else:
            binding_reasons = validate_release_binding(
                evidence,
                expected=expected_binding,
            )
        if binding_reasons:
            if report.get("gate") == "pass":
                report["gate"] = "not_run"
            report["production_gate"] = "not_run"
            report.setdefault("rejection_reasons", []).extend(binding_reasons)
            report.setdefault("production_rejection_reasons", []).extend(binding_reasons)

    report = {
        "schema_version": 1,
        **report,
        "run_id": evidence["run_id"],
        "evidence": evidence,
    }
    if output.is_symlink() or any(
        parent.exists() and parent.is_symlink()
        for parent in (output.parent, *output.parent.parents)
    ):
        raise ValueError("migration acceptance report output must not use a symlink path")
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.is_symlink() or any(
        parent.exists() and parent.is_symlink()
        for parent in (output.parent, *output.parent.parents)
    ):
        raise ValueError("migration acceptance report output must not use a symlink path")
    rendered = json.dumps(report, ensure_ascii=False, indent=2, allow_nan=False) + "\n"
    temporary: str | None = None
    try:
        descriptor, temporary = tempfile.mkstemp(
            prefix=f".{output.name}.", suffix=".tmp", dir=str(output.parent)
        )
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            stream.write(rendered)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, output)
        temporary = None
    finally:
        if temporary is not None:
            try:
                os.unlink(temporary)
            except FileNotFoundError:
                pass
    return report


def _not_run(output: Path, reasons: list[str]) -> dict[str, Any]:
    return _write_report(
        output,
        {
            "baseline": "redis-source",
            "candidate": "postgresql-authoritative",
            "case_deltas": {},
            "gate": "not_run",
            "rejection_reasons": reasons,
            "production_gate": "not_run",
            "production_rejection_reasons": reasons,
        },
    )


def _live_failure(
    output: Path,
    reason: str,
    *,
    progress: _LiveProgress | None = None,
) -> dict[str, Any]:
    """Build a sanitized fail-closed report for a live acceptance failure."""

    case_deltas = progress.case_deltas() if progress is not None else {}
    return _write_report(
        output,
        {
            "baseline": "redis-source",
            "candidate": "postgresql-authoritative",
            "case_deltas": case_deltas,
            "gate": "fail",
            "rejection_reasons": [reason],
            "production_gate": "not_run",
            "production_rejection_reasons": [
                "live acceptance did not complete; production gate remains not_run"
            ],
        },
    )


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    try:
        timeout_value = getattr(args, "timeout_seconds", None)
        if timeout_value is None:
            timeout_value = os.getenv(
                "TRPC_MIGRATION_FULL_ACCEPTANCE_TIMEOUT_SECONDS",
                str(DEFAULT_LIVE_TIMEOUT_SECONDS),
            )
        db_command_timeout_value = getattr(args, "db_command_timeout_seconds", None)
        if db_command_timeout_value is None:
            db_command_timeout_value = os.getenv(
                "TRPC_MIGRATION_DB_COMMAND_TIMEOUT_SECONDS",
                str(DEFAULT_DB_COMMAND_TIMEOUT_SECONDS),
            )
        batch_size = _bounded_positive_int(
            getattr(args, "batch_size", 100),
            name="--batch-size",
            maximum=MAX_MIGRATION_BATCH_SIZE,
        )
        db_pool_size = _bounded_positive_int(
            getattr(args, "db_pool_size", 4),
            name="--db-pool-size",
            maximum=MAX_MIGRATION_DB_POOL_SIZE,
        )
        live_timeout_seconds = _bounded_timeout(
            timeout_value,
            name="--timeout-seconds",
            maximum=MAX_LIVE_TIMEOUT_SECONDS,
        )
        db_command_timeout_seconds = _bounded_timeout(
            db_command_timeout_value,
            name="--db-command-timeout-seconds",
            maximum=MAX_DB_COMMAND_TIMEOUT_SECONDS,
        )
    except ValueError as error:
        return _not_run(args.output, [str(error)])

    if os.getenv("TRPC_RUN_REAL_MIGRATION") != "1":
        return _not_run(
            args.output,
            ["TRPC_RUN_REAL_MIGRATION=1 was not supplied; live migration is opt-in"],
        )
    if os.getenv("TRPC_MIGRATION_FULL_ACCEPTANCE") != "1":
        return _not_run(
            args.output,
            ["TRPC_MIGRATION_FULL_ACCEPTANCE=1 was not supplied; full acceptance is opt-in"],
        )

    missing = [name for name in REQUIRED_ENV if not os.getenv(name)]
    if missing:
        return _not_run(
            args.output,
            [f"missing required migration acceptance environment: {name}" for name in missing],
        )

    tenant_id = os.environ["TRPC_MIGRATION_TENANT_ID"]
    migration_id = os.environ["TRPC_MIGRATION_ID"]
    try:
        _acceptance_identity(
            app_id=os.environ["TRPC_MIGRATION_APP_ID"],
            binding_id=os.environ["TRPC_MIGRATION_BINDING_ID"],
            tenant_id=tenant_id,
            migration_id=migration_id,
        )
    except ValueError as error:
        return _not_run(args.output, [str(error)])
    source_url = os.environ["TRPC_MIGRATION_SOURCE_REDIS_URL"]
    target_dsn = os.environ["TRPC_MIGRATION_TARGET_DATABASE_DSN"]
    try:
        independent_endpoints = _independent_endpoints(source_url, target_dsn)
    except ValueError:
        return _not_run(
            args.output,
            ["source or target endpoint has an invalid URL or port"],
        )
    if not independent_endpoints:
        return _not_run(
            args.output,
            ["source and target endpoints are identical; independent backends are required"],
        )
    try:
        _require_runtime_target_role(target_dsn)
    except ValueError as error:
        return _not_run(args.output, [str(error)])

    try:
        expected_records = _bounded_positive_int(
            os.environ["TRPC_MIGRATION_EXPECTED_RECORDS"],
            name="TRPC_MIGRATION_EXPECTED_RECORDS",
            maximum=MAX_MIGRATION_EXPECTED_RECORDS,
        )
    except ValueError as error:
        return _not_run(args.output, [str(error)])
    if expected_records <= batch_size:
        return _not_run(
            args.output,
            ["expected records must exceed batch size to prove checkpoint resume"],
        )
    try:
        app_revision = _bounded_positive_int(
            os.environ["TRPC_MIGRATION_APP_REVISION"],
            name="TRPC_MIGRATION_APP_REVISION",
            maximum=MAX_MIGRATION_EXPECTED_RECORDS,
        )
        config_version = _bounded_positive_int(
            os.environ["TRPC_MIGRATION_CONFIG_VERSION"],
            name="TRPC_MIGRATION_CONFIG_VERSION",
            maximum=MAX_MIGRATION_EXPECTED_RECORDS,
        )
        binding_revision = _bounded_positive_int(
            os.environ["TRPC_MIGRATION_BINDING_REVISION"],
            name="TRPC_MIGRATION_BINDING_REVISION",
            maximum=MAX_MIGRATION_EXPECTED_RECORDS,
        )
    except ValueError as error:
        return _not_run(args.output, [str(error)])

    try:
        current_release_binding(required=True)
    except ValueError as error:
        return _not_run(args.output, [str(error)])

    progress = _LiveProgress()
    redis: Any | None = None
    pool: asyncpg.Pool | None = None
    guard: PostgresMigrationGuard | None = None
    lease: Any | None = None
    timed_out = False
    deadline = asyncio.get_running_loop().time() + live_timeout_seconds

    def remaining_timeout() -> float:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError
        return remaining

    try:
        progress.started("bootstrap", "redis_connect")
        redis = redis_async.from_url(
            source_url,
            decode_responses=False,
            socket_connect_timeout=db_command_timeout_seconds,
            socket_timeout=db_command_timeout_seconds,
        )
        progress.completed_operation("bootstrap", "redis_connect")
        progress.started("bootstrap", "postgres_pool_connect")
        pool = await asyncio.wait_for(
            asyncpg.create_pool(
                target_dsn.replace("postgresql+asyncpg://", "postgresql://"),
                min_size=1,
                max_size=db_pool_size,
                command_timeout=db_command_timeout_seconds,
                timeout=db_command_timeout_seconds,
                init=_postgres_connection_initializer(db_command_timeout_seconds),
                server_settings={"application_name": "trpc-migration-full-acceptance"},
            ),
            timeout=remaining_timeout(),
        )
        progress.completed_operation("bootstrap", "postgres_pool_connect")
        source_kinds = canonical_migration_kinds(
            item.strip()
            for item in os.getenv("TRPC_MIGRATION_KINDS", "session,memory").split(",")
            if item.strip()
        )
        source = RedisMigrationSource(
            cast(Any, redis),
            kinds=source_kinds,
        )
        progress.started("bootstrap", "manifest_source_snapshot")
        source_snapshot = await asyncio.wait_for(
            source.snapshot(tenant_id), timeout=remaining_timeout()
        )
        progress.completed_operation("bootstrap", "manifest_source_snapshot")
        if source_snapshot.source_count != expected_records:
            raise ValueError("source snapshot count differs from expected migration records")
        manifest = MigrationScopeManifest(
            tenant_id=tenant_id,
            migration_id=migration_id,
            source_kind=MigrationSourceKind.REDIS,
            kinds=source_kinds,
            source_snapshot_id=source_snapshot.source_snapshot_id,
            source_count=source_snapshot.source_count,
            source_checksum=source_snapshot.source_checksum,
            app_id=os.environ["TRPC_MIGRATION_APP_ID"],
            app_revision=app_revision,
            config_version=config_version,
            binding_id=os.environ["TRPC_MIGRATION_BINDING_ID"],
            binding_revision=binding_revision,
        )
        guard = PostgresMigrationGuard(pool)
        factory = _load_control_factory(os.environ["TRPC_MIGRATION_CONTROL_FACTORY"])
        checkpoints = PostgresMigrationCheckpointStore(pool)

        branch_preflight: Any | None = None
        branch_manifests: dict[str, dict[str, Any]] = {}

        async def branch_scope(suffix: str) -> BranchScope:
            nonlocal branch_preflight, lease
            branch_phase = f"{suffix}-branch"
            if progress is not None:
                progress.started(branch_phase, "branch_scope")
            branch_migration_id = f"{migration_id}-{suffix}"
            branch_manifest = manifest.model_copy(update={"migration_id": branch_migration_id})
            if suffix == "cleanup":
                if progress is not None:
                    progress.started(branch_phase, "restage_candidate")
                branch_app_revision = await _restage_acceptance_candidate(
                    pool,
                    tenant_id=tenant_id,
                    app_id=manifest.app_id,
                    source_version=manifest.config_version,
                )
                if progress is not None:
                    progress.completed_operation(branch_phase, "restage_candidate")
                branch_manifest = branch_manifest.model_copy(
                    update={"app_revision": branch_app_revision}
                )
            branch_manifests[suffix] = branch_manifest.model_dump(mode="json")
            if suffix == "rollback":
                if progress is not None:
                    progress.started(branch_phase, "target_preflight_and_lease")
                branch_lease, branch_preflight = await guard.acquire_with_target_preflight(
                    branch_manifest,
                    f"full-acceptance:{branch_migration_id}",
                    lease_for=timedelta(minutes=5),
                )
            else:
                if progress is not None:
                    progress.started(branch_phase, "lease_acquire")
                branch_lease = await guard.acquire(
                    branch_manifest,
                    f"full-acceptance:{branch_migration_id}",
                    lease_for=timedelta(minutes=5),
                )
            if progress is not None:
                progress.completed_operation(
                    branch_phase,
                    "target_preflight_and_lease" if suffix == "rollback" else "lease_acquire",
                )
            lease = branch_lease
            if progress is not None:
                progress.started(branch_phase, "control_factory")
            control_result = factory(
                pool=pool,
                tenant_id=tenant_id,
                migration_id=branch_migration_id,
            )
            control = _require_observable_control(
                await control_result if inspect.isawaitable(control_result) else control_result
            )
            if progress is not None:
                progress.completed_operation(branch_phase, "control_factory")
            target = _PhaseAwareTarget(
                PostgresMigrationTarget(
                    pool,
                    control=control,
                    manifest=branch_manifest,
                )
            )
            return target, control, branch_manifest, branch_lease

        async def release_branch(branch_lease: MigrationLease) -> None:
            nonlocal lease
            await guard.release(branch_lease)
            lease = None

        progress.started("acceptance", "execute_full_acceptance")
        report = await asyncio.wait_for(
            execute_full_acceptance(
                source,
                PostgresMigrationTarget(pool, manifest=manifest),
                checkpoints,
                tenant_id=tenant_id,
                migration_id=migration_id,
                rollback_control=None,
                batch_size=batch_size,
                expected_records=expected_records,
                require_source_snapshot=True,
                guard=guard,
                manifest=manifest,
                branch_scope_factory=branch_scope,
                release_branch=release_branch,
                progress=progress,
            ),
            timeout=remaining_timeout(),
        )
        progress.completed_operation("acceptance", "execute_full_acceptance")
        report["case_deltas"]["source_endpoint_sha256"] = _endpoint_fingerprint(source_url)
        report["case_deltas"]["target_endpoint_sha256"] = _endpoint_fingerprint(target_dsn)
        if branch_preflight is None:
            raise AssertionError("rollback branch target preflight evidence is missing")
        report["case_deltas"]["target_empty_preflight"] = branch_preflight.model_dump(mode="json")
        report["case_deltas"]["branch_manifests"] = branch_manifests
        return _write_report(args.output, report)
    except TimeoutError:
        timed_out = True
        return _live_failure(
            args.output,
            f"live migration acceptance timed out after {live_timeout_seconds:g} seconds",
            progress=progress,
        )
    except asyncpg.exceptions.DeadlockDetectedError:
        return _live_failure(
            args.output,
            "live migration database deadlock detected",
            progress=progress,
        )
    except asyncpg.exceptions.LockNotAvailableError:
        return _live_failure(
            args.output,
            "live migration database lock timeout exceeded",
            progress=progress,
        )
    except asyncpg.exceptions.QueryCanceledError:
        return _live_failure(
            args.output,
            "live migration database statement timeout exceeded",
            progress=progress,
        )
    except Exception as error:
        return _live_failure(
            args.output,
            f"live migration acceptance raised {type(error).__name__}",
            progress=progress,
        )
    finally:
        if lease is not None and guard is not None:
            _LOGGER.warning("migration acceptance write barrier retained after unsuccessful run")
        await _close_live_resources(redis, pool, terminate_pool=timed_out)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--db-pool-size", type=int, default=4)
    parser.add_argument("--timeout-seconds", type=float, default=None)
    parser.add_argument("--db-command-timeout-seconds", type=float, default=None)
    parser.add_argument(
        "--output", type=Path, default=Path("runs/multitenant/migration-full-acceptance.json")
    )
    args = parser.parse_args()
    report = asyncio.run(_run(args))
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["gate"] == "pass" else 1


if __name__ == "__main__":
    raise SystemExit(main())
