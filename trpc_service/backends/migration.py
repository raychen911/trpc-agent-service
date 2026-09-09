"""Observable shadow migration contracts for rebuildable projections."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Protocol

from trpc_service.backends.contracts import (
    SessionProjection,
    SessionProjectionBackend,
    WriteResult,
    canonical_json_hash,
    validate_nonempty,
    validate_tenant_id,
)


class MigrationStateError(RuntimeError):
    """Migration state or transition was missing, stale or unsafe."""


class MigrationPhase(StrEnum):
    """Explicit operator-controlled migration lifecycle."""

    SHADOWING = "shadowing"
    VERIFYING = "verifying"
    CUTOVER_READY = "cutover_ready"
    CUTOVER = "cutover"
    ROLLED_BACK = "rolled_back"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class MigrationPlan:
    """One tenant/category move between named adapters."""

    tenant_id: str
    category: str
    source_backend: str
    target_backend: str


@dataclass(frozen=True, slots=True)
class MigrationStatus:
    """Durable-shape status suitable for an Admin API or persistent adapter."""

    plan: MigrationPlan
    phase: MigrationPhase
    compared: int = 0
    matches: int = 0
    mismatches: int = 0
    last_watermark: int | None = None
    last_error: str | None = None
    revision: int = 1
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass(frozen=True, slots=True)
class ShadowComparison:
    """One source/target comparison result without payload disclosure."""

    tenant_id: str
    session_id: str
    matched: bool
    source_version: int | None
    target_version: int | None
    source_watermark: int | None
    target_watermark: int | None
    reason: str | None = None


class MigrationStatusBackend(Protocol):
    """Status persistence interface; production should provide a durable adapter."""

    async def start(self, plan: MigrationPlan) -> MigrationStatus: ...

    async def get(self, tenant_id: str, category: str) -> MigrationStatus | None: ...

    async def transition(
        self,
        tenant_id: str,
        category: str,
        *,
        expected_phase: MigrationPhase,
        target_phase: MigrationPhase,
    ) -> MigrationStatus: ...

    async def record_comparison(
        self,
        comparison: ShadowComparison,
        *,
        category: str,
    ) -> MigrationStatus: ...


_ALLOWED_TRANSITIONS: dict[MigrationPhase, frozenset[MigrationPhase]] = {
    MigrationPhase.SHADOWING: frozenset(
        {MigrationPhase.VERIFYING, MigrationPhase.ROLLED_BACK, MigrationPhase.FAILED}
    ),
    MigrationPhase.VERIFYING: frozenset(
        {MigrationPhase.CUTOVER_READY, MigrationPhase.ROLLED_BACK, MigrationPhase.FAILED}
    ),
    MigrationPhase.CUTOVER_READY: frozenset(
        {MigrationPhase.CUTOVER, MigrationPhase.ROLLED_BACK, MigrationPhase.FAILED}
    ),
    MigrationPhase.CUTOVER: frozenset({MigrationPhase.ROLLED_BACK, MigrationPhase.FAILED}),
    MigrationPhase.ROLLED_BACK: frozenset(),
    MigrationPhase.FAILED: frozenset({MigrationPhase.ROLLED_BACK}),
}


class InMemoryMigrationStatusBackend:
    """Process-local status adapter for tests; not a production control plane."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._statuses: dict[tuple[str, str], MigrationStatus] = {}

    async def start(self, plan: MigrationPlan) -> MigrationStatus:
        _validate_plan(plan)
        key = (plan.tenant_id, plan.category)
        async with self._lock:
            current = self._statuses.get(key)
            if current is not None:
                if current.phase in {
                    MigrationPhase.ROLLED_BACK,
                    MigrationPhase.FAILED,
                }:
                    restarted = MigrationStatus(
                        plan=plan,
                        phase=MigrationPhase.SHADOWING,
                        revision=current.revision + 1,
                    )
                    self._statuses[key] = restarted
                    return deepcopy(restarted)
                if current.plan != plan:
                    raise MigrationStateError(
                        "migration category already has a different active plan"
                    )
                return deepcopy(current)
            status = MigrationStatus(plan=plan, phase=MigrationPhase.SHADOWING)
            self._statuses[key] = status
            return deepcopy(status)

    async def get(self, tenant_id: str, category: str) -> MigrationStatus | None:
        validate_tenant_id(tenant_id)
        validate_nonempty(category, "category", max_length=32)
        async with self._lock:
            return deepcopy(self._statuses.get((tenant_id, category)))

    async def transition(
        self,
        tenant_id: str,
        category: str,
        *,
        expected_phase: MigrationPhase,
        target_phase: MigrationPhase,
    ) -> MigrationStatus:
        validate_tenant_id(tenant_id)
        async with self._lock:
            key = (tenant_id, category)
            current = self._statuses.get(key)
            if current is None or current.phase is not expected_phase:
                raise MigrationStateError("migration phase changed or plan does not exist")
            if target_phase not in _ALLOWED_TRANSITIONS[current.phase]:
                raise MigrationStateError(
                    f"illegal migration transition {current.phase} -> {target_phase}"
                )
            if target_phase is MigrationPhase.CUTOVER_READY and (
                current.compared == 0 or current.mismatches > 0
            ):
                raise MigrationStateError(
                    "cutover requires at least one comparison and zero mismatches"
                )
            if target_phase is MigrationPhase.VERIFYING:
                updated = replace(
                    current,
                    phase=target_phase,
                    compared=0,
                    matches=0,
                    mismatches=0,
                    last_watermark=None,
                    last_error=None,
                    revision=current.revision + 1,
                    updated_at=datetime.now(UTC),
                )
            else:
                updated = replace(
                    current,
                    phase=target_phase,
                    revision=current.revision + 1,
                    updated_at=datetime.now(UTC),
                )
            self._statuses[key] = updated
            return deepcopy(updated)

    async def record_comparison(
        self,
        comparison: ShadowComparison,
        *,
        category: str,
    ) -> MigrationStatus:
        validate_tenant_id(comparison.tenant_id)
        key = (comparison.tenant_id, category)
        async with self._lock:
            current = self._statuses.get(key)
            if current is None or current.phase not in {
                MigrationPhase.SHADOWING,
                MigrationPhase.VERIFYING,
            }:
                raise MigrationStateError("migration is not accepting shadow comparisons")
            updated = replace(
                current,
                compared=current.compared + 1,
                matches=current.matches + int(comparison.matched),
                mismatches=current.mismatches + int(not comparison.matched),
                last_watermark=comparison.source_watermark,
                last_error=comparison.reason,
                revision=current.revision + 1,
                updated_at=datetime.now(UTC),
            )
            self._statuses[key] = updated
            return deepcopy(updated)


class SessionShadowMigrator:
    """Write the target from canonical input and compare it with the source."""

    def __init__(
        self,
        plan: MigrationPlan,
        *,
        source: SessionProjectionBackend,
        target: SessionProjectionBackend,
        statuses: MigrationStatusBackend,
    ) -> None:
        _validate_plan(plan)
        if plan.category != "session":
            raise ValueError("SessionShadowMigrator requires category='session'")
        self._plan = plan
        self._source = source
        self._target = target
        self._statuses = statuses

    async def write_shadow(
        self,
        projection: SessionProjection,
        *,
        expected_target_version: int | None,
    ) -> WriteResult:
        """Project canonical state into the target; never writes the source."""

        if projection.tenant_id != self._plan.tenant_id:
            raise MigrationStateError("projection tenant does not match migration plan")
        status = await self._statuses.get(self._plan.tenant_id, self._plan.category)
        if status is None or status.phase not in {
            MigrationPhase.SHADOWING,
            MigrationPhase.VERIFYING,
        }:
            raise MigrationStateError("migration is not accepting shadow writes")
        return await self._target.compare_and_set_session(
            projection,
            expected_version=expected_target_version,
        )

    async def compare(self, session_id: str) -> ShadowComparison:
        """Compare full projection digests and record only counters/metadata."""

        validate_nonempty(session_id, "session_id", max_length=128)
        source, target = await asyncio.gather(
            self._source.get_session(self._plan.tenant_id, session_id),
            self._target.get_session(self._plan.tenant_id, session_id),
        )
        matched, reason = _compare_projections(source, target)
        comparison = ShadowComparison(
            tenant_id=self._plan.tenant_id,
            session_id=session_id,
            matched=matched,
            source_version=source.version if source is not None else None,
            target_version=target.version if target is not None else None,
            source_watermark=source.committed_through if source is not None else None,
            target_watermark=target.committed_through if target is not None else None,
            reason=reason,
        )
        await self._statuses.record_comparison(
            comparison,
            category=self._plan.category,
        )
        return comparison


def _validate_plan(plan: MigrationPlan) -> None:
    validate_tenant_id(plan.tenant_id)
    validate_nonempty(plan.category, "category", max_length=32)
    validate_nonempty(plan.source_backend, "source_backend", max_length=128)
    validate_nonempty(plan.target_backend, "target_backend", max_length=128)
    if plan.source_backend == plan.target_backend:
        raise ValueError("migration source and target backends must differ")


def _compare_projections(
    source: SessionProjection | None,
    target: SessionProjection | None,
) -> tuple[bool, str | None]:
    if source is None:
        return False, "source_missing"
    if target is None:
        return False, "target_missing"
    source_digest = canonical_json_hash(
        {
            "version": source.version,
            "committed_through": source.committed_through,
            "state": source.state,
        }
    )
    target_digest = canonical_json_hash(
        {
            "version": target.version,
            "committed_through": target.committed_through,
            "state": target.state,
        }
    )
    if source_digest != target_digest:
        return False, "projection_mismatch"
    return True, None
