"""Durable operations repository adapters (Phase 8).

``PostgresAlertRepository`` keeps deduplicated alert incidents in the
shared PostgreSQL authority: one logical row per fingerprint, cross-node
compare-and-swap transitions on ``state_version``, occurrence merging and
recovery closure. Domain objects stay immutable and are rebuilt from rows.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.operations.models import CanaryRelease
from trpc_service.operations.operations_errors import (
    ReleaseConflict,
    ReleaseNotFound,
    StaleReleaseFence,
)
from trpc_service.observability.alerts import alert_fingerprint, notification_id_for
from trpc_service.observability.models import AlertIncident
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.models import (
    AlertIncidentRow,
    ConfigurationReleaseRow,
    ReleaseTransitionEventRow,
    RollbackDecisionRow,
)


def _now() -> datetime:
    return datetime.now(timezone.utc)


class AlertStateConflict(RuntimeError):
    """Raised when a CAS transition hits a stale ``state_version``."""

    code = "alert_state_conflict"


def _incident_id(fingerprint: str) -> str:
    from uuid import NAMESPACE_URL, uuid5

    return str(uuid5(NAMESPACE_URL, f"incident:{fingerprint}"))


class PostgresAlertRepository:
    """Cross-node alert incident authority (AlertRepository port)."""

    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    async def observe(
        self,
        rule_id: str,
        severity: str,
        scope_digest: str,
        stable_reason: str,
        evidence_digest: str | None = None,
        observed_at: datetime | None = None,
    ) -> AlertIncident:
        """Merge one observation into the fingerprint's logical incident."""

        fingerprint = alert_fingerprint(
            rule_id=rule_id,
            severity=severity,
            role="platform",
            component="platform",
            scope_digest=scope_digest,
            stable_reason=stable_reason,
        )
        at = observed_at or _now()
        async with AsyncSession(self.database.engine) as session, session.begin():
            row = (
                await session.execute(
                    select(AlertIncidentRow)
                    .where(AlertIncidentRow.fingerprint == fingerprint)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                row = AlertIncidentRow(
                    incident_id=_incident_id(fingerprint),
                    fingerprint=fingerprint,
                    rule_id=rule_id,
                    severity=severity,
                    scope_digest=scope_digest,
                    state="pending",
                    first_observed_at=at,
                    last_observed_at=at,
                    state_version=1,
                    occurrence_count=1,
                    evidence_digest=evidence_digest,
                )
                session.add(row)
            else:
                # Fingerprint dedup: merge occurrences into ONE row.
                row.occurrence_count = int(row.occurrence_count) + 1
                row.last_observed_at = at
                if evidence_digest is not None:
                    row.evidence_digest = evidence_digest
            await session.flush()
            return self._to_domain(row)

    async def transition(
        self,
        incident_id: str,
        *,
        expected_version: int,
        state: str,
        observed_at: datetime | None = None,
        notification_id: str | None = None,
        count_occurrence: bool = False,
    ) -> AlertIncident:
        """Compare-and-swap one state transition on ``state_version``.

        Any concurrent writer that bumped the version first makes this
        transition fail loudly instead of silently double-applying.
        """

        at = observed_at or _now()
        async with AsyncSession(self.database.engine) as session, session.begin():
            result = await session.execute(
                update(AlertIncidentRow)
                .where(
                    AlertIncidentRow.incident_id == incident_id,
                    AlertIncidentRow.state_version == expected_version,
                )
                .values(
                    state=state,
                    state_version=expected_version + 1,
                    last_observed_at=at,
                    last_notification_id=notification_id,
                    resolved_at=at if state == "resolved" else None,
                    occurrence_count=(
                        AlertIncidentRow.occurrence_count + 1
                        if count_occurrence
                        else AlertIncidentRow.occurrence_count
                    ),
                )
                .returning(AlertIncidentRow.incident_id)
            )
            if result.scalar_one_or_none() is None:
                raise AlertStateConflict(
                    f"stale state_version {expected_version} for {incident_id}"
                )
            row = (
                await session.execute(
                    select(AlertIncidentRow).where(
                        AlertIncidentRow.incident_id == incident_id
                    )
                )
            ).scalar_one()
            return self._to_domain(row)

    async def get_by_fingerprint(self, fingerprint: str) -> AlertIncident | None:
        async with AsyncSession(self.database.engine) as session:
            row = (
                await session.execute(
                    select(AlertIncidentRow).where(
                        AlertIncidentRow.fingerprint == fingerprint
                    )
                )
            ).scalar_one_or_none()
            return None if row is None else self._to_domain(row)

    async def notification_id(self, incident: AlertIncident) -> str:
        return notification_id_for(incident)

    @staticmethod
    def _to_domain(row: AlertIncidentRow) -> AlertIncident:
        return AlertIncident(
            incident_id=row.incident_id,
            fingerprint=row.fingerprint,
            rule_id=row.rule_id,
            severity=row.severity,
            scope_digest=row.scope_digest,
            state=row.state,
            first_observed_at=row.first_observed_at,
            last_observed_at=row.last_observed_at,
            state_version=int(row.state_version),
            occurrence_count=int(row.occurrence_count),
            evidence_digest=row.evidence_digest,
            last_notification_id=row.last_notification_id,
            resolved_at=row.resolved_at,
        )


class PostgresReleaseRepository:
    """Release state machine authority with revision/fence CAS (DEC-004).

    ``apply_command`` executes one idempotent release command inside a
    single PostgreSQL transaction: the CAS update, the append-only journal
    event and the rollback decision (for rollbacks) commit together or not
    at all. Command retries return the first committed result via the
    release_id + command_id journal lookup.
    """

    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    async def create_release(self, release: CanaryRelease) -> CanaryRelease:
        async with AsyncSession(self.database.engine) as session, session.begin():
            existing = (
                await session.execute(
                    select(ConfigurationReleaseRow).where(
                        ConfigurationReleaseRow.release_id == release.release_id
                    )
                )
            ).scalar_one_or_none()
            if existing is not None:
                return self._to_domain(existing)
            session.add(
                ConfigurationReleaseRow(
                    release_id=release.release_id,
                    candidate_snapshot_id=release.candidate_snapshot_id,
                    rollback_snapshot_id=release.rollback_snapshot_id,
                    cohorts=list(release.cohorts),
                    observation_window_seconds=release.observation_window,
                    minimum_sample=release.minimum_sample,
                    quality_gates=list(release.quality_gates),
                    hard_gate_types=list(release.hard_gate_types),
                    state=release.state,
                    revision=release.revision,
                    owner_fence_generation=release.owner_fence_generation,
                    created_by_digest=release.created_by_digest,
                    created_at=release.created_at,
                    updated_at=release.created_at,
                )
            )
        return release

    async def get_release(self, release_id: str) -> CanaryRelease:
        async with AsyncSession(self.database.engine) as session:
            row = (
                await session.execute(
                    select(ConfigurationReleaseRow).where(
                        ConfigurationReleaseRow.release_id == release_id
                    )
                )
            ).scalar_one_or_none()
            if row is None:
                raise ReleaseNotFound("release not found")
            return self._to_domain(row)

    async def apply_command(
        self,
        *,
        release_id: str,
        command_id: str,
        action: str,
        expected_revision: int,
        fence_generation: int,
        actor_digest: str,
        reason_code: str = "",
    ) -> CanaryRelease:
        from trpc_service.operations.release import ReleaseStateMachine

        async with AsyncSession(self.database.engine) as session, session.begin():
            row = (
                await session.execute(
                    select(ConfigurationReleaseRow)
                    .where(ConfigurationReleaseRow.release_id == release_id)
                    .with_for_update()
                )
            ).scalar_one_or_none()
            if row is None:
                raise ReleaseNotFound("release not found")
            # Idempotent replay: same command returns its first result.
            replay = (
                await session.execute(
                    select(ReleaseTransitionEventRow)
                    .where(
                        ReleaseTransitionEventRow.release_id == release_id,
                        ReleaseTransitionEventRow.command_id == command_id,
                    )
                    .order_by(ReleaseTransitionEventRow.to_revision.desc())
                    .limit(1)
                )
            ).scalar_one_or_none()
            if replay is not None:
                refreshed = (
                    await session.execute(
                        select(ConfigurationReleaseRow).where(
                            ConfigurationReleaseRow.release_id == release_id
                        )
                    )
                ).scalar_one()
                return self._to_domain(refreshed)
            if fence_generation < int(row.owner_fence_generation):
                raise StaleReleaseFence("writer fence below highest seen")
            if expected_revision != int(row.revision):
                raise ReleaseConflict("stale expected_revision")
            current_state = row.state
            current_revision = int(row.revision)
            plan = ReleaseStateMachine.plan(action, current_state, reason_code)
            for to_state, step_reason in plan:
                row.state = to_state
                row.revision = current_revision + 1
                row.owner_fence_generation = max(
                    fence_generation, int(row.owner_fence_generation)
                )
                row.updated_at = _now()
                session.add(
                    ReleaseTransitionEventRow(
                        event_id=str(uuid4()),
                        release_id=release_id,
                        command_id=command_id,
                        from_state=current_state,
                        to_state=to_state,
                        from_revision=current_revision,
                        to_revision=current_revision + 1,
                        actor_digest=actor_digest,
                        reason_code=step_reason,
                        occurred_at=_now(),
                    )
                )
                current_state = to_state
                current_revision += 1
            if action == "rollback":
                release = self._to_domain(row)
                session.add(
                    RollbackDecisionRow(
                        decision_id=str(uuid4()),
                        release_id=release_id,
                        command_id=command_id,
                        actor_digest=actor_digest,
                        reason_code=reason_code or "operator_requested",
                        target_snapshot_id=release.rollback_snapshot_id,
                        affected_tenant_count=len(release.cohorts),
                        from_revision=expected_revision,
                        to_revision=current_revision,
                        created_at=_now(),
                    )
                )
            await session.flush()
            return self._to_domain(row)

    @staticmethod
    def _to_domain(row: ConfigurationReleaseRow) -> CanaryRelease:
        return CanaryRelease(
            release_id=row.release_id,
            candidate_snapshot_id=row.candidate_snapshot_id,
            rollback_snapshot_id=row.rollback_snapshot_id,
            created_by_digest=row.created_by_digest,
            created_at=row.created_at,
            cohorts=tuple(row.cohorts or ()),
            observation_window=int(row.observation_window_seconds),
            minimum_sample=int(row.minimum_sample),
            quality_gates=tuple(row.quality_gates or ()),
            hard_gate_types=tuple(row.hard_gate_types or ()),
            state=row.state,
            revision=int(row.revision),
            owner_fence_generation=int(row.owner_fence_generation),
            updated_at=row.updated_at,
        )
