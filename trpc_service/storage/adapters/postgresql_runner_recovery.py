"""PostgreSQL Runner attempt and checkpoint recovery store."""

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.agent.contracts import AgentTaskClaim
from trpc_service.agent.recovery_state import (
    RunnerAttemptSnapshot,
    RunnerAttemptStatus,
    RunnerCheckpointSnapshot,
    RunnerRecoveryStore,
    StaleRunnerAttempt,
)
from trpc_service.storage.runtime_orm import RunnerAttemptRow, RunnerCheckpointRow


class PostgreSQLRunnerRecoveryStore(RunnerRecoveryStore):
    """Coordinate Runner recovery state across independent Worker nodes."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def start(self, claim: AgentTaskClaim, *, node_id: str) -> RunnerAttemptSnapshot:
        if not node_id.strip():
            raise ValueError("Runner node ID cannot be empty")
        tenant = claim.request.tenant
        attempt_id = f"{claim.task_id}:{claim.attempt_count}:{claim.fencing_token}"
        async with self._sessions.begin() as database:
            rows = (await database.scalars(
                select(RunnerAttemptRow).where(
                    RunnerAttemptRow.tenant_id == tenant.tenant_id,
                    RunnerAttemptRow.task_id == UUID(claim.task_id),
                ).order_by(RunnerAttemptRow.fencing_token.desc()).with_for_update())).all()
            if rows and rows[0].fencing_token > claim.fencing_token:
                raise StaleRunnerAttempt("Runner attempt fence is stale")
            for row in rows:
                if row.attempt_id == attempt_id:
                    return self._snapshot(row)
            if rows and rows[0].fencing_token == claim.fencing_token:
                raise StaleRunnerAttempt("Runner fence is already owned by another attempt")
            if rows and rows[0].status == RunnerAttemptStatus.RUNNING.value:
                rows[0].status = RunnerAttemptStatus.UNKNOWN.value
                rows[0].completed_at = datetime.now(timezone.utc)
                rows[0].error_summary = "superseded after lease expiry"
            row = RunnerAttemptRow(
                tenant_id=tenant.tenant_id,
                attempt_id=attempt_id,
                agent_app_id=tenant.agent_app_id,
                task_id=UUID(claim.task_id),
                request_id=tenant.request_id,
                attempt_no=claim.attempt_count,
                fencing_token=claim.fencing_token,
                node_id=node_id,
                status=RunnerAttemptStatus.RUNNING.value,
                started_at=datetime.now(timezone.utc),
            )
            database.add(row)
            return self._snapshot(row)

    async def checkpoint(
        self,
        attempt: RunnerAttemptSnapshot,
        *,
        stage: str,
        state_ref: str | None,
        occurred_at: datetime | None = None,
    ) -> RunnerCheckpointSnapshot:
        if not stage.strip():
            raise ValueError("Runner checkpoint stage cannot be empty")
        async with self._sessions.begin() as database:
            row = await self._owned(database, attempt)
            current_sequence = await database.scalar(
                select(func.coalesce(func.max(RunnerCheckpointRow.sequence_no), 0)).where(
                    RunnerCheckpointRow.tenant_id == row.tenant_id,
                    RunnerCheckpointRow.attempt_id == row.attempt_id,
                ))
            sequence_no = int(current_sequence or 0) + 1
            checkpoint = RunnerCheckpointRow(
                tenant_id=row.tenant_id,
                attempt_id=row.attempt_id,
                sequence_no=sequence_no,
                stage=stage,
                state_ref=state_ref,
                occurred_at=occurred_at or datetime.now(timezone.utc),
            )
            database.add(checkpoint)
            return RunnerCheckpointSnapshot(
                attempt_id=row.attempt_id,
                sequence_no=sequence_no,
                stage=stage,
                state_ref=state_ref,
                occurred_at=checkpoint.occurred_at,
            )

    async def complete(self, attempt: RunnerAttemptSnapshot) -> RunnerAttemptSnapshot:
        return await self._finish(attempt, RunnerAttemptStatus.SUCCEEDED, None)

    async def fail(
        self,
        attempt: RunnerAttemptSnapshot,
        summary: str,
        *,
        retryable: bool,
    ) -> RunnerAttemptSnapshot:
        status = (RunnerAttemptStatus.RETRYABLE_FAILED
                  if retryable else RunnerAttemptStatus.PERMANENT_FAILED)
        return await self._finish(attempt, status, summary)

    async def mark_unknown(
        self,
        attempt: RunnerAttemptSnapshot,
        summary: str,
    ) -> RunnerAttemptSnapshot:
        return await self._finish(attempt, RunnerAttemptStatus.UNKNOWN, summary)

    async def _finish(
        self,
        attempt: RunnerAttemptSnapshot,
        status: RunnerAttemptStatus,
        summary: str | None,
    ) -> RunnerAttemptSnapshot:
        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(RunnerAttemptRow).where(
                    RunnerAttemptRow.attempt_id == attempt.attempt_id, ).with_for_update())
            if row is None:
                raise StaleRunnerAttempt("Runner attempt does not exist")
            newest_fence = await database.scalar(
                select(func.max(RunnerAttemptRow.fencing_token)).where(
                    RunnerAttemptRow.tenant_id == row.tenant_id,
                    RunnerAttemptRow.task_id == row.task_id,
                ))
            if newest_fence != attempt.fencing_token:
                raise StaleRunnerAttempt("Runner attempt fence is stale")
            safe_summary = None if summary is None else summary[:1000]
            if row.status != RunnerAttemptStatus.RUNNING.value:
                # Terminal transitions are idempotent so a database response
                # loss can be retried without corrupting the recovery state.
                if row.status == status.value and row.error_summary == safe_summary:
                    return self._snapshot(row)
                raise StaleRunnerAttempt("Runner attempt is already terminal")
            row.status = status.value
            row.error_summary = safe_summary
            row.completed_at = datetime.now(timezone.utc)
            await database.flush()
            return self._snapshot(row)

    @staticmethod
    async def _owned(
        database: AsyncSession,
        attempt: RunnerAttemptSnapshot,
    ) -> RunnerAttemptRow:
        row = await database.scalar(
            select(RunnerAttemptRow).where(
                RunnerAttemptRow.attempt_id == attempt.attempt_id, ).with_for_update())
        if row is None:
            raise StaleRunnerAttempt("Runner attempt does not exist")
        newest_fence = await database.scalar(
            select(func.max(RunnerAttemptRow.fencing_token)).where(
                RunnerAttemptRow.tenant_id == row.tenant_id,
                RunnerAttemptRow.task_id == row.task_id,
            ))
        if (newest_fence != attempt.fencing_token
                or row.status != RunnerAttemptStatus.RUNNING.value):
            raise StaleRunnerAttempt("Runner attempt is stale or terminal")
        return row

    @staticmethod
    def _snapshot(row: RunnerAttemptRow) -> RunnerAttemptSnapshot:
        return RunnerAttemptSnapshot(
            attempt_id=row.attempt_id,
            task_id=str(row.task_id),
            request_id=row.request_id,
            attempt_no=row.attempt_no,
            fencing_token=row.fencing_token,
            node_id=row.node_id,
            status=RunnerAttemptStatus(row.status),
            error_summary=row.error_summary,
        )
