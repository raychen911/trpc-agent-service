"""PostgreSQL approval transitions with row-level execution ownership."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.agent.approval import (
    ApprovalDecision,
    ApprovalRequestCreate,
    ApprovalRequestSnapshot,
    ApprovalStatus,
    ApprovalStore,
)
from trpc_service.storage.orm import as_utc, utc_now
from trpc_service.storage.runtime_orm import ApprovalRequestRow


def _snapshot(row: ApprovalRequestRow) -> ApprovalRequestSnapshot:
    return ApprovalRequestSnapshot(
        approval_id=row.approval_id,
        short_code=row.short_code,
        tenant_id=row.tenant_id,
        agent_app_id=row.agent_app_id,
        binding_id=row.binding_id,
        principal_id=row.principal_id,
        session_id=row.session_id,
        tool_call_id=row.tool_call_id,
        capability_kind=row.capability_kind,
        capability_name=row.capability_name,
        action=row.action,
        resource=row.resource,
        arguments_hash=row.arguments_hash,
        risk_level=row.risk_level,
        status=ApprovalStatus(row.status),
        expires_at=as_utc(row.expires_at),
        config_version=row.config_version,
        logical_call_index=row.logical_call_index,
        artifact_refs=tuple(row.artifact_refs),
        operation_arguments=dict(row.operation_arguments),
        decided_by=row.decided_by,
    )


class PostgreSQLApprovalStore(ApprovalStore):
    """Persist approvals in the shared SQL authority used by all nodes."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def create_or_get(
        self,
        request: ApprovalRequestCreate,
    ) -> ApprovalRequestSnapshot:
        async with self._sessions.begin() as database:
            existing = await database.scalar(
                select(ApprovalRequestRow).where(
                    ApprovalRequestRow.tenant_id == request.tenant_id,
                    ApprovalRequestRow.agent_app_id == request.agent_app_id,
                    ApprovalRequestRow.tool_call_id == request.tool_call_id,
                ))
            if existing is not None:
                if existing.arguments_hash != request.arguments_hash:
                    raise PermissionError("approval logical call does not match its arguments")
                return _snapshot(existing)
            row = ApprovalRequestRow(
                approval_id=request.approval_id,
                short_code=request.short_code,
                tenant_id=request.tenant_id,
                agent_app_id=request.agent_app_id,
                binding_id=request.binding_id,
                principal_id=request.principal_id,
                session_id=request.session_id,
                tool_call_id=request.tool_call_id,
                capability_kind=request.capability_kind,
                capability_name=request.capability_name,
                action=request.action,
                resource=request.resource,
                arguments_hash=request.arguments_hash,
                artifact_refs=list(request.artifact_refs),
                operation_arguments=dict(request.operation_arguments),
                config_version=request.config_version,
                logical_call_index=request.logical_call_index,
                risk_level=request.risk_level,
                status=ApprovalStatus.PENDING.value,
                expires_at=request.expires_at,
            )
            try:
                async with database.begin_nested():
                    database.add(row)
                    await database.flush()
            except IntegrityError:
                # Another node may create the same logical approval between
                # our read and insert. Resolve only that identity collision;
                # short-code or unrelated constraint failures still surface.
                existing = await database.scalar(
                    select(ApprovalRequestRow).where(
                        ApprovalRequestRow.tenant_id == request.tenant_id,
                        ApprovalRequestRow.agent_app_id == request.agent_app_id,
                        ApprovalRequestRow.tool_call_id == request.tool_call_id,
                    ))
                if existing is None:
                    raise
                if existing.arguments_hash != request.arguments_hash:
                    raise PermissionError("approval logical call does not match its arguments")
                return _snapshot(existing)
            return _snapshot(row)

    async def decide(
        self,
        *,
        short_code: str,
        tenant_id: UUID,
        agent_app_id: UUID,
        principal_id: str,
        session_id: str,
        decision: ApprovalDecision,
        now: datetime,
    ) -> ApprovalRequestSnapshot:
        expired = False
        snapshot: ApprovalRequestSnapshot | None = None
        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(ApprovalRequestRow).where(
                    ApprovalRequestRow.short_code == short_code,
                    ApprovalRequestRow.tenant_id == tenant_id,
                    ApprovalRequestRow.agent_app_id == agent_app_id,
                ).with_for_update())
            if row is None:
                raise LookupError("approval request does not exist")
            if row.principal_id != principal_id or row.session_id != session_id:
                raise PermissionError("approval can only be decided by the original requester")
            if row.status != ApprovalStatus.PENDING.value:
                raise PermissionError("approval request is no longer pending")
            if as_utc(row.expires_at) <= now:
                row.status = ApprovalStatus.EXPIRED.value
                expired = True
            else:
                row.status = (ApprovalStatus.APPROVED.value if decision is ApprovalDecision.APPROVE
                              else ApprovalStatus.REJECTED.value)
                row.decided_at = now
                row.decided_by = principal_id
            await database.flush()
            snapshot = _snapshot(row)
        if expired:
            # Raise only after the transaction commits the terminal EXPIRED
            # state; raising inside ``begin`` would roll that state back.
            raise PermissionError("approval request has expired")
        assert snapshot is not None
        return snapshot

    async def claim_execution(
        self,
        *,
        approval_id: UUID,
        tenant_id: UUID,
        agent_app_id: UUID,
        principal_id: str,
        session_id: str,
        arguments_hash: str,
        now: datetime,
    ) -> ApprovalRequestSnapshot:
        expired = False
        snapshot: ApprovalRequestSnapshot | None = None
        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(ApprovalRequestRow).where(
                    ApprovalRequestRow.approval_id == approval_id,
                    ApprovalRequestRow.tenant_id == tenant_id,
                    ApprovalRequestRow.agent_app_id == agent_app_id,
                ).with_for_update())
            if row is None:
                raise LookupError("approval request does not exist")
            if (row.principal_id != principal_id or row.session_id != session_id
                    or row.arguments_hash != arguments_hash):
                raise PermissionError("approved capability does not match this execution")
            if as_utc(row.expires_at) <= now:
                row.status = ApprovalStatus.EXPIRED.value
                expired = True
            elif row.status != ApprovalStatus.APPROVED.value:
                raise PermissionError("approval request is not executable")
            else:
                row.status = ApprovalStatus.EXECUTING.value
                row.execution_started_at = now
            await database.flush()
            snapshot = _snapshot(row)
        if expired:
            raise PermissionError("approval request has expired")
        assert snapshot is not None
        return snapshot

    async def complete_execution(self, approval_id: UUID) -> ApprovalRequestSnapshot:
        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(ApprovalRequestRow).where(
                    ApprovalRequestRow.approval_id == approval_id, ).with_for_update())
            if row is None:
                raise LookupError("approval request does not exist")
            if row.status != ApprovalStatus.EXECUTING.value:
                raise PermissionError("approval request has no executing owner")
            row.status = ApprovalStatus.EXECUTED.value
            row.executed_at = utc_now()
            await database.flush()
            return _snapshot(row)

    async def mark_unknown(self, approval_id: UUID) -> ApprovalRequestSnapshot:
        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(ApprovalRequestRow).where(
                    ApprovalRequestRow.approval_id == approval_id, ).with_for_update())
            if row is None:
                raise LookupError("approval request does not exist")
            if row.status != ApprovalStatus.EXECUTING.value:
                raise PermissionError("approval request has no executing owner")
            row.status = ApprovalStatus.UNKNOWN.value
            await database.flush()
            return _snapshot(row)

    async def get(self, approval_id: UUID) -> ApprovalRequestSnapshot:
        async with self._sessions() as database:
            row = await database.get(ApprovalRequestRow, approval_id)
            if row is None:
                raise LookupError("approval request does not exist")
            return _snapshot(row)
