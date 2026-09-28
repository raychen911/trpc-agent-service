"""Tenant-scoped delivery failure inspection and manual recovery APIs."""

from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.audit import append_management_audit
from trpc_service.admin.auth import (
    ManagementActor,
    require_tenant_admin,
)
from trpc_service.channels.schemas import (
    DeliveryFailureList,
    DeliveryFailureRead,
    DeliveryFailureStatus,
    DeliveryReplayRead,
    DeliveryReplayRequest,
)
from trpc_service.storage.database import get_session
from trpc_service.storage.runtime_orm import OutboxMessageRow

router = APIRouter(
    prefix="/tenants/{tenant_id}/delivery-failures",
    tags=["delivery-recovery"],
)

_FAILURE_STATES = tuple(item.value for item in DeliveryFailureStatus)


@router.get("", response_model=DeliveryFailureList)
async def list_delivery_failures(
        tenant_id: UUID,
        failure_status: DeliveryFailureStatus | None = Query(default=None, alias="status"),
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=50, ge=1, le=100),
        _: ManagementActor = Depends(require_tenant_admin),
        session: AsyncSession = Depends(get_session),
) -> DeliveryFailureList:
    """List safe failure metadata within the authenticated tenant boundary."""

    conditions = [
        OutboxMessageRow.tenant_id == tenant_id,
        OutboxMessageRow.category == "IM_REPLY",
        OutboxMessageRow.status.in_(_FAILURE_STATES),
    ]
    if failure_status is not None:
        conditions.append(OutboxMessageRow.status == failure_status.value)
    total = await session.scalar(
        select(func.count()).select_from(OutboxMessageRow).where(*conditions))
    rows = (await session.scalars(
        select(OutboxMessageRow).where(*conditions).order_by(
            OutboxMessageRow.updated_at.desc(),
            OutboxMessageRow.outbox_id,
        ).offset(offset).limit(limit))).all()
    return DeliveryFailureList(
        items=[DeliveryFailureRead.model_validate(row) for row in rows],
        total=total or 0,
    )


@router.post("/{outbox_id}/replay", response_model=DeliveryReplayRead)
async def replay_delivery_failure(
        tenant_id: UUID,
        outbox_id: str,
        payload: DeliveryReplayRequest,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> DeliveryReplayRead:
    """Atomically requeue one terminal failure and append its operator audit."""

    row = await session.scalar(
        select(OutboxMessageRow).where(
            OutboxMessageRow.tenant_id == tenant_id,
            OutboxMessageRow.outbox_id == outbox_id,
            OutboxMessageRow.category == "IM_REPLY",
            OutboxMessageRow.status.in_(("DEAD_LETTER", "UNKNOWN")),
        ).with_for_update())
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="replayable delivery failure not found",
        )
    previous_status = row.status
    row.status = "PENDING"
    row.next_attempt_at = None
    row.lease_owner = None
    row.lease_until = None
    row.last_error_code = None
    row.last_error_summary = None
    # A replay receives a fresh retry budget while attempt_count remains a
    # monotonic key for the immutable provider-attempt audit trail.
    row.retry_count = 0
    append_management_audit(
        session,
        actor,
        action="delivery_failure.replay",
        resource_type="outbox_message",
        resource_id=outbox_id,
        tenant_id=tenant_id,
        reason=support_reason if actor.is_platform_admin else payload.reason,
        details_redacted={
            "from_status": previous_status,
            "to_status": "PENDING"
        },
    )
    await session.commit()
    return DeliveryReplayRead(outbox_id=outbox_id, status="PENDING")
