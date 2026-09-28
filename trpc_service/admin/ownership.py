"""Safety checks that protect the single administrator bound to each tenant."""

from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.models import RoleAssignment
from trpc_service.admin.schemas import ManagementRole
from trpc_service.tenant.models import Tenant


async def lock_tenant_administration(session: AsyncSession, tenant_id: UUID) -> None:
    """Serialize administrator creation, revocation and disable transitions."""

    await session.scalar(
        select(Tenant.tenant_id).where(Tenant.tenant_id == tenant_id).with_for_update())


async def protect_tenant_admin_assignment(
    session: AsyncSession,
    assignment: RoleAssignment,
) -> None:
    """Protect the tenant administrator grant from direct revocation."""

    if assignment.role != ManagementRole.TENANT_ADMIN.value or assignment.tenant_id is None:
        return
    await lock_tenant_administration(session, assignment.tenant_id)
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail="the tenant administrator assignment cannot be revoked",
    )


async def protect_tenant_admin_principal(
    session: AsyncSession,
    principal_id: UUID,
) -> None:
    """Protect every tenant that would lose its administrator if a principal is disabled."""

    tenant_id = await session.scalar(
        select(RoleAssignment.tenant_id).where(
            RoleAssignment.management_principal_id == principal_id,
            RoleAssignment.role == ManagementRole.TENANT_ADMIN.value,
            RoleAssignment.tenant_id.is_not(None),
        ))
    if tenant_id is None:
        return
    # Lock before rejecting so a concurrent tenant-account transaction cannot
    # observe a partially changed control-plane identity.
    await lock_tenant_administration(session, tenant_id)
    raise HTTPException(
        status_code=status.HTTP_409_CONFLICT,
        detail="the tenant administrator identity cannot be disabled",
    )
