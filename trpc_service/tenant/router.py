"""Tenant control-plane CRUD endpoints."""

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.audit import append_management_audit
from trpc_service.admin.auth import ManagementActor, require_platform_admin, require_tenant_admin
from trpc_service.storage.database import get_session
from trpc_service.tenant.models import Tenant
from trpc_service.tenant.schemas import (
    TenantCreate,
    TenantList,
    TenantRead,
    TenantStatus,
    TenantUpdate,
)

router = APIRouter(prefix="/tenants", tags=["tenants"])


@router.post("", response_model=TenantRead, status_code=status.HTTP_201_CREATED)
async def create_tenant(
        payload: TenantCreate,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> Tenant:
    """Create a tenant and translate duplicate names into a stable conflict response."""

    tenant = Tenant(**payload.model_dump(mode="json"))
    session.add(tenant)
    try:
        await session.flush()
        append_management_audit(
            session,
            actor,
            action="tenant.create",
            resource_type="tenant",
            resource_id=str(tenant.tenant_id),
            tenant_id=tenant.tenant_id,
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="tenant name already exists",
        ) from error
    await session.refresh(tenant)
    return tenant


@router.get("", response_model=TenantList)
async def list_tenants(
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=50, ge=1, le=100),
        _: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> TenantList:
    """List tenants using deterministic offset pagination."""

    total = await session.scalar(select(func.count()).select_from(Tenant))
    result = await session.scalars(
        select(Tenant).order_by(Tenant.created_at, Tenant.tenant_id).offset(offset).limit(limit))
    return TenantList(items=[TenantRead.model_validate(item) for item in result], total=total or 0)


@router.get("/{tenant_id}", response_model=TenantRead)
async def get_tenant(
        tenant_id: UUID,
        _: ManagementActor = Depends(require_tenant_admin),
        session: AsyncSession = Depends(get_session),
) -> Tenant:
    """Return one tenant or a public 404 response."""

    tenant = await session.get(Tenant, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="tenant not found")
    return tenant


@router.patch("/{tenant_id}", response_model=TenantRead)
async def update_tenant(
        tenant_id: UUID,
        payload: TenantUpdate,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> Tenant:
    """Apply the explicitly supplied mutable fields to a tenant."""

    tenant = await session.get(Tenant, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="tenant not found")

    for field, value in payload.model_dump(exclude_unset=True, mode="json").items():
        setattr(tenant, field, value)
    try:
        append_management_audit(
            session,
            actor,
            action="tenant.update",
            resource_type="tenant",
            resource_id=str(tenant_id),
            tenant_id=tenant_id,
            details_redacted={"fields": sorted(payload.model_fields_set)},
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="tenant name already exists",
        ) from error
    await session.refresh(tenant)
    return tenant


@router.delete("/{tenant_id}", status_code=status.HTTP_204_NO_CONTENT)
async def disable_tenant(
        tenant_id: UUID,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> Response:
    """Soft-disable a tenant while retaining its configuration and audit history."""

    tenant = await session.get(Tenant, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="tenant not found")

    tenant.status = TenantStatus.DISABLED.value
    append_management_audit(
        session,
        actor,
        action="tenant.disable",
        resource_type="tenant",
        resource_id=str(tenant_id),
        tenant_id=tenant_id,
    )
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
