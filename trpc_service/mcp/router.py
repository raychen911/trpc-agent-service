"""Tenant-admin CRUD and explicit discovery for remote MCP connections."""

from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.audit import append_management_audit
from trpc_service.admin.auth import ManagementActor, require_tenant_admin
from trpc_service.config.secret_scope import validate_tenant_mcp_secret_ref
from trpc_service.mcp.models import MCPConnection
from trpc_service.mcp.schemas import (
    MCPAuthType,
    MCPConnectionCreate,
    MCPConnectionList,
    MCPConnectionRead,
    MCPConnectionUpdate,
    normalize_mcp_tool_risk,
)
from trpc_service.storage.database import get_session
from trpc_service.tenant.models import Tenant

router = APIRouter(prefix="/tenants/{tenant_id}/mcp-connections", tags=["mcp-connections"])


async def _require_tenant(database: AsyncSession, tenant_id: UUID) -> None:
    if await database.get(Tenant, tenant_id) is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="tenant not found")


async def _connection(
    database: AsyncSession,
    tenant_id: UUID,
    connection_id: UUID,
) -> MCPConnection:
    row = await database.scalar(
        select(MCPConnection).where(
            MCPConnection.tenant_id == tenant_id,
            MCPConnection.connection_id == connection_id,
        ))
    if row is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="MCP connection not found")
    return row


def _read(row: MCPConnection) -> MCPConnectionRead:
    # Only the two platform-supported MCP classifications leave the API. Any
    # malformed or legacy value fails closed as a confirmation-required write.
    safe_catalog = [{
        **item, "risk_level": int(normalize_mcp_tool_risk(item.get("risk_level")))
    } for item in row.tool_catalog]
    return MCPConnectionRead(
        connection_id=row.connection_id,
        tenant_id=row.tenant_id,
        name=row.name,
        endpoint_url=row.endpoint_url,
        auth_type=row.auth_type,
        credential_configured=row.secret_ref is not None,
        timeout_seconds=row.timeout_seconds,
        tool_catalog=safe_catalog,
        catalog_refreshed_at=row.catalog_refreshed_at,
        last_error_code=row.last_error_code,
        status=row.status,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


@router.post("", response_model=MCPConnectionRead, status_code=status.HTTP_201_CREATED)
async def create_mcp_connection(
        tenant_id: UUID,
        payload: MCPConnectionCreate,
        request: Request,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        database: AsyncSession = Depends(get_session),
) -> MCPConnectionRead:
    """Save connection metadata and encrypt an optional bearer credential."""

    await _require_tenant(database, tenant_id)
    connection_id = uuid4()
    secret_ref = payload.secret_ref
    if secret_ref is not None:
        try:
            validate_tenant_mcp_secret_ref(secret_ref, tenant_id)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
    if payload.secret_value is not None:
        try:
            secret_ref = await request.app.state.container.tenant_secrets.put(
                database,
                tenant_id,
                f"mcp/{connection_id}/bearer_token",
                payload.secret_value,
                scope="mcp",
            )
        except RuntimeError as error:
            raise HTTPException(status_code=503,
                                detail="tenant SecretStore is not configured") from error
    row = MCPConnection(
        connection_id=connection_id,
        tenant_id=tenant_id,
        name=payload.name,
        endpoint_url=payload.endpoint_url,
        auth_type=payload.auth_type.value,
        secret_ref=secret_ref,
        timeout_seconds=payload.timeout_seconds,
    )
    database.add(row)
    try:
        await database.flush()
        append_management_audit(
            database,
            actor,
            action="mcp_connection.create",
            resource_type="mcp_connection",
            resource_id=str(connection_id),
            tenant_id=tenant_id,
            reason=support_reason if actor.is_platform_admin else None,
            details_redacted={"auth_type": row.auth_type},
        )
        await database.commit()
    except IntegrityError as error:
        await database.rollback()
        raise HTTPException(status_code=409, detail="MCP connection name already exists") from error
    await database.refresh(row)
    return _read(row)


@router.get("", response_model=MCPConnectionList)
async def list_mcp_connections(
        tenant_id: UUID,
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=50, ge=1, le=100),
        _: ManagementActor = Depends(require_tenant_admin),
        database: AsyncSession = Depends(get_session),
) -> MCPConnectionList:
    await _require_tenant(database, tenant_id)
    condition = MCPConnection.tenant_id == tenant_id
    total = await database.scalar(select(func.count()).select_from(MCPConnection).where(condition))
    rows = await database.scalars(
        select(MCPConnection).where(condition).order_by(
            MCPConnection.created_at, MCPConnection.connection_id).offset(offset).limit(limit))
    return MCPConnectionList(items=[_read(row) for row in rows], total=total or 0)


@router.get("/{connection_id}", response_model=MCPConnectionRead)
async def get_mcp_connection(
        tenant_id: UUID,
        connection_id: UUID,
        _: ManagementActor = Depends(require_tenant_admin),
        database: AsyncSession = Depends(get_session),
) -> MCPConnectionRead:
    await _require_tenant(database, tenant_id)
    return _read(await _connection(database, tenant_id, connection_id))


@router.patch("/{connection_id}", response_model=MCPConnectionRead)
async def update_mcp_connection(
        tenant_id: UUID,
        connection_id: UUID,
        payload: MCPConnectionUpdate,
        request: Request,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        database: AsyncSession = Depends(get_session),
) -> MCPConnectionRead:
    await _require_tenant(database, tenant_id)
    row = await _connection(database, tenant_id, connection_id)
    changes = payload.model_dump(
        exclude_unset=True,
        exclude={"secret_value", "secret_ref"},
        mode="json",
    )
    target_auth = str(changes.get("auth_type", row.auth_type))
    secret_ref = payload.secret_ref if "secret_ref" in payload.model_fields_set else row.secret_ref
    if secret_ref is not None:
        try:
            validate_tenant_mcp_secret_ref(secret_ref, tenant_id)
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
    if payload.secret_value is not None:
        try:
            secret_ref = await request.app.state.container.tenant_secrets.put(
                database,
                tenant_id,
                f"mcp/{connection_id}/bearer_token",
                payload.secret_value,
                existing_reference=row.secret_ref,
                scope="mcp",
            )
        except RuntimeError as error:
            raise HTTPException(status_code=503,
                                detail="tenant SecretStore is not configured") from error
    if target_auth == MCPAuthType.BEARER.value and secret_ref is None:
        raise HTTPException(status_code=422, detail="bearer authentication requires a credential")
    if target_auth == MCPAuthType.NONE.value:
        secret_ref = None
    for field, value in changes.items():
        setattr(row, field, value)
    row.secret_ref = secret_ref
    if {"endpoint_url", "auth_type", "secret_ref", "secret_value"} & payload.model_fields_set:
        row.tool_catalog = []
        row.catalog_refreshed_at = None
    append_management_audit(
        database,
        actor,
        action="mcp_connection.update",
        resource_type="mcp_connection",
        resource_id=str(connection_id),
        tenant_id=tenant_id,
        reason=support_reason if actor.is_platform_admin else None,
        details_redacted={"fields": sorted(payload.model_fields_set)},
    )
    try:
        await database.commit()
    except IntegrityError as error:
        await database.rollback()
        raise HTTPException(status_code=409, detail="MCP connection name already exists") from error
    await database.refresh(row)
    return _read(row)


@router.post("/{connection_id}/refresh", response_model=MCPConnectionRead)
async def refresh_mcp_connection(
        tenant_id: UUID,
        connection_id: UUID,
        request: Request,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        database: AsyncSession = Depends(get_session),
) -> MCPConnectionRead:
    await _require_tenant(database, tenant_id)
    await _connection(database, tenant_id, connection_id)
    # Discovery persists with its own short transaction. Release this request's
    # read transaction first so SQLite development deployments do not deadlock;
    # PostgreSQL benefits from the smaller lock window as well.
    await database.rollback()
    try:
        await request.app.state.container.mcp.refresh(tenant_id, connection_id)
    except Exception as error:
        async with request.app.state.session_factory.begin() as error_database:
            failed = await error_database.scalar(
                select(MCPConnection).where(
                    MCPConnection.tenant_id == tenant_id,
                    MCPConnection.connection_id == connection_id,
                ).with_for_update())
            if failed is not None:
                failed.last_error_code = type(error).__name__
        raise HTTPException(status_code=502, detail="MCP connection check failed") from error
    database.expire_all()
    row = await _connection(database, tenant_id, connection_id)
    append_management_audit(
        database,
        actor,
        action="mcp_connection.refresh",
        resource_type="mcp_connection",
        resource_id=str(connection_id),
        tenant_id=tenant_id,
        reason=support_reason if actor.is_platform_admin else None,
        details_redacted={"tool_count": len(row.tool_catalog)},
    )
    await database.commit()
    return _read(row)


@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def disable_mcp_connection(
        tenant_id: UUID,
        connection_id: UUID,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        database: AsyncSession = Depends(get_session),
) -> Response:
    await _require_tenant(database, tenant_id)
    row = await _connection(database, tenant_id, connection_id)
    row.status = "disabled"
    append_management_audit(
        database,
        actor,
        action="mcp_connection.disable",
        resource_type="mcp_connection",
        resource_id=str(connection_id),
        tenant_id=tenant_id,
        reason=support_reason if actor.is_platform_admin else None,
    )
    await database.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
