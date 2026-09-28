"""Platform administrator APIs for identities, catalogs and management audit."""

import secrets
from datetime import datetime, timezone
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.login_guard import password_work
from trpc_service.admin.audit import append_management_audit
from trpc_service.admin.auth import (
    ManagementActor,
    get_management_actor,
    hash_management_password,
    hash_management_token,
    require_platform_admin,
)
from trpc_service.admin.models import (
    ChannelAdapterType,
    ManagementAuditLog,
    ManagementCredential,
    ManagementPasswordCredential,
    ManagementPrincipal,
    ManagementWebSession,
    ModelCatalogEntry,
    ModelProfile,
    ModelProviderCredential,
    RoleAssignment,
)
from trpc_service.admin.ownership import (
    protect_tenant_admin_assignment,
    protect_tenant_admin_principal,
)
from trpc_service.admin.schemas import (
    ChannelAdapterCreate,
    ChannelAdapterList,
    ChannelAdapterRead,
    ChannelAdapterUpdate,
    CredentialCreate,
    CredentialIssued,
    CredentialList,
    CredentialRead,
    ManagementActorRead,
    ManagementAuditList,
    ManagementAuditRead,
    ModelCatalogCreate,
    ModelCatalogList,
    ModelCatalogRead,
    ModelCatalogUpdate,
    ModelCredentialCreate,
    ModelCredentialList,
    ModelCredentialRead,
    ModelCredentialUpdate,
    PasswordCredentialSet,
    PrincipalCreate,
    PrincipalList,
    PrincipalRead,
    PrincipalUpdate,
    RoleAssignmentCreate,
    RoleAssignmentList,
    RoleAssignmentRead,
    TenantAccountCreate,
    UsageLedgerList,
    UsageLedgerRead,
    UsageLedgerSummary,
)
from trpc_service.agent.nodes import RuntimeNodeList
from trpc_service.agent.models import AgentApp
from trpc_service.agent.scaling import (
    WorkerPoolGenerationConflict,
    WorkerPoolScaleRequest,
    WorkerPoolStatus,
    describe_worker_pool,
)
from trpc_service.storage.database import get_session
from trpc_service.tenant.models import Tenant
from trpc_service.storage.runtime_orm import UsageLedgerRow

router = APIRouter(prefix="/admin", tags=["admin"])


async def _reject_dependency_disable_with_active_agents(
    session: AsyncSession,
    *,
    model_catalog_id: UUID | None = None,
    model_credential_id: UUID | None = None,
) -> None:
    """Prevent platform dependency changes from breaking live IM execution."""

    conditions = [AgentApp.status == "active", ModelProfile.status == "active"]
    dependency = "model dependency"
    if model_catalog_id is not None:
        conditions.append(ModelProfile.model_catalog_id == model_catalog_id)
        dependency = "model catalog entry"
    elif model_credential_id is not None:
        conditions.append(ModelProfile.model_credential_id == model_credential_id)
        dependency = "model credential"
    else:
        raise ValueError("one model dependency identifier is required")
    active_agent_id = await session.scalar(
        select(AgentApp.agent_app_id).join(
            ModelProfile,
            (ModelProfile.tenant_id == AgentApp.tenant_id)
            & (ModelProfile.model_profile_id == AgentApp.model_profile_id),
        ).where(*conditions).limit(1))
    if active_agent_id is not None:
        raise HTTPException(
            status_code=409,
            detail=f"{dependency} is used by an active Agent; disable the Agent first",
        )


@router.get("/usage", response_model=UsageLedgerList)
async def list_usage_ledger(
        tenant_id: UUID | None = None,
        occurred_from: datetime | None = Query(default=None),
        occurred_to: datetime | None = Query(default=None),
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=500),
        _: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> UsageLedgerList:
    """Query durable business usage without depending on Prometheus retention."""

    filters = [UsageLedgerRow.status == "completed"]
    if tenant_id is not None:
        filters.append(UsageLedgerRow.tenant_id == tenant_id)
    if occurred_from is not None:
        filters.append(UsageLedgerRow.occurred_at >= occurred_from)
    if occurred_to is not None:
        filters.append(UsageLedgerRow.occurred_at < occurred_to)
    total = await session.scalar(select(func.count()).select_from(UsageLedgerRow).where(*filters))
    sums = (await session.execute(
        select(
            func.coalesce(func.sum(UsageLedgerRow.input_tokens), 0),
            func.coalesce(func.sum(UsageLedgerRow.output_tokens), 0),
            func.coalesce(func.sum(UsageLedgerRow.total_tokens), 0),
            func.coalesce(func.sum(UsageLedgerRow.estimated_cost), 0),
        ).where(*filters))).one()
    rows = (await session.scalars(
        select(UsageLedgerRow).where(*filters).order_by(
            UsageLedgerRow.occurred_at.desc(),
            UsageLedgerRow.usage_id).offset(offset).limit(limit))).all()
    return UsageLedgerList(
        items=[UsageLedgerRead.model_validate(row) for row in rows],
        total=total or 0,
        summary=UsageLedgerSummary(
            input_tokens=int(sums[0]),
            output_tokens=int(sums[1]),
            total_tokens=int(sums[2]),
            estimated_cost=Decimal(str(sums[3])),
        ),
    )


def _model_credential_read(row: ModelProviderCredential) -> ModelCredentialRead:
    """Return credential metadata while keeping the SecretRef write-only."""

    return ModelCredentialRead(
        model_credential_id=row.model_credential_id,
        provider=row.provider,
        name=row.name,
        secret_configured=True,
        status=row.status,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


@router.post(
    "/model-credentials",
    response_model=ModelCredentialRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_model_credential(
        payload: ModelCredentialCreate,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ModelCredentialRead:
    row = ModelProviderCredential(**payload.model_dump())
    session.add(row)
    try:
        await session.flush()
        append_management_audit(
            session,
            actor,
            action="model_credential.create",
            resource_type="model_provider_credential",
            resource_id=str(row.model_credential_id),
            details_redacted={
                "provider": row.provider,
                "name": row.name
            },
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(status_code=409,
                            detail="model credential name already exists") from error
    await session.refresh(row)
    return _model_credential_read(row)


@router.get("/model-credentials", response_model=ModelCredentialList)
async def list_model_credentials(
        _: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ModelCredentialList:
    rows = (await session.scalars(
        select(ModelProviderCredential).order_by(
            ModelProviderCredential.provider,
            ModelProviderCredential.name,
        ))).all()
    return ModelCredentialList(
        items=[_model_credential_read(row) for row in rows],
        total=len(rows),
    )


@router.patch("/model-credentials/{credential_id}", response_model=ModelCredentialRead)
async def update_model_credential(
        credential_id: UUID,
        payload: ModelCredentialUpdate,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ModelCredentialRead:
    row = await session.get(ModelProviderCredential, credential_id)
    if row is None:
        raise HTTPException(status_code=404, detail="model credential not found")
    changes = payload.model_dump(exclude_unset=True, mode="json")
    if changes.get("status") == "disabled" and row.status != "disabled":
        await _reject_dependency_disable_with_active_agents(
            session,
            model_credential_id=credential_id,
        )
    for field, value in changes.items():
        setattr(row, field, value)
    append_management_audit(
        session,
        actor,
        action="model_credential.update",
        resource_type="model_provider_credential",
        resource_id=str(credential_id),
        details_redacted={"fields": sorted(payload.model_fields_set)},
    )
    try:
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(status_code=409,
                            detail="model credential name already exists") from error
    await session.refresh(row)
    return _model_credential_read(row)


@router.delete("/model-credentials/{credential_id}", status_code=status.HTTP_204_NO_CONTENT)
async def disable_model_credential(
        credential_id: UUID,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> Response:
    """Remove a credential from active use while retaining audit references."""

    row = await session.get(ModelProviderCredential, credential_id)
    if row is None:
        raise HTTPException(status_code=404, detail="model credential not found")
    if row.status != "disabled":
        await _reject_dependency_disable_with_active_agents(
            session,
            model_credential_id=credential_id,
        )
        row.status = "disabled"
    append_management_audit(
        session,
        actor,
        action="model_credential.disable",
        resource_type="model_provider_credential",
        resource_id=str(credential_id),
    )
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.get("/runtime-nodes", response_model=RuntimeNodeList)
async def list_runtime_nodes(
        request: Request,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> RuntimeNodeList:
    """List fresh, stale and stopped API/Worker nodes for platform operations."""

    nodes: RuntimeNodeList = await request.app.state.container.node_registry.list()
    append_management_audit(
        session,
        actor,
        action="runtime_node.list",
        resource_type="runtime_node",
        resource_id="*",
        details_redacted={"result_count": nodes.total},
    )
    await session.commit()
    return nodes


@router.get("/worker-pool", response_model=WorkerPoolStatus)
async def get_worker_pool(
        request: Request,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> WorkerPoolStatus:
    """Return desired and observed Worker capacity for platform operations."""

    settings = request.app.state.settings
    target = await request.app.state.container.worker_pool.ensure(
        initial_desired_nodes=settings.local_worker_nodes,
        session=session,
    )
    append_management_audit(
        session,
        actor,
        action="worker_pool.read",
        resource_type="worker_pool",
        resource_id=target.pool_name,
        details_redacted={"generation": target.generation},
    )
    await session.commit()
    return await describe_worker_pool(
        target,
        request.app.state.container.node_registry,
        scaler_mode=settings.worker_scaler_mode,
    )


@router.put("/worker-pool", response_model=WorkerPoolStatus)
async def scale_worker_pool(
        payload: WorkerPoolScaleRequest,
        request: Request,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> WorkerPoolStatus:
    """Set desired Worker capacity; a runtime reconciler applies it asynchronously."""

    settings = request.app.state.settings
    await request.app.state.container.worker_pool.ensure(
        initial_desired_nodes=settings.local_worker_nodes,
        session=session,
    )
    try:
        target = await request.app.state.container.worker_pool.scale(
            desired_nodes=payload.desired_nodes,
            expected_generation=payload.expected_generation,
            updated_by=actor.subject,
            session=session,
        )
    except WorkerPoolGenerationConflict as error:
        await session.rollback()
        raise HTTPException(status_code=409, detail=str(error)) from error
    append_management_audit(
        session,
        actor,
        action="worker_pool.scale",
        resource_type="worker_pool",
        resource_id=target.pool_name,
        details_redacted={
            "desired_nodes": target.desired_nodes,
            "generation": target.generation,
        },
    )
    await session.commit()
    return await describe_worker_pool(
        target,
        request.app.state.container.node_registry,
        scaler_mode=settings.worker_scaler_mode,
    )


def _catalog_read(row: ModelCatalogEntry) -> ModelCatalogRead:
    return ModelCatalogRead(
        model_catalog_id=row.model_catalog_id,
        provider=row.provider,
        model_name=row.model_name,
        display_name=row.display_name,
        capabilities=row.capabilities,
        default_limits=row.default_limits,
        platform_credential_configured=row.platform_secret_ref is not None,
        status=row.status,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


@router.get("/me", response_model=ManagementActorRead)
async def current_actor(
        actor: ManagementActor = Depends(get_management_actor),
        session: AsyncSession = Depends(get_session),
) -> ManagementActorRead:
    append_management_audit(
        session,
        actor,
        action="management_identity.read",
        resource_type="management_actor",
        resource_id=actor.subject,
    )
    await session.commit()
    return ManagementActorRead(
        subject=actor.subject,
        roles=sorted(actor.roles),
        tenant_roles={
            str(key): sorted(value)
            for key, value in actor.tenant_roles.items()
        },
        bootstrap=actor.bootstrap,
    )


@router.post("/principals", response_model=PrincipalRead, status_code=status.HTTP_201_CREATED)
async def create_principal(
        payload: PrincipalCreate,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ManagementPrincipal:
    principal = ManagementPrincipal(**payload.model_dump(mode="json"))
    session.add(principal)
    try:
        await session.flush()
        append_management_audit(
            session,
            actor,
            action="management_principal.create",
            resource_type="management_principal",
            resource_id=str(principal.management_principal_id),
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(status_code=409,
                            detail="external management subject already exists") from error
    await session.refresh(principal)
    return principal


@router.post("/tenant-accounts", response_model=PrincipalRead, status_code=status.HTTP_201_CREATED)
async def create_tenant_account(
        payload: TenantAccountCreate,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ManagementPrincipal:
    """Atomically create the tenant's single administrator identity and password."""

    tenant = await session.scalar(
        select(Tenant).where(Tenant.tenant_id == payload.tenant_id).with_for_update())
    if tenant is None or tenant.status != "active":
        raise HTTPException(status_code=404, detail="active tenant not found")
    existing_admin = await session.scalar(
        select(RoleAssignment.role_assignment_id).where(
            RoleAssignment.tenant_id == payload.tenant_id,
            RoleAssignment.role == "tenant_admin",
        ))
    if existing_admin is not None:
        raise HTTPException(status_code=409, detail="tenant administrator account already exists")
    principal = ManagementPrincipal(
        display_name=f"{tenant.name} 管理员",
        principal_type="human",
        external_subject=f"tenant-console:{payload.username}",
    )
    session.add(principal)
    try:
        await session.flush()
        session.add_all((
            RoleAssignment(
                management_principal_id=principal.management_principal_id,
                tenant_id=payload.tenant_id,
                role="tenant_admin",
            ),
            ManagementPasswordCredential(
                management_principal_id=principal.management_principal_id,
                username=payload.username,
                password_hash=await password_work(hash_management_password,
                                                  payload.password.get_secret_value()),
            ),
        ))
        await session.flush()
        append_management_audit(
            session,
            actor,
            action="tenant_account.create",
            resource_type="management_principal",
            resource_id=str(principal.management_principal_id),
            tenant_id=payload.tenant_id,
            details_redacted={"role": "tenant_admin"},
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(
            status_code=409,
            detail="tenant administrator account or username already exists",
        ) from error
    await session.refresh(principal)
    return principal


@router.get("/principals", response_model=PrincipalList)
async def list_principals(
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=50, ge=1, le=100),
        _: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> PrincipalList:
    total = await session.scalar(select(func.count()).select_from(ManagementPrincipal))
    rows = (await session.scalars(
        select(ManagementPrincipal).order_by(
            ManagementPrincipal.created_at,
            ManagementPrincipal.management_principal_id,
        ).offset(offset).limit(limit))).all()
    return PrincipalList(items=[PrincipalRead.model_validate(row) for row in rows],
                         total=total or 0)


@router.patch("/principals/{principal_id}", response_model=PrincipalRead)
async def update_principal(
        principal_id: UUID,
        payload: PrincipalUpdate,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ManagementPrincipal:
    """Update or disable a management identity without deleting audit history."""

    principal = await session.scalar(
        select(ManagementPrincipal).where(
            ManagementPrincipal.management_principal_id == principal_id).with_for_update())
    if principal is None:
        raise HTTPException(status_code=404, detail="management principal not found")
    changes = payload.model_dump(exclude_unset=True, mode="json")
    if changes.get("status") == "disabled" and principal.status != "disabled":
        await protect_tenant_admin_principal(session, principal_id)
    for field, value in changes.items():
        setattr(principal, field, value)
    append_management_audit(
        session,
        actor,
        action="management_principal.update",
        resource_type="management_principal",
        resource_id=str(principal_id),
        details_redacted={"fields": sorted(payload.model_fields_set)},
    )
    await session.commit()
    await session.refresh(principal)
    return principal


@router.put("/principals/{principal_id}/password", status_code=status.HTTP_204_NO_CONTENT)
async def set_principal_password(
        principal_id: UUID,
        payload: PasswordCredentialSet,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> Response:
    """Create or rotate a human principal's browser login credential."""

    principal = await session.scalar(
        select(ManagementPrincipal).where(
            ManagementPrincipal.management_principal_id == principal_id).with_for_update())
    if principal is None or principal.principal_type != "human":
        raise HTTPException(status_code=404, detail="human management principal not found")
    tenant_admin_assignment = await session.scalar(
        select(RoleAssignment.role_assignment_id).where(
            RoleAssignment.management_principal_id == principal_id,
            RoleAssignment.role == "tenant_admin",
            RoleAssignment.tenant_id.is_not(None),
        ))
    credential = await session.scalar(
        select(ManagementPasswordCredential).where(
            ManagementPasswordCredential.management_principal_id == principal_id).with_for_update())
    username_changed = credential is None or credential.username != payload.username
    password_hash = await password_work(hash_management_password,
                                        payload.password.get_secret_value())
    if credential is None:
        credential = ManagementPasswordCredential(
            management_principal_id=principal_id,
            username=payload.username,
            password_hash=password_hash,
        )
        session.add(credential)
    else:
        credential.username = payload.username
        credential.password_hash = password_hash
        credential.failed_attempts = 0
        credential.locked_until = None
        credential.password_changed_at = datetime.now(timezone.utc)
    if tenant_admin_assignment is not None:
        # Keep the account listing and subsequent password-reset dialog aligned
        # with the real login username, including accounts migrated from old roles.
        principal.external_subject = f"tenant-console:{payload.username}"
    # Password rotation invalidates every previously issued browser session.
    await session.execute(
        update(ManagementWebSession).where(
            ManagementWebSession.management_principal_id == principal_id,
            ManagementWebSession.revoked_at.is_(None),
        ).values(revoked_at=datetime.now(timezone.utc)))
    try:
        await session.flush()
        append_management_audit(
            session,
            actor,
            action="management_password.rotate",
            resource_type="management_principal",
            resource_id=str(principal_id),
            details_redacted={"username_changed": username_changed},
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(status_code=409, detail="management username already exists") from error
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/principals/{principal_id}/role-assignments",
    response_model=RoleAssignmentRead,
    status_code=status.HTTP_201_CREATED,
)
async def assign_role(
        principal_id: UUID,
        payload: RoleAssignmentCreate,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> RoleAssignment:
    principal = await session.scalar(
        select(ManagementPrincipal).where(
            ManagementPrincipal.management_principal_id == principal_id).with_for_update())
    if principal is None or principal.status != "active":
        raise HTTPException(status_code=404, detail="active management principal not found")
    tenant_role = await session.scalar(
        select(RoleAssignment.role_assignment_id).where(
            RoleAssignment.management_principal_id == principal_id,
            RoleAssignment.tenant_id.is_not(None),
        ))
    if tenant_role is not None:
        raise HTTPException(
            status_code=409,
            detail="tenant administrator identity cannot receive a platform role",
        )
    assignment = RoleAssignment(
        management_principal_id=principal_id,
        tenant_id=None,
        role=payload.role.value,
    )
    session.add(assignment)
    try:
        await session.flush()
        append_management_audit(
            session,
            actor,
            action="role_assignment.create",
            resource_type="management_role_assignment",
            resource_id=str(assignment.role_assignment_id),
            tenant_id=None,
            details_redacted={"role": payload.role.value},
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(status_code=409, detail="role assignment already exists") from error
    await session.refresh(assignment)
    return assignment


@router.get(
    "/principals/{principal_id}/role-assignments",
    response_model=RoleAssignmentList,
)
async def list_role_assignments(
        principal_id: UUID,
        _: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> RoleAssignmentList:
    """List every platform and tenant role held by one management identity."""

    if await session.get(ManagementPrincipal, principal_id) is None:
        raise HTTPException(status_code=404, detail="management principal not found")
    rows = (await session.scalars(
        select(RoleAssignment).where(
            RoleAssignment.management_principal_id == principal_id).order_by(
                RoleAssignment.created_at, RoleAssignment.role_assignment_id))).all()
    return RoleAssignmentList(
        items=[RoleAssignmentRead.model_validate(row) for row in rows],
        total=len(rows),
    )


@router.delete("/role-assignments/{role_assignment_id}", status_code=204)
async def revoke_role_assignment(
        role_assignment_id: UUID,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> Response:
    """Revoke one explicit grant while retaining an append-only audit fact."""

    assignment = await session.get(RoleAssignment, role_assignment_id)
    if assignment is None:
        raise HTTPException(status_code=404, detail="role assignment not found")
    await protect_tenant_admin_assignment(session, assignment)
    append_management_audit(
        session,
        actor,
        action="role_assignment.revoke",
        resource_type="management_role_assignment",
        resource_id=str(role_assignment_id),
        tenant_id=assignment.tenant_id,
        details_redacted={"role": assignment.role},
    )
    await session.delete(assignment)
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post(
    "/principals/{principal_id}/credentials",
    response_model=CredentialIssued,
    status_code=status.HTTP_201_CREATED,
)
async def issue_credential(
        principal_id: UUID,
        payload: CredentialCreate,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> CredentialIssued:
    if await session.get(ManagementPrincipal, principal_id) is None:
        raise HTTPException(status_code=404, detail="management principal not found")
    token = f"trpc_admin_{secrets.token_urlsafe(32)}"
    credential = ManagementCredential(
        management_principal_id=principal_id,
        name=payload.name,
        token_hash=hash_management_token(token),
        token_prefix=token[:20],
        expires_at=payload.expires_at,
    )
    session.add(credential)
    await session.flush()
    append_management_audit(
        session,
        actor,
        action="management_credential.issue",
        resource_type="management_credential",
        resource_id=str(credential.credential_id),
    )
    await session.commit()
    await session.refresh(credential)
    return CredentialIssued(**CredentialRead.model_validate(credential).model_dump(), token=token)


@router.get("/principals/{principal_id}/credentials", response_model=CredentialList)
async def list_credentials(
        principal_id: UUID,
        _: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> CredentialList:
    if await session.get(ManagementPrincipal, principal_id) is None:
        raise HTTPException(status_code=404, detail="management principal not found")
    rows = (await session.scalars(
        select(ManagementCredential).where(
            ManagementCredential.management_principal_id == principal_id, ).order_by(
                ManagementCredential.created_at))).all()
    return CredentialList(
        items=[CredentialRead.model_validate(row) for row in rows],
        total=len(rows),
    )


@router.delete("/credentials/{credential_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_credential(
        credential_id: UUID,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> Response:
    credential = await session.get(ManagementCredential, credential_id)
    if credential is None:
        raise HTTPException(status_code=404, detail="management credential not found")
    credential.status = "disabled"
    append_management_audit(
        session,
        actor,
        action="management_credential.revoke",
        resource_type="management_credential",
        resource_id=str(credential_id),
    )
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/model-catalog", response_model=ModelCatalogRead, status_code=201)
async def create_model_catalog_entry(
        payload: ModelCatalogCreate,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ModelCatalogRead:
    row = ModelCatalogEntry(**payload.model_dump(mode="json"))
    session.add(row)
    try:
        await session.flush()
        append_management_audit(
            session,
            actor,
            action="model_catalog.create",
            resource_type="model_catalog_entry",
            resource_id=str(row.model_catalog_id),
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(status_code=409, detail="model already exists in catalog") from error
    await session.refresh(row)
    return _catalog_read(row)


@router.get("/model-catalog", response_model=ModelCatalogList)
async def list_model_catalog(
        _: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ModelCatalogList:
    rows = (await session.scalars(
        select(ModelCatalogEntry).order_by(
            ModelCatalogEntry.provider,
            ModelCatalogEntry.model_name,
        ))).all()
    return ModelCatalogList(items=[_catalog_read(row) for row in rows], total=len(rows))


@router.patch("/model-catalog/{model_catalog_id}", response_model=ModelCatalogRead)
async def update_model_catalog_entry(
        model_catalog_id: UUID,
        payload: ModelCatalogUpdate,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ModelCatalogRead:
    row = await session.get(ModelCatalogEntry, model_catalog_id)
    if row is None:
        raise HTTPException(status_code=404, detail="model catalog entry not found")
    changes = payload.model_dump(exclude_unset=True, mode="json")
    if changes.get("status") == "disabled" and row.status != "disabled":
        await _reject_dependency_disable_with_active_agents(
            session,
            model_catalog_id=model_catalog_id,
        )
    for field, value in changes.items():
        setattr(row, field, value)
    append_management_audit(
        session,
        actor,
        action="model_catalog.update",
        resource_type="model_catalog_entry",
        resource_id=str(model_catalog_id),
        details_redacted={"fields": sorted(payload.model_fields_set)},
    )
    try:
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(status_code=409, detail="model already exists in catalog") from error
    await session.refresh(row)
    return _catalog_read(row)


@router.delete("/model-catalog/{model_catalog_id}", status_code=status.HTTP_204_NO_CONTENT)
async def disable_model_catalog_entry(
        model_catalog_id: UUID,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> Response:
    """Remove a catalog entry from active selection without erasing history."""

    row = await session.get(ModelCatalogEntry, model_catalog_id)
    if row is None:
        raise HTTPException(status_code=404, detail="model catalog entry not found")
    if row.status != "disabled":
        await _reject_dependency_disable_with_active_agents(
            session,
            model_catalog_id=model_catalog_id,
        )
        row.status = "disabled"
    append_management_audit(
        session,
        actor,
        action="model_catalog.disable",
        resource_type="model_catalog_entry",
        resource_id=str(model_catalog_id),
    )
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/channel-adapter-types", response_model=ChannelAdapterRead, status_code=201)
async def create_channel_adapter_type(
        payload: ChannelAdapterCreate,
        request: Request,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ChannelAdapterType:
    if (payload.status.value == "active"
            and payload.channel_type not in request.app.state.container.channels.supported_types):
        raise HTTPException(status_code=409,
                            detail="channel adapter implementation is not registered")
    row = ChannelAdapterType(**payload.model_dump(mode="json"))
    session.add(row)
    try:
        await session.flush()
        append_management_audit(
            session,
            actor,
            action="channel_adapter_type.create",
            resource_type="channel_adapter_type",
            resource_id=row.channel_type,
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(status_code=409,
                            detail="channel adapter type already exists") from error
    await session.refresh(row)
    return row


@router.get("/channel-adapter-types", response_model=ChannelAdapterList)
async def list_channel_adapter_types(
        _: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ChannelAdapterList:
    rows = (await
            session.scalars(select(ChannelAdapterType).order_by(ChannelAdapterType.channel_type)
                            )).all()
    return ChannelAdapterList(
        items=[ChannelAdapterRead.model_validate(row) for row in rows],
        total=len(rows),
    )


@router.patch("/channel-adapter-types/{channel_type}", response_model=ChannelAdapterRead)
async def update_channel_adapter_type(
        channel_type: str,
        payload: ChannelAdapterUpdate,
        request: Request,
        actor: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ChannelAdapterType:
    row = await session.get(ChannelAdapterType, channel_type)
    if row is None:
        raise HTTPException(status_code=404, detail="channel adapter type not found")
    target_status = payload.status.value if payload.status is not None else row.status
    if (target_status == "active"
            and channel_type not in request.app.state.container.channels.supported_types):
        raise HTTPException(status_code=409,
                            detail="channel adapter implementation is not registered")
    for field, value in payload.model_dump(exclude_unset=True, mode="json").items():
        setattr(row, field, value)
    append_management_audit(
        session,
        actor,
        action="channel_adapter_type.update",
        resource_type="channel_adapter_type",
        resource_id=channel_type,
        details_redacted={"fields": sorted(payload.model_fields_set)},
    )
    await session.commit()
    await session.refresh(row)
    return row


@router.get("/audit", response_model=ManagementAuditList)
async def list_management_audit(
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=100, ge=1, le=500),
        _: ManagementActor = Depends(require_platform_admin),
        session: AsyncSession = Depends(get_session),
) -> ManagementAuditList:
    total = await session.scalar(select(func.count()).select_from(ManagementAuditLog))
    rows = (await session.scalars(
        select(ManagementAuditLog).order_by(
            ManagementAuditLog.occurred_at.desc(),
            ManagementAuditLog.audit_id,
        ).offset(offset).limit(limit))).all()
    return ManagementAuditList(
        items=[ManagementAuditRead.model_validate(row) for row in rows],
        total=total or 0,
    )
