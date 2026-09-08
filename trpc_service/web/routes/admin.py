from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from sqlalchemy.orm import Session

from trpc_service.channels import ChannelBindingRepository, ImIdentityRepository
from trpc_service.channels.identity import ImAccountNotBoundError
from trpc_service.domain import ChannelType
from trpc_service.tenant.schemas import (
    ActiveChannelBindingRead,
    AgentAppCreate,
    AgentAppRead,
    DraftConfigRead,
    DraftConfigUpdate,
    EffectiveBackendRead,
    ImUserIdentityRead,
    ImUserIdentityUpsert,
    PublishRequest,
    RollbackRequest,
    TenantCreate,
    TenantRead,
    TenantUpdate,
)
from trpc_service.tenant.service import ControlPlaneService
from trpc_service.web.auth import AdminPrincipal, require_admin_read, require_admin_write
from trpc_service.web.dependencies import get_db_session

router = APIRouter(prefix="/admin/v1", tags=["admin"])
DbSession = Annotated[Session, Depends(get_db_session)]


@router.post(
    "/tenants",
    response_model=TenantRead,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin_write)],
)
def create_tenant(payload: TenantCreate, session: DbSession) -> TenantRead:
    return TenantRead.model_validate(ControlPlaneService(session).create_tenant(payload))


@router.get("/tenants", response_model=list[TenantRead])
def list_tenants(
    session: DbSession,
    principal: Annotated[AdminPrincipal, Depends(require_admin_read)],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> list[TenantRead]:
    if "platform-admin" not in principal.roles and principal.tenant_ids:
        service = ControlPlaneService(session)
        return [
            TenantRead.model_validate(service.get_tenant(tenant_id))
            for tenant_id in sorted(principal.tenant_ids)[offset : offset + limit]
        ]
    return [
        TenantRead.model_validate(item)
        for item in ControlPlaneService(session).list_tenants(offset, limit)
    ]


@router.get(
    "/tenants/{tenant_id}", response_model=TenantRead, dependencies=[Depends(require_admin_read)]
)
def get_tenant(tenant_id: str, session: DbSession) -> TenantRead:
    return TenantRead.model_validate(ControlPlaneService(session).get_tenant(tenant_id))


@router.patch(
    "/tenants/{tenant_id}", response_model=TenantRead, dependencies=[Depends(require_admin_write)]
)
def update_tenant(tenant_id: str, payload: TenantUpdate, session: DbSession) -> TenantRead:
    return TenantRead.model_validate(ControlPlaneService(session).update_tenant(tenant_id, payload))


@router.post(
    "/tenants/{tenant_id}/apps",
    response_model=AgentAppRead,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(require_admin_write)],
)
def create_app(tenant_id: str, payload: AgentAppCreate, session: DbSession) -> AgentAppRead:
    return AgentAppRead.model_validate(ControlPlaneService(session).create_app(tenant_id, payload))


@router.get(
    "/tenants/{tenant_id}/apps",
    response_model=list[AgentAppRead],
    dependencies=[Depends(require_admin_read)],
)
def list_apps(
    tenant_id: str,
    session: DbSession,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> list[AgentAppRead]:
    return [
        AgentAppRead.model_validate(item)
        for item in ControlPlaneService(session).list_apps(tenant_id, offset, limit)
    ]


@router.get(
    "/tenants/{tenant_id}/apps/{app_id}",
    response_model=AgentAppRead,
    dependencies=[Depends(require_admin_read)],
)
def get_app(tenant_id: str, app_id: str, session: DbSession) -> AgentAppRead:
    return AgentAppRead.model_validate(ControlPlaneService(session).get_app(tenant_id, app_id))


@router.get(
    "/tenants/{tenant_id}/apps/{app_id}/draft",
    response_model=DraftConfigRead,
    dependencies=[Depends(require_admin_read)],
)
def get_draft(tenant_id: str, app_id: str, session: DbSession) -> DraftConfigRead:
    return ControlPlaneService(session).get_draft(tenant_id, app_id)


@router.put(
    "/tenants/{tenant_id}/apps/{app_id}/draft",
    response_model=DraftConfigRead,
    dependencies=[Depends(require_admin_write)],
)
def update_draft(
    tenant_id: str,
    app_id: str,
    payload: DraftConfigUpdate,
    session: DbSession,
) -> DraftConfigRead:
    return ControlPlaneService(session).update_draft(tenant_id, app_id, payload)


@router.post(
    "/tenants/{tenant_id}/apps/{app_id}/publish",
    response_model=AgentAppRead,
    dependencies=[Depends(require_admin_write)],
)
def publish_app(
    tenant_id: str,
    app_id: str,
    payload: PublishRequest,
    request: Request,
    session: DbSession,
) -> AgentAppRead:
    return AgentAppRead.model_validate(
        ControlPlaneService(session).publish(
            tenant_id,
            app_id,
            payload.expected_lock_version,
            allow_inmemory_session=request.app.state.settings.environment != "production",
        )
    )


@router.post(
    "/tenants/{tenant_id}/apps/{app_id}/rollback",
    response_model=AgentAppRead,
    dependencies=[Depends(require_admin_write)],
)
def rollback_app(
    tenant_id: str,
    app_id: str,
    payload: RollbackRequest,
    request: Request,
    session: DbSession,
) -> AgentAppRead:
    return AgentAppRead.model_validate(
        ControlPlaneService(session).rollback(
            tenant_id,
            app_id,
            payload.expected_lock_version,
            payload.target_version,
            allow_inmemory_session=request.app.state.settings.environment != "production",
        )
    )


@router.get(
    "/tenants/{tenant_id}/apps/{app_id}/backends/effective",
    response_model=list[EffectiveBackendRead],
    dependencies=[Depends(require_admin_read)],
)
async def get_effective_backends(
    tenant_id: str, app_id: str, request: Request, session: DbSession
) -> list[EffectiveBackendRead]:
    ControlPlaneService(session).get_app(tenant_id, app_id)
    selections = await request.app.state.services.backend_resolver.all_effective(
        tenant_id, app_id
    )
    return [
        EffectiveBackendRead(
            backend_kind=item.kind,
            configured_type=item.configured_type,
            effective_type=item.effective_type,
            source=item.source,
            runtime_supported=item.runtime_supported,
            secret_ref=item.secret_ref,
            options=dict(item.options or {}),
        )
        for item in selections
    ]


@router.get(
    "/tenants/{tenant_id}/apps/{app_id}/channel-bindings",
    response_model=list[ActiveChannelBindingRead],
    dependencies=[Depends(require_admin_read)],
)
async def list_active_channel_bindings(
    tenant_id: str, app_id: str, request: Request, session: DbSession
) -> list[ActiveChannelBindingRead]:
    ControlPlaneService(session).get_app(tenant_id, app_id)
    rows = await ChannelBindingRepository(
        request.app.state.database.session_factory
    ).list_active_for_app(tenant_id, app_id)
    base_url = request.app.state.settings.public_base_url.rstrip("/")
    return [
        ActiveChannelBindingRead(
            channel_type=row.channel_type,
            account_id=row.account_id,
            webhook_url=f"{base_url}{row.webhook_path}",
            token_secret_ref=row.token_secret_ref,
            secret_ref=row.secret_ref,
            identity_mode=str(row.options.get("identity_mode", "passthrough")),
            options=row.options,
        )
        for row in rows
    ]


@router.put(
    "/tenants/{tenant_id}/im-identities",
    response_model=ImUserIdentityRead,
    dependencies=[Depends(require_admin_write)],
)
def upsert_im_identity(
    tenant_id: str, payload: ImUserIdentityUpsert, request: Request, session: DbSession
) -> ImUserIdentityRead:
    ControlPlaneService(session).get_tenant(tenant_id)
    try:
        row = ImIdentityRepository(request.app.state.database.session_factory).upsert(
            tenant_id=tenant_id,
            **payload.model_dump(),
        )
    except ImAccountNotBoundError as error:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_ENTITY, str(error)) from error
    return ImUserIdentityRead.model_validate(row)


@router.get(
    "/tenants/{tenant_id}/im-identities",
    response_model=list[ImUserIdentityRead],
    dependencies=[Depends(require_admin_read)],
)
def list_im_identities(
    tenant_id: str,
    request: Request,
    session: DbSession,
    channel_type: ChannelType | None = None,
    account_id: str | None = None,
) -> list[ImUserIdentityRead]:
    ControlPlaneService(session).get_tenant(tenant_id)
    rows = ImIdentityRepository(request.app.state.database.session_factory).list(
        tenant_id, channel_type, account_id
    )
    return [ImUserIdentityRead.model_validate(row) for row in rows]
