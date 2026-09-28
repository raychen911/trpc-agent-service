"""Tenant-scoped Agent application CRUD endpoints."""

from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.audit import append_management_audit
from trpc_service.admin.auth import (
    ManagementActor,
    require_tenant_admin,
)
from trpc_service.admin.models import ModelProfile
from trpc_service.agent.configuration import (
    AgentConfigurationError,
    resolve_active_model_policy,
    select_default_model_profile_id,
    snapshot_agent_config,
)
from trpc_service.agent.models import AgentApp, AgentConfigVersion
from trpc_service.channels.models import ChannelBinding
from trpc_service.agent.schemas import (
    AgentAppCreate,
    AgentAppList,
    AgentAppRead,
    AgentAppStatus,
    AgentAppUpdate,
    AgentConfigRelease,
    AgentConfigRollback,
    AgentConfigVersionCreate,
    AgentConfigVersionList,
    AgentConfigVersionRead,
    AgentReleaseMode,
    AgentRolloutRead,
)
from trpc_service.storage.database import get_session
from trpc_service.tenant.models import Tenant
from trpc_service.tenant.schemas import TenantStatus

router = APIRouter(prefix="/tenants/{tenant_id}/agents", tags=["agents"])

_EXECUTION_CONFIG_FIELDS = frozenset({
    "model_profile_id",
    "application_config",
    "model_settings",
    "tool_permissions",
    "knowledge_config",
    "backend_config",
})


def _validate_backends(request: Request, configured: object) -> None:
    from trpc_service.storage.router import BackendProfile
    if not isinstance(configured, dict):
        raise HTTPException(status_code=422, detail="backend_config must be an object")
    settings = request.app.state.settings
    try:
        settings.validate_execution_backends(configured)
        request.app.state.container.storage_router.resolve(
            BackendProfile.from_mapping(configured or settings.storage_profile.model_dump()))
    except (ValueError, LookupError) as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


def _enforce_model_policy_ownership(
    actor: ManagementActor,
    fields: set[str] | frozenset[str],
    *,
    model_settings: object | None = None,
) -> None:
    """Keep all model selection and parameters under platform ownership."""

    if "model_settings" in fields and model_settings not in (None, {}):
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="model parameters must be configured through the tenant Model Profile",
        )
    if "model_profile_id" in fields and not actor.is_platform_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="platform administrator role required for model policy",
        )


def _apply_snapshot(agent: AgentApp, snapshot: dict[str, object]) -> None:
    """Maintain compatibility fields as a projection of the stable pointer."""

    profile = snapshot.get("model_profile_id")
    agent.model_profile_id = None if profile is None else UUID(str(profile))
    for field in (
            "application_config",
            "model_settings",
            "tool_permissions",
            "knowledge_config",
            "backend_config",
    ):
        value = snapshot.get(field, {})
        if not isinstance(value, dict):
            raise ValueError(f"configuration snapshot field {field} must be an object")
        setattr(agent, field, dict(value))


async def _require_tenant(session: AsyncSession, tenant_id: UUID) -> Tenant:
    """Load the route tenant before accessing any tenant-owned Agent."""

    tenant = await session.get(Tenant, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="tenant not found")
    return tenant


async def _get_agent(session: AsyncSession, tenant_id: UUID, agent_app_id: UUID) -> AgentApp:
    """Load an Agent only when both its identifier and tenant boundary match."""

    agent = await session.scalar(
        select(AgentApp).where(
            AgentApp.tenant_id == tenant_id,
            AgentApp.agent_app_id == agent_app_id,
        ))
    if agent is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="agent not found")
    return agent


async def _require_model_profile(
    session: AsyncSession,
    tenant_id: UUID,
    model_profile_id: UUID | None,
) -> None:
    """Validate that an optional model selection is active and tenant-owned."""

    if model_profile_id is None:
        return
    profile = await session.scalar(
        select(ModelProfile).where(
            ModelProfile.tenant_id == tenant_id,
            ModelProfile.model_profile_id == model_profile_id,
            ModelProfile.status == "active",
        ))
    if profile is None:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail="model profile is not active for this tenant")


async def _require_executable_model_profile(
    session: AsyncSession,
    tenant_id: UUID,
    model_profile_id: UUID | None,
) -> None:
    """Validate the complete model dependency chain before activation/release."""

    try:
        await resolve_active_model_policy(session, tenant_id, model_profile_id)
    except AgentConfigurationError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error.public_detail,
        ) from error


async def _has_active_channel_binding(
    session: AsyncSession,
    tenant_id: UUID,
    agent_app_id: UUID,
) -> bool:
    """Return whether changing this Agent can affect live external traffic."""

    binding_id = await session.scalar(
        select(ChannelBinding.binding_id).where(
            ChannelBinding.tenant_id == tenant_id,
            ChannelBinding.agent_app_id == agent_app_id,
            ChannelBinding.status == "active",
        ).limit(1))
    return binding_id is not None


@router.post("", response_model=AgentAppRead, status_code=status.HTTP_201_CREATED)
async def create_agent(
        tenant_id: UUID,
        payload: AgentAppCreate,
        request: Request,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> AgentApp:
    """Create an Agent under an active tenant."""

    tenant = await _require_tenant(session, tenant_id)
    if tenant.status != TenantStatus.ACTIVE.value:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="tenant is disabled")
    _enforce_model_policy_ownership(
        actor,
        payload.model_fields_set,
        model_settings=payload.model_settings,
    )
    model_profile_id = payload.model_profile_id
    if "model_profile_id" not in payload.model_fields_set:
        # Tenant administrators do not own model policy. Inherit the default
        # selected by the platform instead of creating an unusable Agent.
        model_profile_id = await select_default_model_profile_id(session, tenant_id)
    await _require_model_profile(session, tenant_id, model_profile_id)

    _validate_backends(request, payload.backend_config)
    values = payload.model_dump()
    values["model_profile_id"] = model_profile_id
    agent = AgentApp(tenant_id=tenant_id, **values)
    session.add(agent)
    try:
        await session.flush()
        session.add(
            AgentConfigVersion(
                tenant_id=tenant_id,
                agent_app_id=agent.agent_app_id,
                version=1,
                status="released",
                snapshot=snapshot_agent_config(agent),
                created_by=actor.subject,
                reason="initial Agent configuration",
            ))
        append_management_audit(
            session,
            actor,
            action="agent_app.create",
            resource_type="agent_app",
            resource_id=str(agent.agent_app_id),
            tenant_id=tenant_id,
            reason=support_reason if actor.is_platform_admin else None,
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="agent name already exists for tenant",
        ) from error
    await session.refresh(agent)
    return agent


@router.get(
    "/{agent_app_id}/config-versions",
    response_model=AgentConfigVersionList,
)
async def list_agent_config_versions(
        tenant_id: UUID,
        agent_app_id: UUID,
        _: ManagementActor = Depends(require_tenant_admin),
        session: AsyncSession = Depends(get_session),
) -> AgentConfigVersionList:
    """List immutable snapshots without crossing the route tenant."""

    await _get_agent(session, tenant_id, agent_app_id)
    rows = (await session.scalars(
        select(AgentConfigVersion).where(
            AgentConfigVersion.tenant_id == tenant_id,
            AgentConfigVersion.agent_app_id == agent_app_id,
        ).order_by(AgentConfigVersion.version))).all()
    return AgentConfigVersionList(
        items=[AgentConfigVersionRead.model_validate(row) for row in rows],
        total=len(rows),
    )


@router.post(
    "/{agent_app_id}/config-versions",
    response_model=AgentConfigVersionRead,
    status_code=status.HTTP_201_CREATED,
)
async def create_agent_config_version(
        tenant_id: UUID,
        agent_app_id: UUID,
        payload: AgentConfigVersionCreate,
        request: Request,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> AgentConfigVersion:
    """Draft one immutable snapshot from the current stable configuration."""

    agent = await session.scalar(
        select(AgentApp).where(
            AgentApp.tenant_id == tenant_id,
            AgentApp.agent_app_id == agent_app_id,
        ).with_for_update())
    if agent is None:
        raise HTTPException(status_code=404, detail="agent not found")
    stable = await session.scalar(
        select(AgentConfigVersion).where(
            AgentConfigVersion.tenant_id == tenant_id,
            AgentConfigVersion.agent_app_id == agent_app_id,
            AgentConfigVersion.version == agent.stable_config_version,
        ))
    snapshot = (dict(stable.snapshot) if stable is not None else snapshot_agent_config(agent))
    changes = payload.model_dump(
        exclude={"reason"},
        exclude_unset=True,
        mode="json",
    )
    _enforce_model_policy_ownership(
        actor,
        set(changes),
        model_settings=changes.get("model_settings"),
    )
    if "model_profile_id" in changes:
        profile_value = changes["model_profile_id"]
        await _require_model_profile(
            session,
            tenant_id,
            None if profile_value is None else UUID(str(profile_value)),
        )
    snapshot.update(changes)
    _validate_backends(request, snapshot.get("backend_config", {}))
    latest = await session.scalar(
        select(func.max(AgentConfigVersion.version)).where(
            AgentConfigVersion.tenant_id == tenant_id,
            AgentConfigVersion.agent_app_id == agent_app_id,
        ))
    version = AgentConfigVersion(
        tenant_id=tenant_id,
        agent_app_id=agent_app_id,
        version=int(latest or 0) + 1,
        status="draft",
        snapshot=snapshot,
        created_by=actor.subject,
        reason=payload.reason,
    )
    session.add(version)
    await session.flush()
    append_management_audit(
        session,
        actor,
        action="agent_config_version.create",
        resource_type="agent_config_version",
        resource_id=f"{agent_app_id}:{version.version}",
        tenant_id=tenant_id,
        reason=support_reason if actor.is_platform_admin else payload.reason,
        details_redacted={"fields": sorted(changes)},
    )
    await session.commit()
    await session.refresh(version)
    return version


async def _config_version(
    session: AsyncSession,
    tenant_id: UUID,
    agent_app_id: UUID,
    version: int,
) -> AgentConfigVersion:
    row = await session.scalar(
        select(AgentConfigVersion).where(
            AgentConfigVersion.tenant_id == tenant_id,
            AgentConfigVersion.agent_app_id == agent_app_id,
            AgentConfigVersion.version == version,
        ).with_for_update())
    if row is None:
        raise HTTPException(status_code=404, detail="Agent configuration version not found")
    return row


@router.post(
    "/{agent_app_id}/config-versions/{version}/release",
    response_model=AgentRolloutRead,
)
async def release_agent_config_version(
        tenant_id: UUID,
        agent_app_id: UUID,
        version: int,
        payload: AgentConfigRelease,
        request: Request,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> AgentRolloutRead:
    """Atomically update stable or canary pointers without editing snapshots."""

    agent = await session.scalar(
        select(AgentApp).where(
            AgentApp.tenant_id == tenant_id,
            AgentApp.agent_app_id == agent_app_id,
        ).with_for_update())
    if agent is None:
        raise HTTPException(status_code=404, detail="agent not found")
    config = await _config_version(session, tenant_id, agent_app_id, version)
    stable_profile = None if agent.model_profile_id is None else str(agent.model_profile_id)
    _validate_backends(request, config.snapshot.get("backend_config", {}))
    target_profile = config.snapshot.get("model_profile_id")
    if target_profile != stable_profile:
        _enforce_model_policy_ownership(actor, {"model_profile_id"})
    if await _has_active_channel_binding(session, tenant_id, agent_app_id):
        await _require_executable_model_profile(
            session,
            tenant_id,
            None if target_profile is None else UUID(str(target_profile)),
        )
    config.status = "released"
    if payload.mode is AgentReleaseMode.STABLE:
        agent.stable_config_version = version
        agent.canary_config_version = None
        agent.canary_percent = 0
        _apply_snapshot(agent, config.snapshot)
    else:
        agent.canary_config_version = version
        agent.canary_percent = payload.canary_percent or 0
    append_management_audit(
        session,
        actor,
        action="agent_config_version.release",
        resource_type="agent_config_version",
        resource_id=f"{agent_app_id}:{version}",
        tenant_id=tenant_id,
        reason=support_reason if actor.is_platform_admin else payload.reason,
        details_redacted={
            "mode": payload.mode.value,
            "canary_percent": agent.canary_percent,
        },
    )
    await session.commit()
    return AgentRolloutRead.model_validate(agent, from_attributes=True)


@router.post(
    "/{agent_app_id}/config-versions/{version}/rollback",
    response_model=AgentRolloutRead,
)
async def rollback_agent_config_version(
        tenant_id: UUID,
        agent_app_id: UUID,
        version: int,
        payload: AgentConfigRollback,
        request: Request,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> AgentRolloutRead:
    """Return the stable pointer to a released snapshot in one transaction."""

    agent = await session.scalar(
        select(AgentApp).where(
            AgentApp.tenant_id == tenant_id,
            AgentApp.agent_app_id == agent_app_id,
        ).with_for_update())
    if agent is None:
        raise HTTPException(status_code=404, detail="agent not found")
    config = await _config_version(session, tenant_id, agent_app_id, version)
    if config.status != "released":
        raise HTTPException(status_code=409, detail="only a released configuration can be restored")
    stable_profile = None if agent.model_profile_id is None else str(agent.model_profile_id)
    _validate_backends(request, config.snapshot.get("backend_config", {}))
    target_profile = config.snapshot.get("model_profile_id")
    if target_profile != stable_profile:
        _enforce_model_policy_ownership(actor, {"model_profile_id"})
    if await _has_active_channel_binding(session, tenant_id, agent_app_id):
        await _require_executable_model_profile(
            session,
            tenant_id,
            None if target_profile is None else UUID(str(target_profile)),
        )
    prior_stable = agent.stable_config_version
    agent.stable_config_version = version
    agent.canary_config_version = None
    agent.canary_percent = 0
    _apply_snapshot(agent, config.snapshot)
    append_management_audit(
        session,
        actor,
        action="agent_config_version.rollback",
        resource_type="agent_config_version",
        resource_id=f"{agent_app_id}:{version}",
        tenant_id=tenant_id,
        reason=support_reason if actor.is_platform_admin else payload.reason,
        details_redacted={
            "from_version": prior_stable,
            "to_version": version
        },
    )
    await session.commit()
    return AgentRolloutRead.model_validate(agent, from_attributes=True)


@router.get("", response_model=AgentAppList)
async def list_agents(
        tenant_id: UUID,
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=50, ge=1, le=100),
        _: ManagementActor = Depends(require_tenant_admin),
        session: AsyncSession = Depends(get_session),
) -> AgentAppList:
    """List only Agent applications owned by the route tenant."""

    await _require_tenant(session, tenant_id)
    condition = AgentApp.tenant_id == tenant_id
    total = await session.scalar(select(func.count()).select_from(AgentApp).where(condition))
    result = await session.scalars(
        select(AgentApp).where(condition).order_by(
            AgentApp.created_at, AgentApp.agent_app_id).offset(offset).limit(limit))
    return AgentAppList(
        items=[AgentAppRead.model_validate(item) for item in result],
        total=total or 0,
    )


@router.get("/{agent_app_id}", response_model=AgentAppRead)
async def get_agent(
        tenant_id: UUID,
        agent_app_id: UUID,
        _: ManagementActor = Depends(require_tenant_admin),
        session: AsyncSession = Depends(get_session),
) -> AgentApp:
    """Return a tenant-owned Agent without revealing cross-tenant existence."""

    await _require_tenant(session, tenant_id)
    return await _get_agent(session, tenant_id, agent_app_id)


@router.patch("/{agent_app_id}", response_model=AgentAppRead)
async def update_agent(
        tenant_id: UUID,
        agent_app_id: UUID,
        payload: AgentAppUpdate,
        request: Request,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> AgentApp:
    """Apply a validated partial update within the route tenant."""

    tenant = await _require_tenant(session, tenant_id)
    agent = await session.scalar(
        select(AgentApp).where(
            AgentApp.tenant_id == tenant_id,
            AgentApp.agent_app_id == agent_app_id,
        ).with_for_update())
    if agent is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="agent not found")
    changes = payload.model_dump(exclude_unset=True)
    _enforce_model_policy_ownership(
        actor,
        payload.model_fields_set,
        model_settings=changes.get("model_settings"),
    )
    target_status = changes.get("status", agent.status)
    protects_live_binding = await _has_active_channel_binding(session, tenant_id, agent_app_id)
    if (changes.get("status") == AgentAppStatus.ACTIVE.value
            and tenant.status != TenantStatus.ACTIVE.value):
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="tenant is disabled")
    if (target_status == AgentAppStatus.ACTIVE.value and protects_live_binding
            and (changes.get("status") == AgentAppStatus.ACTIVE.value
                 or "model_profile_id" in payload.model_fields_set)):
        await _require_executable_model_profile(
            session,
            tenant_id,
            changes.get("model_profile_id", agent.model_profile_id),
        )
    elif "model_profile_id" in payload.model_fields_set:
        await _require_model_profile(session, tenant_id, payload.model_profile_id)
    _validate_backends(request, changes.get("backend_config", agent.backend_config))
    for field, value in changes.items():
        setattr(agent, field, value)
    try:
        config_fields = _EXECUTION_CONFIG_FIELDS.intersection(payload.model_fields_set)
        if config_fields:
            # Keep the legacy PATCH surface compatible, but never let it bypass
            # immutable configuration history or leave canary pointers dangling.
            latest = await session.scalar(
                select(func.max(AgentConfigVersion.version)).where(
                    AgentConfigVersion.tenant_id == tenant_id,
                    AgentConfigVersion.agent_app_id == agent_app_id,
                ))
            new_version = int(latest or 0) + 1
            session.add(
                AgentConfigVersion(
                    tenant_id=tenant_id,
                    agent_app_id=agent_app_id,
                    version=new_version,
                    status="released",
                    snapshot=snapshot_agent_config(agent),
                    created_by=actor.subject,
                    reason="configuration updated through Agent API",
                ))
            agent.stable_config_version = new_version
            agent.canary_config_version = None
            agent.canary_percent = 0
        append_management_audit(
            session,
            actor,
            action="agent_app.update",
            resource_type="agent_app",
            resource_id=str(agent_app_id),
            tenant_id=tenant_id,
            reason=support_reason if actor.is_platform_admin else None,
            details_redacted={
                "fields": sorted(payload.model_fields_set),
                "stable_config_version": agent.stable_config_version,
            },
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="agent name already exists for tenant",
        ) from error
    await session.refresh(agent)
    return agent


@router.delete("/{agent_app_id}", status_code=status.HTTP_204_NO_CONTENT)
async def disable_agent(
        tenant_id: UUID,
        agent_app_id: UUID,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> Response:
    """Soft-disable an Agent while preserving references and history."""

    await _require_tenant(session, tenant_id)
    agent = await _get_agent(session, tenant_id, agent_app_id)
    agent.status = AgentAppStatus.DISABLED.value
    append_management_audit(
        session,
        actor,
        action="agent_app.disable",
        resource_type="agent_app",
        resource_id=str(agent_app_id),
        tenant_id=tenant_id,
        reason=support_reason if actor.is_platform_admin else None,
    )
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
