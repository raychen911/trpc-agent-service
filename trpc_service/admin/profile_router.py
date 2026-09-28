"""Tenant administrator APIs for approved model profiles."""

from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Request, Response, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.audit import append_management_audit
from trpc_service.admin.auth import (
    ManagementActor,
    require_platform_tenant_access,
    require_tenant_admin,
)
from trpc_service.admin.models import ModelCatalogEntry, ModelProfile, ModelProviderCredential
from trpc_service.admin.schemas import (
    CredentialMode,
    ModelCatalogList,
    ModelCatalogRead,
    ModelProfileCreate,
    ModelProfileList,
    ModelProfileRead,
    ModelProfileUpdate,
)
from trpc_service.agent.configuration import attach_unassigned_agents_to_default_profile
from trpc_service.agent.models import AgentApp
from trpc_service.storage.database import get_session
from trpc_service.tenant.models import Tenant

router = APIRouter(prefix="/tenants/{tenant_id}/model-profiles", tags=["model-profiles"])
catalog_router = APIRouter(prefix="/tenants/{tenant_id}/available-models", tags=["model-profiles"])


def _validate_budget_configuration(
    catalog: ModelCatalogEntry,
    parameter_config: dict[str, object],
    limits: dict[str, object],
    *,
    default_max_output_tokens: int,
) -> None:
    """Validate the effective Catalog/Profile model bounds used by runtime."""

    effective = {
        "max_output_tokens": default_max_output_tokens,
        **catalog.default_limits,
        **parameter_config,
    }
    maximum_output = effective.get("max_output_tokens")
    context_window = effective.get("context_window_tokens")
    daily_tokens = limits.get("daily_tokens")
    if daily_tokens is None:
        return
    if (isinstance(maximum_output, bool) or not isinstance(maximum_output, int)
            or maximum_output < 1):
        raise HTTPException(status_code=409,
                            detail="effective max_output_tokens must be a positive integer")
    if (isinstance(context_window, bool) or not isinstance(context_window, int)
            or context_window < maximum_output):
        raise HTTPException(
            status_code=409,
            detail="daily_tokens requires context_window_tokens >= max_output_tokens",
        )
    if (isinstance(daily_tokens, int) and not isinstance(daily_tokens, bool)
            and daily_tokens < context_window):
        raise HTTPException(status_code=409,
                            detail="daily_tokens must be at least context_window_tokens")


def _catalog_read(row: ModelCatalogEntry) -> ModelCatalogRead:
    """Expose model capability metadata without returning its SecretRef."""

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


@catalog_router.get("", response_model=ModelCatalogList)
async def list_available_models(
        tenant_id: UUID,
        _: ManagementActor = Depends(require_tenant_admin),
        session: AsyncSession = Depends(get_session),
) -> ModelCatalogList:
    """Let a tenant discover active platform-approved models safely."""

    if await session.get(Tenant, tenant_id) is None:
        raise HTTPException(status_code=404, detail="tenant not found")
    rows = (await session.scalars(
        select(ModelCatalogEntry).where(ModelCatalogEntry.status == "active").order_by(
            ModelCatalogEntry.provider, ModelCatalogEntry.model_name))).all()
    return ModelCatalogList(items=[_catalog_read(row) for row in rows], total=len(rows))


def _profile_read(row: ModelProfile) -> ModelProfileRead:
    return ModelProfileRead(
        model_profile_id=row.model_profile_id,
        tenant_id=row.tenant_id,
        model_catalog_id=row.model_catalog_id,
        credential_id=row.model_credential_id,
        name=row.name,
        credential_mode=row.credential_mode,
        secret_configured=(row.model_credential_id is not None
                           or row.credential_mode == CredentialMode.PLATFORM_MANAGED.value),
        parameter_config=row.parameter_config,
        limits=row.limits,
        status=row.status,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


async def _get_profile(
    session: AsyncSession,
    tenant_id: UUID,
    model_profile_id: UUID,
) -> ModelProfile:
    row = await session.scalar(
        select(ModelProfile).where(
            ModelProfile.tenant_id == tenant_id,
            ModelProfile.model_profile_id == model_profile_id,
        ))
    if row is None:
        raise HTTPException(status_code=404, detail="model profile not found")
    return row


async def _reject_profile_disable_with_active_agents(
    session: AsyncSession,
    tenant_id: UUID,
    model_profile_id: UUID,
) -> None:
    """Keep a live Agent from losing its model policy between IM requests."""

    active_agent_id = await session.scalar(
        select(AgentApp.agent_app_id).where(
            AgentApp.tenant_id == tenant_id,
            AgentApp.model_profile_id == model_profile_id,
            AgentApp.status == "active",
        ).limit(1))
    if active_agent_id is not None:
        raise HTTPException(
            status_code=409,
            detail="model profile is used by an active Agent; disable the Agent first",
        )


@router.post("", response_model=ModelProfileRead, status_code=status.HTTP_201_CREATED)
async def create_model_profile(
        tenant_id: UUID,
        payload: ModelProfileCreate,
        request: Request,
        actor: ManagementActor = Depends(require_platform_tenant_access),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> ModelProfileRead:
    tenant = await session.get(Tenant, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="tenant not found")
    if tenant.status != "active":
        raise HTTPException(status_code=409, detail="tenant is not active")
    catalog = await session.get(ModelCatalogEntry, payload.model_catalog_id)
    if catalog is None or catalog.status != "active":
        raise HTTPException(status_code=409, detail="model is not active in platform catalog")
    credential = await session.get(ModelProviderCredential, payload.credential_id)
    if credential is None or credential.status != "active":
        raise HTTPException(status_code=409, detail="model credential is not active")
    if credential.provider != catalog.provider:
        raise HTTPException(status_code=409, detail="model credential provider does not match")
    _validate_budget_configuration(
        catalog,
        payload.parameter_config,
        payload.limits,
        default_max_output_tokens=request.app.state.settings.llm.max_output_tokens,
    )
    row = ModelProfile(
        tenant_id=tenant_id,
        model_catalog_id=payload.model_catalog_id,
        model_credential_id=payload.credential_id,
        name=payload.name,
        credential_mode=CredentialMode.PLATFORM_MANAGED.value,
        parameter_config=payload.parameter_config,
        limits=payload.limits,
    )
    session.add(row)
    try:
        await session.flush()
        # An Agent may have been scaffolded before the platform assigned its
        # model policy. Attach it through a new released snapshot atomically.
        attached_agents = await attach_unassigned_agents_to_default_profile(
            session,
            tenant_id,
            actor_subject=actor.subject,
        )
        append_management_audit(
            session,
            actor,
            action="model_profile.create",
            resource_type="model_profile",
            resource_id=str(row.model_profile_id),
            tenant_id=tenant_id,
            reason=support_reason if actor.is_platform_admin else None,
            details_redacted={
                "credential_mode": row.credential_mode,
                "model_credential_id": str(row.model_credential_id),
                "attached_agents": attached_agents,
            },
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(status_code=409, detail="model profile name already exists") from error
    await session.refresh(row)
    return _profile_read(row)


@router.get("", response_model=ModelProfileList)
async def list_model_profiles(
        tenant_id: UUID,
        _: ManagementActor = Depends(require_platform_tenant_access),
        session: AsyncSession = Depends(get_session),
) -> ModelProfileList:
    if await session.get(Tenant, tenant_id) is None:
        raise HTTPException(status_code=404, detail="tenant not found")
    rows = (await session.scalars(
        select(ModelProfile).where(ModelProfile.tenant_id == tenant_id).order_by(
            ModelProfile.created_at, ModelProfile.model_profile_id))).all()
    return ModelProfileList(items=[_profile_read(row) for row in rows], total=len(rows))


@router.patch("/{model_profile_id}", response_model=ModelProfileRead)
async def update_model_profile(
        tenant_id: UUID,
        model_profile_id: UUID,
        payload: ModelProfileUpdate,
        request: Request,
        actor: ManagementActor = Depends(require_platform_tenant_access),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> ModelProfileRead:
    row = await _get_profile(session, tenant_id, model_profile_id)
    changes = payload.model_dump(exclude_unset=True, mode="json")
    if changes.get("status") == "disabled" and row.status != "disabled":
        await _reject_profile_disable_with_active_agents(session, tenant_id, model_profile_id)
    catalog_id = UUID(str(changes.pop("model_catalog_id", row.model_catalog_id)))
    catalog = await session.get(ModelCatalogEntry, catalog_id)
    if catalog is None:
        raise HTTPException(status_code=409, detail="model is not in platform catalog")
    raw_credential_id = changes.pop("credential_id", row.model_credential_id)
    credential_id = None if raw_credential_id is None else UUID(str(raw_credential_id))
    credential = (None if credential_id is None else await session.get(
        ModelProviderCredential, credential_id))
    if credential_id is not None and credential is None:
        raise HTTPException(status_code=409, detail="model credential does not exist")
    target_status = str(changes.get("status", row.status))
    dependencies_changed = (catalog_id != row.model_catalog_id
                            or credential_id != row.model_credential_id)
    if target_status == "active" or dependencies_changed:
        if catalog.status != "active":
            raise HTTPException(status_code=409, detail="model is not active in platform catalog")
        if credential is not None and credential.status != "active":
            raise HTTPException(status_code=409, detail="model credential is not active")
        if credential is None and catalog.platform_secret_ref is None:
            raise HTTPException(status_code=409, detail="model has no platform-managed credential")
    if credential is not None and credential.provider != catalog.provider:
        raise HTTPException(
            status_code=409,
            detail="model credential provider does not match",
        )
    _validate_budget_configuration(
        catalog,
        changes.get("parameter_config", row.parameter_config),
        changes.get("limits", row.limits),
        default_max_output_tokens=request.app.state.settings.llm.max_output_tokens,
    )
    for field, value in changes.items():
        setattr(row, field, value)
    row.model_catalog_id = catalog_id
    row.model_credential_id = credential_id
    append_management_audit(
        session,
        actor,
        action="model_profile.update",
        resource_type="model_profile",
        resource_id=str(model_profile_id),
        tenant_id=tenant_id,
        reason=support_reason if actor.is_platform_admin else None,
        details_redacted={"fields": sorted(payload.model_fields_set)},
    )
    try:
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(status_code=409, detail="model profile name already exists") from error
    await session.refresh(row)
    return _profile_read(row)


@router.delete("/{model_profile_id}", status_code=status.HTTP_204_NO_CONTENT)
async def disable_model_profile(
        tenant_id: UUID,
        model_profile_id: UUID,
        actor: ManagementActor = Depends(require_platform_tenant_access),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> Response:
    row = await _get_profile(session, tenant_id, model_profile_id)
    if row.status != "disabled":
        await _reject_profile_disable_with_active_agents(session, tenant_id, model_profile_id)
    row.status = "disabled"
    append_management_audit(
        session,
        actor,
        action="model_profile.disable",
        resource_type="model_profile",
        resource_id=str(model_profile_id),
        tenant_id=tenant_id,
        reason=support_reason if actor.is_platform_admin else None,
    )
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
