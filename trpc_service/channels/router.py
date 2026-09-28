"""Tenant-scoped external channel binding CRUD endpoints."""

import hashlib
import json
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response, status
from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.admin.audit import append_management_audit
from trpc_service.admin.auth import (
    ManagementActor,
    require_tenant_admin,
)
from trpc_service.admin.models import ChannelAdapterType
from trpc_service.admin.schemas import ChannelAdapterList, ChannelAdapterRead
from trpc_service.agent.models import AgentApp
from trpc_service.agent.configuration import (
    AgentConfigurationError,
    require_agent_execution_ready,
)
from trpc_service.channels.models import ChannelBinding
from trpc_service.channels.schemas import (
    ChannelBindingCreate,
    ChannelBindingList,
    ChannelBindingRead,
    ChannelBindingStatus,
    ChannelBindingUpdate,
)
from trpc_service.config.secret_scope import validate_tenant_channel_secret_ref
from trpc_service.storage.database import get_session
from trpc_service.tenant.models import Tenant
from trpc_service.tenant.schemas import TenantStatus

router = APIRouter(prefix="/tenants/{tenant_id}/channel-bindings", tags=["channel-bindings"])
catalog_router = APIRouter(
    prefix="/tenants/{tenant_id}/channel-adapter-types",
    tags=["channel-adapter-types"],
)

_ACCOUNT_IDENTITY_FIELDS = {
    "wecom": ("bot_id", ),
    "feishu": ("app_id", ),
}


@catalog_router.get("", response_model=ChannelAdapterList)
async def list_available_channel_adapter_types(
        tenant_id: UUID,
        request: Request,
        _: ManagementActor = Depends(require_tenant_admin),
        session: AsyncSession = Depends(get_session),
) -> ChannelAdapterList:
    """Expose active adapter schemas so the tenant UI can render cards dynamically."""

    await _require_tenant(session, tenant_id)
    rows = (await session.scalars(
        select(ChannelAdapterType).where(
            ChannelAdapterType.status == "active",
            ChannelAdapterType.channel_type.in_(
                request.app.state.container.channels.supported_types),
        ).order_by(ChannelAdapterType.channel_type))).all()
    return ChannelAdapterList(
        items=[ChannelAdapterRead.model_validate(row) for row in rows],
        total=len(rows),
    )


def _binding_read(binding: ChannelBinding) -> ChannelBindingRead:
    """Expose configured secret field names without returning their references."""

    return ChannelBindingRead(
        binding_id=binding.binding_id,
        binding_public_id=binding.binding_public_id,
        tenant_id=binding.tenant_id,
        agent_app_id=binding.agent_app_id,
        channel_type=binding.channel_type,
        external_account_hash=binding.external_account_hash,
        account_config=binding.account_config,
        secret_fields=sorted(binding.secret_ref_map),
        capabilities=binding.capabilities,
        status=binding.status,
        created_at=binding.created_at,
        updated_at=binding.updated_at,
    )


def _external_account_hash(channel_type: str, account_config: dict[str, object]) -> str:
    """Derive a stable non-secret provider identity when clients omit one."""

    fields = _ACCOUNT_IDENTITY_FIELDS.get(channel_type)
    identity = ({
        field: account_config.get(field)
        for field in fields
    } if fields is not None else account_config)
    canonical = json.dumps(identity, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(f"{channel_type}:{canonical}".encode("utf-8")).hexdigest()


async def _require_tenant(session: AsyncSession, tenant_id: UUID) -> Tenant:
    """Load the route tenant before accessing any binding."""

    tenant = await session.get(Tenant, tenant_id)
    if tenant is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="tenant not found")
    return tenant


async def _require_agent(session: AsyncSession, tenant_id: UUID, agent_app_id: UUID) -> AgentApp:
    """Require an Agent to belong to the same tenant as its binding."""

    agent = await session.scalar(
        select(AgentApp).where(
            AgentApp.tenant_id == tenant_id,
            AgentApp.agent_app_id == agent_app_id,
        ))
    if agent is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="agent not found")
    return agent


async def _require_executable_agent(
    session: AsyncSession,
    tenant_id: UUID,
    agent_app_id: UUID,
) -> AgentApp:
    """Fail during IM configuration instead of after a user sends a message."""

    agent = await _require_agent(session, tenant_id, agent_app_id)
    try:
        await require_agent_execution_ready(session, agent)
    except AgentConfigurationError as error:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=error.public_detail,
        ) from error
    return agent


async def _get_binding(
    session: AsyncSession,
    tenant_id: UUID,
    binding_id: UUID,
) -> ChannelBinding:
    """Load a binding only when its identifier and tenant boundary both match."""

    binding = await session.scalar(
        select(ChannelBinding).where(
            ChannelBinding.tenant_id == tenant_id,
            ChannelBinding.binding_id == binding_id,
        ))
    if binding is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND,
                            detail="channel binding not found")
    return binding


async def _require_adapter_type(
    session: AsyncSession,
    tenant_id: UUID,
    channel_type: str,
    account_config: dict[str, object],
    secret_ref_map: dict[str, str],
    available_channel_types: tuple[str, ...],
) -> ChannelAdapterType:
    """Require an active installed adapter and its declared mandatory fields."""

    adapter = await session.get(ChannelAdapterType, channel_type)
    if adapter is None or adapter.status != "active":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail="channel adapter type is not active")
    if channel_type not in available_channel_types:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail="channel adapter implementation is unavailable on this node")
    try:
        for reference in secret_ref_map.values():
            validate_tenant_channel_secret_ref(reference, tenant_id)
    except ValueError as error:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
                            detail=str(error)) from error
    try:
        config_validator = Draft202012Validator(adapter.config_schema)
        secret_validator = Draft202012Validator(adapter.secret_schema)
    except SchemaError as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT,
                            detail="channel adapter schema is invalid") from error
    config_errors = sorted(config_validator.iter_errors(account_config),
                           key=lambda item: list(item.absolute_path))
    secret_errors = sorted(secret_validator.iter_errors(secret_ref_map),
                           key=lambda item: list(item.absolute_path))
    if config_errors or secret_errors:
        # Return only schema paths and keywords; provider values may be sensitive.
        def summarize(error: ValidationError) -> dict[str, object]:
            return {
                "path": [str(part) for part in error.absolute_path],
                "rule": str(error.validator),
            }

        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT,
            detail={
                "config_errors": [summarize(error) for error in config_errors],
                "secret_errors": [summarize(error) for error in secret_errors],
            },
        )
    return adapter


@router.post("", response_model=ChannelBindingRead, status_code=status.HTTP_201_CREATED)
async def create_channel_binding(
        tenant_id: UUID,
        payload: ChannelBindingCreate,
        request: Request,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> ChannelBindingRead:
    """Bind an external account to an Agent under an active tenant."""

    tenant = await _require_tenant(session, tenant_id)
    if tenant.status != TenantStatus.ACTIVE.value:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="tenant is disabled")
    await _require_executable_agent(session, tenant_id, payload.agent_app_id)
    account_hash = (payload.external_account_hash
                    or _external_account_hash(payload.channel_type, payload.account_config))
    secret_references = dict(payload.secret_ref_map)
    if payload.secret_values:
        try:
            secret_references = await request.app.state.container.tenant_secrets.put_many(
                session,
                tenant_id,
                f"channels/{payload.channel_type}/{account_hash}",
                payload.secret_values,
            )
        except RuntimeError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="tenant SecretStore is not configured",
            ) from error
    await _require_adapter_type(
        session,
        tenant_id,
        payload.channel_type,
        payload.account_config,
        secret_references,
        request.app.state.container.channels.supported_types,
    )

    binding = ChannelBinding(
        tenant_id=tenant_id,
        agent_app_id=payload.agent_app_id,
        channel_type=payload.channel_type,
        external_account_hash=account_hash,
        account_config=payload.account_config,
        secret_ref_map=secret_references,
        capabilities=payload.capabilities,
    )
    session.add(binding)
    try:
        await session.flush()
        append_management_audit(
            session,
            actor,
            action="channel_binding.create",
            resource_type="channel_binding",
            resource_id=str(binding.binding_id),
            tenant_id=tenant_id,
            reason=support_reason if actor.is_platform_admin else None,
            details_redacted={"channel_type": binding.channel_type},
        )
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="external channel account is already bound",
        ) from error
    await session.refresh(binding)
    return _binding_read(binding)


@router.get("", response_model=ChannelBindingList)
async def list_channel_bindings(
        tenant_id: UUID,
        offset: int = Query(default=0, ge=0),
        limit: int = Query(default=50, ge=1, le=100),
        _: ManagementActor = Depends(require_tenant_admin),
        session: AsyncSession = Depends(get_session),
) -> ChannelBindingList:
    """List only bindings owned by the route tenant."""

    await _require_tenant(session, tenant_id)
    condition = ChannelBinding.tenant_id == tenant_id
    total = await session.scalar(select(func.count()).select_from(ChannelBinding).where(condition))
    result = await session.scalars(
        select(ChannelBinding).where(condition).order_by(
            ChannelBinding.created_at, ChannelBinding.binding_id).offset(offset).limit(limit))
    return ChannelBindingList(
        items=[_binding_read(item) for item in result],
        total=total or 0,
    )


@router.get("/{binding_id}", response_model=ChannelBindingRead)
async def get_channel_binding(
        tenant_id: UUID,
        binding_id: UUID,
        _: ManagementActor = Depends(require_tenant_admin),
        session: AsyncSession = Depends(get_session),
) -> ChannelBindingRead:
    """Return a tenant-owned binding without revealing cross-tenant existence."""

    await _require_tenant(session, tenant_id)
    return _binding_read(await _get_binding(session, tenant_id, binding_id))


@router.patch("/{binding_id}", response_model=ChannelBindingRead)
async def update_channel_binding(
        tenant_id: UUID,
        binding_id: UUID,
        payload: ChannelBindingUpdate,
        request: Request,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> ChannelBindingRead:
    """Apply a partial update and revalidate a changed Agent reference."""

    await _require_tenant(session, tenant_id)
    binding = await _get_binding(session, tenant_id, binding_id)
    changes = payload.model_dump(
        exclude_unset=True,
        exclude={"secret_values", "secret_ref_map"},
    )
    target_agent_id = UUID(str(changes.get("agent_app_id", binding.agent_app_id)))
    target_status = str(changes.get("status", binding.status))
    if target_status == ChannelBindingStatus.ACTIVE.value:
        # The same gate protects reactivation, Agent reassignment and every
        # provider adapter registered through the common binding API.
        await _require_executable_agent(session, tenant_id, target_agent_id)
    elif "agent_app_id" in changes:
        await _require_agent(session, tenant_id, target_agent_id)
    secret_references = (dict(payload.secret_ref_map)
                         if payload.secret_ref_map is not None else dict(binding.secret_ref_map))
    if payload.secret_values is not None:
        try:
            rotated = await request.app.state.container.tenant_secrets.put_many(
                session,
                tenant_id,
                f"channels/{binding.channel_type}/{binding.external_account_hash}",
                payload.secret_values,
                existing=binding.secret_ref_map,
            )
        except RuntimeError as error:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="tenant SecretStore is not configured",
            ) from error
        secret_references.update(rotated)
    if (changes.get("status") == ChannelBindingStatus.ACTIVE.value
            or any(field in changes for field in {"channel_type", "account_config"})
            or payload.secret_ref_map is not None or payload.secret_values is not None):
        await _require_adapter_type(
            session,
            tenant_id,
            str(changes.get("channel_type", binding.channel_type)),
            changes.get("account_config", binding.account_config),
            secret_references,
            request.app.state.container.channels.supported_types,
        )
    for field, value in changes.items():
        setattr(binding, field, value)
    binding.secret_ref_map = secret_references
    append_management_audit(
        session,
        actor,
        action="channel_binding.update",
        resource_type="channel_binding",
        resource_id=str(binding_id),
        tenant_id=tenant_id,
        reason=support_reason if actor.is_platform_admin else None,
        details_redacted={"fields": sorted(payload.model_fields_set)},
    )
    try:
        await session.commit()
    except IntegrityError as error:
        await session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="external channel account is already bound",
        ) from error
    await session.refresh(binding)
    return _binding_read(binding)


@router.delete("/{binding_id}", status_code=status.HTTP_204_NO_CONTENT)
async def disable_channel_binding(
        tenant_id: UUID,
        binding_id: UUID,
        actor: ManagementActor = Depends(require_tenant_admin),
        support_reason: str | None = Header(default=None, alias="X-Support-Reason"),
        session: AsyncSession = Depends(get_session),
) -> Response:
    """Soft-disable a binding while retaining its audit-relevant configuration."""

    await _require_tenant(session, tenant_id)
    binding = await _get_binding(session, tenant_id, binding_id)
    binding.status = ChannelBindingStatus.DISABLED.value
    append_management_audit(
        session,
        actor,
        action="channel_binding.disable",
        resource_type="channel_binding",
        resource_id=str(binding_id),
        tenant_id=tenant_id,
        reason=support_reason if actor.is_platform_admin else None,
    )
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
