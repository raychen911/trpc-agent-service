import asyncio
from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session, sessionmaker

from trpc_service.domain import AppStatus, BackendKind, ConfigStatus, PermissionEffect, TenantStatus
from trpc_service.governance.tools import ToolPolicy
from trpc_service.storage.models import (
    AgentApp,
    AgentAppRevision,
    BackendConfig,
    ModelConfig,
    Tenant,
    ToolPermission,
)
from trpc_service.tenant.errors import InvalidStateError, NotFoundError


@dataclass(frozen=True, slots=True)
class RuntimeModelConfig:
    provider: str
    model_name: str
    base_url: str | None
    api_key_secret_ref: str | None
    parameters: dict[str, Any]


@dataclass(frozen=True, slots=True)
class RuntimeBackendConfig:
    kind: BackendKind
    backend_type: str
    secret_ref: str | None
    options: dict[str, Any]


@dataclass(frozen=True, slots=True)
class AgentRuntimeConfig:
    tenant_id: str
    agent_app_id: str
    app_slug: str
    app_name: str
    version: int
    description: str
    instruction: str
    application_config: dict[str, Any]
    model: RuntimeModelConfig
    allowed_tools: tuple[str, ...]
    tool_policies: tuple[ToolPolicy, ...]
    backends: tuple[RuntimeBackendConfig, ...]


class AgentRuntimeConfigRepository:
    def __init__(self, session_factory: sessionmaker[Session]) -> None:
        self._factory = session_factory

    async def load_active(self, tenant_id: str, app_id: str) -> AgentRuntimeConfig:
        return await asyncio.to_thread(self._load_active_sync, tenant_id, app_id)

    def _load_active_sync(self, tenant_id: str, app_id: str) -> AgentRuntimeConfig:
        with self._factory() as session:
            tenant = session.get(Tenant, tenant_id)
            if tenant is None:
                raise NotFoundError("tenant not found")
            if tenant.status != TenantStatus.ACTIVE:
                raise InvalidStateError("tenant is not active")
            app = session.scalar(
                select(AgentApp).where(
                    AgentApp.tenant_id == tenant_id,
                    AgentApp.id == app_id,
                )
            )
            if app is None:
                raise NotFoundError("agent app not found")
            if app.status != AppStatus.ACTIVE or app.active_version is None:
                raise InvalidStateError("agent app has no active configuration")
            version = app.active_version
            revision = session.scalar(
                select(AgentAppRevision).where(
                    AgentAppRevision.tenant_id == tenant_id,
                    AgentAppRevision.agent_app_id == app_id,
                    AgentAppRevision.version == version,
                    AgentAppRevision.status == ConfigStatus.PUBLISHED,
                )
            )
            model = session.scalar(
                select(ModelConfig).where(
                    ModelConfig.tenant_id == tenant_id,
                    ModelConfig.agent_app_id == app_id,
                    ModelConfig.config_version == version,
                )
            )
            if revision is None or model is None:
                raise InvalidStateError("active configuration is incomplete")
            tools = session.scalars(
                select(ToolPermission).where(
                    ToolPermission.tenant_id == tenant_id,
                    ToolPermission.agent_app_id == app_id,
                    ToolPermission.config_version == version,
                    ToolPermission.effect == PermissionEffect.ALLOW,
                )
            )
            backends = session.scalars(
                select(BackendConfig).where(
                    BackendConfig.tenant_id == tenant_id,
                    BackendConfig.agent_app_id == app_id,
                    BackendConfig.config_version == version,
                )
            )
            return AgentRuntimeConfig(
                tenant_id=tenant_id,
                agent_app_id=app_id,
                app_slug=app.slug,
                app_name=app.name,
                version=version,
                description=revision.description,
                instruction=revision.instruction,
                application_config=dict(revision.application_config),
                model=RuntimeModelConfig(
                    provider=model.provider,
                    model_name=model.model_name,
                    base_url=model.base_url,
                    api_key_secret_ref=model.api_key_secret_ref,
                    parameters=dict(model.parameters),
                ),
                allowed_tools=tuple(sorted(item.tool_name for item in tools)),
                tool_policies=tuple(
                    ToolPolicy(
                        name=item.tool_name,
                        requires_confirmation=item.requires_confirmation,
                        constraints=dict(item.constraints),
                    )
                    for item in tools
                ),
                backends=tuple(
                    RuntimeBackendConfig(
                        kind=item.backend_kind,
                        backend_type=item.backend_type,
                        secret_ref=item.secret_ref,
                        options=dict(item.options),
                    )
                    for item in backends
                ),
            )
