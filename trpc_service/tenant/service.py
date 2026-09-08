from datetime import datetime, timezone

from sqlalchemy import delete, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from trpc_service.domain import AppStatus, BackendKind, ConfigStatus, TenantStatus
from trpc_service.storage.models import (
    AgentApp,
    AgentAppRevision,
    BackendConfig,
    ChannelBinding,
    ModelConfig,
    Tenant,
    ToolPermission,
)
from trpc_service.tenant.errors import ConflictError, InvalidStateError, NotFoundError
from trpc_service.tenant.schemas import (
    AgentAppCreate,
    BackendConfigDraft,
    ChannelBindingDraft,
    DraftConfigRead,
    DraftConfigUpdate,
    ModelConfigDraft,
    TenantCreate,
    TenantUpdate,
    ToolPermissionDraft,
)


class ControlPlaneService:
    def __init__(self, session: Session) -> None:
        self.session = session

    def _commit(self) -> None:
        try:
            self.session.commit()
        except IntegrityError as error:
            self.session.rollback()
            raise ConflictError("resource conflicts with an existing record") from error

    def get_tenant(self, tenant_id: str) -> Tenant:
        tenant = self.session.get(Tenant, tenant_id)
        if tenant is None:
            raise NotFoundError("tenant not found")
        return tenant

    def list_tenants(self, offset: int, limit: int) -> list[Tenant]:
        statement = (
            select(Tenant).order_by(Tenant.created_at, Tenant.id).offset(offset).limit(limit)
        )
        return list(self.session.scalars(statement))

    def create_tenant(self, payload: TenantCreate) -> Tenant:
        tenant = Tenant(
            slug=payload.slug,
            name=payload.name,
            audit_policy=payload.audit_policy,
            key_namespace=payload.key_namespace or "pending",
        )
        self.session.add(tenant)
        try:
            self.session.flush()
            if payload.key_namespace is None:
                tenant.key_namespace = f"tenant/{tenant.id}"
            self._commit()
        except IntegrityError as error:
            self.session.rollback()
            raise ConflictError("tenant slug already exists") from error
        return tenant

    def update_tenant(self, tenant_id: str, payload: TenantUpdate) -> Tenant:
        tenant = self.get_tenant(tenant_id)
        values: dict[str, object] = {"version": payload.expected_version + 1}
        if payload.name is not None:
            values["name"] = payload.name
        if payload.status is not None:
            values["status"] = payload.status
        if payload.audit_policy is not None:
            values["audit_policy"] = payload.audit_policy
        result = self.session.execute(
            update(Tenant)
            .where(Tenant.id == tenant_id, Tenant.version == payload.expected_version)
            .values(**values)
        )
        if result.rowcount != 1:
            self.session.rollback()
            raise ConflictError("tenant was modified; reload it and retry")
        self._commit()
        self.session.refresh(tenant)
        return tenant

    def _get_app(self, tenant_id: str, app_id: str) -> AgentApp:
        statement = select(AgentApp).where(AgentApp.id == app_id, AgentApp.tenant_id == tenant_id)
        app = self.session.scalar(statement)
        if app is None:
            raise NotFoundError("agent app not found")
        return app

    def get_app(self, tenant_id: str, app_id: str) -> AgentApp:
        self.get_tenant(tenant_id)
        return self._get_app(tenant_id, app_id)

    def list_apps(self, tenant_id: str, offset: int, limit: int) -> list[AgentApp]:
        self.get_tenant(tenant_id)
        statement = (
            select(AgentApp)
            .where(AgentApp.tenant_id == tenant_id)
            .order_by(AgentApp.created_at, AgentApp.id)
            .offset(offset)
            .limit(limit)
        )
        return list(self.session.scalars(statement))

    def create_app(self, tenant_id: str, payload: AgentAppCreate) -> AgentApp:
        tenant = self.get_tenant(tenant_id)
        if tenant.status != TenantStatus.ACTIVE:
            raise InvalidStateError("only active tenants can create agent apps")
        app = AgentApp(tenant_id=tenant_id, slug=payload.slug, name=payload.name)
        self.session.add(app)
        try:
            self.session.flush()
            self.session.add(
                AgentAppRevision(
                    tenant_id=tenant_id,
                    agent_app_id=app.id,
                    version=1,
                    description=payload.description,
                    instruction=payload.instruction,
                    application_config=payload.application_config,
                )
            )
            self._commit()
        except IntegrityError as error:
            self.session.rollback()
            raise ConflictError("agent app slug already exists for this tenant") from error
        return app

    def _get_revision(self, app: AgentApp, version: int) -> AgentAppRevision:
        statement = select(AgentAppRevision).where(
            AgentAppRevision.tenant_id == app.tenant_id,
            AgentAppRevision.agent_app_id == app.id,
            AgentAppRevision.version == version,
        )
        revision = self.session.scalar(statement)
        if revision is None:
            raise NotFoundError("agent app configuration version not found")
        return revision

    def _bump_app_lock(self, app: AgentApp, expected: int) -> None:
        result = self.session.execute(
            update(AgentApp)
            .where(
                AgentApp.id == app.id,
                AgentApp.tenant_id == app.tenant_id,
                AgentApp.lock_version == expected,
            )
            .values(lock_version=expected + 1)
        )
        if result.rowcount != 1:
            self.session.rollback()
            raise ConflictError("agent app was modified; reload it and retry")
        app.lock_version = expected + 1

    def get_draft(self, tenant_id: str, app_id: str) -> DraftConfigRead:
        app = self.get_app(tenant_id, app_id)
        return self._build_config_read(app, app.draft_version)

    def _build_config_read(self, app: AgentApp, version: int) -> DraftConfigRead:
        revision = self._get_revision(app, version)
        model_row = self.session.scalar(
            select(ModelConfig).where(
                ModelConfig.agent_app_id == app.id, ModelConfig.config_version == version
            )
        )
        tool_rows = list(
            self.session.scalars(
                select(ToolPermission)
                .where(
                    ToolPermission.agent_app_id == app.id,
                    ToolPermission.config_version == version,
                )
                .order_by(ToolPermission.tool_name)
            )
        )
        channel_rows = list(
            self.session.scalars(
                select(ChannelBinding)
                .where(
                    ChannelBinding.agent_app_id == app.id,
                    ChannelBinding.config_version == version,
                )
                .order_by(ChannelBinding.channel_type, ChannelBinding.account_id)
            )
        )
        backend_rows = list(
            self.session.scalars(
                select(BackendConfig)
                .where(
                    BackendConfig.agent_app_id == app.id,
                    BackendConfig.config_version == version,
                )
                .order_by(BackendConfig.backend_kind)
            )
        )
        model = None
        if model_row is not None:
            model = ModelConfigDraft(
                provider=model_row.provider,
                model_name=model_row.model_name,
                base_url=model_row.base_url,
                api_key_secret_ref=model_row.api_key_secret_ref,
                parameters=model_row.parameters,
            )
        return DraftConfigRead(
            agent_app_id=app.id,
            tenant_id=app.tenant_id,
            version=version,
            description=revision.description,
            instruction=revision.instruction,
            application_config=revision.application_config,
            model=model,
            tools=[
                ToolPermissionDraft(
                    tool_name=row.tool_name,
                    effect=row.effect,
                    requires_confirmation=row.requires_confirmation,
                    constraints=row.constraints,
                )
                for row in tool_rows
            ],
            channels=[
                ChannelBindingDraft(
                    channel_type=row.channel_type,
                    account_id=row.account_id,
                    webhook_path=row.webhook_path,
                    token_secret_ref=row.token_secret_ref,
                    secret_ref=row.secret_ref,
                    enabled=row.enabled,
                    options=row.options,
                )
                for row in channel_rows
            ],
            backends=[
                BackendConfigDraft(
                    backend_kind=row.backend_kind,
                    backend_type=row.backend_type,
                    secret_ref=row.secret_ref,
                    options=row.options,
                )
                for row in backend_rows
            ],
        )

    def update_draft(
        self, tenant_id: str, app_id: str, payload: DraftConfigUpdate
    ) -> DraftConfigRead:
        app = self.get_app(tenant_id, app_id)
        revision = self._get_revision(app, app.draft_version)
        if revision.status != ConfigStatus.DRAFT:
            raise InvalidStateError("published configurations are immutable")
        self._bump_app_lock(app, payload.expected_lock_version)
        revision.description = payload.description
        revision.instruction = payload.instruction
        revision.application_config = payload.application_config

        filters = (
            ModelConfig.agent_app_id == app.id,
            ModelConfig.config_version == app.draft_version,
        )
        self.session.execute(delete(ModelConfig).where(*filters))
        self.session.execute(
            delete(ToolPermission).where(
                ToolPermission.agent_app_id == app.id,
                ToolPermission.config_version == app.draft_version,
            )
        )
        self.session.execute(
            delete(ChannelBinding).where(
                ChannelBinding.agent_app_id == app.id,
                ChannelBinding.config_version == app.draft_version,
            )
        )
        self.session.execute(
            delete(BackendConfig).where(
                BackendConfig.agent_app_id == app.id,
                BackendConfig.config_version == app.draft_version,
            )
        )
        self._add_configuration_rows(app, app.draft_version, payload)
        self._commit()
        return self._build_config_read(app, app.draft_version)

    def _add_configuration_rows(
        self, app: AgentApp, version: int, payload: DraftConfigUpdate
    ) -> None:
        if payload.model is not None:
            self.session.add(
                ModelConfig(
                    tenant_id=app.tenant_id,
                    agent_app_id=app.id,
                    config_version=version,
                    **payload.model.model_dump(),
                )
            )
        self.session.add_all(
            [
                ToolPermission(
                    tenant_id=app.tenant_id,
                    agent_app_id=app.id,
                    config_version=version,
                    **item.model_dump(),
                )
                for item in payload.tools
            ]
        )
        self.session.add_all(
            [
                ChannelBinding(
                    tenant_id=app.tenant_id,
                    agent_app_id=app.id,
                    config_version=version,
                    **item.model_dump(),
                )
                for item in payload.channels
            ]
        )
        self.session.add_all(
            [
                BackendConfig(
                    tenant_id=app.tenant_id,
                    agent_app_id=app.id,
                    config_version=version,
                    **item.model_dump(),
                )
                for item in payload.backends
            ]
        )

    def publish(
        self,
        tenant_id: str,
        app_id: str,
        expected_lock_version: int,
        *,
        allow_inmemory_session: bool = True,
    ) -> AgentApp:
        app = self.get_app(tenant_id, app_id)
        revision = self._get_revision(app, app.draft_version)
        if revision.status != ConfigStatus.DRAFT:
            raise InvalidStateError("configuration is not a draft")
        model = self.session.scalar(
            select(ModelConfig).where(
                ModelConfig.agent_app_id == app.id,
                ModelConfig.config_version == app.draft_version,
            )
        )
        if model is None:
            raise InvalidStateError("a model configuration is required before publishing")

        self._ensure_channel_accounts_available(app, app.draft_version)
        self._ensure_session_backend_allowed(app, app.draft_version, allow_inmemory_session)

        self._bump_app_lock(app, expected_lock_version)
        published_version = app.draft_version
        next_version = published_version + 1
        revision.status = ConfigStatus.PUBLISHED
        revision.published_at = datetime.now(timezone.utc)
        app.active_version = published_version
        app.draft_version = next_version
        app.status = AppStatus.ACTIVE

        current = self._build_config_read(app, published_version)
        self.session.add(
            AgentAppRevision(
                tenant_id=app.tenant_id,
                agent_app_id=app.id,
                version=next_version,
                description=current.description,
                instruction=current.instruction,
                application_config=current.application_config,
                status=ConfigStatus.DRAFT,
            )
        )
        self.session.flush()
        clone_payload = DraftConfigUpdate(
            expected_lock_version=expected_lock_version + 1,
            description=current.description,
            instruction=current.instruction,
            application_config=current.application_config,
            model=current.model,
            tools=current.tools,
            channels=current.channels,
            backends=current.backends,
        )
        self._add_configuration_rows(app, next_version, clone_payload)
        self._commit()
        self.session.refresh(app)
        return app

    def rollback(
        self,
        tenant_id: str,
        app_id: str,
        expected_lock_version: int,
        target_version: int,
        *,
        allow_inmemory_session: bool = True,
    ) -> AgentApp:
        app = self.get_app(tenant_id, app_id)
        revision = self._get_revision(app, target_version)
        if revision.status != ConfigStatus.PUBLISHED:
            raise InvalidStateError("rollback target must be a published version")
        self._ensure_channel_accounts_available(app, target_version)
        self._ensure_session_backend_allowed(app, target_version, allow_inmemory_session)
        self._bump_app_lock(app, expected_lock_version)
        app.active_version = target_version
        app.status = AppStatus.ACTIVE
        self._commit()
        self.session.refresh(app)
        return app

    def _ensure_channel_accounts_available(self, app: AgentApp, version: int) -> None:
        desired = list(
            self.session.scalars(
                select(ChannelBinding).where(
                    ChannelBinding.agent_app_id == app.id,
                    ChannelBinding.config_version == version,
                    ChannelBinding.enabled.is_(True),
                )
            )
        )
        for binding in desired:
            owner = self.session.scalar(
                select(AgentApp.id)
                .join(
                    ChannelBinding,
                    (ChannelBinding.agent_app_id == AgentApp.id)
                    & (ChannelBinding.tenant_id == AgentApp.tenant_id),
                )
                .where(
                    AgentApp.id != app.id,
                    AgentApp.status == AppStatus.ACTIVE,
                    ChannelBinding.config_version == AgentApp.active_version,
                    ChannelBinding.enabled.is_(True),
                    ChannelBinding.channel_type == binding.channel_type,
                    ChannelBinding.account_id == binding.account_id,
                )
                .limit(1)
            )
            if owner is not None:
                raise ConflictError(
                    f"{binding.channel_type.value} account {binding.account_id} "
                    "is already bound to another active Agent App"
                )

    def _ensure_session_backend_allowed(
        self, app: AgentApp, version: int, allow_inmemory: bool
    ) -> None:
        if allow_inmemory:
            return
        configured = self.session.scalar(
            select(BackendConfig).where(
                BackendConfig.agent_app_id == app.id,
                BackendConfig.config_version == version,
                BackendConfig.backend_kind == BackendKind.SESSION,
                BackendConfig.backend_type == "inmemory",
            )
        )
        if configured is not None:
            raise InvalidStateError(
                "production does not allow tenant session backend=inmemory; use sql"
            )
