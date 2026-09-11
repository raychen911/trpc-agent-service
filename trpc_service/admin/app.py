"""Admin FastAPI application factory."""

from __future__ import annotations

from collections.abc import Mapping
from contextlib import asynccontextmanager

from fastapi import FastAPI

from trpc_service.admin.auth import AdminToken
from trpc_service.admin.routes import _domain_error_handlers
from trpc_service.admin.routes import register_admin_routes
from trpc_service.config.tenant_repository import TenantConfigAdminRepository
from trpc_service.storage.database import DatabaseSettings
from trpc_service.storage.database import create_database_engine
from trpc_service.storage.approval_repository import SqlToolApprovalRepository
from trpc_service.storage.execution_audit_repository import ExecutionAuditRepository
from trpc_service.storage.execution_audit_repository import SqlExecutionAuditRepository
from trpc_service.storage.message_repository import SqlMessageReceiptRepository
from trpc_service.storage.usage_repository import SqlUsageRepository
from trpc_service.storage.tenant_repository import SqlTenantConfigRepository
from trpc_service.storage.channel_binding_repository import SqlChannelBindingRepository
from trpc_service.storage.audit_query_repository import SqlAuditQueryRepository
from trpc_service.storage.rollout_repository import SqlTenantConfigRolloutRepository
from trpc_service.version import __version__
from trpc_service.web.schemas import HealthResponse


def create_admin_app(
    tenant_repository: TenantConfigAdminRepository | None = None,
    message_repository: SqlMessageReceiptRepository | None = None,
    execution_repository: ExecutionAuditRepository | None = None,
    usage_repository=None,
    approval_repository=None,
    channel_binding_repository=None,
    audit_query_repository=None,
    rollout_repository=None,
    environ: Mapping[str, str] | None = None,
) -> FastAPI:
    """Build the standalone Admin FastAPI app.

    Without injection: creates SqlTenantConfigRepository,
    SqlMessageReceiptRepository, SqlExecutionAuditRepository,
    SqlUsageRepository and SqlToolApprovalRepository from
    TRPC_DATABASE_URL (over one engine the factory owns and disposes once).
    With injection: all four core repositories must be injected together (for
    tests); the SQL repositories are never constructed and never closed by
    this factory — their owner stays responsible.  Half injection raises a
    fixed configuration error immediately.  The Stage 6D approval repository
    rides the same engine and the same lifecycle.
    """
    injected = (tenant_repository, message_repository, execution_repository, usage_repository)
    provided = sum(repo is not None for repo in injected)
    if provided not in (0, len(injected)):
        raise ValueError("Admin repositories must be injected together or not at all")
    token = AdminToken.from_env(environ)

    # Production mode: create all repositories over one owned engine.
    owns_repositories = provided == 0
    engine = None

    if owns_repositories:
        settings = DatabaseSettings.from_env(environ)
        engine = create_database_engine(settings)
        tenant_repository = SqlTenantConfigRepository(engine, owns_engine=False)
        message_repository = SqlMessageReceiptRepository(engine, owns_engine=False)
        execution_repository = SqlExecutionAuditRepository(engine, owns_engine=False)
        usage_repository = SqlUsageRepository(engine, owns_engine=False)
        approval_repository = SqlToolApprovalRepository(engine, owns_engine=False)
        channel_binding_repository = SqlChannelBindingRepository(engine, owns_engine=False)
        audit_query_repository = SqlAuditQueryRepository(engine, owns_engine=False)
        rollout_repository = SqlTenantConfigRolloutRepository(engine, owns_engine=False)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            await tenant_repository.check_ready()
            await message_repository.check_ready()
            await execution_repository.check_ready()
            await usage_repository.check_ready()
            if approval_repository is not None:
                await approval_repository.check_ready()
            if channel_binding_repository is not None:
                await channel_binding_repository.check_ready()
            if audit_query_repository is not None:
                await audit_query_repository.check_ready()
            if rollout_repository is not None:
                await rollout_repository.check_ready()
            yield
        finally:
            # Factory-built repositories are closed exactly once here;
            # injected ones are left untouched for their owner to close.
            if owns_repositories:
                await tenant_repository.close()
                await message_repository.close()
                await execution_repository.close()
                await usage_repository.close()
                await approval_repository.close()
                await channel_binding_repository.close()
                await audit_query_repository.close()
                await rollout_repository.close()
            if engine is not None:
                await engine.dispose()

    application = FastAPI(
        title="tRPC Agent Admin",
        version=__version__,
        lifespan=lifespan,
    )
    application.state.tenant_repository = tenant_repository
    application.state.message_repository = message_repository
    application.state.execution_repository = execution_repository
    application.state.usage_repository = usage_repository
    application.state.approval_repository = approval_repository
    application.state.channel_binding_repository = channel_binding_repository
    application.state.audit_query_repository = audit_query_repository
    application.state.rollout_repository = rollout_repository

    @application.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse(
            status="ok",
            service="trpc-agent-admin",
            version=__version__,
        )

    _domain_error_handlers(application)
    register_admin_routes(
        application,
        tenant_repository,
        token,
        message_repository=message_repository,
        execution_repository=execution_repository,
        usage_repository=usage_repository,
        approval_repository=approval_repository,
        channel_binding_repository=channel_binding_repository,
        audit_query_repository=audit_query_repository,
        rollout_repository=rollout_repository,
    )
    return application


__all__ = ["create_admin_app"]
