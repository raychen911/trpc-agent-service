"""Worker FastAPI application factory."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from contextlib import asynccontextmanager

from fastapi import FastAPI

from trpc_service.agent.app import AgentApp
from trpc_service.storage.backend_resolver import TenantStateBackendResolver
from trpc_service.agent.execution_coordinator import RedisSessionExecutionCoordinator
from trpc_service.agent.tool_registry import AllowedToolRegistry
from trpc_service.config.model import close_model_http_clients
from trpc_service.config.tenant_repository import TenantConfigRepository
from trpc_service.storage.approval_repository import SqlToolApprovalRepository
from trpc_service.storage.database import DatabaseSettings, create_database_engine
from trpc_service.storage.message_repository import SqlMessageReceiptRepository
from trpc_service.storage.backend_capabilities import S3Settings, TenantBackendCapabilitiesResolver
from trpc_service.storage.tenant_repository import SqlTenantConfigRepository
from trpc_service.storage.usage_repository import SqlUsageRepository
from trpc_service.usage.pricing import ModelPricing
from trpc_service.telemetry.asgi import TraceRequestMiddleware
from trpc_service.telemetry.runtime import SPAN_WORKER_REQUEST, TelemetryRuntime
from trpc_service.telemetry.settings import TelemetrySettings
from trpc_service.telemetry.tool import tracer_for
from trpc_service.transport.auth import InternalToken
from trpc_service.worker.approval_service import ToolApprovalService
from trpc_service.worker.routes import register_worker_routes
from trpc_service.worker.service import WorkerService

logger = logging.getLogger(__name__)


def create_worker_app(
    worker_service: WorkerService | None = None,
    internal_token: InternalToken | None = None,
    tenant_repository: TenantConfigRepository | None = None,
    environ: Mapping[str, str] | None = None,
    approval_service: ToolApprovalService | None = None,
    telemetry: TelemetryRuntime | None = None,
) -> FastAPI:
    if worker_service is not None and tenant_repository is not None:
        raise ValueError("Pass worker_service or tenant_repository, not both")

    if telemetry is None:
        # Factory-as-entrypoint (uvicorn calls create_worker_app() bare):
        # build the app-scoped runtime from the environment.  Disabled by
        # default; disabled config never touches the network.
        telemetry = TelemetryRuntime(TelemetrySettings.from_env("worker", environ))
    worker_tracer = tracer_for(telemetry, "trpc-service.worker")

    owns_repository = False
    owns_engine = False
    engine = None
    receipt_repository = None
    approval_repository = None
    usage_repository = None
    capabilities_resolver = None

    if worker_service is not None:
        if internal_token is None:
            raise ValueError("internal_token is required when worker_service is injected")
        service = worker_service
        token = internal_token
    else:
        if internal_token is None:
            internal_token = InternalToken.from_env(environ)
        token = internal_token

        owns_repository = tenant_repository is None
        if tenant_repository is None:
            settings = DatabaseSettings.from_env(environ)
            engine = create_database_engine(settings)
            owns_engine = True
            tenant_repository = SqlTenantConfigRepository(engine, owns_engine=False)

        # Stage 6A2: message + approval repositories share ONE engine; the
        # pause transaction spans both domains atomically.
        receipt_repository = SqlMessageReceiptRepository(engine, owns_engine=False) if engine else None
        approval_repository = SqlToolApprovalRepository(engine, owns_engine=False) if engine else None
        # Stage 6C: usage accounting shares the same engine; the pricing
        # table is a strict startup input (invalid file = hard failure).
        usage_repository = SqlUsageRepository(engine, owns_engine=False) if engine else None
        pricing = ModelPricing.from_env(environ)

        # R1A: one Redis + one SQL state backend per Worker; tenants select
        # per their versioned backend_profile.  Readiness gate unchanged: the
        # resolver pings the shared Redis backend (mandatory for the
        # coordinator and Redis-state tenants); SQL reachability stays covered
        # by the repository check_ready below and fail-closed request paths.
        backend_resolver = TenantStateBackendResolver.from_env(environ)
        backend_resolver.check_ready()
        if engine is not None:
            capabilities_resolver = TenantBackendCapabilitiesResolver(
                backend_resolver,
                engine,
                S3Settings.from_env(environ),
            )

        # The Redis execution coordinator stays mandatory for BOTH backend
        # choices so same-Session execution remains serialized across Workers.
        coordinator = RedisSessionExecutionCoordinator.from_env(environ)

        # P1-4: exactly ONE registry instance serves both the runtime tool
        # construction (AgentApp) and the approved-execution boundary.
        tool_registry = AllowedToolRegistry.default(telemetry=telemetry)
        agent_app = AgentApp.from_env(
            environ,
            backend_resolver=None if capabilities_resolver is not None else backend_resolver,
            backend_capabilities_resolver=capabilities_resolver,
            coordinator=coordinator,
            tool_registry=tool_registry,
            telemetry=telemetry,
        )
        service = WorkerService(
            tenant_repository=tenant_repository,
            agent_app=agent_app,
            receipt_repository=receipt_repository,
            approval_repository=approval_repository,
            usage_repository=usage_repository,
            pricing=pricing,
            telemetry=telemetry,
        )
        if engine is not None:
            approval_service = ToolApprovalService(
                tenant_repository=tenant_repository,
                receipt_repository=receipt_repository,
                approval_repository=approval_repository,
                agent_app=agent_app,
                tool_registry=tool_registry,
                usage_repository=usage_repository,
                pricing=pricing,
                telemetry=telemetry,
            )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.worker_service = service
        try:
            await telemetry.start()
            if owns_repository:
                await tenant_repository.check_ready()
            if receipt_repository is not None:
                await receipt_repository.check_ready()
            if approval_repository is not None:
                await approval_repository.check_ready()
            if usage_repository is not None:
                await usage_repository.check_ready()
            if capabilities_resolver is not None:
                await capabilities_resolver.check_ready()
            yield
        finally:
            await service.close()
            # Model execution and the Agent runtime are now stopped; release
            # the shared HTTP keep-alive clients. Bounded and non-raising, and
            # guarded so a cleanup fault can never block resource shutdown.
            try:
                await close_model_http_clients()
            except Exception:
                logger.warning("model http client cleanup failed at shutdown (component=worker)")
            if approval_repository is not None:
                await approval_repository.close()
            if usage_repository is not None:
                await usage_repository.close()
            if receipt_repository is not None:
                await receipt_repository.close()
            if owns_repository:
                await tenant_repository.close()
            if owns_engine and engine is not None:
                await engine.dispose()
            # Bounded, never raises; readiness/shutdown semantics unchanged.
            await telemetry.close()

    app = FastAPI(lifespan=lifespan)
    register_worker_routes(app, worker_service=service, internal_token=token, approval_service=approval_service)
    if worker_tracer is not None:
        # Outermost tracing layer: one worker.request SERVER span per route
        # hit; /health excluded inside the middleware.
        app.add_middleware(TraceRequestMiddleware, tracer=worker_tracer, span_name=SPAN_WORKER_REQUEST)
    return app


__all__ = ["create_worker_app"]
