"""Gateway FastAPI application factory."""

from __future__ import annotations

import logging
from collections.abc import Mapping
from contextlib import asynccontextmanager

from fastapi import FastAPI

from trpc_service.channels.adapter import AdapterRegistry
from trpc_service.channels.service_manager import ChannelServiceFactory, ChannelServiceManager
from trpc_service.channels.web_console import WebConsoleChannelAdapter
from trpc_service.config.secret_resolver import EnvSecretResolver
from trpc_service.config.tenant_repository import TenantConfigRepository
from trpc_service.gateway.channel_routes import register_console_routes
from trpc_service.gateway.channel_service import ChannelIngressService
from trpc_service.gateway.webhook_routes import register_wecom_webhook_routes
from trpc_service.gateway.client import HttpWorkerClient
from trpc_service.gateway.client import WorkerClient
from trpc_service.gateway.health import WorkerHealthManager
from trpc_service.gateway.routes import register_gateway_routes
from trpc_service.gateway.routing import RendezvousRouter, WorkerPoolSettings
from trpc_service.gateway.routed_client import RoutedWorkerClient
from trpc_service.storage.tenant_repository import SqlTenantConfigRepository
from trpc_service.storage.channel_binding_repository import (
    ChannelBindingRepository,
    SqlChannelBindingRepository,
)
from trpc_service.telemetry.asgi import TraceRequestMiddleware
from trpc_service.telemetry.runtime import SPAN_GATEWAY_REQUEST, TelemetryRuntime
from trpc_service.telemetry.settings import TelemetrySettings
from trpc_service.telemetry.tool import tracer_for
from trpc_service.transport.auth import InternalToken
from trpc_service.version import __version__
from trpc_service.web.schemas import HealthResponse

logger = logging.getLogger(__name__)


def create_gateway_app(
    worker_client: WorkerClient | None = None,
    tenant_repository: TenantConfigRepository | None = None,
    adapter_registry: AdapterRegistry | None = None,
    environ: Mapping[str, str] | None = None,
    telemetry: TelemetryRuntime | None = None,
    channel_binding_repository: ChannelBindingRepository | None = None,
    channel_service_factory: ChannelServiceFactory | None = None,
    channel_order_gate: object | None = None,
    rollout_repository=None,
) -> FastAPI:
    """Build the Gateway FastAPI app.

    Without injection: loads WorkerPoolSettings, creates RoutedWorkerClient.
    With injection: uses the provided client directly (for tests).
    """
    if environ is None:
        import os
        environ = os.environ
    if telemetry is None:
        # Factory-as-entrypoint: app-scoped runtime from env, disabled by
        # default (no network, no provider, no wrappers).
        telemetry = TelemetryRuntime(TelemetrySettings.from_env("gateway", environ))
    gateway_tracer = tracer_for(telemetry, "trpc-service.gateway")

    if adapter_registry is None:
        adapter_registry = AdapterRegistry()
        adapter_registry.register(WebConsoleChannelAdapter())
    if worker_client is not None:
        client: object = worker_client
        routed_client: RoutedWorkerClient | None = None
        health_manager: WorkerHealthManager | None = None
    else:
        token = InternalToken.from_env(environ)
        settings = WorkerPoolSettings.from_env(environ)

        clients: dict[str, HttpWorkerClient] = {}
        for ep in settings.endpoints:
            clients[ep.endpoint_id] = HttpWorkerClient(
                base_url=ep.base_url,
                internal_token=token,
                tracer=gateway_tracer,
            )

        async def _probe(ep, timeout):
            c = clients.get(ep.endpoint_id)
            if c is None:
                return False
            return await c.check_health(timeout)

        health_manager = WorkerHealthManager(
            endpoints=list(settings.endpoints),
            probe_fn=_probe,
            interval_seconds=settings.health_interval_seconds,
            timeout_seconds=settings.health_timeout_seconds,
            failure_threshold=settings.failure_threshold,
            recovery_threshold=settings.recovery_threshold,
        )
        router = RendezvousRouter()
        routed_client = RoutedWorkerClient(
            clients=clients,
            router=router,
            health_manager=health_manager,
        )
        client = routed_client

    owns_repository = tenant_repository is None
    if tenant_repository is None:
        tenant_repository = SqlTenantConfigRepository.from_env(environ)

    owns_rollout_repository = False
    if rollout_repository is None and owns_repository:
        from trpc_service.storage.rollout_repository import SqlTenantConfigRolloutRepository
        from trpc_service.storage.database import DatabaseSettings, create_database_engine
        rollout_repository = SqlTenantConfigRolloutRepository(create_database_engine(
            DatabaseSettings.from_env(environ)),
                                                              owns_engine=True)
        owns_rollout_repository = True

    # IM account authority is persisted in ChannelBinding, never in process
    # environment.  A normal production Gateway owns this repository; tests
    # can inject a small read-only double without acquiring another database
    # resource.
    owns_channel_binding_repository = False
    if channel_binding_repository is None and owns_repository:
        channel_binding_repository = SqlChannelBindingRepository.from_env(environ)
        owns_channel_binding_repository = True

    # Stage 6B2: delivery_result audit facts need the append repository.
    # Absent/unconfigured TRPC_DATABASE_URL keeps the Gateway working with
    # delivery audit disabled (same optionality as the Worker's receipts).
    execution_repository = None
    if environ.get("TRPC_DATABASE_URL"):
        from trpc_service.storage.execution_audit_repository import (
            ExecutionAuditRepositoryConfigurationError,
            SqlExecutionAuditRepository,
        )

        try:
            execution_repository = SqlExecutionAuditRepository.from_env(environ)
        except ExecutionAuditRepositoryConfigurationError:
            logger.warning("gateway delivery audit disabled (database not configured)")

    # Stage 6C: Redis atomic fixed-window rate limiter.  Created whenever
    # TRPC_REDIS_URL is present (tenants without limits never touch it);
    # absent Redis + configured limits fails closed in the ingress gate.
    rate_limiter = None
    if environ.get("TRPC_REDIS_URL"):
        from trpc_service.governance.limits import RedisTenantRateLimiter

        rate_limiter = RedisTenantRateLimiter.from_env(dict(environ))

    # The inbound-order watermark is separate from tenant rate limiting.  It
    # is only constructed when IM bindings are active; its service-level use
    # is fail-closed if Redis cannot accept an ordered platform event.
    if channel_order_gate is None and channel_binding_repository is not None and environ.get("TRPC_REDIS_URL"):
        from trpc_service.channels.order_gate import RedisChannelOrderGate

        channel_order_gate = RedisChannelOrderGate.from_env(environ)

    channel_service = ChannelIngressService(
        tenant_repository=tenant_repository,
        worker_client=client,
        execution_repository=execution_repository,
        rate_limiter=rate_limiter,
        telemetry=telemetry,
        rollout_repository=rollout_repository,
    )

    if channel_binding_repository is not None:
        if channel_service_factory is None:

            def channel_service_factory(binding, secret):
                if binding.channel == "wecom":
                    from trpc_service.channels.wecom.service import create_wecom_service
                    from trpc_service.channels.wecom.settings import WeComSettings

                    return create_wecom_service(
                        WeComSettings(bot_id=binding.external_account_id, secret=secret),
                        channel_service,
                        binding=binding,
                        order_gate=channel_order_gate,
                        tracer=gateway_tracer,
                    )
                if binding.channel == "feishu":
                    from trpc_service.channels.feishu.service import create_feishu_service
                    from trpc_service.channels.feishu.settings import FeishuSettings

                    return create_feishu_service(
                        FeishuSettings(app_id=binding.external_account_id, app_secret=secret),
                        channel_service,
                        binding=binding,
                        order_gate=channel_order_gate,
                        tracer=gateway_tracer,
                    )
                raise ValueError("unsupported channel binding")

        channel_service_manager = ChannelServiceManager(
            channel_binding_repository,
            EnvSecretResolver(environ),
            channel_service_factory,
        )
    else:
        channel_service_manager = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        try:
            await telemetry.start()
            await tenant_repository.check_ready()
            if rollout_repository is not None:
                await rollout_repository.check_ready()
            if channel_binding_repository is not None:
                await channel_binding_repository.check_ready()
            if execution_repository is not None:
                await execution_repository.check_ready()
            if rate_limiter is not None:
                await rate_limiter.check_ready()
            if channel_order_gate is not None:
                await channel_order_gate.check_ready()
            if routed_client is not None:
                await routed_client.start()
            elif hasattr(client, "start"):
                await client.start()
            app.state.worker_client = client
            if channel_service_manager is not None:
                await channel_service_manager.start()
            yield
        finally:
            if channel_service_manager is not None:
                await channel_service_manager.close()
            if routed_client is not None:
                await routed_client.close()
            else:
                await client.close()
            if channel_order_gate is not None:
                await channel_order_gate.close()
            if rate_limiter is not None:
                await rate_limiter.close()
            if execution_repository is not None:
                await execution_repository.close()
            if owns_repository:
                await tenant_repository.close()
            if owns_rollout_repository and rollout_repository is not None:
                await rollout_repository.close()
            if owns_channel_binding_repository and channel_binding_repository is not None:
                await channel_binding_repository.close()
            # Bounded, never raises; readiness/shutdown semantics unchanged.
            await telemetry.close()

    application = FastAPI(
        title="tRPC Agent Gateway",
        version=__version__,
        lifespan=lifespan,
    )
    application.state.tenant_repository = tenant_repository
    application.state.channel_binding_repository = channel_binding_repository
    application.state.channel_service_manager = channel_service_manager
    application.state.rollout_repository = rollout_repository
    if gateway_tracer is not None:
        # Outermost tracing layer: one gateway.request SERVER span per HTTP
        # request (Console entry root); /health excluded inside the middleware.
        application.add_middleware(TraceRequestMiddleware, tracer=gateway_tracer, span_name=SPAN_GATEWAY_REQUEST)

    @application.get("/health", response_model=HealthResponse)
    async def health() -> HealthResponse:
        return HealthResponse(
            status="ok",
            service="trpc-agent-service",
            version=__version__,
        )

    register_gateway_routes(application, worker_client=client)

    register_console_routes(application, channel_service, adapter_registry)
    if channel_binding_repository is not None:
        register_wecom_webhook_routes(
            application,
            channel_binding_repository,
            channel_service,
            EnvSecretResolver(environ),
        )

    return application


__all__ = ["create_gateway_app"]
