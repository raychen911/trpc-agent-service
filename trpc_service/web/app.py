import asyncio
import logging
import time
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from opentelemetry.propagate import extract
from prometheus_client import make_asgi_app

from trpc_service.config import (
    AwsKmsSecretResolver,
    CompositeSecretResolver,
    EnvironmentSecretResolver,
    Settings,
    VaultSecretResolver,
    get_settings,
)
from trpc_service.log import configure_logging
from trpc_service.metrics import PlatformMetrics, configure_tracing, tracer
from trpc_service.storage import Database
from trpc_service.storage.runtime import (
    StorageRuntime,
    run_inbound_loop,
    run_node_heartbeat,
    run_outbox_loop,
)
from trpc_service.tenant.errors import ControlPlaneError
from trpc_service.web.auth import OidcTokenVerifier
from trpc_service.web.routes import admin_router, gateway_router, health_router, root_router

logger = logging.getLogger(__name__)


def create_app(
    settings: Settings | None = None,
    database: Database | None = None,
) -> FastAPI:
    """Create an application instance without performing import-time I/O."""

    runtime_settings = settings or get_settings()
    runtime_database = database or Database(runtime_settings.database_url)
    platform_metrics = PlatformMetrics()
    runtime_services = StorageRuntime.build(
        runtime_settings, runtime_database, metrics=platform_metrics
    )
    configure_tracing(runtime_settings)
    configure_logging(runtime_settings.log_level)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        if runtime_settings.auto_create_schema:
            runtime_database.create_schema()
        await runtime_services.node_directory.heartbeat(
            runtime_settings.node_id,
            runtime_settings.node_base_url,
            capacity=runtime_settings.node_capacity,
            ttl_seconds=runtime_settings.node_ttl_seconds,
            metadata={
                "environment": runtime_settings.environment,
                "release_track": runtime_settings.release_track,
            },
        )
        node_stop = asyncio.Event()
        node_task = asyncio.create_task(
            run_node_heartbeat(runtime_services.node_directory, runtime_settings, node_stop)
        )
        outbox_stop = asyncio.Event()
        outbox_task = None
        if runtime_settings.outbox_worker_enabled:
            outbox_task = asyncio.create_task(
                run_outbox_loop(
                    runtime_services.outbox_worker,
                    outbox_stop,
                    runtime_settings.outbox_poll_seconds,
                )
            )
        inbound_stop = asyncio.Event()
        inbound_task = None
        if runtime_settings.inbound_worker_enabled:
            inbound_task = asyncio.create_task(
                run_inbound_loop(
                    runtime_services.inbound_worker,
                    inbound_stop,
                    runtime_settings.inbound_poll_seconds,
                )
            )
        logger.info(
            "service_started",
            extra={
                "service": runtime_settings.app_name,
                "version": runtime_settings.app_version,
                "environment": runtime_settings.environment,
            },
        )
        yield
        node_stop.set()
        await node_task
        await runtime_services.node_directory.unregister(runtime_settings.node_id)
        inbound_stop.set()
        if inbound_task is not None:
            await inbound_task
        outbox_stop.set()
        if outbox_task is not None:
            await outbox_task
        await runtime_services.close()
        runtime_database.dispose()
        logger.info("service_stopped", extra={"service": runtime_settings.app_name})

    app = FastAPI(
        title=runtime_settings.app_name,
        version=runtime_settings.app_version,
        debug=runtime_settings.debug,
        lifespan=lifespan,
    )
    app.state.settings = runtime_settings
    app.state.database = runtime_database
    app.state.services = runtime_services
    vault = (
        VaultSecretResolver(
            runtime_settings.vault_address,
            runtime_settings.vault_token.get_secret_value(),
            runtime_settings.vault_namespace,
        )
        if runtime_settings.vault_address and runtime_settings.vault_token
        else None
    )
    app.state.secret_resolver = CompositeSecretResolver(
        EnvironmentSecretResolver(),
        vault,
        AwsKmsSecretResolver(runtime_settings.aws_region),
    )
    app.state.oidc_verifier = (
        OidcTokenVerifier(
            runtime_settings.admin_oidc_issuer,
            runtime_settings.admin_oidc_audience,
            runtime_settings.admin_oidc_jwks_url,
        )
        if runtime_settings.admin_oidc_enabled
        else None
    )
    app.state.metrics = platform_metrics

    @app.exception_handler(ControlPlaneError)
    async def control_plane_error_handler(
        _request: Request, error: ControlPlaneError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=error.status_code,
            content={"error": {"code": error.code, "message": error.message}},
        )

    @app.middleware("http")
    async def request_context(request: Request, call_next) -> Response:
        request_id = request.headers.get("x-request-id") or str(uuid.uuid4())
        started_at = time.perf_counter()
        parent_context = extract(dict(request.headers))
        route = request.url.path
        with tracer.start_as_current_span(
            f"HTTP {request.method} {route}", context=parent_context
        ) as span:
            span.set_attribute("http.request.method", request.method)
            span.set_attribute("url.path", route)
            span.set_attribute("trpc.request_id", request_id)
            trace_id = format(span.get_span_context().trace_id, "032x")
            request.state.request_id = request_id
            request.state.trace_id = trace_id
            response = await call_next(request)
            span.set_attribute("http.response.status_code", response.status_code)
        response.headers["x-request-id"] = request_id
        response.headers["x-trace-id"] = trace_id
        elapsed = time.perf_counter() - started_at
        platform_metrics.http_requests.labels(
            request.method, route, str(response.status_code)
        ).inc()
        platform_metrics.http_latency.labels(request.method, route).observe(elapsed)
        logger.info(
            "http_request",
            extra={
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "status_code": response.status_code,
                "latency_ms": round(elapsed * 1000, 2),
                "trace_id": trace_id,
            },
        )
        return response

    app.include_router(root_router)
    app.include_router(health_router)
    app.include_router(admin_router)
    app.include_router(gateway_router)
    if runtime_settings.prometheus_enabled:
        app.mount("/metrics", make_asgi_app(registry=platform_metrics.registry))
    return app


app = create_app()
