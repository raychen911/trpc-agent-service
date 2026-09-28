"""FastAPI application factory and system health endpoints."""

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from time import perf_counter
from uuid import uuid4

from fastapi import FastAPI, Request, status
from fastapi.responses import FileResponse, Response
from fastapi.responses import JSONResponse
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncEngine

from trpc_service.admin.login_guard import LoginGuard
from trpc_service.agent.router import router as agent_router
from trpc_service.admin.profile_router import (
    catalog_router as tenant_model_catalog_router,
    router as model_profile_router,
)
from trpc_service.admin.router import router as admin_router
from trpc_service.admin.session_router import router as management_auth_router
from trpc_service.channels.router import (
    catalog_router as tenant_channel_catalog_router,
    router as channel_binding_router,
)
from trpc_service.channels.recovery_router import router as delivery_recovery_router
from trpc_service.config import Settings, get_settings
from trpc_service.container import ApplicationContainer, build_application_container
from trpc_service.log import bind_log_context
from trpc_service.mcp.router import router as mcp_connection_router
from trpc_service.skill.router import router as skill_catalog_router
from trpc_service.storage import build_engine, build_session_factory
from trpc_service.storage.knowledge_router import router as tenant_knowledge_router
from trpc_service.storage.orm import Base
from trpc_service.tenant.router import router as tenant_router
from trpc_service.version import __version__
from trpc_service.web.errors import install_exception_handlers
from trpc_service.web.body_limit import RequestBodyLimitMiddleware


def create_app(
    settings: Settings,
    engine: AsyncEngine,
    container: ApplicationContainer,
) -> FastAPI:
    """Attach HTTP routes and lifecycle hooks to an explicit dependency graph."""

    app_settings = settings
    app_engine = engine
    app_container = container

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncGenerator[None, None]:
        """Prepare optional test schemas and release the engine on shutdown."""

        try:
            # Production schemas are always migrated by Alembic. Automatic creation
            # is reserved for isolated tests that inject their own database engine.
            if app_settings.auto_create_schema:
                async with app_engine.begin() as connection:
                    await connection.run_sync(Base.metadata.create_all)
            await app_container.storage_composition.initialize()
            await app_container.start()
            yield
        finally:
            # Dispose both resource groups even when initialization or shutdown fails.
            try:
                await app_container.close()
            finally:
                try:
                    await app_container.storage_composition.close()
                finally:
                    await app_engine.dispose()

    app = FastAPI(title=app_settings.service_name, version=__version__, lifespan=lifespan)
    app.add_middleware(RequestBodyLimitMiddleware,
                       api_prefix=app_settings.api_prefix,
                       max_bytes=app_settings.http_max_body_bytes)

    @app.middleware("http")
    async def observe_http(request: Request, call_next):  # type: ignore[no-untyped-def]
        """Create one safe Gateway span and bounded HTTP metrics per request."""

        started = perf_counter()
        status_code = 500
        supplied_request_id = request.headers.get("X-Request-ID", "").strip()
        request_id = (supplied_request_id if 0 < len(supplied_request_id) <= 128 else uuid4().hex)
        parent = app_container.telemetry.extract_context(dict(request.headers))
        with app_container.telemetry.start_span("gateway.handle", context=parent) as span, \
                bind_log_context(
                    service=app_settings.service_name,
                    environment=app_settings.environment,
                    node_id=app_settings.resolved_node_id,
                    node_role=app_settings.runtime_role,
                    request_id=request_id,
                    trace_id=app_container.telemetry.current_trace_id(),
                ):
            try:
                response = await call_next(request)
                status_code = response.status_code
                span.set_attribute("result", "success" if status_code < 500 else "error")
                response.headers["X-Request-ID"] = request_id
                return response
            except Exception as error:
                span.set_attribute("result", "error")
                span.set_attribute("error.type", type(error).__name__)
                raise
            finally:
                app_container.telemetry.record_http(
                    request.method,
                    status_code,
                    perf_counter() - started,
                )

    console_assets = Path(__file__).with_name("static")
    admin_assets = console_assets / "admin"
    tenant_assets = console_assets / "tenant"
    install_exception_handlers(app)
    app.state.login_guard = LoginGuard(app_settings.management_login_concurrency,
                                       app_settings.management_login_per_minute)
    app.state.settings = app_settings
    app.state.engine = app_engine
    app.state.session_factory = app_container.session_factory
    app.state.container = app_container
    app.include_router(tenant_router, prefix=app_settings.api_prefix)
    app.include_router(admin_router, prefix=app_settings.api_prefix)
    app.include_router(management_auth_router, prefix=app_settings.api_prefix)
    app.include_router(model_profile_router, prefix=app_settings.api_prefix)
    app.include_router(tenant_model_catalog_router, prefix=app_settings.api_prefix)
    app.include_router(agent_router, prefix=app_settings.api_prefix)
    app.include_router(channel_binding_router, prefix=app_settings.api_prefix)
    app.include_router(tenant_channel_catalog_router, prefix=app_settings.api_prefix)
    app.include_router(mcp_connection_router, prefix=app_settings.api_prefix)
    app.include_router(skill_catalog_router, prefix=app_settings.api_prefix)
    app.include_router(tenant_knowledge_router, prefix=app_settings.api_prefix)
    app.include_router(delivery_recovery_router, prefix=app_settings.api_prefix)

    @app.get("/console/config.js", include_in_schema=False)
    async def console_config() -> Response:
        config = json.dumps({"apiPrefix": app_settings.api_prefix}, ensure_ascii=True)
        return Response("window.ConsoleConfig = Object.freeze(" + config + ");",
                        media_type="text/javascript",
                        headers={"Cache-Control": "no-store"})

    @app.get("/console/assets/console.css", include_in_schema=False, response_class=FileResponse)
    async def console_stylesheet() -> FileResponse:
        """Serve the shared visual tokens without exposing either console HTML file."""

        return FileResponse(console_assets / "console.css", media_type="text/css")

    @app.get("/console/assets/console.js", include_in_schema=False, response_class=FileResponse)
    async def console_script() -> FileResponse:
        """Serve the shared, dependency-free console browser helpers."""

        return FileResponse(console_assets / "console.js", media_type="text/javascript")

    @app.get("/admin/assets/admin.css", include_in_schema=False, response_class=FileResponse)
    async def admin_stylesheet() -> FileResponse:
        """Serve only the public stylesheet, not the adjacent console document."""

        return FileResponse(admin_assets / "admin.css", media_type="text/css")

    @app.get("/admin/assets/admin.js", include_in_schema=False, response_class=FileResponse)
    async def admin_script() -> FileResponse:
        """Serve the platform-console script through an explicit allowlist."""

        return FileResponse(admin_assets / "admin.js", media_type="text/javascript")

    @app.get("/tenant/assets/tenant.css", include_in_schema=False, response_class=FileResponse)
    async def tenant_stylesheet() -> FileResponse:
        """Serve only the tenant console stylesheet."""

        return FileResponse(tenant_assets / "tenant.css", media_type="text/css")

    @app.get("/tenant/assets/tenant.js", include_in_schema=False, response_class=FileResponse)
    async def tenant_script() -> FileResponse:
        """Serve only the tenant console browser logic."""

        return FileResponse(tenant_assets / "tenant.js", media_type="text/javascript")

    @app.get("/admin", include_in_schema=False, response_class=FileResponse)
    async def admin_console() -> FileResponse:
        """Serve the platform-administrator console from the API process."""

        response = FileResponse(admin_assets / "index.html", media_type="text/html")
        # The page handles a bearer credential, so prevent cached copies and
        # restrict executable content to project-owned static assets.
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
        return response

    @app.get("/tenant", include_in_schema=False, response_class=FileResponse)
    async def tenant_console() -> FileResponse:
        """Serve the isolated tenant-administrator console."""

        response = FileResponse(tenant_assets / "index.html", media_type="text/html")
        response.headers["Cache-Control"] = "no-store"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'")
        return response

    @app.get("/health", tags=["system"])
    async def health() -> dict[str, str]:
        """Report process liveness without depending on external services."""

        return {
            "status": "ok",
            "service": app_settings.service_name,
            "version": __version__,
        }

    @app.get("/metrics", tags=["system"], include_in_schema=False)
    async def metrics() -> Response:
        """Expose only the process-local low-cardinality Prometheus registry."""

        return Response(
            content=app_container.telemetry.render_prometheus(),
            media_type=app_container.telemetry.prometheus_content_type,
        )

    @app.get("/ready", tags=["system"], response_model=None)
    async def ready() -> dict[str, object] | JSONResponse:
        """Report readiness only after the primary database accepts a query."""

        try:
            async with asyncio.timeout(app_settings.readiness_timeout_seconds):
                async with app_engine.connect() as connection:
                    await connection.execute(text("SELECT 1"))
                active_workers = await app_container.node_registry.active_worker_count()
        except (SQLAlchemyError, TimeoutError):
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={
                    "status": "not_ready",
                    "checks": {
                        "database": "error"
                    }
                },
            )

        return {
            "status": "ready",
            "checks": {
                "database": "ok",
                "worker_nodes": active_workers,
            },
        }

    return app


def create_default_app() -> FastAPI:
    """Build the production ASGI application from environment configuration."""

    settings = get_settings()
    engine = build_engine(settings)
    session_factory = build_session_factory(engine)
    container = build_application_container(
        settings=settings,
        session_factory=session_factory,
    )
    return create_app(settings, engine, container)
