"""FastAPI composition root for Gateway and Admin API."""

from __future__ import annotations

import hashlib
import hmac
import logging
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated
from uuid import uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from opentelemetry import trace
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from pydantic import BaseModel, Field
from sqlalchemy import text

from trpc_service.channels.contracts import CallbackRequest, Channel
from trpc_service.channels.telegram import TelegramAuthenticationError, TelegramProtocolError
from trpc_service.channels.wecom import (
    WeComAdapter,
    WeComProtocolError,
    WeComSignatureError,
)
from trpc_service.config import Settings, get_settings
from trpc_service.log import bind_log_context, configure_logging
from trpc_service.storage import Database
from trpc_service.telemetry import configure_telemetry
from trpc_service.tenant import TenantConfigService, TenantSpec
from trpc_service.tenant.service import (
    RevisionConflictError,
    RevisionSequenceError,
    TenantConfigError,
    TenantNotFoundError,
)
from trpc_service.version import __version__
from trpc_service.web.admin import (
    AuditConsoleEntry,
    OperatorConsoleService,
    PlatformConsoleOverview,
    TenantRevisionSummary,
)
from trpc_service.web.ingress import (
    ChannelIngressService,
    IngressConfigurationError,
    IngressRouteNotFoundError,
    make_callback_request,
)

LOGGER = logging.getLogger(__name__)
_REQUEST_ID = re.compile(r"\A[A-Za-z0-9._-]{1,64}\Z")
_WEB_ROOT = Path(__file__).with_name("static")


class RollbackRequest(BaseModel):
    """Admin request to rematerialize an immutable revision."""

    target_revision: int = Field(ge=1)


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings: Settings = app.state.settings
    configure_logging(settings.log_level)
    provider = configure_telemetry(settings)
    database = Database(settings.database_url)
    app.state.database = database
    app.state.tenant_configs = TenantConfigService(database.session_factory)
    app.state.operator_console = OperatorConsoleService(database.session_factory)
    app.state.channel_ingress = ChannelIngressService(
        session_factory=database.session_factory,
        settings=settings,
    )
    LOGGER.info("service_started", extra={"version": __version__, "environment": settings.env})
    try:
        yield
    finally:
        await database.dispose()
        if provider is not None:
            provider.shutdown()
        LOGGER.info("service_stopped")


def create_app(
    settings: Settings | None = None,
    *,
    clock: Callable[[], datetime] | None = None,
) -> FastAPI:
    """Create an isolated app instance for production or tests."""

    resolved = settings or get_settings()
    app = FastAPI(
        title="tRPC-Agent Multi-Tenant Platform",
        version=__version__,
        docs_url="/docs" if resolved.env.value != "production" else None,
        redoc_url=None,
        lifespan=_lifespan,
    )
    app.state.settings = resolved
    app.state.clock = clock or (lambda: datetime.now(UTC))
    app.mount(
        "/console/assets",
        StaticFiles(directory=_WEB_ROOT),
        name="console-assets",
    )
    app.middleware("http")(_correlation_middleware)
    _register_system_routes(app)
    _register_admin_routes(app)
    _register_channel_routes(app)
    _register_exception_handlers(app)
    FastAPIInstrumentor.instrument_app(app, excluded_urls="health/live,metrics")
    return app


async def _correlation_middleware(
    request: Request,
    call_next: Callable[[Request], Awaitable[Response]],
) -> Response:
    supplied = request.headers.get("x-request-id", "")
    request_id = supplied if _REQUEST_ID.fullmatch(supplied) else uuid4().hex
    current_span = trace.get_current_span().get_span_context()
    trace_id = (
        f"{current_span.trace_id:032x}"
        if current_span.is_valid
        else hashlib.sha256(request_id.encode()).hexdigest()[:32]
    )
    request.state.request_id = request_id
    request.state.trace_id = trace_id
    with bind_log_context(request_id=request_id, trace_id=trace_id):
        response = await call_next(request)
    response.headers["x-request-id"] = request_id
    response.headers["x-trace-id"] = trace_id
    response.headers["x-content-type-options"] = "nosniff"
    response.headers["cache-control"] = "no-store"
    response.headers["referrer-policy"] = "no-referrer"
    response.headers["x-frame-options"] = "DENY"
    response.headers["permissions-policy"] = "camera=(), microphone=(), geolocation=()"
    if request.url.path == "/console" or request.url.path.startswith("/console/"):
        response.headers["content-security-policy"] = (
            "default-src 'self'; script-src 'self'; style-src 'self'; "
            "img-src 'self' data:; connect-src 'self'; object-src 'none'; "
            "base-uri 'none'; frame-ancestors 'none'; form-action 'self'"
        )
    return response


def _register_system_routes(app: FastAPI) -> None:
    @app.get("/", include_in_schema=False)
    async def root() -> RedirectResponse:
        return RedirectResponse("/console", status_code=status.HTTP_307_TEMPORARY_REDIRECT)

    @app.get("/console", include_in_schema=False, response_class=HTMLResponse)
    async def console() -> HTMLResponse:
        return HTMLResponse((_WEB_ROOT / "index.html").read_text(encoding="utf-8"))

    @app.get("/health/live", include_in_schema=False)
    async def live() -> dict[str, str]:
        return {"status": "alive", "version": __version__}

    @app.get("/health/ready", include_in_schema=False)
    async def ready(request: Request) -> JSONResponse:
        database: Database = request.app.state.database
        try:
            async with database.session_factory() as session:
                await session.execute(text("SELECT 1"))
        except Exception:
            LOGGER.exception("readiness_database_failed")
            return JSONResponse({"status": "not_ready"}, status_code=503)
        return JSONResponse({"status": "ready"})

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


def _admin_actor(x_admin_actor: Annotated[str | None, Header()] = None) -> str:
    return (x_admin_actor or "admin:unknown")[:128]


def _admin_authorized(
    request: Request,
    x_admin_key: Annotated[str | None, Header()] = None,
) -> None:
    expected = request.app.state.settings.admin_api_key.get_secret_value()
    if x_admin_key is None or not hmac.compare_digest(x_admin_key, expected):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="unauthorized")


def _register_admin_routes(app: FastAPI) -> None:
    authorization = Depends(_admin_authorized)

    @app.get(
        "/v1/admin/overview",
        response_model=PlatformConsoleOverview,
        dependencies=[authorization],
    )
    async def platform_overview(request: Request) -> PlatformConsoleOverview:
        service: OperatorConsoleService = request.app.state.operator_console
        return await service.overview(environment=request.app.state.settings.env.value)

    @app.get(
        "/v1/admin/tenants/{tenant_id}/revisions",
        response_model=list[TenantRevisionSummary],
        dependencies=[authorization],
    )
    async def tenant_revisions(
        tenant_id: str,
        request: Request,
    ) -> tuple[TenantRevisionSummary, ...]:
        service: OperatorConsoleService = request.app.state.operator_console
        return await service.revisions(tenant_id)

    @app.get(
        "/v1/admin/tenants/{tenant_id}/activity",
        response_model=list[AuditConsoleEntry],
        dependencies=[authorization],
    )
    async def tenant_activity(
        tenant_id: str,
        request: Request,
        limit: int = 30,
    ) -> tuple[AuditConsoleEntry, ...]:
        if limit < 1 or limit > 100:
            raise HTTPException(status_code=422, detail="limit must be between 1 and 100")
        service: OperatorConsoleService = request.app.state.operator_console
        return await service.activity(tenant_id, limit=limit)

    @app.post(
        "/v1/admin/tenants/{tenant_id}/revisions",
        status_code=status.HTTP_201_CREATED,
        dependencies=[authorization],
    )
    async def publish_tenant(
        tenant_id: str,
        spec: TenantSpec,
        request: Request,
        actor: Annotated[str, Depends(_admin_actor)],
    ) -> dict[str, object]:
        if tenant_id != spec.tenant_id:
            raise HTTPException(status_code=400, detail="tenant_id path/body mismatch")
        service: TenantConfigService = request.app.state.tenant_configs
        try:
            result = await service.publish(spec, actor=actor)
        except (RevisionConflictError, RevisionSequenceError) as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        return {
            "tenant_id": result.tenant_id,
            "revision": result.revision,
            "content_hash": result.content_hash,
            "idempotent": result.idempotent,
        }

    @app.post(
        "/v1/admin/tenants/{tenant_id}/rollback",
        dependencies=[authorization],
    )
    async def rollback_tenant(
        tenant_id: str,
        body: RollbackRequest,
        request: Request,
    ) -> dict[str, object]:
        service: TenantConfigService = request.app.state.tenant_configs
        try:
            result = await service.rollback(tenant_id, target_revision=body.target_revision)
        except TenantNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        return {
            "tenant_id": result.tenant_id,
            "active_revision": result.revision,
            "content_hash": result.content_hash,
        }

    @app.get(
        "/v1/admin/tenants/{tenant_id}/active",
        response_model=TenantSpec,
        dependencies=[authorization],
    )
    async def active_tenant(tenant_id: str, request: Request) -> TenantSpec:
        service: TenantConfigService = request.app.state.tenant_configs
        try:
            return await service.load_active(tenant_id)
        except TenantNotFoundError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc


async def _limited_body(request: Request, limit: int) -> bytes:
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > limit:
            raise HTTPException(status_code=413, detail="callback body too large")
    return bytes(body)


def _callback_material(request: Request, body: bytes, binding_id: str) -> CallbackRequest:
    headers = tuple(
        (key.decode("latin-1"), value.decode("latin-1")) for key, value in request.headers.raw
    )
    return make_callback_request(
        binding_id=binding_id,
        headers=headers,
        query=tuple(request.query_params.multi_items()),
        body=body,
        received_at=request.app.state.clock(),
        request_id=request.state.request_id,
        trace_id=request.state.trace_id,
    )


def _register_channel_routes(app: FastAPI) -> None:
    @app.get("/v1/channels/wecom/{public_callback_id}/callback", include_in_schema=False)
    async def verify_wecom_url(public_callback_id: str, request: Request) -> Response:
        ingress: ChannelIngressService = request.app.state.channel_ingress
        resolved, adapter = await ingress.adapter_for(public_callback_id, Channel.WECOM)
        if not isinstance(adapter, WeComAdapter):
            raise IngressConfigurationError("WeCom route resolved to another adapter")
        callback = _callback_material(request, b"", resolved.context.binding_id)
        plaintext = adapter.verify_url(callback, resolved.context)
        return Response(content=plaintext, media_type="text/plain; charset=utf-8")

    @app.post("/v1/channels/{channel}/{public_callback_id}/callback", include_in_schema=False)
    async def receive_channel_callback(
        channel: Channel,
        public_callback_id: str,
        request: Request,
    ) -> Response:
        content_type = request.headers.get("content-type", "").partition(";")[0].strip()
        if content_type != "application/json":
            raise HTTPException(status_code=415, detail="application/json required")
        body = await _limited_body(
            request,
            request.app.state.settings.inbox_payload_limit_bytes,
        )
        ingress: ChannelIngressService = request.app.state.channel_ingress
        resolved, adapter = await ingress.adapter_for(public_callback_id, channel)
        callback = _callback_material(request, body, resolved.context.binding_id)
        await ingress.accept_resolved(
            resolved=resolved,
            adapter=adapter,
            callback_request=callback,
        )
        # A successful response is emitted only after Inbox/credential commit.
        return Response(status_code=200, content=b"")


def _register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(IngressRouteNotFoundError)
    async def missing_callback_route(
        request: Request,
        exc: IngressRouteNotFoundError,
    ) -> JSONResponse:
        del exc
        LOGGER.warning("callback_route_rejected")
        return JSONResponse(
            {"title": "Callback route not found", "status": 404},
            status_code=404,
        )

    @app.exception_handler(IngressConfigurationError)
    async def unavailable_callback_binding(
        request: Request,
        exc: IngressConfigurationError,
    ) -> JSONResponse:
        del request
        LOGGER.error(
            "callback_binding_unavailable",
            extra={"error_type": type(exc).__name__},
        )
        return JSONResponse(
            {"title": "Callback temporarily unavailable", "status": 503},
            status_code=503,
        )

    @app.exception_handler(TelegramAuthenticationError)
    @app.exception_handler(WeComSignatureError)
    async def callback_authentication_failed(request: Request, exc: Exception) -> JSONResponse:
        del request
        LOGGER.warning(
            "callback_authentication_failed",
            extra={"error_type": type(exc).__name__},
        )
        return JSONResponse({"title": "Invalid callback", "status": 401}, status_code=401)

    @app.exception_handler(TelegramProtocolError)
    @app.exception_handler(WeComProtocolError)
    async def callback_protocol_failed(request: Request, exc: Exception) -> JSONResponse:
        del request
        LOGGER.warning(
            "callback_protocol_rejected",
            extra={"error_type": type(exc).__name__},
        )
        return JSONResponse({"title": "Invalid callback", "status": 400}, status_code=400)

    @app.exception_handler(TenantConfigError)
    async def tenant_error(request: Request, exc: TenantConfigError) -> JSONResponse:
        LOGGER.warning("tenant_configuration_rejected", extra={"error_type": type(exc).__name__})
        return JSONResponse(
            {
                "type": "about:blank",
                "title": "Tenant configuration rejected",
                "status": 409,
                "request_id": request.state.request_id,
            },
            status_code=409,
            media_type="application/problem+json",
        )

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, exc: Exception) -> JSONResponse:
        LOGGER.exception("unhandled_request_error", extra={"error_type": type(exc).__name__})
        return JSONResponse(
            {
                "type": "about:blank",
                "title": "Internal Server Error",
                "status": 500,
                "request_id": request.state.request_id,
            },
            status_code=500,
            media_type="application/problem+json",
        )
