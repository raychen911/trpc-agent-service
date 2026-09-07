"""Public channel, browser streaming, health, and administration APIs."""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Annotated, Any, cast

import httpx
from fastapi import APIRouter, Body, Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from tenant_agent.channels.base import ChannelError, WebhookRequest
from tenant_agent.container import ApplicationContainer
from tenant_agent.models import AgentEvent, AuditRecord, ChannelType, RoutedEnvelope, TenantConfig
from tenant_agent.observability import inject_trace_context, trace_id, traced
from tenant_agent.security import verify_token
from tenant_agent.services.broker import BrokerCapacityError
from tenant_agent.settings import ServiceRole
from tenant_agent.storage.base import ConcurrentWriteError, OutboxRepository

MAX_WEBHOOK_BYTES = 2 * 1024 * 1024


def _container(request: Request) -> ApplicationContainer:
    return cast(ApplicationContainer, request.app.state.container)


async def _admin_auth(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    container = _container(request)
    if not verify_token(authorization, container.settings.admin_bearer_token.get_secret_value()):
        raise HTTPException(status_code=401, detail="invalid admin credentials")
    assert authorization is not None
    scheme, separator, value = authorization.strip().partition(" ")
    credential = value if separator and scheme.casefold() == "bearer" else authorization.strip()
    request.state.admin_actor = "admin-credential:" + hashlib.sha256(credential.encode()).hexdigest()[:16]


async def _internal_auth(
    request: Request,
    authorization: Annotated[str | None, Header()] = None,
) -> None:
    container = _container(request)
    if not verify_token(authorization, container.settings.internal_bearer_token.get_secret_value()):
        raise HTTPException(status_code=401, detail="invalid internal credentials")


async def _webhook_request(request: Request) -> WebhookRequest:
    content_length = request.headers.get("content-length")
    if content_length:
        try:
            declared_size = int(content_length)
        except ValueError:
            raise HTTPException(status_code=400, detail="invalid content length") from None
        if declared_size < 0:
            raise HTTPException(status_code=400, detail="invalid content length")
        if declared_size > MAX_WEBHOOK_BYTES:
            raise HTTPException(status_code=413, detail="webhook body is too large")
    body = bytearray()
    async for chunk in request.stream():
        if len(body) + len(chunk) > MAX_WEBHOOK_BYTES:
            raise HTTPException(status_code=413, detail="webhook body is too large")
        body.extend(chunk)
    return WebhookRequest(
        method=request.method,
        headers={key.casefold(): value for key, value in request.headers.items()},
        query=dict(request.query_params),
        body=bytes(body),
        client_host=request.client.host if request.client else None,
    )


def _binding(tenant: TenantConfig, channel: ChannelType, binding_id: str) -> Any:
    return next(
        (
            binding
            for binding in tenant.channels
            if binding.channel == channel and binding.binding_id == binding_id and binding.enabled
        ),
        None,
    )


async def _publish(container: ApplicationContainer, routed: RoutedEnvelope) -> str:
    try:
        return await container.broker.publish(routed)
    except BrokerCapacityError as exc:
        raise HTTPException(
            status_code=503,
            detail="broker queue capacity is temporarily exhausted",
            headers={"Retry-After": "5"},
        ) from exc


async def _audit_admin_action(
    container: ApplicationContainer,
    tenant: TenantConfig,
    request: Request,
    *,
    decision: str,
    record_id: str,
) -> None:
    repository = await container.storage.audit_for_tenant(tenant)
    await repository.append_audit(
        AuditRecord(
            audit_id=uuid.uuid4().hex,
            tenant_id=tenant.tenant_id,
            channel="admin",
            user_id=str(request.state.admin_actor),
            session_id=record_id,
            agent_name="AdminAPI",
            decision=decision,
            trace_id=trace_id(),
            details={"record_id": record_id},
        )
    )


async def _preflight_activation(container: ApplicationContainer, tenant: TenantConfig) -> None:
    try:
        await container.preflight_tenant(tenant)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail=f"configuration preflight rejected ({exc.__class__.__name__})",
        ) from exc
    except Exception as exc:
        raise HTTPException(
            status_code=503,
            detail=f"configuration preflight unavailable ({exc.__class__.__name__})",
        ) from exc


def install_routes(app: FastAPI) -> None:
    public = APIRouter()
    channel_api = APIRouter()
    admin = APIRouter(prefix="/admin/v1", dependencies=[Depends(_admin_auth)])
    internal = APIRouter(prefix="/internal/v1", dependencies=[Depends(_internal_auth)])

    @public.get("/health/live")
    async def live() -> dict[str, str]:
        return {"status": "live"}

    @public.get("/health/ready")
    async def ready(request: Request) -> dict[str, str]:
        container = _container(request)
        try:
            healthy = await container.healthcheck()
        except Exception as exc:
            raise HTTPException(status_code=503, detail="dependency is unavailable") from exc
        if not healthy:
            raise HTTPException(status_code=503, detail="control plane is unavailable")
        return {"status": "ready", "node_id": container.settings.node_id}

    @public.get("/metrics")
    async def metrics(request: Request) -> Response:
        if not _container(request).settings.expose_metrics:
            raise HTTPException(status_code=404)
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    @channel_api.get("/", response_class=HTMLResponse)
    async def index(request: Request) -> HTMLResponse:
        if not _container(request).settings.enable_browser_ui:
            raise HTTPException(status_code=404)
        path = Path(__file__).with_name("static") / "index.html"
        return HTMLResponse(path.read_text(encoding="utf-8"))

    @channel_api.api_route(
        "/v1/channels/{channel}/{binding_id}/webhook",
        methods=["GET", "POST"],
    )
    async def channel_webhook(
        request: Request,
        channel: ChannelType,
        binding_id: str,
        synchronous: bool = Query(default=False),
    ) -> Response:
        container = _container(request)
        if channel is ChannelType.WEB and container.settings.environment == "production":
            raise HTTPException(status_code=404, detail="browser fallback is disabled in production")
        try:
            tenant = await container.configs.resolve_binding(channel.value, binding_id)
            binding = _binding(tenant, channel, binding_id)
            if binding is None:
                raise KeyError("binding not active")
            adapter = container.channels.get(channel)
            with traced(
                "im.callback",
                {
                    "tenant.id": tenant.tenant_id,
                    "messaging.system": channel.value,
                    "messaging.destination": binding_id,
                },
                redactor=container.redactor,
            ):
                parsed = await adapter.parse(
                    await _webhook_request(request),
                    tenant=tenant,
                    binding=binding,
                    secrets=container.secrets,
                )
                direct_results: list[dict[str, Any]] = []
                for envelope in parsed.messages:
                    envelope = envelope.model_copy(update={"trace_context": inject_trace_context()})
                    routed = container.gateway.route(envelope, tenant)
                    use_direct = (
                        channel is ChannelType.WEB
                        and (synchronous or bool(envelope.metadata.get("synchronous")))
                        and container.settings.service_role is ServiceRole.ALL
                    )
                    if use_direct:
                        result = await container.dispatcher.process(tenant=tenant, routed=routed)
                        direct_results.append(
                            {
                                "status": result.status,
                                "session_id": routed.session_id,
                                "messages": [item.model_dump(mode="json") for item in result.responses],
                            }
                        )
                    else:
                        if (
                            container.settings.service_role is ServiceRole.CHANNEL
                            and container.settings.gateway_internal_url
                        ):
                            async with httpx.AsyncClient(timeout=5.0) as client:
                                internal_headers = {
                                    "Authorization": "Bearer "
                                    + container.settings.internal_bearer_token.get_secret_value(),
                                    **routed.inbound.trace_context,
                                }
                                forwarded = await client.post(
                                    f"{container.settings.gateway_internal_url.rstrip('/')}/internal/v1/inbound",
                                    headers=internal_headers,
                                    json=routed.model_dump(mode="json"),
                                )
                                if not forwarded.is_success:
                                    raise HTTPException(
                                        status_code=503,
                                        detail="internal gateway is unavailable",
                                    )
                                broker_id = str(forwarded.json()["broker_id"])
                        else:
                            broker_id = await _publish(container, routed)
                        direct_results.append(
                            {"status": "accepted", "broker_id": broker_id, "session_id": routed.session_id}
                        )
            if direct_results:
                status = (
                    200 if channel is ChannelType.WEB and synchronous else parsed.acknowledgement.status_code
                )
                return JSONResponse({"results": direct_results}, status_code=status)
            acknowledgement = parsed.acknowledgement
            return Response(
                acknowledgement.body,
                status_code=acknowledgement.status_code,
                media_type=acknowledgement.media_type,
                headers=acknowledgement.headers,
            )
        except ChannelError as exc:
            raise HTTPException(status_code=exc.public_status, detail=exc.__class__.__name__) from exc
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="unknown channel binding") from exc

    @channel_api.post("/v1/chat/stream/{binding_id}")
    async def stream_chat(request: Request, binding_id: str) -> StreamingResponse:
        container = _container(request)
        if container.settings.environment == "production":
            raise HTTPException(status_code=404, detail="browser fallback is disabled in production")
        if container.settings.service_role is not ServiceRole.ALL:
            raise HTTPException(
                status_code=503,
                detail="browser streaming is available on the all-in-one profile",
            )
        try:
            tenant = await container.configs.resolve_binding(ChannelType.WEB.value, binding_id)
            binding = _binding(tenant, ChannelType.WEB, binding_id)
            if binding is None:
                raise KeyError("binding not active")
            adapter = container.channels.get(ChannelType.WEB)
            parsed = await adapter.parse(
                await _webhook_request(request),
                tenant=tenant,
                binding=binding,
                secrets=container.secrets,
            )
        except ChannelError as exc:
            raise HTTPException(status_code=exc.public_status, detail=exc.__class__.__name__) from exc
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="unknown web binding") from exc
        if len(parsed.messages) != 1:
            raise HTTPException(status_code=422, detail="one message is required")
        envelope = parsed.messages[0].model_copy(update={"trace_context": inject_trace_context()})
        routed = container.gateway.route(envelope, tenant)
        queue: asyncio.Queue[AgentEvent | dict[str, Any] | None] = asyncio.Queue(maxsize=100)
        disconnected = asyncio.Event()

        async def emit(item: AgentEvent | dict[str, Any] | None) -> None:
            if not disconnected.is_set():
                await queue.put(item)

        async def sink(event: AgentEvent) -> None:
            await emit(event)

        async def run() -> None:
            try:
                result = await container.dispatcher.process(tenant=tenant, routed=routed, event_sink=sink)
                await emit(
                    {
                        "status": result.status,
                        "session_id": routed.session_id,
                        "messages": [item.model_dump(mode="json") for item in result.responses],
                    }
                )
            except Exception as exc:
                await emit({"status": "error", "error_type": exc.__class__.__name__})
            finally:
                await emit(None)

        stream_task = asyncio.create_task(run(), name=f"web-stream:{routed.session_id}")

        async def generate() -> Any:
            try:
                while True:
                    item = await queue.get()
                    if item is None:
                        yield "event: done\ndata: {}\n\n"
                        await stream_task
                        return
                    payload = item.model_dump(mode="json") if isinstance(item, AgentEvent) else item
                    yield f"event: agent\ndata: {json.dumps(payload, ensure_ascii=False)}\n\n"
            finally:
                disconnected.set()
                if not stream_task.done():
                    stream_task.cancel()
                await asyncio.gather(stream_task, return_exceptions=True)
                while True:
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        break

        return StreamingResponse(
            generate(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    @admin.get("/tenants")
    async def list_tenants(request: Request) -> dict[str, Any]:
        tenants = await _container(request).configs.repository.list_active_tenants()
        return {
            "items": [
                {
                    "tenant_id": tenant.tenant_id,
                    "display_name": tenant.display_name,
                    "revision": tenant.revision,
                    "status": tenant.status.value,
                }
                for tenant in tenants
            ]
        }

    @admin.post("/tenants/config-versions", status_code=201)
    async def create_config_version(
        request: Request,
        config: Annotated[TenantConfig, Body()],
        activate: bool = Query(default=False),
    ) -> dict[str, Any]:
        actor = str(request.state.admin_actor)
        container = _container(request)
        version = await container.configs.create_version(config, actor=actor, activate=False)
        if activate:
            await _preflight_activation(container, version.config)
            await _audit_admin_action(
                container,
                version.config,
                request,
                decision="config_activate_requested",
                record_id=f"config:{version.revision}",
            )
            version = await container.configs.activate(config.tenant_id, version.revision)
        return version.model_dump(mode="json", exclude={"config"})

    @admin.get("/tenants/{tenant_id}/config-versions")
    async def list_versions(request: Request, tenant_id: str) -> dict[str, Any]:
        versions = await _container(request).configs.repository.list_config_versions(tenant_id)
        return {"items": [item.model_dump(mode="json", exclude={"config"}) for item in versions]}

    @admin.post("/tenants/{tenant_id}/config-versions/{revision}/activate")
    async def activate_config(request: Request, tenant_id: str, revision: int) -> dict[str, Any]:
        container = _container(request)
        try:
            target = await container.configs.exact_revision(tenant_id, revision)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="unknown configuration revision") from exc
        await _preflight_activation(container, target)
        await _audit_admin_action(
            container,
            target,
            request,
            decision="config_activate_requested",
            record_id=f"config:{revision}",
        )
        version = await container.configs.activate(tenant_id, revision)
        return version.model_dump(mode="json", exclude={"config"})

    @admin.post("/tenants/{tenant_id}/rollback/{revision}")
    async def rollback_config(request: Request, tenant_id: str, revision: int) -> dict[str, Any]:
        container = _container(request)
        try:
            target = await container.configs.exact_revision(tenant_id, revision)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="unknown configuration revision") from exc
        await _preflight_activation(container, target)
        await _audit_admin_action(
            container,
            target,
            request,
            decision="config_rollback_requested",
            record_id=f"config:{revision}",
        )
        version = await container.configs.rollback(tenant_id, revision)
        return version.model_dump(mode="json", exclude={"config"})

    @admin.get("/tenants/{tenant_id}/sessions/{session_id}")
    async def inspect_session(request: Request, tenant_id: str, session_id: str) -> dict[str, Any]:
        container = _container(request)
        tenant = await container.configs.repository.get_active_tenant(tenant_id)
        if tenant is None:
            raise HTTPException(status_code=404, detail="unknown tenant")
        sessions = await container.storage.session_for_tenant(tenant)
        summaries = await container.storage.summary_for_tenant(tenant)
        session = await sessions.get_session(tenant_id, session_id)
        if session is None:
            raise HTTPException(status_code=404, detail="unknown session")
        events = await sessions.list_events(tenant_id, session_id)
        summary = await summaries.get_summary(tenant_id, session_id)
        return {
            "session": session.model_dump(mode="json"),
            "events": [event.model_dump(mode="json") for event in events],
            "summary": summary.model_dump(mode="json") if summary else None,
        }

    @admin.get("/tenants/{tenant_id}/audit")
    async def audit_log(
        request: Request,
        tenant_id: str,
        limit: int = Query(default=100, ge=1, le=1_000),
        before: datetime | None = None,
    ) -> dict[str, Any]:
        container = _container(request)
        tenant = await container.configs.repository.get_active_tenant(tenant_id)
        if tenant is None:
            raise HTTPException(status_code=404, detail="unknown tenant")
        audit = await container.storage.audit_for_tenant(tenant)
        rows = await audit.query_audit(tenant_id, limit=limit, before=before)
        return {"items": [row.model_dump(mode="json") for row in rows]}

    @admin.get("/tenants/{tenant_id}/outbox/dead")
    async def list_dead_outbox(
        request: Request,
        tenant_id: str,
        limit: int = Query(default=100, ge=1, le=1_000),
    ) -> dict[str, Any]:
        container = _container(request)
        if await container.configs.repository.get_active_tenant(tenant_id) is None:
            raise HTTPException(status_code=404, detail="unknown tenant")
        repository = cast(OutboxRepository, container.control)
        rows = await repository.list_dead_outbox(tenant_id, limit=limit)
        return {
            "items": [
                {
                    "outbox_id": row.outbox_id,
                    "kind": row.kind,
                    "status": row.status,
                    "attempts": row.attempts,
                    "last_error_type": row.last_error_type,
                    "available_at": row.available_at.isoformat(),
                    "next_segment": row.payload.get("next_segment"),
                }
                for row in rows
            ]
        }

    @admin.post("/tenants/{tenant_id}/outbox/{outbox_id}/requeue")
    async def requeue_dead_outbox(request: Request, tenant_id: str, outbox_id: str) -> dict[str, Any]:
        container = _container(request)
        tenant = await container.configs.repository.get_active_tenant(tenant_id)
        if tenant is None:
            raise HTTPException(status_code=404, detail="unknown tenant")
        await _audit_admin_action(
            container,
            tenant,
            request,
            decision="outbox_requeue_requested",
            record_id=outbox_id,
        )
        repository = cast(OutboxRepository, container.control)
        try:
            item = await repository.requeue_dead_outbox(tenant_id, outbox_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="unknown dead outbox item") from exc
        except ConcurrentWriteError as exc:
            raise HTTPException(status_code=409, detail="outbox item is not dead") from exc
        return {"status": item.status, "outbox_id": item.outbox_id}

    @admin.get("/tenants/{tenant_id}/broker/dead")
    async def list_dead_broker_messages(
        request: Request,
        tenant_id: str,
        limit: int = Query(default=100, ge=1, le=1_000),
    ) -> dict[str, Any]:
        container = _container(request)
        if await container.configs.repository.get_active_tenant(tenant_id) is None:
            raise HTTPException(status_code=404, detail="unknown tenant")
        rows = await container.broker.list_dead(tenant_id, limit=limit)
        return {
            "items": [
                {
                    "broker_id": row.broker_id,
                    "attempts": row.attempts,
                    "channel": row.routed.inbound.channel.value,
                    "binding_id": row.routed.inbound.binding_id,
                    "message_id": row.routed.inbound.message_id,
                }
                for row in rows
            ]
        }

    @admin.post("/tenants/{tenant_id}/broker/dead/{broker_id}/requeue")
    async def requeue_dead_broker_message(
        request: Request,
        tenant_id: str,
        broker_id: str,
    ) -> dict[str, str]:
        container = _container(request)
        tenant = await container.configs.repository.get_active_tenant(tenant_id)
        if tenant is None:
            raise HTTPException(status_code=404, detail="unknown tenant")
        await _audit_admin_action(
            container,
            tenant,
            request,
            decision="broker_requeue_requested",
            record_id=broker_id,
        )
        try:
            new_id = await container.broker.requeue_dead(tenant_id, broker_id)
        except KeyError as exc:
            raise HTTPException(status_code=404, detail="unknown broker dead letter") from exc
        except BrokerCapacityError as exc:
            raise HTTPException(
                status_code=503,
                detail="broker queue capacity is temporarily exhausted",
                headers={"Retry-After": "5"},
            ) from exc
        return {"status": "requeued", "broker_id": new_id}

    @internal.post("/inbound", status_code=202)
    async def internal_inbound(request: Request, routed: Annotated[RoutedEnvelope, Body()]) -> dict[str, str]:
        container = _container(request)
        envelope = routed
        tenant = await container.configs.exact_revision(envelope.inbound.tenant_id, envelope.config_revision)
        expected = container.gateway.route(envelope.inbound, tenant)
        if expected != envelope:
            raise HTTPException(status_code=422, detail="invalid derived route")
        broker_id = await _publish(container, envelope)
        return {"status": "accepted", "broker_id": broker_id}

    role = app.state.container.settings.service_role
    app.include_router(public)
    if role in {ServiceRole.ALL, ServiceRole.CHANNEL}:
        app.include_router(channel_api)
    if role in {ServiceRole.ALL, ServiceRole.ADMIN}:
        app.include_router(admin)
    if role in {ServiceRole.ALL, ServiceRole.GATEWAY}:
        app.include_router(internal)
