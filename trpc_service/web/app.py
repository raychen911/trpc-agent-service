# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""FastAPI, SSE and IM webhook entry points."""

from __future__ import annotations

import hmac
import base64
import os
import time
from pathlib import Path
from datetime import date
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi import Header
from fastapi import Depends
from fastapi import HTTPException
from fastapi import Request
from fastapi.responses import HTMLResponse
from fastapi.responses import JSONResponse
from fastapi.responses import PlainTextResponse
from fastapi.responses import StreamingResponse
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from trpc_service.channels import ChannelAuthenticationError
from trpc_service.channels import UnsupportedMessageError
from trpc_service.channels.simulator import ImFaultRequest
from trpc_service.channels.simulator import ImSimulationRequest
from trpc_service.config import BackendType, TenantConfig
from trpc_service.config import ChannelType, ServiceRole
from trpc_service.config import load_environment_file
from trpc_service.config import load_settings
from trpc_service.config import load_tenant_configs
from trpc_service.gateway.models import ErrorBody
from trpc_service.gateway.models import RequestState
from trpc_service.gateway.models import TaskAccepted
from trpc_service.gateway.models import TraceContext
from trpc_service.gateway.models import StreamEventType
from trpc_service.gateway.queue import AgentTaskEnvelope
from trpc_service.gateway.idempotency import IdempotencyConflictError
from trpc_service.gateway.idempotency import AdmissionInDoubtError
from trpc_service.gateway.requests import RequestNotFoundError
from trpc_service.gateway.service import DuplicateRequestError
from trpc_service.gateway.service import AgentExecutionError
from trpc_service.gateway.errors import MigrationTransitionError
from trpc_service.gateway.models import AgentStreamEvent
from trpc_service.storage import SessionLockTimeoutError
from trpc_service.log import AuditEvent
from trpc_service.tenant import AccessDeniedError
from trpc_service.tenant import BudgetBackendUnavailableError
from trpc_service.tenant import BudgetExceededError
from trpc_service.tenant import TenantNotFoundError
from trpc_service.tenant import TenantUnavailableError
from trpc_service.web.container import ServiceContainer
from trpc_service.web.container import build_container
from trpc_service.web.schemas import ChatRequest
from trpc_service.web.schemas import RollbackRequest
from trpc_service.web.schemas import ArtifactUploadRequest
from trpc_service.web.schemas import KnowledgeWriteRequest
from trpc_service.web.schemas import ApprovalCreateRequest
from trpc_service.web.schemas import ApprovalDecisionRequest
from trpc_service.web.schemas import MigrationCreateRequest
from trpc_service.resources import ArtifactNotFoundError
from trpc_service.resources import KnowledgeDocument


def _error(status: int, code: str, message: str, *, request_id: str = "", retryable: bool = False) -> JSONResponse:
    return JSONResponse(status_code=status,
                        content=ErrorBody(code=code, message=message, request_id=request_id,
                                          retryable=retryable).model_dump())


def create_app(container: ServiceContainer) -> FastAPI:
    """Create an app around an explicit container, enabling isolated tests."""

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        await container.start()
        try:
            yield
        finally:
            await container.close()

    app = FastAPI(title="tRPC Agent Multi-tenant Service", version="0.1.0", lifespan=lifespan)
    app.state.container = container
    if container.im_simulator is not None:
        static_dir = Path(__file__).with_name("static")
        app.mount("/im-static", StaticFiles(directory=static_dir), name="im-static")

    @app.middleware("http")
    async def enforce_role(request: Request, call_next):
        path = request.url.path
        if container.settings.environment != "development" and path.startswith("/api/"):
            needed = ServiceRole.ADMIN if path.startswith("/api/v1/admin/") else ServiceRole.GATEWAY
            if needed not in container.settings.roles:
                return _error(404, "role_endpoint_unavailable", "endpoint is not served by this role")
        return await call_next(request)

    async def verify_admin(x_admin_token: str = Header(default="")) -> None:
        if container.settings.environment == "development":
            return
        configured = container.settings.admin_token
        if configured is None or not hmac.compare_digest(x_admin_token, configured.get_secret_value()):
            raise HTTPException(status_code=401, detail="admin authentication failed")

    async def verify_tenant(tenant: TenantConfig, supplied: str) -> None:
        if container.settings.environment == "development":
            return
        if not tenant.api_token_ref:
            raise HTTPException(status_code=401, detail="tenant API token is not configured")
        expected = await container.secrets.resolve(tenant.api_token_ref)
        if not hmac.compare_digest(supplied, expected):
            raise HTTPException(status_code=401, detail="tenant authentication failed")

    async def apply_approval(body: ChatRequest, agent_request: Any) -> None:
        supplied = [body.approval_id, body.approval_token, body.approval_tool_name, body.approval_arguments_json]
        if not any(supplied):
            return
        if not all(supplied):
            await container.gateway.mark_failed(agent_request, "invalid_approval_fields")
            raise HTTPException(status_code=400, detail="approval fields must be supplied together")
        arguments_hash = container.approvals.arguments_hash(body.approval_arguments_json)
        try:
            await container.approvals.consume(body.approval_id,
                                              body.approval_token,
                                              tenant_id=body.tenant_id,
                                              user_id=agent_request.user_id,
                                              session_id=agent_request.session_id,
                                              tool_name=body.approval_tool_name,
                                              arguments_sha256=arguments_hash)
        except (PermissionError, KeyError, ValueError) as error:
            await container.gateway.mark_failed(agent_request, "approval_rejected")
            raise HTTPException(status_code=403, detail="approval rejected") from error
        agent_request.metadata["approved_arguments"] = {body.approval_tool_name: arguments_hash}

    async def reserve_budget(tenant: TenantConfig, agent_request: Any) -> None:
        app_config = await container.registry.get_app(tenant.tenant_id, agent_request.app_id,
                                                      agent_request.config_version)
        estimated_input = max(1, len(agent_request.text) // 4)
        estimated_output = app_config.runtime.estimated_output_tokens
        estimated_cost = (estimated_input * app_config.model.input_cost_per_million_usd +
                          estimated_output * app_config.model.output_cost_per_million_usd) / 1_000_000
        reserve = getattr(container.budgets, "reserve", None)
        if reserve:
            await reserve(tenant,
                          input_tokens=estimated_input,
                          output_tokens=estimated_output,
                          cost_usd=estimated_cost,
                          request_id=agent_request.request_id)
            agent_request.metadata["budget_reservation"] = {
                "input_tokens": estimated_input,
                "output_tokens": estimated_output,
                "cost_usd": estimated_cost,
                "day": date.today().isoformat(),
            }
        else:
            await container.budgets.reserve_request(tenant)
        await container.gateway.prepare(agent_request)

    async def settle_usage(tenant: TenantConfig, agent_request: Any, result: Any) -> None:
        app_config = await container.registry.get_app(tenant.tenant_id, agent_request.app_id,
                                                      agent_request.config_version)
        if container.usage_ledger:
            await container.usage_ledger.record(tenant_id=tenant.tenant_id,
                                                app_id=agent_request.app_id,
                                                request_id=agent_request.request_id,
                                                model_name=app_config.model.model_name,
                                                input_tokens=result.usage.input_tokens,
                                                output_tokens=result.usage.output_tokens,
                                                cost_usd=result.usage.cost_usd)
        settle = getattr(container.budgets, "settle_actual", None)
        if settle:
            reserved = agent_request.metadata.get("budget_reservation", {})
            await settle(tenant,
                         request_id=agent_request.request_id,
                         budget_day=str(reserved.get("day", "")),
                         input_tokens=result.usage.input_tokens - int(reserved.get("input_tokens", 0)),
                         output_tokens=result.usage.output_tokens - int(reserved.get("output_tokens", 0)),
                         cost_usd=result.usage.cost_usd - float(reserved.get("cost_usd", 0)))

    @app.exception_handler(TenantNotFoundError)
    async def tenant_not_found(_: Request, error: TenantNotFoundError) -> JSONResponse:
        return _error(404, "resource_not_found", str(error).strip("'"))

    @app.exception_handler(TenantUnavailableError)
    async def tenant_unavailable(_: Request, error: TenantUnavailableError) -> JSONResponse:
        return _error(403, "tenant_unavailable", str(error))

    @app.exception_handler(DuplicateRequestError)
    async def duplicate(_: Request, error: DuplicateRequestError) -> JSONResponse:
        return _error(409, "duplicate_request", str(error), request_id=error.request_id)

    @app.exception_handler(IdempotencyConflictError)
    async def idempotency_conflict(_: Request, error: IdempotencyConflictError) -> JSONResponse:
        return _error(409, "idempotency_payload_conflict", str(error))

    @app.exception_handler(AdmissionInDoubtError)
    async def admission_uncertain(_: Request, error: AdmissionInDoubtError):
        return _error(503, "admission_in_doubt", "legacy request requires reconciliation", retryable=False)

    @app.exception_handler(MigrationTransitionError)
    async def migration_transition(_: Request, error: MigrationTransitionError):
        return _error(503, "migration_transition", str(error), retryable=True)

    @app.exception_handler(ChannelAuthenticationError)
    async def customer_authentication_failed(_: Request, error: ChannelAuthenticationError):
        return _error(403, "channel_authentication_failed", "invalid channel callback")

    @app.exception_handler(UnsupportedMessageError)
    async def unsupported_channel_message(_: Request, error: UnsupportedMessageError):
        return _error(422, "unsupported_message", "unsupported channel notification")

    @app.exception_handler(RequestNotFoundError)
    async def request_not_found(_: Request, error: RequestNotFoundError) -> JSONResponse:
        return _error(404, "request_not_found", str(error).strip("'"))

    @app.exception_handler(ArtifactNotFoundError)
    async def artifact_not_found(_: Request, error: ArtifactNotFoundError) -> JSONResponse:
        return _error(404, "artifact_not_found", str(error).strip("'"))

    @app.exception_handler(SessionLockTimeoutError)
    async def lock_timeout(_: Request, error: SessionLockTimeoutError) -> JSONResponse:
        return _error(503, "session_busy", str(error), retryable=True)

    @app.exception_handler(BudgetExceededError)
    async def budget_exceeded(_: Request, error: BudgetExceededError) -> JSONResponse:
        return _error(429, "budget_exceeded", str(error), retryable=False)

    @app.exception_handler(BudgetBackendUnavailableError)
    async def budget_backend_unavailable(_: Request, error: BudgetBackendUnavailableError) -> JSONResponse:
        del error
        return _error(503, "budget_backend_unavailable", "budget service is temporarily unavailable", retryable=True)

    @app.exception_handler(AccessDeniedError)
    async def access_denied(_: Request, error: AccessDeniedError) -> JSONResponse:
        return _error(403, "access_denied", str(error), retryable=False)

    @app.exception_handler(NotImplementedError)
    async def not_implemented(_: Request, error: NotImplementedError) -> JSONResponse:
        return _error(501, "provider_not_implemented", str(error), retryable=False)

    @app.exception_handler(AgentExecutionError)
    async def agent_failure(_: Request, error: AgentExecutionError) -> JSONResponse:
        return _error(502, "agent_execution_failed", "model or agent execution failed", retryable=True)

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return """<!doctype html><html><body><h1>tRPC Agent Service</h1>
<p>Use <code>POST /api/v1/chat</code> or <code>/api/v1/chat/stream</code>.</p>
<p>Interactive API: <a href=\"/docs\">/docs</a></p></body></html>"""

    if container.im_simulator is not None:

        @app.get("/im", include_in_schema=False)
        async def im_demo_page() -> FileResponse:
            return FileResponse(Path(__file__).with_name("static") / "im.html")

        @app.get("/api/v1/dev/im/bootstrap")
        async def im_demo_bootstrap() -> dict[str, Any]:
            return container.im_simulator.bootstrap()

        @app.post("/api/v1/dev/im/messages", status_code=202)
        async def im_demo_message(body: ImSimulationRequest) -> dict[str, Any]:
            try:
                return await container.im_simulator.send(body)
            except (ValueError, UnsupportedMessageError) as error:
                raise HTTPException(status_code=422, detail=str(error)) from error
            except ChannelAuthenticationError as error:
                raise HTTPException(status_code=401, detail=str(error)) from error

        @app.get("/api/v1/dev/im/messages/{request_id}")
        async def im_demo_status(request_id: str) -> dict[str, Any]:
            try:
                return await container.im_simulator.status(request_id)
            except KeyError as error:
                raise HTTPException(status_code=404, detail="simulated IM request was not found") from error

        @app.post("/api/v1/dev/im/faults")
        async def im_demo_fault(body: ImFaultRequest) -> dict[str, str]:
            try:
                return container.im_simulator.set_fault(body)
            except ValueError as error:
                raise HTTPException(status_code=422, detail=str(error)) from error

    @app.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def ready() -> dict[str, Any]:
        try:
            result = await container.readiness()
        except Exception as error:
            raise HTTPException(status_code=503, detail={
                "status": "not_ready",
                "error_code": type(error).__name__
            }) from error
        if result["status"] != "ready":
            raise HTTPException(status_code=503, detail=result)
        return result

    @app.get("/metrics", response_class=PlainTextResponse)
    async def metrics() -> str:
        return container.metrics.render()

    @app.post("/api/v1/chat")
    async def chat(
        http_request: Request,
        body: ChatRequest,
        x_idempotency_key: str = Header(default=""),
        x_tenant_token: str = Header(default="")
    ) -> dict[str, Any]:
        started = time.perf_counter()
        tenant = await container.registry.get(body.tenant_id)
        await verify_tenant(tenant, x_tenant_token)
        key = x_idempotency_key or body.idempotency_key
        try:
            agent_request, reservation = await container.gateway.web_request(
                tenant_id=body.tenant_id,
                app_id=body.app_id,
                external_user_id=body.user_id,
                session_id=body.session_id,
                text=body.message,
                idempotency_key=key,
                trace=TraceContext(
                    traceparent=http_request.headers.get("traceparent", ""),
                    tracestate=http_request.headers.get("tracestate", ""),
                    baggage=http_request.headers.get("baggage", ""),
                ),
            )
        except DuplicateRequestError as duplicate_error:
            existing = await container.gateway.request_status(body.tenant_id, duplicate_error.request_id)
            if existing.state == RequestState.SUCCEEDED and existing.result:
                if existing.request:
                    await settle_usage(tenant, existing.request, existing.result)
                return existing.result.model_dump(mode="json")
            raise
        await apply_approval(body, agent_request)
        try:
            await reserve_budget(tenant, agent_request)
        except BudgetExceededError:
            await container.gateway.mark_failed(agent_request, "budget_exceeded")
            raise
        except BudgetBackendUnavailableError:
            await container.gateway.mark_retryable(agent_request, "budget_backend_unavailable")
            raise
        container.metrics.increment("trpc_service_requests_total", tenant=body.tenant_id, channel="web")
        result = await container.gateway.chat(agent_request, reservation)
        await settle_usage(tenant, agent_request, result)
        latency_seconds = time.perf_counter() - started
        container.metrics.observe(
            "trpc_service_request_duration_seconds",
            latency_seconds,
            tenant=body.tenant_id,
            channel="web",
        )
        await container.audit.write(
            AuditEvent(
                tenant_id=body.tenant_id,
                channel="web",
                user_id=result.user_id,
                session_id=result.session_id,
                agent_name=body.app_id,
                action="agent_run",
                latency_ms=latency_seconds * 1000,
                input_tokens=result.usage.input_tokens,
                output_tokens=result.usage.output_tokens,
                cost_usd=result.usage.cost_usd,
                request_id=result.request_id,
            ))
        return result.model_dump(mode="json")

    @app.post("/api/v1/chat/async", status_code=202)
    async def chat_async(
        http_request: Request,
        body: ChatRequest,
        x_idempotency_key: str = Header(default=""),
        x_tenant_token: str = Header(default="")
    ) -> dict[str, Any]:
        tenant = await container.registry.get(body.tenant_id)
        await verify_tenant(tenant, x_tenant_token)
        started = time.perf_counter()
        outcome = "error"
        container.metrics.increment("trpc_service_requests_total", tenant=body.tenant_id, channel="web")
        try:
            try:
                agent_request, reservation = await container.gateway.web_request(
                    tenant_id=body.tenant_id,
                    app_id=body.app_id,
                    external_user_id=body.user_id,
                    session_id=body.session_id,
                    text=body.message,
                    idempotency_key=x_idempotency_key or body.idempotency_key,
                    trace=TraceContext(
                        traceparent=http_request.headers.get("traceparent", ""),
                        tracestate=http_request.headers.get("tracestate", ""),
                        baggage=http_request.headers.get("baggage", ""),
                    ),
                )
            except DuplicateRequestError as duplicate_error:
                existing = await container.gateway.request_status(body.tenant_id, duplicate_error.request_id)
                outcome = "duplicate"
                return TaskAccepted(
                    request_id=existing.request_id,
                    state=existing.state,
                    status_url=f"/api/v1/tenants/{body.tenant_id}/requests/{existing.request_id}",
                ).model_dump(mode="json")
            await apply_approval(body, agent_request)
            try:
                await reserve_budget(tenant, agent_request)
                await container.queue.enqueue(AgentTaskEnvelope(request=agent_request, idempotency_key=reservation))
                await container.gateway.mark_queued(agent_request)
                await container.audit.write(
                    AuditEvent(
                        tenant_id=tenant.tenant_id,
                        channel="web",
                        user_id=agent_request.user_id,
                        session_id=agent_request.session_id,
                        agent_name=body.app_id,
                        action="chat_queued",
                        request_id=agent_request.request_id,
                    ))
            except Exception as error:
                if isinstance(error, BudgetExceededError):
                    await container.gateway.mark_failed(agent_request, "budget_exceeded")
                else:
                    await container.gateway.mark_retryable(agent_request, type(error).__name__)
                raise
            outcome = "queued"
            return TaskAccepted(
                request_id=agent_request.request_id,
                state=RequestState.QUEUED,
                status_url=f"/api/v1/tenants/{body.tenant_id}/requests/{agent_request.request_id}",
            ).model_dump(mode="json")
        finally:
            labels = {"tenant": body.tenant_id, "channel": "web", "result": outcome}
            container.metrics.increment("trpc_service_gateway_admissions_total", **labels)
            container.metrics.observe(
                "trpc_service_gateway_admission_duration_seconds",
                time.perf_counter() - started,
                **labels,
            )

    @app.get("/api/v1/tenants/{tenant_id}/requests/{request_id}")
    async def request_status(tenant_id: str, request_id: str,
                             x_tenant_token: str = Header(default="")) -> dict[str, Any]:
        await verify_tenant(await container.registry.get(tenant_id), x_tenant_token)
        record = await container.gateway.request_status(tenant_id, request_id)
        return record.model_dump(mode="json")

    @app.post("/api/v1/artifacts", status_code=201)
    async def upload_artifact(body: ArtifactUploadRequest, x_tenant_token: str = Header(default="")) -> dict[str, Any]:
        await verify_tenant(await container.registry.get(body.tenant_id), x_tenant_token)
        await container.registry.get_app(body.tenant_id, body.app_id)
        try:
            content = base64.b64decode(body.content_base64, validate=True)
        except ValueError as error:
            raise HTTPException(status_code=400, detail="content_base64 is invalid") from error
        metadata = await container.artifacts.put(body.tenant_id, body.app_id, body.name, body.mime_type, content)
        return metadata.model_dump(mode="json")

    @app.get("/api/v1/tenants/{tenant_id}/artifacts/{artifact_id}")
    async def get_artifact(tenant_id: str, artifact_id: str,
                           x_tenant_token: str = Header(default="")) -> dict[str, Any]:
        await verify_tenant(await container.registry.get(tenant_id), x_tenant_token)
        metadata, content = await container.artifacts.get(tenant_id, artifact_id)
        return {
            "metadata": metadata.model_dump(mode="json"),
            "content_base64": base64.b64encode(content).decode("ascii")
        }

    @app.post("/api/v1/knowledge", status_code=201)
    async def write_knowledge(body: KnowledgeWriteRequest, x_tenant_token: str = Header(default="")) -> dict[str, Any]:
        await verify_tenant(await container.registry.get(body.tenant_id), x_tenant_token)
        await container.registry.get_app(body.tenant_id, body.app_id)
        document = await container.knowledge.add(
            KnowledgeDocument(tenant_id=body.tenant_id, app_id=body.app_id, title=body.title, text=body.text))
        return document.model_dump(mode="json")

    @app.get("/api/v1/tenants/{tenant_id}/apps/{app_id}/knowledge/search")
    async def search_knowledge(tenant_id: str,
                               app_id: str,
                               q: str,
                               limit: int = 5,
                               x_tenant_token: str = Header(default="")) -> list[dict[str, Any]]:
        await verify_tenant(await container.registry.get(tenant_id), x_tenant_token)
        await container.registry.get_app(tenant_id, app_id)
        hits = await container.knowledge.search(tenant_id, app_id, q, max(1, min(limit, 20)))
        return [hit.model_dump(mode="json") for hit in hits]

    @app.post("/api/v1/chat/stream")
    async def chat_stream(
        http_request: Request,
        body: ChatRequest,
        x_idempotency_key: str = Header(default=""),
        x_tenant_token: str = Header(default="")
    ) -> StreamingResponse:
        tenant = await container.registry.get(body.tenant_id)
        await verify_tenant(tenant, x_tenant_token)
        agent_request, reservation = await container.gateway.web_request(
            tenant_id=body.tenant_id,
            app_id=body.app_id,
            external_user_id=body.user_id,
            session_id=body.session_id,
            text=body.message,
            idempotency_key=x_idempotency_key or body.idempotency_key,
            trace=TraceContext(
                traceparent=http_request.headers.get("traceparent", ""),
                tracestate=http_request.headers.get("tracestate", ""),
                baggage=http_request.headers.get("baggage", ""),
            ),
        )
        await apply_approval(body, agent_request)
        try:
            await reserve_budget(tenant, agent_request)
        except BudgetExceededError:
            await container.gateway.mark_failed(agent_request, "budget_exceeded")
            raise
        except BudgetBackendUnavailableError:
            await container.gateway.mark_retryable(agent_request, "budget_backend_unavailable")
            raise

        async def event_source():
            sequence = 0
            try:
                async for event in container.gateway.stream(agent_request, reservation):
                    if event.type == StreamEventType.COMPLETED:
                        record = await container.gateway.request_status(body.tenant_id, agent_request.request_id)
                        await settle_usage(tenant, agent_request, record.result)
                    sequence = event.sequence + 1
                    data = event.model_dump_json()
                    yield f"id: {event.sequence}\nevent: {event.type}\ndata: {data}\n\n"
            except Exception:
                event = AgentStreamEvent(request_id=agent_request.request_id,
                                         sequence=sequence,
                                         type=StreamEventType.ERROR,
                                         text="agent execution failed",
                                         data={
                                             "error_code": "agent_execution_failed",
                                             "retryable": True
                                         })
                yield f"id: {sequence}\nevent: error\ndata: {event.model_dump_json()}\n\n"

        return StreamingResponse(event_source(),
                                 media_type="text/event-stream",
                                 headers={
                                     "Cache-Control": "no-cache",
                                     "X-Accel-Buffering": "no"
                                 })

    @app.get("/api/v1/channels/{binding_id}/webhook")
    async def verify_customer_callback(binding_id: str, request: Request):
        runtime = container.customer_runtimes.get(binding_id)
        if runtime is None or runtime.adapter.crypto is None:
            raise HTTPException(status_code=404, detail="customer-service callback is not configured")
        query = request.query_params
        echo = runtime.adapter.crypto.decrypt(query.get("echostr", ""), query.get("msg_signature", ""),
                                              query.get("timestamp", ""), query.get("nonce", ""))
        return PlainTextResponse(echo)

    @app.post("/api/v1/channels/{binding_id}/webhook", status_code=202)
    async def channel_webhook(binding_id: str, request: Request) -> dict[str, str]:
        adapter = container.channel_adapters.get(binding_id)
        if adapter is None:
            raise HTTPException(status_code=404, detail="channel adapter is not active on this process")
        if binding_id in container.customer_runtimes:
            query = request.query_params
            try:
                await container.customer_runtimes[binding_id].notification(
                    await request.body(), query.get("msg_signature", ""), query.get("timestamp", ""),
                    query.get("nonce", ""), TraceContext(traceparent=request.headers.get("traceparent", "")))
            except (ChannelAuthenticationError, UnsupportedMessageError):
                raise
            except Exception:
                return _error(503, "customer_notification_not_saved", "retry customer notification", retryable=True)
            return PlainTextResponse("success", status_code=200)
        try:
            tenant, binding = await container.registry.resolve_binding(binding_id)
            if binding.channel == ChannelType.WECOM:
                return _error(405, "websocket_only", "WeCom frames require an authenticated WebSocket connection")
            payload = await request.json()
            normalized = await adapter.normalize(binding_id, payload, {
                key.lower(): value
                for key, value in request.headers.items()
            })
            normalized.trace = TraceContext(traceparent=request.headers.get("traceparent", ""),
                                            tracestate=request.headers.get("tracestate", ""),
                                            baggage=request.headers.get("baggage", ""))
            request_id = await container.admit_channel(normalized)
        except DuplicateRequestError as error:
            return {"request_id": error.request_id, "status": "duplicate"}
        except ChannelAuthenticationError as error:
            return _error(401, "channel_authentication_failed", str(error))
        except UnsupportedMessageError as error:
            return _error(422, "unsupported_message", str(error))
        # The in-process path is a runnable development fallback. Production
        # Gateway roles enqueue the request and a Delivery role sends this reply.
        return {"request_id": request_id, "status": "queued"}

    @app.get("/api/v1/admin/tenants", dependencies=[Depends(verify_admin)])
    async def list_tenants() -> list[dict[str, Any]]:
        tenants = await container.registry.list_active()
        return [tenant.model_dump(mode="json") for tenant in tenants]

    @app.put("/api/v1/admin/tenants/{tenant_id}/config", dependencies=[Depends(verify_admin)])
    async def publish_config(tenant_id: str, config: TenantConfig) -> dict[str, Any]:
        if config.tenant_id != tenant_id:
            raise HTTPException(status_code=400, detail="path tenant_id must match configuration")
        published = await container.configuration.publish(
            config) if container.configuration else await container.registry.publish(config)
        return published.model_dump(mode="json")

    @app.post("/api/v1/admin/tenants/{tenant_id}/rollback", dependencies=[Depends(verify_admin)])
    async def rollback_config(tenant_id: str, body: RollbackRequest) -> dict[str, Any]:
        active = await container.configuration.rollback(
            tenant_id, body.version) if container.configuration else await container.registry.rollback(
                tenant_id, body.version)
        return active.model_dump(mode="json")

    @app.post("/api/v1/admin/approvals", dependencies=[Depends(verify_admin)])
    async def create_approval(body: ApprovalCreateRequest) -> dict[str, Any]:
        arguments_hash = container.approvals.arguments_hash(body.arguments_json)
        approval = await container.approvals.create(body.tenant_id, body.user_id, body.session_id, body.tool_name,
                                                    arguments_hash)
        return approval.model_dump(mode="json")

    @app.post("/api/v1/admin/approvals/{approval_id}/decision", dependencies=[Depends(verify_admin)])
    async def decide_approval(approval_id: str, body: ApprovalDecisionRequest) -> dict[str, Any]:
        approval, token = await container.approvals.decide(approval_id, approve=body.approve, actor=body.actor)
        return {"approval": approval.model_dump(mode="json"), "token": token}

    @app.post("/api/v1/admin/migrations", dependencies=[Depends(verify_admin)])
    async def create_migration(body: MigrationCreateRequest) -> dict[str, Any]:
        if body.resource_type != "session_memory":
            raise HTTPException(status_code=400, detail="resource_type must be session_memory")
        if (body.source_backend, body.target_backend) not in {("redis", "sql"), ("sql", "redis")}:
            raise HTTPException(status_code=400,
                                detail="supported migration directions are redis -> sql and sql -> redis")
        tenant = await container.registry.get(body.tenant_id)
        source = BackendType(body.source_backend)
        if tenant.storage.session != source or tenant.storage.memory != source:
            raise HTTPException(status_code=409,
                                detail="source_backend must match both active Session and Memory backends")
        if not tenant.storage.sql_url.startswith("postgresql"):
            raise HTTPException(status_code=400, detail="online SQL migration requires PostgreSQL")
        job = await container.migrations.create(body.tenant_id,
                                                body.resource_type,
                                                body.source_backend,
                                                body.target_backend,
                                                batch_size=body.batch_size,
                                                shadow_sample_rate=body.shadow_sample_rate,
                                                rollback_window_seconds=body.rollback_window_seconds)
        return job.model_dump(mode="json")

    @app.get("/api/v1/admin/migrations/{job_id}", dependencies=[Depends(verify_admin)])
    async def get_migration(job_id: str) -> dict[str, Any]:
        return (await container.migrations.get(job_id)).model_dump(mode="json")

    @app.get("/api/v1/admin/migrations/{job_id}/items", dependencies=[Depends(verify_admin)])
    async def get_migration_items(job_id: str, state: str = "", limit: int = 200) -> list[dict[str, Any]]:
        if container.migration_control is None:
            return []
        items = await container.migration_control.list_items(job_id, state=state, limit=max(1, min(limit, 1000)))
        return [item.model_dump(mode="json") for item in items]

    @app.post("/api/v1/admin/migrations/{job_id}/advance", dependencies=[Depends(verify_admin)])
    async def advance_migration(job_id: str) -> dict[str, Any]:
        return (await container.migrations.advance(job_id)).model_dump(mode="json")

    @app.post("/api/v1/admin/migrations/{job_id}/rollback", dependencies=[Depends(verify_admin)])
    async def rollback_migration(job_id: str) -> dict[str, Any]:
        return (await container.migrations.rollback(job_id)).model_dump(mode="json")

    return app


def create_default_app() -> FastAPI:
    load_environment_file(os.environ.get("TRPC_SERVICE_ENV_FILE", ".env"))
    settings = load_settings()
    configs = load_tenant_configs(settings.config_file)
    if settings.environment != "development":
        raise RuntimeError("production requires the async composition path; run `trpc_service serve` instead")
    return create_app(build_container(settings, configs))
