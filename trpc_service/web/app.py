"""
FastAPI Application Gateway providing Webhook endpoints, Chat API, Admin APIs, and Prometheus Metrics.
"""

from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Request, Response, HTTPException, BackgroundTasks, Header, Query
from fastapi.responses import PlainTextResponse, JSONResponse
import logging

from trpc_service.version import __version__
from trpc_service.config.models import (
    ChannelType,
    TenantConfig,
    AgentAppConfig,
    ChannelBindingConfig,
    InboundMessage,
    OutboundMessage,
    AuditLogEntry,
)
from trpc_service.tenant.manager import tenant_manager, TenantContext
from trpc_service.storage.factory import storage_factory
from trpc_service.channels.registry import channel_registry
from trpc_service.agent.runner import agent_runner
from trpc_service.metrics.telemetry import get_prometheus_metrics, IM_DELIVERY_TOTAL
from trpc_service.log.logger import setup_logger

logger = setup_logger("trpc_gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize sample default tenant for out-of-the-box readiness
    default_tenant = TenantConfig(
        tenant_id="default_corp",
        name="Default Enterprise Corp",
    )
    tenant_manager.register_tenant(default_tenant)

    default_app = AgentAppConfig(
        app_id="app_default",
        tenant_id="default_corp",
        name="Enterprise Assistant",
        allowed_tools=["calculator", "knowledge_search", "database_query"],
        require_confirmation_tools=["fund_transfer"],
    )
    tenant_manager.register_app(default_app)

    wecom_binding = ChannelBindingConfig(
        binding_id="bind_wecom_demo",
        tenant_id="default_corp",
        channel_type=ChannelType.WECOM,
        bot_id="mock_wecom_bot",
    )
    tenant_manager.register_channel_binding(wecom_binding)

    logger.info("tRPC-Agent Gateway initialized with version %s", __version__)
    yield
    logger.info("tRPC-Agent Gateway shutting down.")


app = FastAPI(
    title="tRPC-Agent Multi-tenant Service",
    version=__version__,
    description="Multi-tenant Node-based Agent Deployment Platform",
    lifespan=lifespan,
)


# =====================================================================
# 1. Health & Prometheus Metrics Endpoints
# =====================================================================

@app.get("/health")
async def health_check():
    return {"status": "ok", "version": __version__}


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics_endpoint():
    return Response(content=get_prometheus_metrics(), media_type="text/plain; version=0.0.4; charset=utf-8")


# =====================================================================
# 2. IM Webhook Gateway Endpoints (WeCom, Telegram, Custom)
# =====================================================================

async def _process_inbound_and_reply(inbound: InboundMessage, binding: Optional[ChannelBindingConfig]):
    """Background task to run Agent reasoning and actively push reply to IM."""
    try:
        outbound = await agent_runner.execute(inbound)
        if binding:
            adapter = channel_registry.get_adapter(binding.channel_type)
            if adapter:
                delivered = await adapter.send_message(outbound, binding)
                IM_DELIVERY_TOTAL.labels(
                    tenant_id=inbound.tenant_id,
                    channel=inbound.channel_type.value,
                    status="success" if delivered else "failed",
                ).inc()
    except Exception as e:
        logger.error("Error processing inbound message %s: %s", inbound.message_id, e, exc_info=True)


@app.api_route("/webhook/{channel_type}/{tenant_id}", methods=["GET", "POST"])
async def handle_im_webhook(
    channel_type: str,
    tenant_id: str,
    request: Request,
    background_tasks: BackgroundTasks,
    echostr: Optional[str] = Query(None),
):
    # Validate Channel Adapter
    try:
        c_type = ChannelType(channel_type.lower())
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Unsupported channel type: {channel_type}")

    adapter = channel_registry.get_adapter(c_type)
    if not adapter:
        raise HTTPException(status_code=404, detail="Channel adapter not found")

    tenant = tenant_manager.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail=f"Tenant '{tenant_id}' not found")

    query_params = dict(request.query_params)
    headers = dict(request.headers)
    raw_body = await request.body()

    # Special handling for WeCom URL Verification GET request
    if request.method == "GET" and echostr:
        if adapter.verify_signature(query_params, headers, raw_body):
            return PlainTextResponse(content=echostr)
        raise HTTPException(status_code=403, detail="Signature verification failed")

    # Verify signature
    if not adapter.verify_signature(query_params, headers, raw_body):
        logger.warning("[Webhook] Signature verification failed for tenant: %s channel: %s", tenant_id, channel_type)
        raise HTTPException(status_code=403, detail="Invalid message signature")

    # Parse standard InboundMessage
    inbound = adapter.parse_inbound_message(tenant_id, query_params, headers, raw_body)

    # Global Idempotency Deduplication (Redis/SQL SETNX)
    storage = storage_factory.get_adapter(tenant.storage_config)
    is_new = await storage.check_and_set_idempotency(f"{c_type.value}:{inbound.message_id}", ttl_seconds=60.0)
    if not is_new:
        logger.info("[Idempotency] Dropping duplicate message: %s from %s", inbound.message_id, inbound.raw_user_id)
        return PlainTextResponse("success")  # Immediate 200 ACK to satisfy IM platform

    # Dispatch to background Worker runner to satisfy 5s timeout limits
    binding = tenant_manager.find_channel_binding_by_bot(c_type.value, inbound.raw_payload.get("ToUserName", "mock_bot"))
    background_tasks.add_task(_process_inbound_and_reply, inbound, binding)

    # Return fast ACK HTTP 200
    return PlainTextResponse("success")


# =====================================================================
# 3. Direct HTTP Chat API
# =====================================================================

@app.post("/v1/chat/completions")
async def direct_chat_completion(payload: Dict[str, Any], x_tenant_id: str = Header(default="default_corp")):
    content = payload.get("message") or payload.get("prompt") or ""
    user_id = payload.get("user_id") or "web_user"
    app_id = payload.get("app_id")

    inbound = InboundMessage(
        trace_id=payload.get("trace_id") or adapter_generate_trace_id(),
        tenant_id=x_tenant_id,
        channel_type=ChannelType.CUSTOM,
        raw_user_id=user_id,
        message_id=payload.get("message_id") or adapter_generate_trace_id(),
        content=content,
    )

    outbound = await agent_runner.execute(inbound, app_id=app_id)
    return {
        "trace_id": outbound.trace_id,
        "tenant_id": outbound.tenant_id,
        "reply": outbound.content,
        "msg_type": outbound.msg_type,
    }


def adapter_generate_trace_id() -> str:
    import uuid
    return uuid.uuid4().hex[:16]


# =====================================================================
# 4. Admin Management APIs (Tenants, Apps, Bindings, Audit Logs)
# =====================================================================

@app.post("/admin/tenants", response_model=TenantConfig)
async def create_or_update_tenant(tenant: TenantConfig):
    return tenant_manager.register_tenant(tenant)


@app.get("/admin/tenants", response_model=List[TenantConfig])
async def list_tenants():
    return tenant_manager.list_tenants()


@app.post("/admin/apps", response_model=AgentAppConfig)
async def create_or_update_app(app_config: AgentAppConfig):
    return tenant_manager.register_app(app_config)


@app.get("/admin/apps/{tenant_id}", response_model=List[AgentAppConfig])
async def get_tenant_apps(tenant_id: str):
    return tenant_manager.get_tenant_apps(tenant_id)


@app.post("/admin/channel-bindings", response_model=ChannelBindingConfig)
async def create_channel_binding(binding: ChannelBindingConfig):
    return tenant_manager.register_channel_binding(binding)


@app.get("/admin/audit-logs/{tenant_id}", response_model=List[AuditLogEntry])
async def get_audit_logs(tenant_id: str, limit: int = 50):
    tenant = tenant_manager.get_tenant(tenant_id)
    if not tenant:
        raise HTTPException(status_code=404, detail="Tenant not found")
    storage = storage_factory.get_adapter(tenant.storage_config)
    return await storage.query_audit_logs(tenant_id, limit=limit)
