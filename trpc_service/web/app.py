"""FastAPI 入口：Web UI 聊天 + Admin API + 审计。"""
from __future__ import annotations
import json
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import parse_qs

import uvicorn
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from trpc_service.agent.factory import AgentFactory
from trpc_service.agent.runner import AgentRunner
from trpc_service.channels.base import WebhookRequest
from trpc_service.channels.feishu import FeishuAdapter
from trpc_service.channels.wecom import WeComAdapter
from trpc_service.chat import ChatBlocked, execute_chat
from trpc_service.config.manager import ConfigManager
from trpc_service.config.settings import ServerConfig
from trpc_service.metrics.collector import metrics_collector
from trpc_service.tenant.audit.service import audit_service
from trpc_service.tenant.storage.factory import create_storage
from trpc_service.version import __version__
from trpc_service.web.auth import require_admin_key, require_chat_key

STATIC_DIR = Path(__file__).resolve().parent / "static"
_smartbot_channels: Dict[str, object] = {}


class AppState:
    """进程级共享状态：配置管理器 + 各租户 Runner + 存储适配。"""

    def __init__(self) -> None:
        self.config_manager = ConfigManager()
        self.runners: Dict[str, AgentRunner] = {}
        self.database = None   # ②生产级：平台 Database（lifespan 注入）
        self.tenant_store = None  # P1：租户 SQL 持久化（lifespan 注入）

    def build_runner(self, tenant_id: str) -> Optional[AgentRunner]:
        """按租户配置装配 Agent + 存储后端并构建 Runner。"""
        tenant_config = self.config_manager.get(tenant_id)
        if not tenant_config:
            return None
        agent = AgentFactory.create_agent(tenant_config)
        storage = create_storage(tenant_config)
        return AgentRunner(
            app_name=tenant_config.app.app_name,
            agent=agent,
            session_service=storage.session_service,
            memory_service=storage.memory_service,
        )


state = AppState()


@asynccontextmanager
async def lifespan(app: FastAPI):
    from trpc_service.metrics.setup import setup_telemetry

    setup_telemetry()  # OTEL_ENABLED=1 时生效，否则 no-op

    # ---- 平台数据库（②生产级：显式 Database + 依赖注入） ----
    from trpc_service.tenant.storage.database import Database, platform_db_url

    db_url = platform_db_url()
    if db_url:
        state.database = Database(db_url)
        audit_service.attach(state.database)
        await audit_service.start()
        # 租户持久化：从 tenant 表恢复 YAML 中没有的租户并装配 Runner
        from trpc_service.tenant.sql_store import SqlTenantStore

        state.tenant_store = SqlTenantStore(state.database)
        for tenant_config in state.tenant_store.load_all_configs():
            if not state.config_manager.get(tenant_config.tenant_id):
                state.config_manager.register(tenant_config)
        # AUTO_MIGRATE=1 时启动即把 schema 迁移到最新；生产建议显式 `cli migrate`
        if os.getenv("AUTO_MIGRATE", "").lower() in ("1", "true"):
            _run_migrations()

    # 启动时执行一次审计保留期清理（文件后端；SQL 后端的清理在②收尾接 DELETE）
    retention_map = {
        t.tenant_id: t.audit.retention_days
        for t in state.config_manager.all().values()
    }
    audit_service.cleanup(retention_map)

    for tenant_id in state.config_manager.all():
        runner = state.build_runner(tenant_id)
        if runner:
            state.runners[tenant_id] = runner

    # ---- 企微智能机器人长连接（免公网通道） ----
    for tenant_id, tenant_config in state.config_manager.all().items():
        smart_cfg = tenant_config.channels.get("wecom_smartbot")
        if smart_cfg and smart_cfg.enabled:
            from trpc_service.channels.wecom_smartbot import WeComSmartBotChannel

            channel = WeComSmartBotChannel(
                tenant_config=tenant_config,
                channel_config=smart_cfg,
                runner_getter=state.runners.get,
                database=state.database,
            )
            try:
                await channel.start()
                _smartbot_channels[tenant_id] = channel
            except Exception as exc:  # noqa: BLE001  单通道失败不阻断启动
                from trpc_service.log import get_logger

                get_logger("channels").error(
                    "smartbot start failed tenant=%s err=%s", tenant_id, exc
                )
    yield
    for channel in _smartbot_channels.values():
        try:
            await channel.stop()
        except Exception:  # noqa: BLE001
            pass
    for runner in state.runners.values():
        await runner.close()
    await audit_service.stop()   # 关停前把审计缓冲刷净
    if state.database is not None:
        state.database.dispose()


def _run_migrations() -> None:
    """程序化执行 Alembic 迁移到 head（与 `python -m trpc_service._cli migrate` 等价）。"""
    from alembic import command
    from alembic.config import Config

    cfg = Config(str(PROJECT_ROOT / "alembic.ini"))
    command.upgrade(cfg, "head")


PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent


app = FastAPI(title="trpc agent service", version=__version__, lifespan=lifespan)
# CORS 白名单：CORS_ORIGINS 逗号分隔（默认仅本机前端），不再全开
_cors_origins = [
    o.strip() for o in os.getenv("CORS_ORIGINS", "http://localhost:8000").split(",") if o.strip()
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=_cors_origins or ["http://localhost:8000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    tenant_id: str
    message: str
    session_id: Optional[str] = None
    user_id: Optional[str] = None


class ChatResponse(BaseModel):
    reply: str
    reasoning: str = ""
    session_id: str
    trace_id: str


@app.post("/api/v1/chat", dependencies=[Depends(require_chat_key)])
async def chat(req: ChatRequest) -> ChatResponse:
    """Web 聊天接口：inline 直连执行，或 QUEUE_MODE=redis 时入队给 worker。"""
    tenant_config = state.config_manager.get(req.tenant_id)
    if not tenant_config:
        raise HTTPException(status_code=404, detail=f"租户不存在: {req.tenant_id}")

    # ---- 队列模式：gateway 只做校验/入队/等结果，Agent 在 worker 执行 ----
    if os.getenv("QUEUE_MODE", "inline") == "redis":
        from trpc_service.metrics.context import new_trace_id
        from trpc_service.worker import TaskQueue

        queue = TaskQueue(os.getenv("REDIS_URL", "redis://localhost:6379/3"))
        request_id = await queue.enqueue(
            {
                "tenant_id": req.tenant_id,
                "message": req.message,
                "session_id": req.session_id,
                "user_id": req.user_id or "web_user",
                "channel": "web",
                "trace_id": new_trace_id(),
            }
        )
        result = await queue.wait_result(
            request_id, timeout=float(os.getenv("TASK_WAIT_TIMEOUT", "60"))
        )
        if result is None:
            raise HTTPException(status_code=504, detail="worker 执行超时，请稍后重试")
        return ChatResponse(
            reply=result.get("reply", ""),
            reasoning=result.get("reasoning", ""),
            session_id=result.get("session_id", ""),
            trace_id=result.get("trace_id", ""),
        )

    # ---- inline 模式：本进程直接执行（默认，开发/单机部署） ----
    runner = state.runners.get(req.tenant_id)
    if not runner:
        raise HTTPException(status_code=503, detail=f"租户 {req.tenant_id} 的 Agent 未就绪")
    try:
        result = await execute_chat(
            tenant_config,
            runner,
            message=req.message,
            user_id=req.user_id or "web_user",
            session_id=req.session_id,
            channel="web",
        )
    except ChatBlocked as ex:
        raise HTTPException(status_code=429, detail=str(ex)) from ex
    return ChatResponse(**result)


class HistoryItem(BaseModel):
    role: str  # "user" | "agent"
    text: str
    ts: float = 0.0


@app.get("/api/v1/chat/history", dependencies=[Depends(require_chat_key)])
async def chat_history(
    tenant_id: str, session_id: str, user_id: str = "web_user"
) -> List[HistoryItem]:
    """拉取指定会话的历史消息（web 刷新/重开后渲染）。"""
    tenant_config = state.config_manager.get(tenant_id)
    if not tenant_config:
        raise HTTPException(status_code=404, detail=f"租户不存在: {tenant_id}")
    runner = state.runners.get(tenant_id)
    if not runner:
        raise HTTPException(status_code=503, detail=f"租户 {tenant_id} 的 Agent 未就绪")
    session = await runner.runner.session_service.get_session(
        app_name=runner.app_name, user_id=user_id, session_id=session_id
    )
    if session is None:
        return []
    items: List[HistoryItem] = []
    for event in session.events:
        text = (event.get_text() or "").strip()
        if not text:
            continue
        items.append(
            HistoryItem(
                role="user" if event.author == "user" else "agent",
                text=text,
                ts=float(event.timestamp or 0.0),
            )
        )
    return items


class TenantInfo(BaseModel):
    tenant_id: str
    name: str
    status: str
    app_name: str
    release_stage: str = "stable"


@app.get("/api/v1/tenants", dependencies=[Depends(require_admin_key)])
async def list_tenants() -> List[TenantInfo]:
    """租户列表。"""
    return [
        TenantInfo(
            tenant_id=t.tenant_id, name=t.name, status=t.status,
            app_name=t.app.app_name, release_stage=t.release_stage
        )
        for t in state.config_manager.all().values()
    ]


class TenantCreateRequest(BaseModel):
    tenant_id: str
    name: str
    instruction: str = "你是一个有帮助的助手。"
    app_name: Optional[str] = None


@app.post("/api/v1/tenants", dependencies=[Depends(require_admin_key)])
async def create_tenant(req: TenantCreateRequest) -> TenantInfo:
    """创建租户并即时装配 Runner（Admin API）。"""
    if state.config_manager.get(req.tenant_id):
        raise HTTPException(status_code=409, detail=f"租户已存在: {req.tenant_id}")
    from trpc_service.config.tenant_config import TenantConfig

    tenant = TenantConfig(
        tenant_id=req.tenant_id,
        name=req.name,
        app={"app_name": req.app_name or req.tenant_id, "instruction": req.instruction},
    )
    state.config_manager.register(tenant)
    runner = state.build_runner(req.tenant_id)
    if runner:
        state.runners[req.tenant_id] = runner
    if state.tenant_store is not None:
        state.tenant_store.save_config(tenant)  # 持久化，重启后自动恢复
    return TenantInfo(
        tenant_id=tenant.tenant_id, name=tenant.name, status=tenant.status,
        app_name=tenant.app.app_name, release_stage=tenant.release_stage,
    )


@app.delete("/api/v1/tenants/{tenant_id}", dependencies=[Depends(require_admin_key)])
async def delete_tenant(tenant_id: str) -> dict:
    """删除租户并下线其 Runner 与通道适配器。"""
    if not state.config_manager.get(tenant_id):
        raise HTTPException(status_code=404, detail=f"租户不存在: {tenant_id}")
    state.config_manager.remove(tenant_id)
    if state.tenant_store is not None:
        state.tenant_store.delete_config(tenant_id)
    runner = state.runners.pop(tenant_id, None)
    if runner:
        await runner.close()
    adapter = _feishu_adapters.pop(tenant_id, None)
    if adapter:
        adapter.deduper.clear()
    return {"deleted": tenant_id}


@app.get("/api/v1/tenants/{tenant_id}/revisions", dependencies=[Depends(require_admin_key)])
async def list_tenant_revisions(tenant_id: str) -> dict:
    """租户配置历史版本列表（新→旧）。"""
    if not state.config_manager.get(tenant_id):
        raise HTTPException(status_code=404, detail=f"租户不存在: {tenant_id}")
    if state.tenant_store is None:
        raise HTTPException(status_code=503, detail="租户持久化未启用（未配置 SQL_URL）")
    return {"tenant_id": tenant_id, "current": state.tenant_store.current_revision(tenant_id),
            "revisions": state.tenant_store.list_revisions(tenant_id)}


class TenantRollbackRequest(BaseModel):
    revision: int


@app.post("/api/v1/tenants/{tenant_id}/rollback", dependencies=[Depends(require_admin_key)])
async def rollback_tenant(tenant_id: str, req: TenantRollbackRequest) -> TenantInfo:
    """租户级配置回滚：读历史版本重新保存并热重建 Runner（回滚本身产生新版本）。"""
    if state.tenant_store is None:
        raise HTTPException(status_code=503, detail="租户持久化未启用（未配置 SQL_URL）")
    tenant_config = state.tenant_store.rollback_config(tenant_id, req.revision)
    if tenant_config is None:
        raise HTTPException(status_code=404, detail=f"版本不存在: r{req.revision}")
    # 热替换：下线旧 Runner → 重新注册配置 → 重建 Runner
    old_runner = state.runners.pop(tenant_id, None)
    if old_runner:
        await old_runner.close()
    state.config_manager.remove(tenant_id)
    state.config_manager.register(tenant_config)
    runner = state.build_runner(tenant_id)
    if runner:
        state.runners[tenant_id] = runner
    return TenantInfo(
        tenant_id=tenant_config.tenant_id,
        name=tenant_config.name,
        status=tenant_config.status,
        app_name=tenant_config.app.app_name,
    )


@app.post("/api/v1/tenants/reload", dependencies=[Depends(require_admin_key)])
async def reload_tenants() -> dict:
    """热加载 YAML 配置并重建配置有变化的租户 Runner。"""
    def _sig(cfg):
        return (
            cfg.app.app_name, cfg.model.model_dump_json(), cfg.storage.model_dump_json(),
            cfg.app.instruction, cfg.skills.enabled, cfg.rate_limit_per_minute,
        )

    before = {tid: _sig(t) for tid, t in state.config_manager.all().items()}
    after = state.config_manager.maybe_reload()

    # Admin API 创建、仅存在于 SQL 的租户重新合入
    if state.tenant_store is not None:
        for tenant_config in state.tenant_store.load_all_configs():
            if not after.get(tenant_config.tenant_id):
                after = state.config_manager.register(tenant_config)

    removed = [tid for tid in list(state.runners) if tid not in after]
    for tid in removed:
        runner = state.runners.pop(tid, None)
        if runner:
            await runner.close()
        state.config_manager.remove(tid)

    changed = [tid for tid, cfg in after.items() if before.get(tid) != _sig(cfg)]
    for tid in changed:
        old = state.runners.get(tid)
        if old:
            await old.close()
        runner = state.build_runner(tid)
        if runner:
            state.runners[tid] = runner
    return {"reloaded": True, "changed": changed, "removed": removed}


class TenantReleaseRequest(BaseModel):
    revision: int
    stage: str = "canary"  # canary（灰度先行）| stable（全量）


@app.post("/api/v1/tenants/{tenant_id}/release", dependencies=[Depends(require_admin_key)])
async def release_tenant(tenant_id: str, req: TenantReleaseRequest) -> TenantInfo:
    """灰度发布：把租户切到指定 revision 并标记灰度阶段。"""
    if req.stage not in ("canary", "stable"):
        raise HTTPException(status_code=422, detail=f"非法 stage: {req.stage}")
    if state.tenant_store is None:
        raise HTTPException(status_code=503, detail="租户持久化未启用（未配置 SQL_URL）")
    tenant_config = state.tenant_store.load_revision(tenant_id, req.revision)
    if tenant_config is None:
        raise HTTPException(status_code=404, detail=f"版本不存在: r{req.revision}")
    tenant_config.release_stage = req.stage
    state.tenant_store.save_config(tenant_config)  # 灰度动作本身产生新版本（可审计可回滚）

    old_runner = state.runners.pop(tenant_id, None)
    if old_runner:
        await old_runner.close()
    state.config_manager.remove(tenant_id)
    state.config_manager.register(tenant_config)
    runner = state.build_runner(tenant_id)
    if runner:
        state.runners[tenant_id] = runner
    return TenantInfo(
        tenant_id=tenant_config.tenant_id,
        name=tenant_config.name,
        status=tenant_config.status,
        app_name=tenant_config.app.app_name,
    )


@app.get("/api/v1/audit", dependencies=[Depends(require_admin_key)])
async def query_audit(limit: int = 20) -> List[dict]:
    """最近审计记录（脱敏后返回）。"""
    return audit_service.tail(limit=min(max(limit, 1), 200))


@app.get("/api/v1/metrics", dependencies=[Depends(require_admin_key)])
async def query_metrics() -> dict:
    """每租户业务指标快照（请求量/错误率/IM 投递成功率/token）。"""
    return {"tenants": metrics_collector.snapshot()}


# ---- 飞书通道（第二阶段） ----

_feishu_adapters: Dict[str, FeishuAdapter] = {}


def _get_feishu_adapter(tenant_id: str) -> FeishuAdapter:
    adapter = _feishu_adapters.get(tenant_id)
    if adapter is None:
        tenant_config = state.config_manager.get(tenant_id)
        channel_config = tenant_config.channels.get("feishu") if tenant_config else None
        if not tenant_config or not channel_config or not channel_config.enabled:
            raise HTTPException(status_code=404, detail=f"租户 {tenant_id} 未启用飞书通道")
        adapter = FeishuAdapter(
            tenant_config=tenant_config,
            channel_config=channel_config,
            runner_getter=state.runners.get,
            database=state.database,  # channel_binding / idempotency 落库
        )
        _feishu_adapters[tenant_id] = adapter
    return adapter


def _to_webhook_request(request: Request, method: str, body: str = "") -> WebhookRequest:
    flat_query = {k: v[0] for k, v in parse_qs(request.url.query, keep_blank_values=True).items()}
    return WebhookRequest(
        method=method, query=flat_query, body=body, headers=dict(request.headers)
    )


@app.post("/api/v1/channels/feishu/webhook/{tenant_id}")
async def feishu_callback(tenant_id: str, request: Request) -> JSONResponse:
    """飞书事件订阅回调（url_verification challenge / 消息事件）。"""
    adapter = _get_feishu_adapter(tenant_id)
    body = (await request.body()).decode("utf-8", errors="replace")
    response = await adapter.handle_webhook(tenant_id, _to_webhook_request(request, "POST", body))
    if response.content_type == "application/json":
        return JSONResponse(json.loads(response.body), status_code=response.status_code)
    return PlainTextResponse(response.body, status_code=response.status_code)


# ---- 企业微信通道（第二阶段） ----

_wecom_adapters: Dict[str, WeComAdapter] = {}


def _get_wecom_adapter(tenant_id: str) -> WeComAdapter:
    adapter = _wecom_adapters.get(tenant_id)
    if adapter is None:
        tenant_config = state.config_manager.get(tenant_id)
        channel_config = tenant_config.channels.get("wecom") if tenant_config else None
        if not tenant_config or not channel_config or not channel_config.enabled:
            raise HTTPException(status_code=404, detail=f"租户 {tenant_id} 未启用企业微信通道")
        adapter = WeComAdapter(
            tenant_config=tenant_config,
            channel_config=channel_config,
            runner_getter=state.runners.get,
            database=state.database,  # channel_binding / idempotency 落库
        )
        _wecom_adapters[tenant_id] = adapter
    return adapter


@app.get("/api/v1/channels/wecom/webhook/{tenant_id}")
async def wecom_verify(tenant_id: str, request: Request) -> PlainTextResponse:
    """企业微信 URL 可靠性验证（GET echostr）。"""
    adapter = _get_wecom_adapter(tenant_id)
    response = await adapter.handle_webhook(tenant_id, _to_webhook_request(request, "GET"))
    return PlainTextResponse(response.body, status_code=response.status_code)


@app.post("/api/v1/channels/wecom/webhook/{tenant_id}")
async def wecom_callback(tenant_id: str, request: Request) -> PlainTextResponse:
    """企业微信消息回调（POST 加密 XML）。"""
    adapter = _get_wecom_adapter(tenant_id)
    body = (await request.body()).decode("utf-8", errors="replace")
    response = await adapter.handle_webhook(tenant_id, _to_webhook_request(request, "POST", body))
    return PlainTextResponse(response.body, status_code=response.status_code, media_type=response.content_type)


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


def main() -> None:
    server = ServerConfig()
    uvicorn.run("trpc_service.web.app:app", host=server.host, port=server.port, workers=server.workers)


if __name__ == "__main__":
    main()
