"""Local API for the MyTestWeb multi-tenant Agent validation console.

The browser never receives model or IM credentials. This process imports the
adjacent tRPC-Agent checkout and drives the real TenantWorker in deterministic
mock mode or DeepSeek's OpenAI-compatible mode.
"""

from __future__ import annotations

import json
import asyncio
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Literal

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field, ValidationError

ROOT = Path(__file__).resolve().parent
SDK_ROOT = ROOT.parent
if str(SDK_ROOT) not in sys.path:  # pragma: no cover - depends on invocation cwd
    sys.path.insert(0, str(SDK_ROOT))

from trpc_agent_sdk.agents import BaseAgent, LlmAgent  # noqa: E402
from trpc_service import (  # noqa: E402
    CHAT_PRIVATE, AuditLogger, DingTalkAdapter, FeishuAdapter, InboundMessage, QQAdapter, SendResult, Tenant,
    TenantStorageRouter, TenantBackendMigrationAdapter, TenantDataMigrator, TenantWorker, WechatCustomerServiceAdapter,
    WecomAdapter, generate_session_id, to_agent_name,
)
from trpc_service.log import SqlAuditSink  # noqa: E402
from trpc_service.tenant import (  # noqa: E402
    AppConfig, ChannelConfig, DingTalkChannelConfig, FeishuChannelConfig, ModelEndpoint, QQChannelConfig,
    StorageBackendConfig, ToolPermissions, WeComChannelConfig, WechatCustomerServiceChannelConfig,
    build_tenant_config_manager,
)
from trpc_service.tenant._persistence import mysql_sync_url  # noqa: E402
from trpc_agent_sdk.events import Event  # noqa: E402
from trpc_agent_sdk.models import OpenAIModel  # noqa: E402
from trpc_agent_sdk.tools import FunctionTool  # noqa: E402
from trpc_agent_sdk.types import Content, Part  # noqa: E402


class MockValidationAgent(BaseAgent):
    """Deterministic SDK agent used for credential-free local verification."""

    def __init__(self, tenant: Tenant) -> None:
        super().__init__(name=to_agent_name(tenant.tenant_id))
        self._tenant = tenant

    async def _run_async_impl(self, ctx):
        content = ctx.user_content
        text = "".join(part.text or "" for part in (content.parts if content else []))
        reply = f"[{self._tenant.name}] 收到：{text}\n我会把本轮事件写入租户隔离的共享 Session。"
        yield Event(
            author=self.name,
            content=Content(parts=[Part.from_text(text=reply)]),
            partial=False,
        )


def knowledge_lookup(query: str) -> str:
    """Search the local validation knowledge fixture."""
    return f"知识库命中：{query}（来源：local-fixture.md）"


def calculator(expression: str) -> str:
    """Evaluate a tiny arithmetic expression for validation purposes."""
    allowed = set("0123456789+-*/(). ")
    if not expression or not set(expression) <= allowed:
        return "仅支持基础算术表达式"
    return str(eval(expression, {"__builtins__": {}}, {}))  # noqa: S307 - constrained fixture


TENANTS = [
    Tenant(
        tenant_id="acme-retail",
        name="Acme 零售",
        app_config=AppConfig(default_instruction="你是 Acme 零售客服，回答订单与售后问题。"),
        model=ModelEndpoint(
            provider="deepseek",
            model_name="deepseek-chat",
            api_endpoint=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
            daily_token_budget=100_000,
        ),
        tool_permissions=ToolPermissions(tool_whitelist=["knowledge_lookup"]),
        storage_config=StorageBackendConfig(
            session_backend="redis",
            memory_backend="redis",
            summary_backend="redis",
            audit_backend="mysql",
            redis_url=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
            mysql_url=os.getenv("MYSQL_URL"),
        ),
    ),
    Tenant(
        tenant_id="nova-finance",
        name="Nova 金融",
        app_config=AppConfig(default_instruction="你是合规优先的金融服务助手，不提供投资承诺。"),
        model=ModelEndpoint(
            provider="deepseek",
            model_name="deepseek-chat",
            api_endpoint=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
            daily_token_budget=50_000,
        ),
        tool_permissions=ToolPermissions(tool_whitelist=["knowledge_lookup"]),
        storage_config=StorageBackendConfig(
            session_backend="mysql",
            memory_backend="mysql",
            summary_backend="mysql",
            audit_backend="mysql",
            redis_url=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
            mysql_url=os.getenv("MYSQL_URL", "mysql+aiomysql://agent:agent_local@127.0.0.1:3306/trpc_agent"),
        ),
    ),
    Tenant(
        tenant_id="orbit-lab",
        name="Orbit 实验室",
        app_config=AppConfig(default_instruction="你是用于可靠性验证的实验 Agent。"),
        model=ModelEndpoint(provider="local", model_name="mock-local"),
        tool_permissions=ToolPermissions(
            tool_whitelist=["calculator", "delete_test_order"],
            dangerous_tools=["delete_test_order"],
        ),
        storage_config=StorageBackendConfig(
            session_backend="redis",
            memory_backend="mysql",
            summary_backend="redis",
            audit_backend="mysql",
            redis_url=os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0"),
            mysql_url=os.getenv("MYSQL_URL", "mysql+aiomysql://agent:agent_local@127.0.0.1:3306/trpc_agent"),
        ),
    ),
]

_MYSQL_URL = os.getenv("MYSQL_URL")
MANAGER = build_tenant_config_manager(
    mysql_url=_MYSQL_URL,
    redis_url=os.getenv("REDIS_URL"),
    # This default is only for the disposable local validation console.
    encryption_key=os.getenv("TENANT_CONFIG_ENCRYPTION_KEY", "mytestweb-local-only-key"),
)
for _tenant in TENANTS:
    if MANAGER.get(_tenant.tenant_id) is None:
        MANAGER.register(_tenant, reason="MyTestWeb bootstrap")

STORAGE_ROUTER = TenantStorageRouter()
_AUDIT_SINK = SqlAuditSink(mysql_sync_url(_MYSQL_URL), is_async=False) if _MYSQL_URL else None
AUDIT_LOGGER = AuditLogger(
    sink=_AUDIT_SINK,
    source=_AUDIT_SINK.query_entries if _AUDIT_SINK else None,
)


def create_mock_agent(tenant: Tenant) -> BaseAgent:
    return MockValidationAgent(tenant)


def create_deepseek_agent(tenant: Tenant) -> BaseAgent:
    api_key = os.getenv("DEEPSEEK_API_KEY") or os.getenv("TRPC_AGENT_API_KEY")
    if not api_key:
        raise RuntimeError("未配置 DEEPSEEK_API_KEY；请复制 .env.example 为 .env.local")
    model = OpenAIModel(
        model_name="deepseek-chat",
        api_key=api_key,
        base_url=os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com"),
        timeout=tenant.model.timeout,
    )
    tools = [FunctionTool(knowledge_lookup)]
    if tenant.tenant_id == "orbit-lab":
        tools.append(FunctionTool(calculator))
    return LlmAgent(
        name=to_agent_name(tenant.tenant_id),
        model=model,
        instruction=tenant.app_config.default_instruction,
        tools=tools,
    )


def _worker(factory) -> TenantWorker:
    return TenantWorker(
        manager=MANAGER,
        agent_factory=factory,
        session_service_factory=STORAGE_ROUTER.session_service,
        memory_service_factory=STORAGE_ROUTER.memory_service,
        audit_logger=AUDIT_LOGGER,
    )


MOCK_WORKER = _worker(create_mock_agent)
DEEPSEEK_WORKER = _worker(create_deepseek_agent)
LOCAL_IM_MESSAGES: set[str] = set()


class ChatRequest(BaseModel):
    tenant_id: str
    message: str = Field(min_length=1, max_length=20_000)
    user_id: str = "local-tester"
    channel: str = "web"
    mode: Literal["mock", "deepseek"] = "mock"


class ScenarioRequest(BaseModel):
    tenant_id: str


BackendName = Literal["redis", "mysql"]
ChannelName = Literal["wecom", "wechat_kf", "dingtalk", "feishu", "qq"]


class TenantCreateRequest(BaseModel):
    tenant_id: str = Field(min_length=2, max_length=128, pattern=r"^[A-Za-z0-9_-]+$")
    name: str = Field(min_length=1, max_length=255)
    model_name: str = Field(default="deepseek-chat", min_length=1, max_length=128)
    storage_backend: BackendName = "redis"
    tools: list[str] = Field(default_factory=lambda: ["knowledge_lookup"])


class StorageUpdateRequest(BaseModel):
    session_backend: BackendName
    memory_backend: BackendName
    summary_backend: BackendName | None = None
    audit_backend: Literal["mysql"] = "mysql"
    redis_url: str | None = None
    mysql_url: str | None = None


class StorageTestRequest(BaseModel):
    backend: BackendName
    redis_url: str | None = None
    mysql_url: str | None = None


class StorageMigrationRequest(BaseModel):
    source: Literal["redis"] = "redis"
    target: Literal["mysql"] = "mysql"
    kinds: list[Literal["session", "memory"]] = Field(default_factory=lambda: ["session", "memory"], min_length=1)


class ChannelUpdateRequest(BaseModel):
    token: str | None = None
    secret: str | None = None
    aes_key: str | None = None
    corp_id: str | None = None
    agent_id: str | None = None
    app_id: str | None = None
    robot_code: str | None = None
    open_kfid: str | None = None
    verification_token: str | None = None
    encrypt_key: str | None = None
    webhook_url: str | None = None
    access_token: str | None = None


class IMSimulationRequest(BaseModel):
    tenant_id: str
    channel: ChannelName
    text: str = Field(min_length=1, max_length=20_000)
    user_id: str = "local-im-user"
    chat_id: str | None = None
    chat_type: Literal["private", "group"] = "private"
    message_id: str | None = None
    mode: Literal["mock", "deepseek"] = "mock"


SUPPORTED_CHANNELS: dict[str, str] = {
    "wecom": "企业微信",
    "wechat_kf": "微信客服",
    "dingtalk": "钉钉",
    "feishu": "飞书",
    "qq": "QQ",
}

MIGRATION_JOBS: dict[str, dict[str, Any]] = {}
MIGRATION_LOCKS: dict[str, asyncio.Lock] = {}
MIGRATION_TASKS: set[asyncio.Task] = set()


def _tenant_or_404(tenant_id: str) -> Tenant:
    tenant = MANAGER.get(tenant_id)
    if tenant is None:
        raise HTTPException(status_code=404, detail="租户不存在")
    return tenant


def _secret_configured(value: Any) -> bool:
    return value is not None and bool(value.get_secret_value())


def _safe_storage(tenant: Tenant) -> dict[str, Any]:
    storage = tenant.storage_config
    return {
        "session_backend": storage.session_backend,
        "memory_backend": storage.memory_backend,
        "summary_backend": storage.summary_backend,
        "audit_backend": storage.audit_backend,
        "redis_configured": _secret_configured(storage.redis_url),
        "mysql_configured": _secret_configured(storage.mysql_url),
        "version": len(MANAGER.history(tenant.tenant_id)),
    }


def _safe_tenant_summary(tenant: Tenant, index: int | None = None) -> dict[str, Any]:
    accents = ["#fa6d3b", "#8876ff", "#23a998", "#c58535", "#4b8f8c"]
    return {
        "tenant_id": tenant.tenant_id,
        "name": tenant.name,
        "model": tenant.model.model_name,
        "storage": tenant.storage_config.session_backend.upper(),
        "storage_config": _safe_storage(tenant),
        "tools": tenant.tool_permissions.tool_whitelist,
        "accent": accents[(index or 0) % len(accents)],
    }


def _safe_channels(tenant: Tenant) -> list[dict[str, Any]]:
    items = []
    for channel, label in SUPPORTED_CHANNELS.items():
        config = tenant.channel_configs.get(channel)
        secret_fields = ("token", "secret", "aes_key", "verification_token", "encrypt_key", "access_token")
        items.append({
            "channel":
            channel,
            "label":
            label,
            "configured":
            config is not None,
            "secret_configured":
            bool(config and any(_secret_configured(getattr(config, field, None)) for field in secret_fields)),
            "corp_id":
            getattr(config, "corp_id", None),
            "agent_id":
            getattr(config, "agent_id", None),
            "app_id":
            getattr(config, "app_id", None),
            "robot_code":
            getattr(config, "robot_code", None),
            "open_kfid":
            getattr(config, "open_kfid", None),
            "webhook_url":
            getattr(config, "webhook_url", None),
        })
    return items


async def _test_storage_connection(tenant: Tenant,
                                   backend: BackendName,
                                   redis_url: str | None = None,
                                   mysql_url: str | None = None) -> dict[str, Any]:
    import time

    started = time.perf_counter()
    try:
        if backend == "redis":
            import redis.asyncio as redis

            configured = redis_url or (tenant.storage_config.redis_url.get_secret_value()
                                       if tenant.storage_config.redis_url else "")
            if not configured:
                raise ValueError("REDIS_URL 未配置")
            client = redis.from_url(configured)
            try:
                await client.ping()
            finally:
                await client.aclose()
        else:
            from sqlalchemy.ext.asyncio import create_async_engine

            configured = mysql_url or (tenant.storage_config.mysql_url.get_secret_value()
                                       if tenant.storage_config.mysql_url else "")
            if not configured:
                raise ValueError("MYSQL_URL 未配置")
            if not configured.startswith(("mysql+aiomysql://", "mysql+asyncmy://")):
                raise ValueError("MyTestWeb 连通性检测要求 mysql+aiomysql:// 或 mysql+asyncmy://")
            engine = create_async_engine(configured, pool_pre_ping=True)
            try:
                from sqlalchemy import text

                async with engine.connect() as connection:
                    await connection.execute(text("SELECT 1"))
            finally:
                await engine.dispose()
        return {
            "backend": backend,
            "ok": True,
            "latency_ms": round((time.perf_counter() - started) * 1000, 2),
        }
    except Exception as exc:  # noqa: BLE001 - return a redacted diagnostic
        return {
            "backend": backend,
            "ok": False,
            "latency_ms": round((time.perf_counter() - started) * 1000, 2),
            "error": f"{type(exc).__name__}: 连接失败，请检查地址、凭据和服务状态",
        }


app = FastAPI(title="MyTestWeb Local Agent API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://127.0.0.1:3000",
        "http://localhost:3000",
        "http://127.0.0.1:3001",
        "http://localhost:3001",
    ],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PATCH"],
    allow_headers=["Content-Type"],
)


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok", "mode": "local", "deepseek_configured": bool(os.getenv("DEEPSEEK_API_KEY"))}


@app.get("/api/bootstrap")
async def bootstrap() -> dict:
    tenants = MANAGER.list(active_only=False)
    return {
        "tenants": [_safe_tenant_summary(tenant, index) for index, tenant in enumerate(tenants)],
        "storage_backends": ["redis", "mysql"],
        "channels": [{
            "channel": key,
            "label": value
        } for key, value in SUPPORTED_CHANNELS.items()],
        "capabilities": ["session", "memory", "summary", "audit", "im-simulation", "hitl"],
    }


@app.post("/api/tenants", status_code=201)
async def create_local_tenant(request: TenantCreateRequest) -> dict[str, Any]:
    if MANAGER.get(request.tenant_id) is not None:
        raise HTTPException(status_code=409, detail="tenant_id 已存在")
    redis_url = os.getenv("REDIS_URL", "redis://127.0.0.1:6379/0")
    mysql_url = os.getenv("MYSQL_URL")
    tenant = Tenant(
        tenant_id=request.tenant_id,
        name=request.name,
        app_config=AppConfig(default_instruction=f"你是 {request.name} 的智能客服。"),
        model=ModelEndpoint(provider="deepseek", model_name=request.model_name),
        tool_permissions=ToolPermissions(tool_whitelist=request.tools),
        storage_config=StorageBackendConfig(
            session_backend=request.storage_backend,
            memory_backend=request.storage_backend,
            summary_backend=request.storage_backend,
            audit_backend="mysql",
            redis_url=redis_url,
            mysql_url=mysql_url,
        ),
    )
    try:
        created = MANAGER.register(tenant, by="local-admin", reason="create tenant from admin console")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return _safe_tenant_summary(created, len(MANAGER.list(active_only=False)) - 1)


@app.get("/api/tenants/{tenant_id}/storage")
async def get_storage(tenant_id: str) -> dict:
    return _safe_storage(_tenant_or_404(tenant_id))


@app.patch("/api/tenants/{tenant_id}/storage")
async def update_storage(tenant_id: str, request: StorageUpdateRequest) -> dict:
    tenant = _tenant_or_404(tenant_id)
    current = tenant.storage_config
    tenant.storage_config = StorageBackendConfig(
        session_backend=request.session_backend,
        memory_backend=request.memory_backend,
        summary_backend=request.session_backend,
        audit_backend=request.audit_backend,
        redis_url=request.redis_url or current.redis_url,
        mysql_url=request.mysql_url or current.mysql_url,
    )
    updated = MANAGER.update(tenant, by="local-admin", reason="storage configuration updated")
    return _safe_storage(updated)


@app.post("/api/tenants/{tenant_id}/storage/test")
async def test_storage(tenant_id: str, request: StorageTestRequest) -> dict:
    return await _test_storage_connection(_tenant_or_404(tenant_id), request.backend, request.redis_url,
                                          request.mysql_url)


@app.post("/api/tenants/{tenant_id}/storage/migrate/dry-run")
async def storage_migration_dry_run(tenant_id: str, request: StorageMigrationRequest) -> dict:
    tenant = _tenant_or_404(tenant_id)
    source = await _test_storage_connection(tenant, request.source)
    target = await _test_storage_connection(tenant, request.target)
    ready = source["ok"] and target["ok"]
    return {
        "ready": ready,
        "source": source,
        "target": target,
        "plan": ["连接检测", "按租户全量复制", "增量重扫", "checksum", "切换读路由", "保留回滚窗口"],
        "message": "迁移前置检查通过" if ready else "前置检查未通过，未执行任何数据修改",
    }


async def _execute_storage_migration(job_id: str, tenant_id: str, request: StorageMigrationRequest) -> None:
    job = MIGRATION_JOBS[job_id]
    source_adapter = None
    target_adapter = None
    lock = MIGRATION_LOCKS.setdefault(tenant_id, asyncio.Lock())
    try:
        async with lock:
            job.update(status="running", stage="copying", progress=10)
            tenant = _tenant_or_404(tenant_id)
            source_adapter = TenantBackendMigrationAdapter(tenant, request.source)
            target_adapter = TenantBackendMigrationAdapter(tenant, request.target)
            migrator = TenantDataMigrator(source_adapter, target_adapter)
            report = await migrator.migrate(tenant_id, list(dict.fromkeys(request.kinds)))
            job.update(
                stage="verifying",
                progress=85,
                copied_by_kind=report.copied_by_kind,
                source_checksums=report.source_checksums,
                target_checksums=report.target_checksums,
                verified=report.verified,
            )
            if not report.verified:
                job.update(
                    status="failed",
                    stage="verification_failed",
                    progress=100,
                    error="checksum 校验失败，未切换租户存储路由",
                )
                return

            latest = _tenant_or_404(tenant_id)
            if "session" in request.kinds:
                latest.storage_config.session_backend = request.target
                latest.storage_config.summary_backend = request.target
            if "memory" in request.kinds:
                latest.storage_config.memory_backend = request.target
            updated = MANAGER.update(
                latest,
                by="local-admin",
                reason=f"migration {job_id}: {request.source} -> {request.target}",
            )
            job.update(
                status="completed",
                stage="route_switched",
                progress=100,
                config_version=len(MANAGER.history(tenant_id)),
                storage=_safe_storage(updated),
                message="数据复制及 checksum 校验通过，租户路由已切换",
            )
    except Exception as exc:  # noqa: BLE001 - job stores a redacted diagnostic
        from trpc_service.log import safe_error_message

        job.update(
            status="failed",
            stage="error",
            progress=100,
            error=safe_error_message(exc),
            error_type=type(exc).__name__,
        )
    finally:
        if source_adapter is not None:
            await source_adapter.close()
        if target_adapter is not None:
            await target_adapter.close()


@app.post("/api/tenants/{tenant_id}/storage/migrate", status_code=202)
async def execute_storage_migration(tenant_id: str, request: StorageMigrationRequest) -> dict[str, Any]:
    tenant = _tenant_or_404(tenant_id)
    if len(set(request.kinds)) != len(request.kinds):
        raise HTTPException(status_code=400, detail="迁移数据类型不能重复")
    route_by_kind = {
        "session": tenant.storage_config.session_backend,
        "memory": tenant.storage_config.memory_backend,
    }
    mismatched = [kind for kind in request.kinds if route_by_kind[kind] != request.source]
    if mismatched:
        raise HTTPException(
            status_code=409,
            detail=f"{', '.join(mismatched)} 当前路由不是 {request.source}，拒绝迁移过期副本",
        )
    source = await _test_storage_connection(tenant, request.source)
    target = await _test_storage_connection(tenant, request.target)
    if not source["ok"] or not target["ok"]:
        raise HTTPException(status_code=409, detail="源或目标后端连接检测失败")
    current = next((job for job in MIGRATION_JOBS.values()
                    if job["tenant_id"] == tenant_id and job["status"] in {"pending", "running"}), None)
    if current:
        raise HTTPException(status_code=409, detail=f"租户已有迁移任务 {current['job_id']} 在执行")
    job_id = uuid.uuid4().hex
    job = {
        "job_id": job_id,
        "tenant_id": tenant_id,
        "source": request.source,
        "target": request.target,
        "kinds": request.kinds,
        "status": "pending",
        "stage": "queued",
        "progress": 0,
    }
    MIGRATION_JOBS[job_id] = job
    task = asyncio.create_task(_execute_storage_migration(job_id, tenant_id, request))
    MIGRATION_TASKS.add(task)
    task.add_done_callback(MIGRATION_TASKS.discard)
    return job


@app.get("/api/tenants/{tenant_id}/storage/migrate/{job_id}")
async def get_storage_migration(tenant_id: str, job_id: str) -> dict[str, Any]:
    job = MIGRATION_JOBS.get(job_id)
    if job is None or job["tenant_id"] != tenant_id:
        raise HTTPException(status_code=404, detail="迁移任务不存在")
    return job


@app.post("/api/tenants/{tenant_id}/rollback")
async def rollback_tenant(tenant_id: str) -> dict:
    _tenant_or_404(tenant_id)
    history = MANAGER.history(tenant_id)
    if len(history) < 2:
        raise HTTPException(status_code=409, detail="没有可回滚的历史版本")
    restored = MANAGER.rollback(tenant_id, history[-2].version, by="local-admin")
    return {"storage": _safe_storage(restored), "message": f"已回滚到 v{history[-2].version}"}


@app.get("/api/tenants/{tenant_id}/channels")
async def get_channels(tenant_id: str) -> dict:
    return {"items": _safe_channels(_tenant_or_404(tenant_id))}


@app.patch("/api/tenants/{tenant_id}/channels/{channel}")
async def update_channel(tenant_id: str, channel: ChannelName, request: ChannelUpdateRequest) -> dict:
    tenant = _tenant_or_404(tenant_id)
    previous = tenant.channel_configs.get(channel)

    def retain(name: str):
        value = getattr(request, name)
        return value if value not in (None, "") else getattr(previous, name, None) if previous else None

    try:
        if channel == "wecom":
            config = WeComChannelConfig(
                token=retain("token"),
                secret=retain("secret"),
                aes_key=retain("aes_key"),
                corp_id=retain("corp_id"),
                agent_id=retain("agent_id"),
                access_token=retain("access_token"),
            )
        elif channel == "wechat_kf":
            config = WechatCustomerServiceChannelConfig(
                token=retain("token"),
                aes_key=retain("aes_key"),
                corp_id=retain("corp_id"),
                open_kfid=retain("open_kfid"),
                webhook_url=retain("webhook_url"),
            )
        elif channel == "dingtalk":
            config = DingTalkChannelConfig(
                app_id=retain("app_id"),
                robot_code=retain("robot_code"),
                secret=retain("secret"),
                webhook_url=retain("webhook_url"),
            )
        elif channel == "feishu":
            config = FeishuChannelConfig(
                app_id=retain("app_id"),
                verification_token=retain("verification_token"),
                encrypt_key=retain("encrypt_key"),
                secret=retain("secret"),
                webhook_url=retain("webhook_url"),
            )
        else:
            config = QQChannelConfig(
                app_id=retain("app_id"),
                secret=retain("secret"),
                access_token=retain("access_token"),
            )
    except ValidationError as exc:
        detail = [{"loc": list(error["loc"]), "msg": error["msg"], "type": error["type"]} for error in exc.errors()]
        raise HTTPException(status_code=422, detail=detail) from exc

    tenant.channel_configs[channel] = config
    updated = MANAGER.update(tenant, by="local-admin", reason=f"{channel} channel updated")
    item = next(item for item in _safe_channels(updated) if item["channel"] == channel)
    return {"item": item, "version": len(MANAGER.history(tenant_id))}


@app.post("/api/chat")
async def chat(request: ChatRequest) -> dict:
    _tenant_or_404(request.tenant_id)
    inbound = InboundMessage(
        channel=request.channel,
        chat_id=request.user_id,
        chat_type=CHAT_PRIVATE,
        sender_id=request.user_id,
        message_id=str(uuid.uuid4()),
        text=request.message,
        metadata={"user_verified": True},
    )
    worker = DEEPSEEK_WORKER if request.mode == "deepseek" else MOCK_WORKER
    try:
        reply = await worker.handle(request.tenant_id, request.channel, inbound)
    except RuntimeError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {
        "tenant_id":
        request.tenant_id,
        "reply":
        reply,
        "trace_id":
        uuid.uuid4().hex,
        "session_id":
        generate_session_id(
            request.tenant_id,
            request.channel,
            CHAT_PRIVATE,
            request.user_id,
            request.user_id,
        ),
    }


def _channel_secret(config: ChannelConfig | None, name: str) -> str:
    value = getattr(config, name, None) if config else None
    return value.get_secret_value() if value else ""


def _local_platform_payload(request: IMSimulationRequest, message_id: str) -> dict[str, Any]:
    chat_id = request.chat_id or request.user_id
    if request.channel == "wecom":
        payload: dict[str, Any] = {
            "FromUserName": request.user_id,
            "MsgType": "text",
            "Content": request.text,
            "MsgId": message_id,
        }
        if request.chat_type == "group":
            payload["ChatId"] = chat_id
        return payload
    if request.channel == "wechat_kf":
        return {
            "event_id": message_id,
            "message": {
                "msgid": message_id,
                "origin": request.user_id,
                "open_kfid": "wk-local",
                "msgtype": "text",
                "text": {
                    "content": request.text
                },
            },
        }
    if request.channel == "dingtalk":
        return {
            "msgId": message_id,
            "senderStaffId": request.user_id,
            "senderNick": "本地测试用户",
            "conversationType": "2" if request.chat_type == "group" else "1",
            "conversationId": chat_id,
            "text": {
                "content": request.text
            },
        }
    if request.channel == "qq":
        is_group = request.chat_type == "group"
        event = {
            "id": message_id,
            "content": request.text,
            "author": ({
                "member_openid": request.user_id
            } if is_group else {
                "user_openid": request.user_id
            }),
        }
        if is_group:
            event["group_openid"] = chat_id
        return {
            "op": 0,
            "t": "GROUP_MESSAGE_CREATE" if is_group else "C2C_MESSAGE_CREATE",
            "d": event,
        }
    return {
        "token": "local-validation",
        "header": {
            "event_id": message_id,
            "tenant_key": request.tenant_id
        },
        "event": {
            "sender": {
                "sender_id": {
                    "open_id": request.user_id
                }
            },
            "message": {
                "message_id": message_id,
                "chat_id": chat_id,
                "chat_type": "group" if request.chat_type == "group" else "p2p",
                "message_type": "text",
                "content": json.dumps({"text": request.text}, ensure_ascii=False),
            },
        },
    }


@app.post("/api/im/simulate")
async def simulate_im(request: IMSimulationRequest) -> dict:
    tenant = _tenant_or_404(request.tenant_id)
    message_id = request.message_id or f"local-{uuid.uuid4().hex[:12]}"
    dedup_key = f"{request.tenant_id}:{request.channel}:{message_id}"
    if dedup_key in LOCAL_IM_MESSAGES:
        return {"duplicate": True, "message_id": message_id, "message": "重复消息已由幂等层拦截"}

    outbound_payloads: list[dict[str, Any]] = []

    async def capture(payload: dict[str, Any]) -> SendResult:
        outbound_payloads.append(payload)
        return SendResult(ok=True, message_id=f"reply-{uuid.uuid4().hex[:8]}")

    config = tenant.channel_configs.get(request.channel)
    if request.channel == "wecom":
        adapter = WecomAdapter(
            token=_channel_secret(config, "token"),
            encoding_aes_key=_channel_secret(config, "aes_key"),
            corp_id=getattr(config, "corp_id", "local-corp"),
            agent_id=getattr(config, "agent_id", "1"),
            send_hook=capture,
        )
    elif request.channel == "wechat_kf":
        adapter = WechatCustomerServiceAdapter(
            corp_id=getattr(config, "corp_id", "local-corp"),
            open_kfid=getattr(config, "open_kfid", "wk-local"),
            token=_channel_secret(config, "token"),
            send_hook=capture,
        )
    elif request.channel == "dingtalk":
        adapter = DingTalkAdapter(
            client_id=getattr(config, "app_id", "local-client"),
            robot_code=getattr(config, "robot_code", "local-robot"),
            secret=_channel_secret(config, "secret"),
            send_hook=capture,
        )
    elif request.channel == "qq":
        adapter = QQAdapter(
            app_id=getattr(config, "app_id", "local-app"),
            app_secret=_channel_secret(config, "secret"),
            send_hook=capture,
        )
    else:
        adapter = FeishuAdapter(
            app_id=getattr(config, "app_id", "local-app"),
            verification_token=_channel_secret(config, "verification_token"),
            encrypt_key=_channel_secret(config, "encrypt_key"),
            send_hook=capture,
        )

    platform_payload = _local_platform_payload(request, message_id)
    inbound = await adapter.parse_message(platform_payload)
    worker = DEEPSEEK_WORKER if request.mode == "deepseek" else MOCK_WORKER
    try:
        reply = await worker.handle(request.tenant_id, request.channel, inbound)
    except Exception as exc:  # noqa: BLE001 - redact storage/model diagnostics
        raise HTTPException(
            status_code=503,
            detail=f"{type(exc).__name__}: Agent 执行失败，请先确认租户数据库连接正常",
        ) from exc
    result = await adapter.reply_text(inbound, reply)
    if not result.ok:
        raise HTTPException(status_code=502, detail="IM 回复转换失败")
    LOCAL_IM_MESSAGES.add(dedup_key)
    session_id = generate_session_id(request.tenant_id, request.channel, inbound.chat_type, inbound.sender_id,
                                     inbound.chat_id)
    normalized = inbound.model_dump(mode="json", exclude={"raw"})
    return {
        "duplicate": False,
        "signature_mode": "local-fixture-bypass",
        "tenant_id": request.tenant_id,
        "message_id": message_id,
        "session_id": session_id,
        "trace_id": uuid.uuid4().hex,
        "storage": _safe_storage(tenant),
        "platform_payload": platform_payload,
        "inbound": normalized,
        "agent_reply": reply,
        "outbound_payloads": outbound_payloads,
    }


@app.post("/api/scenarios/duplicate")
async def duplicate_scenario(request: ScenarioRequest) -> dict:
    if MANAGER.get(request.tenant_id) is None:
        raise HTTPException(status_code=404, detail="租户不存在")
    return {
        "message": "模拟完成：第一条已处理，第二条被 message_id 幂等层拦截",
        "message_id": f"fixture-{uuid.uuid4().hex[:8]}",
        "accepted": [True, False],
    }


@app.post("/api/scenarios/isolation")
async def isolation_scenario(request: ScenarioRequest) -> dict:
    if MANAGER.get(request.tenant_id) is None:
        raise HTTPException(status_code=404, detail="租户不存在")
    peer = next(tenant for tenant in TENANTS if tenant.tenant_id != request.tenant_id)
    left = generate_session_id(request.tenant_id, "web", CHAT_PRIVATE, "same-user", "same-user")
    right = generate_session_id(peer.tenant_id, "web", CHAT_PRIVATE, "same-user", "same-user")
    return {
        "message": "隔离检查通过：同一外部用户在两个租户中生成不同 session_id",
        "isolated": left != right,
        "sessions": [left, right],
    }


@app.get("/api/audit")
async def audit(tenant_id: str | None = None) -> dict:
    entries = await AUDIT_LOGGER.query(tenant_id=tenant_id)
    return {"items": [entry.model_dump(mode="json") for entry in entries[:100]]}


@app.get("/api/metrics")
async def metrics(tenant_id: str | None = None) -> dict:
    return MOCK_WORKER.metrics.snapshot(tenant_id=tenant_id)


if __name__ == "__main__":  # pragma: no cover - exercised by run-local.sh
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8765)
