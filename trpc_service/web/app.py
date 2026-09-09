# ===================================================================
# web.app - Gateway FastAPI 应用（webhook 入口 + 治理链 + Runtime）
# ===================================================================
# 说明: Agent Gateway 服务化（PRD 0.2/0.3）:
#   - POST /webhook/{channel_type}/{binding_id}: IM 回调入口
#     （Channel Adapter 解析 -> 验签 -> Filter 链治理 -> Runtime 执行 -> 回复投递）
#   - GET /webhook/... : 企微 URL 验证（echostr，验签后回显明文）
#   - /metrics: Prometheus 指标；/healthz /readyz: 健康探针（PRD 5.4）
#   - /chat: Web UI 自测入口（前端 JSON -> 同一条链路）
# 规范: Gateway 无状态（多实例水平扩展），所有状态读写共享后端。
#   适配器按租户 im 配置懒创建并缓存（ChannelFactory），租户配置热更新后
#   建议重启或显式失效缓存（密钥变更需重建适配器）。
# ===================================================================

from __future__ import annotations

import asyncio
import inspect
import threading
import time
import uuid
from typing import Any, Optional
from pathlib import Path

from fastapi import FastAPI, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse

from ..channels.base import IMAdapter, RecallEvent, mark_message_revoked
from ..channels.factory import ChannelFactory
from ..channels.web import WebImAdapter
from ..config.redaction import redact
from ..events import AgentEvent, AgentResponse, ResponseType
from ..filters.impl import build_filter_chain
from ..filters.rate_limiter import RedisFixedWindowLimiter, TenantRateLimiter
from ..log.logger import bind_logger, get_logger
from ..metrics.metrics import get_metrics
from ..runtime.pipeline import process_event
from ..runtime.runtime import Runtime
from ..storage.base import Storage, acquire_lock_with_retry
from ..storage.inmemory import InMemoryStorage
from ..storage.manager import StorageManager
from ..tenant.registry import TenantRegistry
from ..tenant.resolver import generate_session_id

# 经 _BoundAdapter 包装：调用点 extra 并入 extra_fields，JSON 日志才带得出
# 排障字段（如 im send failed 的 error 详情——普通 Logger 的 extra 会被
# JsonFormatter 丢弃，09-07 排查企微 60020 时实测踩坑）。
log = bind_logger(get_logger("web.gateway"))

# Web UI 自测页面（内嵌，避免依赖静态文件服务器）
# 技术栈与 Admin 控制台统一（Vue3 + TDesign CDN，单文件零工具链，09-04）。
# 请求契约保持不变: POST /chat {tenant_id, user_id, content}（msg_id/session_id
# 由后端生成，确定性 session 保证多轮连续）。
# 前端模板拆分自本文件（此前以字符串常量内嵌 139 行，
# 视图与接口层耦合且无法 lint/高亮）；见 templates/ 目录。
WEB_UI_HTML = (Path(__file__).parent / "templates" / "web_ui.html").read_text(encoding="utf-8")


def _load_tenant_fn() -> Any:
    """构造 TenantRegistry 的回源加载函数（内置 demo 租户开箱即用）。

    其余 tenant_id 暂不回源（生产接入 SqlTenantStore 后在此读取租户配置）。
    """

    async def load_fn(tenant_id: str) -> Optional[dict[str, Any]]:
        if tenant_id == "demo":
            return {
                "tenant_id": "demo",
                "name": "演示租户",
                "status": "active",
                "app": {
                    "agent_type": "llm",
                    "system_prompt": "你是 Teneuris 演示助手。"
                },
                "model": {
                    "provider": "mock",
                    "model_name": "mock"
                },
                "tools": {
                    "allowlist": ["echo", "get_time", "calculator", "web_search"],
                    "dangerous_tools": ["delete_file"]
                },
                "im": [{
                    "channel_type": "web",
                    "webhook_path": ""
                }],
                "backends": {
                    "session": "inmemory",
                    "memory": "inmemory",
                    "summary": "inmemory",
                    "audit": "inmemory"
                },
                "rate_limit_per_min": 60,
            }
        return None

    return load_fn


def build_gateway_app(
    registry: Optional[TenantRegistry] = None,
    storage: Optional[Storage] = None,
    runtime: Optional[Runtime] = None,
    channel_factory: Optional[ChannelFactory] = None,
    storage_manager: Optional[StorageManager] = None,
    readiness_checks: Optional[dict[str, Any]] = None,
    limiter: Optional[TenantRateLimiter | RedisFixedWindowLimiter] = None,
) -> FastAPI:
    """构建 Gateway FastAPI 应用（依赖可注入，便于测试）。

    Args:
        registry: 租户注册表（默认新建，demo 租户开箱即用）
        storage: 存储后端（默认 InMemory；幂等 / 锁等共享基础设施语义）
        runtime: Runtime（默认用 MockAgentRunner）
        channel_factory: 通道工厂（默认新建）
        storage_manager: 按租户懒建 Storage 的容器（PRD 2.1）；None 时
            所有租户共用 `storage`（测试 / 单机默认路径）。
        readiness_checks: 就绪探针 {名称: async 可调用}，/readyz 逐一检查
            （redis ping / sql connect）；None 或空时 readyz 仅存活探针。
        limiter: 租户限流器（TenantRateLimiter / RedisFixedWindowLimiter）；
            None 时进程内令牌桶（多节点部署应注入共享实现）。

    Returns:
        FastAPI: Gateway 应用
    """
    if storage is None:
        storage = InMemoryStorage()
    if registry is None:
        registry = TenantRegistry(load_fn=_load_tenant_fn())
    if channel_factory is None:
        channel_factory = ChannelFactory()
    if runtime is None:
        from ..runtime.runner import MockAgentRunner

        runtime = Runtime(registry=registry,
                          storage=storage,
                          metrics=get_metrics(),
                          runner=MockAgentRunner(),
                          storage_manager=storage_manager)

    chain, ctx = build_filter_chain(
        registry=registry,
        storage=storage,
        metrics=get_metrics(),
        storage_manager=storage_manager,
        limiter=limiter,
    )

    app = FastAPI(title="Teneuris Agent Gateway", version="0.1.0")
    app.state.registry = registry
    app.state.storage = storage
    app.state.storage_manager = storage_manager
    app.state.runtime = runtime
    app.state.chain = chain
    app.state.chain_ctx = ctx
    app.state.channels = channel_factory
    app.state.readiness_checks = readiness_checks or {}

    @app.get("/", response_class=HTMLResponse)
    async def index() -> str:
        return WEB_UI_HTML

    @app.get("/chat", response_class=HTMLResponse)
    async def chat_page() -> str:
        return WEB_UI_HTML

    @app.post("/chat")
    async def web_chat(request: Request) -> JSONResponse:
        """Web UI 自测入口（前端 JSON -> 同一治理+执行链路）。"""
        body = await request.body()
        adapter = WebImAdapter()
        try:
            parsed = adapter.parse_webhook(body, dict(request.headers))
        except ValueError as exc:
            return JSONResponse({"response_type": "error", "content": str(exc)}, status_code=400)
        response = await _process(parsed.event)
        return _to_json_response(response)

    @app.post("/webhook/{channel_type}/{binding_id}")
    async def webhook(channel_type: str, binding_id: str, request: Request) -> Response:
        """IM 回调入口（PRD 3.4: /webhook/{channel_type}/{binding_id}）。

        流程: 解析 -> 验签 -> Filter 链治理 -> Runtime 执行 -> 投递回复。
        适配器按租户 im 配置懒创建；web 通道（自测）直接在 HTTP 响应回显，
        真实 IM 通道经 adapter.send_message 主动投递（异步回复语义）。
        """
        body = await request.body()
        # headers 合并 URL query（企微 msg_signature/timestamp/nonce 走 query）
        headers = dict(request.headers)
        headers.update({k: v for k, v in request.query_params.items()})
        # binding_id 前缀约定: {tenant_id}__{channel_binding_id}
        tenant_id = binding_id.split("__", 1)[0] if "__" in binding_id else binding_id

        adapter = await _resolve_adapter(tenant_id, channel_type, webhook_path=f"/webhook/{channel_type}/{binding_id}")
        if adapter is None:
            return JSONResponse({"error": "channel not configured"}, status_code=404)
        # 飞书等以 POST 发起 URL 验证（url_verification challenge），
        # 验签前短路回显 challenge，避免被当作消息处理（PRD 3.4）。
        challenge = adapter.url_verification_response(body, headers)
        if challenge is not None:
            return JSONResponse(challenge)
        try:
            parsed = adapter.parse_webhook(body, headers)
        except ValueError as exc:
            # body 为外部可控输入，落日志前先过脱敏（PRD 4.5；审查 09-04）
            log.warning(f"webhook 解析失败: {channel_type}/{binding_id}: {exc} | "
                        f"body_head={redact(body[:800].decode('utf-8', errors='replace'))!r}")
            return JSONResponse({"error": str(exc)}, status_code=400)
        except Exception as exc:  # noqa: BLE001 - 记录未归一化异常（含解密错误）
            log.warning(f"webhook 处理异常: {channel_type}/{binding_id}: {type(exc).__name__}: {redact(str(exc))}")
            return JSONResponse({"error": "bad request"}, status_code=400)
        # 验签（PRD 3.4；未配置 token 的通道放行）
        if not adapter.verify_signature(body, parsed.signature):
            return JSONResponse({"error": "invalid signature"}, status_code=401)

        # 撤回事件（PRD 3.7）: 识别平台撤回回调 → 审计留痕 + 历史标记 revoked，
        # **不触发 Agent**（撤回不是用户新输入）。验签后优先处理，短路返回 ack。
        recall = adapter.parse_recall_event(body, headers)
        if recall is not None:
            try:
                tenant = await app.state.registry.get(tenant_id) if tenant_id else None
                agent_name = tenant.name if tenant else ""
            except Exception:  # noqa: BLE001 - 租户名解析失败仅影响审计字段，不阻断撤回
                agent_name = ""
            await _handle_recall(app.state.storage, adapter, tenant_id, recall, agent_name=agent_name)
            if adapter.channel_type == "web":
                return JSONResponse({"recalled": True})
            return Response(content="success", media_type="text/plain")

        parsed.event.tenant_id = tenant_id or parsed.event.tenant_id
        # 身份映射（PRD 3.4）: 外部 user_id -> 内部 user_id（供 user_acl 白名单 /
        # 审计 / session 使用）；原始外部 id 保留在 metadata，发送回复时回投用。
        mapped = adapter.map_user_id(parsed.event.user_id)
        if mapped != parsed.event.user_id:
            parsed.event.metadata["external_user_id"] = parsed.event.user_id
            parsed.event.user_id = mapped

        response = await _process(parsed.event)
        # 真实 IM 通道: 主动投递回复（企微应用消息 / 飞书机器人消息）。
        # 治理阻断 / 重复消息等错误响应不投递（audit 已留痕），仅 ack。
        if adapter.channel_type != "web":
            if response.response_type == ResponseType.TEXT:
                try:
                    # 回投使用原始外部 user_id（企微成员 userid / 飞书 open_id）
                    reply_user = parsed.event.metadata.get("external_user_id") or parsed.event.user_id
                    response.metadata["user_id"] = reply_user
                    response.metadata["from_user"] = reply_user
                    attempts = await _send_reply_with_retry(adapter, tenant_id, response, channel_type)
                    if attempts > 1:
                        log.info("im send recovered after retry", extra={"tenant": tenant_id, "channel": channel_type})
                    _record_im_delivery(tenant_id, channel_type, response, success=True)
                except Exception as exc:  # noqa: BLE001 - 投递失败不吞链路异常
                    log.warning("im send failed",
                                extra={
                                    "tenant": tenant_id,
                                    "channel": channel_type,
                                    "error": str(exc)
                                })
                    _record_im_delivery(tenant_id, channel_type, response, success=False)
                    return JSONResponse({"error": f"reply delivery failed: {exc}"}, status_code=502)
            # 企微回调要求 5s 内回 ack（PRD 3.6）；空文本 200 即 ack
            return Response(content="success", media_type="text/plain")
        return _to_json_response(response)

    @app.get("/webhook/{channel_type}/{binding_id}")
    async def webhook_verify(channel_type: str, binding_id: str, request: Request) -> Response:
        """企微 URL 验证: GET /webhook/...?echostr=xxx（PRD 3.4）。"""
        echostr = request.query_params.get("echostr", "")
        if not echostr:
            return JSONResponse({"error": "missing echostr"}, status_code=400)
        tenant_id = binding_id.split("__", 1)[0] if "__" in binding_id else binding_id
        adapter = await _resolve_adapter(tenant_id, channel_type, webhook_path=f"/webhook/{channel_type}/{binding_id}")
        if adapter is None:
            return JSONResponse({"error": "channel not configured"}, status_code=404)
        signature = request.query_params.get("msg_signature", "") or request.query_params.get("signature", "")
        timestamp = request.query_params.get("timestamp", "")
        nonce = request.query_params.get("nonce", "")
        # 验签 + 解密（企微 echostr 加密；web 自测通道原样回显）
        if not adapter.verify_echostr(echostr, timestamp, nonce, signature):
            return JSONResponse({"error": "invalid signature"}, status_code=401)
        try:
            plain = adapter.decrypt_echostr(echostr)
        except Exception as exc:  # noqa: BLE001 - 解密失败回 401
            return JSONResponse({"error": f"echostr decrypt failed: {exc}"}, status_code=401)
        return Response(content=plain, media_type="text/plain")

    @app.get("/healthz")
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    async def readyz() -> JSONResponse:
        """就绪探针：逐一检查外部依赖连通（Redis / SQL）。

        readiness_checks 为空（单机 InMemory 开发）时仅表示进程存活；
        redis/sql 生产模式下注入真实 ping/connect 探针，任一失败返回 503。
        """
        checks: dict[str, Any] = dict(app.state.readiness_checks or {})
        if not checks:
            return JSONResponse({"status": "ready"})
        results: dict[str, str] = {}
        ok = True
        for name, probe in checks.items():
            try:
                out = probe()
                if inspect.isawaitable(out):
                    await out
                results[name] = "ok"
            except Exception as exc:  # noqa: BLE001 - 依赖不可用标记失败
                results[name] = f"down: {type(exc).__name__}"
                ok = False
        status = "ready" if ok else "not_ready"
        return JSONResponse({"status": status, "checks": results}, status_code=200 if ok else 503)

    from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

    @app.get("/metrics")
    async def metrics() -> Response:
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)

    async def _resolve_adapter(tenant_id: str, channel_type: str, webhook_path: str = ""):
        """按租户 im 配置懒创建适配器（ChannelFactory 缓存实例）。

        返回 None 表示该租户未配置此通道或 binding 不匹配；密钥变更后需重建
        （重启/显式失效）。webhook_path 非空时要求与租户声明的绑定精确一致
        （PRD 3.4「webhook URL 与租户绑定」）——否则任何 {tenant}__任意 后缀
        都能命中该租户通道（联调 2026-09-06 发现的伪造回调缺口）。
        """
        tenant = await registry.get(tenant_id)
        if tenant is None:
            return None
        cfg = tenant.find_channel(channel_type, webhook_path=webhook_path)  # type: ignore[arg-type]
        if cfg is None:
            return None
        try:
            return channel_factory.create(tenant_id, cfg, channel_type)  # type: ignore[arg-type]
        except ValueError:
            return None

    async def _process(event: AgentEvent) -> AgentResponse:
        """同一链路（幂等 -> Filter 链 -> Runtime）；实现见 runtime/pipeline。"""
        return await process_event(event, chain=chain, ctx=ctx, runtime=runtime)

    def _to_json_response(response: AgentResponse) -> JSONResponse:
        """AgentResponse -> Web/HTTP JSON（自测通道与错误响应）。"""
        if response.response_type == ResponseType.ERROR:
            return JSONResponse({"response_type": "error", "content": response.content})
        return JSONResponse({
            "response_type": "text",
            "content": response.content,
            "session_id": response.session_id,
            "trace_id": response.trace_id,
        })

    return app


def _record_im_delivery(tenant_id: str, channel_type: str, response: AgentResponse, *, success: bool) -> None:
    """IM 投递结果指标（PRD 4.2 im_delivery_success/failed）。"""
    metrics = get_metrics()
    labels = {
        "tenant_id": tenant_id,
        "channel": channel_type,
        "msg_type": response.response_type.value,
    }
    if success:
        metrics.im_delivery_success.labels(**labels).inc()
    else:
        metrics.im_delivery_failed.labels(**labels).inc()


# 只对「请求大概率未到达平台」的错误重试（连接建立失败）；ReadTimeout 等
# 响应未知的错误不重试——平台可能已收到并投递，重试会造成重复消息（PRD 3.6）。
_RETRYABLE_SEND_ERRORS: tuple[type[Exception], ...]
try:
    import httpx as _httpx

    _RETRYABLE_SEND_ERRORS = (_httpx.ConnectError, _httpx.ConnectTimeout)
except ImportError:  # pragma: no cover - httpx 为必装依赖，仅防御
    _RETRYABLE_SEND_ERRORS = ()

_SEND_RETRY_DELAY_S = 0.5

# 出站限频状态（PRD 3.6 频率限制）：per (tenant, channel) 最近发送时刻。
# 进程内实现——多节点部署时各节点按同一 rate_limit_per_sec 各自限速，
# 平台侧总速率最高放大 N 倍（风险清单已列）；Redis 共享计数为生产演进。
_OUTBOUND_THROTTLE_LOCK = threading.Lock()
_OUTBOUND_LAST_SEND: dict[tuple[str, str], float] = {}


async def _throttle_outbound(adapter: IMAdapter, tenant_id: str, channel_type: str) -> None:
    """出站限频：消费 PlatformLimits.rate_limit_per_sec（此前仅声明未执行）。

    按最小发送间隔（1/rate 秒）对 per (tenant, channel) 错峰；rate<=0 或
    adapter 未实现 platform_limits 时不限速。超限 sleep 至_slot，不丢消息。
    """
    try:
        rate = float(adapter.platform_limits().rate_limit_per_sec)  # type: ignore[union-attr]
    except (AttributeError, TypeError):
        return
    if rate <= 0:
        return
    min_interval = 1.0 / rate
    key = (tenant_id, channel_type)
    now = time.monotonic()
    wait = 0.0
    with _OUTBOUND_THROTTLE_LOCK:
        last = _OUTBOUND_LAST_SEND.get(key)
        if last is not None and last > now:
            wait = last - now
        # 预占本槽位：并发投递按序排队，不会挤在同一瞬间
        _OUTBOUND_LAST_SEND[key] = max(now, _OUTBOUND_LAST_SEND.get(key, now)) + min_interval
    if wait > 0:
        await asyncio.sleep(wait)


async def _send_reply_with_retry(adapter: IMAdapter, tenant_id: str, response: AgentResponse, channel_type: str) -> int:
    """投递 IM 回复，未送达类连接错误重试一次（PRD 3.6「失败重试」）。

    重试只针对 `_RETRYABLE_SEND_ERRORS`（连接未建立，请求必然没到平台），
    首次失败记 im_delivery_retry 指标；重试仍失败向上抛出，由调用方按
    投递失败处理。返回实际尝试次数（1 = 一次成功，2 = 重试后成功）。
    """
    await _throttle_outbound(adapter, tenant_id, channel_type)
    for attempt in (1, 2):
        try:
            await adapter.send_message(tenant_id, response)
            return attempt
        except _RETRYABLE_SEND_ERRORS as exc:
            if attempt == 2:
                raise
            get_metrics().im_delivery_retry.labels(
                tenant_id=tenant_id,
                channel=channel_type,
                msg_type=response.response_type.value,
            ).inc()
            log.warning("im send retryable failure, retrying",
                        extra={
                            "tenant": tenant_id,
                            "channel": channel_type,
                            "error": str(exc)
                        })
            await asyncio.sleep(_SEND_RETRY_DELAY_S)
    return 1  # pragma: no cover - 循环必经 return/raise


async def _handle_recall(storage: Storage,
                         adapter: IMAdapter,
                         tenant_id: str,
                         recall: RecallEvent,
                         *,
                         agent_name: str = "") -> None:
    """处理撤回事件（PRD 3.7）: 审计留痕 + 尽力标记会话历史 revoked。

    定位会话：RecallEvent 带 session_id 直接用；否则按单聊规则
    generate_session_id(tenant, channel, channel_id, user) 推算（撤回事件
    多为单聊撤回，群聊撤回若平台事件未带群信息则无法精确定位——按 PRD 3.7
    「知悉并留痕」处理，标记失败不影响审计）。

    失败不抛错：撤回是低频异步事件，处理失败仅告警，不阻塞回调 ack。
    """
    # 身份映射（与入站消息一致）：外部 user_id -> 内部 user_id
    mapped = adapter.map_user_id(recall.user_id)
    if mapped != recall.user_id:
        recall.user_id = mapped

    session_id = recall.session_id
    if not session_id:
        try:
            session_id = generate_session_id(
                tenant_id=tenant_id,
                channel_type=recall.channel_type or adapter.channel_type,  # type: ignore[arg-type]
                channel_id=recall.channel_id,
                external_user_id=recall.user_id,
                is_group=recall.is_group,
            )
        except Exception:  # noqa: BLE001 - 定位失败仅记审计
            session_id = ""

    # 1) 尽力标记历史：锁内重读改写（PRD 2.3-A；撤回标记失败可接受，审计已留痕）
    if session_id:
        lock_key = f"lock:session:{tenant_id}:{session_id}"
        acquired = await acquire_lock_with_retry(storage.lock, lock_key, ttl=10, wait_seconds=2.0)
        if not acquired:
            log.warning(f"recall lock timeout, skip history mark: tenant={tenant_id} session={session_id}")
        try:
            if acquired:
                sess = await storage.session.get_session(tenant_id, session_id)
                if sess:
                    state = dict(sess.get("state", {}))
                    if mark_message_revoked(state, recall.msg_id):
                        await storage.session.update_state(tenant_id, session_id, state)
        except Exception as exc:  # noqa: BLE001 - 历史标记失败不阻断
            log.warning("recall mark history failed", extra={"tenant": tenant_id, "error": str(exc)})
        finally:
            if acquired:
                await storage.lock.release(lock_key)

    # 2) 审计留痕（decision=recall，含被撤回消息 id）；字段与其余审计写入
    #    保持同一 schema（agent_name/tool_name/latency_ms），便于统一查询口径。
    #    trace_id: 撤回短路不经过 Filter 链（无 TraceFilter 注入），此处自行
    #    生成，保证审计行可串联（Problem 4.4 / 验收标准 5）。
    log_entry: dict[str, Any] = {
        "trace_id": uuid.uuid4().hex,
        "channel": recall.channel_type or adapter.channel_type,  # type: ignore[arg-type]
        "user_id": recall.user_id,
        "session_id": session_id,
        "agent_name": agent_name,
        "tool_name": None,
        "latency_ms": None,
        "decision": "recall",
        "error_type": None,
        "cost": 0.0,
        "payload": {
            "recalled_msg_id": recall.msg_id,
            "event_type": recall.event_type
        },
    }
    try:
        await storage.audit.write_log(tenant_id, log_entry)
    except Exception as exc:  # noqa: BLE001 - 审计失败不阻断
        log.warning("recall audit failed", extra={"tenant": tenant_id, "error": str(exc)})
