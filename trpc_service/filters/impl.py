# ===================================================================
# filters.impl - 治理 Filter 实现（9 个）
# ===================================================================
# 说明: 对应 PRD 0.3-2 / 4.1 的 Filter 链，按注册顺序:
#   Trace -> Audit -> TenantResolve -> Signature -> UserAuth
#   -> RateLimit -> Budget -> ToolWhitelist -> PII
#   Audit 置于洋葱外层（Trace 之内），借助 _after 无论链路成败都执行的
#   语义，保证被阻断 / 异常流量同样写入审计（PRD 4.4）。
# 规范: 阻断抛 FilterBlocked；审计字段对齐 PRD 4.4。
# ===================================================================

from __future__ import annotations

import inspect
import uuid
from typing import Any, Optional

from ..config.redaction import Redactor
from ..events import AgentEvent
from ..metrics.metrics import Metrics, get_metrics
from ..storage.base import Storage
from ..storage.manager import StorageManager
from ..tenant.registry import TenantRegistry
from .base import FilterBlocked, FilterChain, FilterResult, GatewayFilter
from .context import GatewayContext
from .rate_limiter import TenantRateLimiter


class TraceFilter(GatewayFilter):
    """注入 trace_id 并绑定日志上下文（PRD 4.3）。"""

    name = "trace"

    async def _before(self, ctx: GatewayContext, event: AgentEvent, result: FilterResult) -> None:
        if not event.trace_id:
            event.trace_id = uuid.uuid4().hex
        result.decisions.append(f"trace:{event.trace_id[:8]}")


class TenantResolveFilter(GatewayFilter):
    """解析 tenant_id -> 加载租户配置到 ctx（PRD 1.3-2 / 1.4 配置隔离）。"""

    name = "tenant_resolve"

    async def _before(self, ctx: GatewayContext, event: AgentEvent, result: FilterResult) -> None:
        if not event.tenant_id:
            raise FilterBlocked("missing tenant_id", "missing_tenant")
        if ctx.registry is None:
            raise FilterBlocked("tenant registry not configured", "misconfigured")
        tenant = await ctx.registry.get(event.tenant_id)
        if tenant is None:
            raise FilterBlocked(f"tenant not found: {event.tenant_id}", "tenant_not_found")
        if not tenant.is_active:
            raise FilterBlocked(f"tenant suspended: {event.tenant_id}", "tenant_suspended")
        ctx.tenant = tenant
        result.decisions.append(f"tenant:{event.tenant_id}")


class SignatureFilter(GatewayFilter):
    """IM 回调验签（PRD 3.4）；未配置验签器时放行（本地自测）。"""

    name = "signature"

    async def _before(self, ctx: GatewayContext, event: AgentEvent, result: FilterResult) -> None:
        verifier = ctx.signature_verifiers.get(event.channel_type)
        if verifier is None:
            result.decisions.append("signature:skip")
            return
        signature = event.metadata.get("signature", "")
        body = event.metadata.get("raw_body", b"")
        if not verifier(body, signature):
            raise FilterBlocked("invalid signature", "signature_mismatch")
        result.decisions.append("signature:ok")


class UserAuthFilter(GatewayFilter):
    """IM 用户级权限校验（PRD 4.1，区别于租户级鉴权）。

    同一租户下按 `im_channel_config.user_acl` 白/黑名单判定该 IM 用户能否
    使用 bot；命中黑名单或不在白名单（白名单非空时）即阻断。
    """

    name = "user_auth"

    async def _before(self, ctx: GatewayContext, event: AgentEvent, result: FilterResult) -> None:
        if ctx.tenant is None:
            return
        channel = ctx.tenant.find_channel(event.channel_type)
        if channel is None:
            result.decisions.append("user_auth:no_channel")
            return
        acl = channel.user_acl
        if not acl.is_allowed(event.user_id):
            if event.user_id in acl.blocklist:
                raise FilterBlocked(f"user blocked: {event.user_id}", "user_blocked")
            raise FilterBlocked(f"user not allowed: {event.user_id}", "user_not_allowed")
        result.decisions.append("user_auth:ok")


class RateLimitFilter(GatewayFilter):
    """租户级限流（PRD 4.1 / 6-10），超限阻断。"""

    name = "rate_limit"

    def __init__(self, limiter: Optional[TenantRateLimiter] = None) -> None:
        super().__init__()
        self._limiter = limiter or TenantRateLimiter()

    async def _before(self, ctx: GatewayContext, event: AgentEvent, result: FilterResult) -> None:
        if ctx.tenant is None:
            return
        rate = ctx.tenant.rate_limit_per_min
        outcome = self._limiter.allow(event.tenant_id, rate)
        if inspect.isawaitable(outcome):
            # 共享限流器（RedisFixedWindowLimiter）为异步接口
            outcome = await outcome
        allowed, wait = outcome
        if not allowed:
            raise FilterBlocked(f"rate limited, retry in {wait:.1f}s", "rate_limited")
        result.decisions.append("rate:ok")


class BudgetFilter(GatewayFilter):
    """租户预算硬限（PRD 6-10 成本失控），超预算阻断。

    每请求顺手刷新 tenant_budget_usd Gauge（PRD 4.2）: 放在硬限判定之前，
    被预算阻断的请求也能观测到当前预算值。
    """

    name = "budget"

    def __init__(self, metrics: Optional[Metrics] = None) -> None:
        super().__init__()
        self._metrics = metrics if metrics is not None else get_metrics()

    async def _before(self, ctx: GatewayContext, event: AgentEvent, result: FilterResult) -> None:
        if ctx.tenant is None:
            return
        self._metrics.tenant_budget.labels(tenant_id=ctx.tenant.tenant_id).set(ctx.tenant.monthly_budget_usd)
        if ctx.tenant.budget_exceeded():
            raise FilterBlocked(f"budget exceeded: {ctx.tenant.used_budget_usd:.4f}", "budget_exceeded")
        result.decisions.append("budget:ok")


class ToolWhitelistFilter(GatewayFilter):
    """工具白名单 / 黑名单 / 危险工具二次确认（PRD 4.1）。

    网关侧拦截: 若事件已声明要调用的工具（metadata.tool_name），
    校验租户权限；危险工具未确认则要求二次确认。
    """

    name = "tool_whitelist"

    async def _before(self, ctx: GatewayContext, event: AgentEvent, result: FilterResult) -> None:
        if ctx.tenant is None:
            return
        tool_name = event.metadata.get("tool_name")
        if not tool_name:
            result.decisions.append("tools:none")
            return
        perms = ctx.tenant.tools
        if not perms.is_allowed(tool_name):
            raise FilterBlocked(f"tool not allowed: {tool_name}", "tool_not_allowed")
        if perms.requires_confirmation(tool_name):
            confirmed = bool(event.metadata.get("tool_confirmed"))
            if not confirmed:
                raise FilterBlocked(f"tool requires confirmation: {tool_name}", "tool_confirmation_required")
        result.decisions.append(f"tool:allow:{tool_name}")


class PIIFilter(GatewayFilter):
    """PII 脱敏（PRD 4.5 / 1.4 日志脱敏），替换事件内容中的敏感信息。"""

    name = "pii"

    def __init__(self, redactor: Optional[Redactor] = None) -> None:
        super().__init__()
        self._redactor = redactor

    async def _before(self, ctx: GatewayContext, event: AgentEvent, result: FilterResult) -> None:
        redactor = self._redactor or ctx.redactor
        if redactor is None or not getattr(redactor, "enabled", True):
            return
        event.content = redactor.redact(event.content)
        if "signature" in event.metadata:
            event.metadata["signature"] = redactor.redact(str(event.metadata["signature"]))
        result.decisions.append("pii:redacted" if event.content else "pii:noop")


class AuditFilter(GatewayFilter):
    """审计日志（PRD 4.4），链路结束后异步记录决策 / 错误。"""

    name = "audit"

    def __init__(self, redactor: Optional[Redactor] = None) -> None:
        super().__init__()
        self._redactor = redactor

    async def _before(self, ctx: GatewayContext, event: AgentEvent, result: FilterResult) -> None:
        # 审计在链路结束后统一记录（_after），前置无操作
        return None

    async def _after(self, ctx: GatewayContext, event: AgentEvent, result: FilterResult) -> None:
        # 无论链路成败都记审计（decision 记录治理结论）
        # 按租户解析存储（PRD 2.1）：_after 在 TenantResolve 之后执行，ctx.tenant 已就绪
        storage: Optional[Storage] = await ctx.resolve_storage(ctx.tenant)
        if storage is None:
            return
        decision = "allow" if result.passed else "block"
        error_type = None
        if result.error is not None:
            if isinstance(result.error, FilterBlocked):
                error_type = result.error.error_type
            else:
                error_type = type(result.error).__name__
        log_entry: dict[str, Any] = {
            "trace_id": event.trace_id,
            "channel": event.channel_type,
            "user_id": event.user_id,
            "session_id": event.session_id,
            "agent_name": ctx.tenant.name if ctx.tenant else "",
            "tool_name": event.metadata.get("tool_name"),
            "decision": decision,
            "latency_ms": result.duration_ms,
            "error_type": error_type,
            # 结构性限制：本审计在 chain.run 结束即写（此时 LLM 尚未产生
            # token 消耗），cost 恒为 0；真实 cost 由 runtime._settle_usage
            # 的执行审计补记（PRD 4.4 网关治理审计与执行审计两层语义）。
            "cost": 0.0,
            "payload": {
                "content_preview": (event.content or "")[:200]
            },
        }
        # 审计内容脱敏（PRD 4.5）
        redactor = self._redactor or ctx.redactor
        if redactor is not None:
            log_entry = redactor.redact_dict(log_entry)
        await storage.audit.write_log(event.tenant_id, log_entry)
        result.decisions.append(f"audit:{decision}")


# ---------------------------------------------------------------------------
# 链组装
# ---------------------------------------------------------------------------


def build_filter_chain(
    *,
    registry: TenantRegistry,
    storage: Storage,
    metrics: Optional[Metrics] = None,
    redactor: Optional[Redactor] = None,
    limiter: Optional[TenantRateLimiter] = None,
    signature_verifiers: Optional[dict[str, Any]] = None,
    storage_manager: Optional[StorageManager] = None,
) -> tuple[FilterChain, GatewayContext]:
    """按 PRD 0.3-2 顺序组装 Filter 链。

    Args:
        registry: 租户配置注册表
        storage: 存储后端（幂等 / 审计兜底）
        metrics: 指标注册表（可空）
        redactor: PII 脱敏器（默认内置规则）
        limiter: 租户限流器（默认新建）
        signature_verifiers: channel_type -> (body, signature) -> bool
        storage_manager: 按租户懒建 Storage 的容器（PRD 2.1）；None 时
            所有租户共用 `storage`（幂等 / 锁等共享基础设施语义）。

    Returns:
        (FilterChain, GatewayContext): 链与共享上下文
    """
    ctx = GatewayContext(
        registry=registry,
        storage=storage,
        storage_manager=storage_manager,
        metrics=metrics,
        redactor=redactor or Redactor(),
        rate_limiter=limiter or TenantRateLimiter(),
        signature_verifiers=signature_verifiers or {},
    )
    chain = FilterChain([
        TraceFilter(),
        AuditFilter(ctx.redactor),
        TenantResolveFilter(),
        SignatureFilter(),
        UserAuthFilter(),
        RateLimitFilter(ctx.rate_limiter),
        BudgetFilter(),
        ToolWhitelistFilter(),
        PIIFilter(ctx.redactor),
    ])
    return chain, ctx
