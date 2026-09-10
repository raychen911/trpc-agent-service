"""聊天执行核心：inline 与 worker 队列模式共用的执行管线。"""
from __future__ import annotations

import time
from typing import Optional

from trpc_service.agent.resilience import resilience_policy
from trpc_service.agent.routing import SessionRouter
from trpc_service.config.tenant_config import TenantConfig
from trpc_service.metrics.collector import metrics_collector
from trpc_service.metrics.context import build_agent_context
from trpc_service.tenant.audit.model import AuditEvent
from trpc_service.tenant.audit.service import audit_service
from trpc_service.tenant.budget import BudgetExceeded
from trpc_service.tenant.governance.budget_limit import budget_manager
from trpc_service.tenant.governance.pii_mask import mask_text


class ChatBlocked(Exception):
    """预算超限（调用方负责转 HTTP 429 / IM 提示语）。"""

    def __init__(self, ex: BudgetExceeded):
        self.ex = ex
        super().__init__(str(ex))


async def execute_chat(
    tenant_config: TenantConfig,
    runner,
    *,
    message: str,
    user_id: str,
    session_id: Optional[str] = None,
    channel: str = "web",
    trace_id: Optional[str] = None,
) -> dict:
    """执行一次聊天并返回 {reply, session_id, trace_id}。"""
    tenant_id = tenant_config.tenant_id
    session_id = session_id or SessionRouter.default_session_id(tenant_config, user_id)
    agent_context = build_agent_context(
        tenant_id=tenant_id,
        user_id=user_id,
        session_id=session_id,
        channel=channel,
        trace_id=trace_id,
    )
    trace_id = agent_context.get_metadata("trace_id")

    # 预算前置校验（工具层还有二次拦截）
    try:
        budget_manager.check(tenant_id)
    except BudgetExceeded as ex:
        metrics_collector.inc_request(tenant_id, channel, error=True)
        audit_service.emit(
            AuditEvent(
                tenant_id=tenant_id, channel=channel, user_id=user_id, session_id=session_id,
                agent_name=runner.app_name, decision="block", error_type="budget_exceeded",
                trace_id=trace_id,
            )
        )
        raise ChatBlocked(ex) from ex

    start = time.monotonic()
    outcome = await resilience_policy.execute(
        tenant_id,
        runner.run,
        user_id=user_id,
        session_id=session_id,
        message=message,
        agent_context=agent_context,
    )
    latency_ms = int((time.monotonic() - start) * 1000)

    # 记录预算用量（token 以字符数粗估，接入 usage 后精确化）
    budget_manager.record(tenant_id, api_calls=1, tokens=len(message) + len(outcome.text))

    # 业务指标（请求量/耗时/工具调用/token）
    metrics_collector.inc_request(
        tenant_id, channel, error=bool(outcome.error_type), latency_ms=latency_ms
    )
    metrics_collector.add_tokens(tenant_id, len(message) + len(outcome.text))
    for call in outcome.tool_calls:
        metrics_collector.inc_tool_call(tenant_id, call.get("name", ""))

    # 审计落盘（密钥/PII 由 redact 处理）
    audit_service.emit(
        AuditEvent(
            tenant_id=tenant_id,
            channel=channel,
            user_id=user_id,
            session_id=session_id,
            agent_name=runner.app_name,
            tool_name=",".join(c.get("name", "") for c in outcome.tool_calls),
            decision="error" if outcome.error_type else "allow",
            latency_ms=latency_ms,
            error_type=outcome.error_type,
            trace_id=trace_id,
        )
    )

    # 结构化日志（键值对文本，可按 tenant/trace grep）
    from trpc_service.log import get_logger

    get_logger("web.chat").info(
        "chat done tenant=%s trace=%s tool=%s latency=%sms error=%s",
        tenant_id,
        trace_id,
        outcome.tool_calls[0]["name"] if outcome.tool_calls else "-",
        latency_ms,
        outcome.error_type or "-",
    )

    # 输出脱敏
    reply = mask_text(outcome.text) if tenant_config.audit.mask_pii else outcome.text
    if outcome.error_type and not reply:
        reply = f"服务暂时不可用（{outcome.error_type}），请稍后重试。"
    # 思考过程单独透出（展示层折叠展示，不混入正文）
    reasoning = ""
    if outcome.reasoning:
        reasoning = (
            mask_text(outcome.reasoning) if tenant_config.audit.mask_pii else outcome.reasoning
        )
    return {
        "reply": reply,
        "reasoning": reasoning,
        "session_id": session_id,
        "trace_id": trace_id,
    }
