"""
Stateless Agent Runner Orchestrator.
"""

import time
import uuid
from typing import Any, Dict, List, Optional
import logging
from trpc_service.config.models import (
    TenantConfig,
    AgentAppConfig,
    Session,
    SessionEvent,
    EventType,
    InboundMessage,
    OutboundMessage,
    AuditLogEntry,
    AuditDecision,
    MemoryItem,
)
from trpc_service.tenant.manager import TenantContext, tenant_manager
from trpc_service.storage.factory import storage_factory
from trpc_service.tool.base import tool_registry
from trpc_service.agent.filter import FilterChain, AgentExecutionContext
from trpc_service.metrics.telemetry import (
    AGENT_REQUESTS_TOTAL,
    AGENT_LATENCY_SECONDS,
    AGENT_TOKENS_TOTAL,
    TOOL_EXECUTIONS_TOTAL,
    trace_span,
)

logger = logging.getLogger(__name__)


class AgentRunner:
    """
    Stateless Worker Execution Engine.
    Coordinates context loading, filter chains, tool invocations, and state persistence.
    """

    def __init__(self, filter_chain: Optional[FilterChain] = None):
        self.filter_chain = filter_chain or FilterChain()

    def generate_session_id(self, inbound: InboundMessage) -> str:
        """Construct deterministic session ID based on channel and user/chat ID."""
        if inbound.is_group and inbound.raw_chat_id:
            return f"{inbound.tenant_id}:{inbound.channel_type.value}:group:{inbound.raw_chat_id}"
        return f"{inbound.tenant_id}:{inbound.channel_type.value}:direct:{inbound.raw_user_id}"

    async def execute(self, inbound: InboundMessage, app_id: Optional[str] = None) -> OutboundMessage:
        start_time = time.time()
        tenant_id = inbound.tenant_id
        trace_id = inbound.trace_id
        user_id = inbound.raw_user_id

        # 1. Establish Tenant Context
        tokens = TenantContext.set_context(tenant_id, trace_id, user_id)

        try:
            async with trace_span("agent_execute", trace_id, tenant_id) as span:
                span.set_tag("channel", inbound.channel_type.value)
                span.set_tag("user_id", user_id)

                # 2. Retrieve Tenant & App Configuration
                tenant = tenant_manager.get_tenant(tenant_id)
                if not tenant:
                    # Fallback tenant config
                    tenant = TenantConfig(tenant_id=tenant_id, name=f"Tenant-{tenant_id}")
                    tenant_manager.register_tenant(tenant)

                apps = tenant_manager.get_tenant_apps(tenant_id)
                if app_id:
                    app = tenant_manager.get_app(app_id)
                else:
                    app = apps[0] if apps else None

                if not app:
                    app = AgentAppConfig(
                        app_id=f"app_default_{tenant_id}",
                        tenant_id=tenant_id,
                        name="Default Assistant",
                        allowed_tools=["calculator", "knowledge_search", "database_query"],
                        require_confirmation_tools=["fund_transfer"],
                    )
                    tenant_manager.register_app(app)

                # 3. Resolve Session ID and Storage Adapter
                session_id = self.generate_session_id(inbound)
                storage = storage_factory.get_adapter(tenant.storage_config)

                # 4. Acquire Session Lock to ensure cross-node concurrency consistency
                lock_acquired = await storage.acquire_session_lock(session_id, ttl_seconds=20.0)
                if not lock_acquired:
                    logger.warning("Session lock contention on %s. Returning busy response.", session_id)
                    return OutboundMessage(
                        trace_id=trace_id,
                        tenant_id=tenant_id,
                        channel_type=inbound.channel_type,
                        target_user_id=inbound.raw_user_id,
                        target_chat_id=inbound.raw_chat_id,
                        is_group=inbound.is_group,
                        content="系统正在处理您上一条请求，请稍候...",
                    )

                try:
                    # 5. Load or Initialize Session
                    session = await storage.get_session(tenant_id, session_id)
                    if not session:
                        session = Session(
                            session_id=session_id,
                            tenant_id=tenant_id,
                            app_id=app.app_id,
                            channel_type=inbound.channel_type.value,
                            external_user_id=inbound.raw_user_id,
                            external_chat_id=inbound.raw_chat_id,
                            is_group=inbound.is_group,
                        )
                        await storage.save_session(session)

                    # 6. Load Recent Events & Relevant Long-term Memory
                    recent_events = await storage.get_events(tenant_id, session_id, limit=app.max_context_turns * 2)
                    memories = await storage.search_memory(tenant_id, user_id, inbound.content, top_k=3)

                    # 7. Run Pre-Process Filter Chain
                    ctx = AgentExecutionContext(
                        tenant=tenant,
                        app=app,
                        inbound=inbound,
                        session_id=session_id,
                        trace_id=trace_id,
                        user_id=user_id,
                    )
                    filter_res = await self.filter_chain.run_pre_process(ctx)
                    if not filter_res.passed:
                        # Record blocked audit log
                        await storage.record_audit_log(
                            AuditLogEntry(
                                log_id=uuid.uuid4().hex[:16],
                                trace_id=trace_id,
                                tenant_id=tenant_id,
                                channel=inbound.channel_type.value,
                                user_id=user_id,
                                session_id=session_id,
                                agent_name=app.name,
                                decision=filter_res.decision,
                                latency_ms=int((time.time() - start_time) * 1000),
                                error_type="POLICY_BLOCKED",
                            )
                        )
                        return OutboundMessage(
                            trace_id=trace_id,
                            tenant_id=tenant_id,
                            channel_type=inbound.channel_type,
                            target_user_id=inbound.raw_user_id,
                            target_chat_id=inbound.raw_chat_id,
                            is_group=inbound.is_group,
                            content=filter_res.reason or "请求已被安全策略拦截。",
                        )

                    # 8. Agent Execution & Tool Invocation
                    agent_reply_text, tool_used, prompt_tokens, completion_tokens = await self._run_reasoning_and_tools(
                        ctx, app, inbound.content, recent_events, memories, storage
                    )

                    # 9. Post-Process Filter
                    post_res = await self.filter_chain.run_post_process(ctx, agent_reply_text)
                    final_text = post_res.modified_text or agent_reply_text

                    # 10. Persist Events (User Message + Assistant Message)
                    user_event = SessionEvent(
                        session_id=session_id,
                        tenant_id=tenant_id,
                        event_type=EventType.USER_MESSAGE,
                        payload={"content": inbound.content, "sender_id": user_id},
                    )
                    agent_event = SessionEvent(
                        session_id=session_id,
                        tenant_id=tenant_id,
                        event_type=EventType.AGENT_MESSAGE,
                        payload={"content": final_text, "app_id": app.app_id},
                    )
                    await storage.append_events(tenant_id, session_id, [user_event, agent_event], expected_version=session.current_version)

                    # 11. Record Token Usage & Audit Log
                    total_tokens = prompt_tokens + completion_tokens
                    tenant_manager.record_token_usage(tenant_id, total_tokens)
                    AGENT_TOKENS_TOTAL.labels(tenant_id=tenant_id, model=app.llm_config.model_name, type="prompt").inc(prompt_tokens)
                    AGENT_TOKENS_TOTAL.labels(tenant_id=tenant_id, model=app.llm_config.model_name, type="completion").inc(completion_tokens)

                    duration_ms = int((time.time() - start_time) * 1000)
                    await storage.record_audit_log(
                        AuditLogEntry(
                            log_id=uuid.uuid4().hex[:16],
                            trace_id=trace_id,
                            tenant_id=tenant_id,
                            channel=inbound.channel_type.value,
                            user_id=user_id,
                            session_id=session_id,
                            agent_name=app.name,
                            tool_name=tool_used,
                            decision=AuditDecision.ALLOWED,
                            prompt_tokens=prompt_tokens,
                            completion_tokens=completion_tokens,
                            cost_usd=round(total_tokens * 0.000002, 6),
                            latency_ms=duration_ms,
                        )
                    )

                    AGENT_REQUESTS_TOTAL.labels(tenant_id=tenant_id, channel=inbound.channel_type.value, status="success").inc()
                    AGENT_LATENCY_SECONDS.labels(tenant_id=tenant_id, channel=inbound.channel_type.value).observe(time.time() - start_time)

                    return OutboundMessage(
                        trace_id=trace_id,
                        tenant_id=tenant_id,
                        channel_type=inbound.channel_type,
                        target_user_id=inbound.raw_user_id,
                        target_chat_id=inbound.raw_chat_id,
                        is_group=inbound.is_group,
                        content=final_text,
                    )

                finally:
                    # 12. Always Release Session Lock
                    await storage.release_session_lock(session_id)

        finally:
            TenantContext.reset_context(tokens)

    async def _run_reasoning_and_tools(
        self,
        ctx: AgentExecutionContext,
        app: AgentAppConfig,
        user_input: str,
        events: List[SessionEvent],
        memories: List[MemoryItem],
        storage: Any,
    ):
        """Execute reasoning loop with tool invocation detection."""
        tool_used = None
        prompt_tokens = 150 + len(user_input) // 4
        completion_tokens = 50

        # Heuristic / Tool trigger parsing
        if "calc" in user_input.lower() or any(op in user_input for op in ["+", "-", "*", "/"]) and any(c.isdigit() for c in user_input):
            tool_name = "calculator"
            expr = "".join(c for c in user_input if c in "0123456789+-*/(). ")
            # Tool Filter Check
            t_check = await self.filter_chain.run_before_tool_call(ctx, tool_name, {"expression": expr})
            if not t_check.passed:
                return f"[权限拦截] {t_check.reason}", tool_name, prompt_tokens, completion_tokens

            tool = tool_registry.get_tool(tool_name)
            if tool:
                tool_res = await tool.execute(expression=expr)
                TOOL_EXECUTIONS_TOTAL.labels(tenant_id=ctx.tenant.tenant_id, tool_name=tool_name, status="success").inc()
                tool_used = tool_name
                return f"计算结果：{expr} = {tool_res.get('result', '计算异常')}", tool_used, prompt_tokens, completion_tokens

        if "知识" in user_input or "文档" in user_input or "架构" in user_input:
            tool_name = "knowledge_search"
            t_check = await self.filter_chain.run_before_tool_call(ctx, tool_name, {"query": user_input})
            if not t_check.passed:
                return f"[权限拦截] {t_check.reason}", tool_name, prompt_tokens, completion_tokens

            tool = tool_registry.get_tool(tool_name)
            if tool:
                tool_res = await tool.execute(query=user_input)
                TOOL_EXECUTIONS_TOTAL.labels(tenant_id=ctx.tenant.tenant_id, tool_name=tool_name, status="success").inc()
                tool_used = tool_name
                docs_text = "\n".join(tool_res.get("results", []))
                return f"根据企业知识库检索：\n{docs_text}", tool_used, prompt_tokens, completion_tokens + 50

        if "转账" in user_input or "transfer" in user_input.lower():
            tool_name = "fund_transfer"
            t_check = await self.filter_chain.run_before_tool_call(ctx, tool_name, {"amount": 100})
            if t_check.decision == AuditDecision.CONFIRM_REQUIRED:
                return f"[安全二次确认] 检测到高危资金操作，已向管理员发送审批通知，请等待确认后再继续。", tool_name, prompt_tokens, completion_tokens

        # Standard reasoning response
        memory_hint = f"（已加载关于您的长期记忆：{memories[-1].content}）\n" if memories else ""
        reply = f"{memory_hint}您好！我是租户【{ctx.tenant.name}】的智能助手【{app.name}】。已收到您的问题：\"{user_input}\"，服务已为您就绪。"
        return reply, None, prompt_tokens, completion_tokens + len(reply) // 4


agent_runner = AgentRunner()
