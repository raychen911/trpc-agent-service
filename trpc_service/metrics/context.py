"""链路追踪上下文工具。"""
from __future__ import annotations

import uuid
from typing import Optional

from trpc_agent_sdk.context import AgentContext

from trpc_service.tenant.governance.context import META_CHANNEL
from trpc_service.tenant.governance.context import META_SESSION
from trpc_service.tenant.governance.context import META_TENANT
from trpc_service.tenant.governance.context import META_TRACE
from trpc_service.tenant.governance.context import META_USER


def new_trace_id() -> str:
    """生成 32 位 trace_id。"""
    return uuid.uuid4().hex


def build_agent_context(
    tenant_id: str,
    user_id: str,
    session_id: str,
    channel: str = "web",
    trace_id: Optional[str] = None,
) -> AgentContext:
    """构造贯穿链路的 AgentContext（治理 Filter 与审计都从这里取值）。"""
    ctx = AgentContext()
    ctx.with_metadata(META_TENANT, tenant_id)
    ctx.with_metadata(META_USER, user_id)
    ctx.with_metadata(META_SESSION, session_id)
    ctx.with_metadata(META_CHANNEL, channel)
    ctx.with_metadata(META_TRACE, trace_id or new_trace_id())
    return ctx


def trace_id_of(ctx: Optional[AgentContext]) -> str:
    return (ctx.get_metadata(META_TRACE, "") if ctx else "") or ""
