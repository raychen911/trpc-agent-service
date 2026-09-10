"""Filter 公共工具：从 AgentContext 解析租户上下文。"""
from __future__ import annotations

from typing import Optional

from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.tools._context_var import get_tool_var

from trpc_service.config.registry import config_manager
from trpc_service.config.tenant_config import TenantConfig

# AgentContext 元数据键
META_TENANT = "tenant_id"
META_USER = "user_id"
META_CHANNEL = "channel"
META_TRACE = "trace_id"
META_SESSION = "session_id"


def current_tool_name() -> str:
    """获取当前正在执行的工具名（由框架 tool context var 提供）。"""
    tool = get_tool_var()
    return getattr(tool, "name", "") or ""


def resolve_tenant(ctx: Optional[AgentContext]) -> Optional[TenantConfig]:
    """从 AgentContext 元数据中解析租户配置。"""
    if ctx is None:
        return None
    tenant_id = ctx.get_metadata(META_TENANT)
    if not tenant_id:
        return None
    return config_manager.get(tenant_id)
