# ===================================================================
# agent.builder - 按租户动态组装 Agent（平台层新增）
# ===================================================================
# 说明: PRD 0.3-4b「按配置动态构建 LlmAgent（模型 / 工具白名单 / 知识库）」。
#   本模块是 Agent 组装的唯一归属: 模型（model_factory）+ 工具集（tool 包
#   过滤与框架适配）+ 指令 -> 框架 LlmAgent。
#
# 依赖方向（单向，无环）: agent.builder -> tool.builder / agent.model_factory
#   工具相关能力（select_tools / make_framework_tool / make_knowledge_search_spec）
#   由 tool 包提供；agent 不向 tool 反向导出，tool 也不知道 agent 的存在。
# 规范: 运行期编排在 runtime/runner.py，本模块只负责「造出 Agent」。
# ===================================================================

from __future__ import annotations

from typing import Any, Callable, Optional

from ..storage.base import KnowledgeStore
from ..tenant.models import TenantConfig
from ..tool.builder import make_framework_tool, make_knowledge_search_spec, select_tools
from ..tool.registry import ToolSpec
from .model_factory import build_llm_model


def build_framework_agent(
        tenant: TenantConfig,
        tools: Optional[list[ToolSpec]] = None,
        model_factory: Optional[Callable[..., Any]] = None,
        knowledge: Optional[KnowledgeStore] = None,
        confirmed_tools: frozenset[str] = frozenset(),
) -> Any:
    """按租户配置构建框架 LlmAgent（生产）。

    Args:
        tenant: 租户配置（model / app / tools）
        tools: 已按权限过滤的工具列表（默认从 registry 过滤）
        model_factory: `(ModelConfig) -> LLMModel`，默认走真实模型工厂；
            测试可注入假模型以脱离真实 HTTP 依赖（与 AgentRunner 对齐）。
        knowledge: 平台知识库（RAG）。租户白名单含 knowledge_search
            时注入检索工具（LLM 按需调用，走框架工具调用链路）。
        confirmed_tools: 本轮已通过二次确认的危险工具名集合（PRD 4.1）；
            运行时门控见 tool.builder.make_framework_tool。

    Returns:
        框架 LlmAgent 实例；未安装 trpc-agent-py 时抛 ImportError。
    """
    try:
        from trpc_agent_sdk.agents import LlmAgent  # type: ignore
    except ImportError as exc:  # pragma: no cover - 环境相关
        raise ImportError("build_framework_agent 需要安装 trpc-agent-py；自测请使用 MockAgentRunner") from exc

    factory = model_factory or build_llm_model
    tool_list = tools if tools is not None else select_tools(tenant)
    if knowledge is not None and tenant.tools.is_allowed("knowledge_search"):
        tool_list = [*tool_list, make_knowledge_search_spec(knowledge, tenant.tenant_id)]

    framework_tools = [make_framework_tool(spec, tenant, confirmed_tools) for spec in tool_list]

    # 实测（trpc-agent-py v1.1.19）: LlmAgent 字段是 name / instruction，
    # 没有 app_name / system_prompt / max_rounds；model 必须是 LLMModel
    # 实例，传配置 dict 直接 ValidationError。旧写法 4 处全错。
    return LlmAgent(
        name=f"{tenant.tenant_id}:{tenant.app.agent_type}",
        model=factory(tenant.model),
        tools=framework_tools,
        instruction=tenant.app.system_prompt,
    )
