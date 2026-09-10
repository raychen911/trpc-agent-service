"""Agent 工厂：根据租户配置构建 LlmAgent。"""
from __future__ import annotations

from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.tools import LoadMemoryTool
import trpc_service.tenant.governance  # noqa: F401  # 导入即完成过滤器注册
from trpc_service.config.tenant_config import TenantConfig
from trpc_service.tool.functions import build_example_tools

# 挂载到每个工具上的治理过滤链（顺序即执行顺序；tool_latency 收尾计时）
TOOL_FILTER_CHAIN = [
    "tool_whitelist",
    "pii_mask",
    "budget_limit",
    "dangerous_confirm",
    "tool_latency",
]


class AgentFactory:
    """根据租户配置构建 Agent。"""

    @staticmethod
    def create_agent(tenant_config: TenantConfig) -> LlmAgent:
        model = OpenAIModel(
            model_name=tenant_config.model.model_name,
            api_key=tenant_config.model.api_key,
            base_url=tenant_config.model.base_url,
        )
        tools = build_example_tools(tenant_config.app.app_name, filters_name=TOOL_FILTER_CHAIN)
        tools.append(LoadMemoryTool())  # Memory 检索入口：post-turn 写入的记忆靠它读回上下文

        # Skill 能力（租户级开关）：SkillToolSet 基础工具 + DynamicSkillToolSet
        # 按需装载，脚本经租户沙箱（local/container）执行
        skill_repository = None
        if tenant_config.skills.enabled:
            from trpc_service.skill import create_skill_bundle

            skill_toolset, dynamic_skill_toolset, skill_repository = create_skill_bundle(
                tenant_config
            )
            tools.extend([skill_toolset, dynamic_skill_toolset])

        return LlmAgent(
            name=tenant_config.app.app_name,
            description=tenant_config.app.description,
            model=model,
            instruction=tenant_config.app.instruction,
            tools=tools,
            skill_repository=skill_repository,
        )
