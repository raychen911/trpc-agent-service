"""Restricted tRPC Skill Toolset that can load guidance but cannot execute it."""

from copy import deepcopy
from typing import Optional

from trpc_agent_sdk.abc import ToolABC, ToolSetABC
from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.skills import (
    BaseSkillRepository,
    SKILL_REPOSITORY_KEY,
    SkillLoadTool,
    SkillProfileNames,
    get_skill_config,
    set_skill_config,
)


class KnowledgeOnlySkillToolset(ToolSetABC):  # type: ignore[misc]
    """Expose only tRPC's Skill loader and explicitly disable execution Tools."""

    def __init__(self, repository: BaseSkillRepository) -> None:
        super().__init__(name="knowledge_only_skills")
        self._repository = repository
        self._load_tool = SkillLoadTool(repository=repository)

    async def get_tools(
        self,
        invocation_context: Optional[InvocationContext] = None,
    ) -> list[ToolABC]:
        if invocation_context is not None:
            invocation_context.agent_context.with_metadata(
                SKILL_REPOSITORY_KEY,
                self._repository,
            )
            config = deepcopy(get_skill_config(invocation_context.agent_context))
            config["skill_processor"].update({
                "tool_profile": str(SkillProfileNames.KNOWLEDGE_ONLY),
                "exec_tools_disabled": True,
                "max_loaded_skills": 1,
            })
            set_skill_config(invocation_context.agent_context, config)
        return [self._load_tool]
