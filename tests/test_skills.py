from dataclasses import replace
from types import SimpleNamespace
from typing import cast

import pytest
from trpc_agent_sdk.context import InvocationContext, create_agent_context
from trpc_agent_sdk.skills import get_skill_config

from trpc_service.agent.contracts import AgentRuntimeConfig
from trpc_service.skill import BuiltinSkillCatalog, KnowledgeOnlySkillToolset


def test_builtin_skill_catalog_uses_trpc_repository_and_filters_agent_grants() -> None:
    """Only explicitly granted knowledge-only Skills are visible to one Agent."""

    catalog = BuiltinSkillCatalog()
    summaries = {item.name: item.description for item in catalog.summaries()}

    assert set(summaries) == {"code-review", "hr-assistant"}
    assert all(summaries.values())

    config = AgentRuntimeConfig(
        config_version=1,
        runner_name="trpc_agent",
        tools={
            "grants": [
                {
                    "kind": "skill",
                    "name": "code-review",
                    "actions": ["load"],
                    "resources": [],
                    "risk_level": 0,
                },
                {
                    "kind": "tool",
                    "name": "calculate",
                    "actions": ["execute"],
                    "resources": [],
                    "risk_level": 0,
                },
            ]
        },
    )
    repository = catalog.repository_for(config)

    assert repository.skill_list() == ["code-review"]
    assert [item.name for item in repository.summaries()] == ["code-review"]
    assert repository.get("code-review").summary.name == "code-review"
    assert repository.path("code-review").endswith("code-review")
    repository.refresh()
    with pytest.raises(ValueError, match="not granted"):
        repository.get("hr-assistant")
    with pytest.raises(ValueError, match="not granted"):
        repository.path("hr-assistant")

    no_grants = catalog.repository_for(replace(config, tools={"allowlist": ["calculate"]}))
    assert no_grants.skill_list() == []
    assert catalog.repository_for(replace(config, tools={"grants": "invalid"})).skill_list() == []
    assert catalog.repository_for(replace(config, tools={"grants": ["invalid"]})).skill_list() == []


@pytest.mark.anyio
async def test_knowledge_only_skill_toolset_never_exposes_execution_tools() -> None:
    """The upstream tRPC Skill integration may load guidance but cannot run commands."""

    catalog = BuiltinSkillCatalog()
    config = AgentRuntimeConfig(
        config_version=1,
        runner_name="trpc_agent",
        tools={
            "grants": [{
                "kind": "skill",
                "name": "hr-assistant",
                "actions": ["load"],
                "resources": [],
                "risk_level": 0,
            }]
        },
    )

    tools = await KnowledgeOnlySkillToolset(catalog.repository_for(config)).get_tools()

    assert [tool.name for tool in tools] == ["skill_load"]

    context = cast(
        InvocationContext,
        SimpleNamespace(agent_context=create_agent_context()),
    )
    contextual_tools = await KnowledgeOnlySkillToolset(catalog.repository_for(config)
                                                       ).get_tools(context)
    skill_config = get_skill_config(context.agent_context)

    assert [tool.name for tool in contextual_tools] == ["skill_load"]
    assert skill_config["skill_processor"]["exec_tools_disabled"] is True
    assert skill_config["skill_processor"]["max_loaded_skills"] == 1
