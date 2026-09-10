"""skill 运行时测试：技能目录装载与工厂集成。"""
from trpc_service.agent.factory import AgentFactory
from trpc_service.config.tenant_config import SkillConfig, TenantConfig
from trpc_service.skill import SKILLS_ROOT, create_skill_bundle


def _tenant(skills_enabled: bool) -> TenantConfig:
    return TenantConfig(
        tenant_id="tenant_skill_t",
        name="技能测试租户",
        skills=SkillConfig(enabled=skills_enabled),
    )


def test_skill_bundle_loads_example_skill():
    """skill 目录被 SDK repository 识别（SKILL.md frontmatter 合法）。"""
    _, _, repository = create_skill_bundle(_tenant(True))
    assert "text-stats" in repository.skill_list()


def test_factory_without_skills_has_no_repository():
    agent = AgentFactory.create_agent(_tenant(False))
    assert getattr(agent, "skill_repository", None) is None


def test_factory_with_skills_attaches_repository():
    """skills.enabled=True 时 Agent 挂载技能仓库与两个技能 toolset。"""
    agent = AgentFactory.create_agent(_tenant(True))
    assert agent.skill_repository is not None
    toolset_types = {type(t).__name__ for t in agent.tools}
    assert "SkillToolSet" in toolset_types
    assert "DynamicSkillToolSet" in toolset_types


def test_skills_root_has_example():
    assert (SKILLS_ROOT / "text_stats" / "SKILL.md").exists()
