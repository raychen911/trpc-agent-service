"""Skill 运行时。"""
from __future__ import annotations

from pathlib import Path

from trpc_agent_sdk.skills import BaseSkillRepository
from trpc_agent_sdk.skills import DynamicSkillToolSet
from trpc_agent_sdk.skills import SkillToolSet
from trpc_agent_sdk.skills import create_default_skill_repository

from trpc_service.config.tenant_config import TenantConfig
from trpc_service.workspace import create_tenant_workspace

SKILLS_ROOT = Path(__file__).resolve().parent / "skills"


def create_skill_bundle(
    tenant_config: TenantConfig,
) -> tuple[SkillToolSet, DynamicSkillToolSet, BaseSkillRepository]:
    """构建租户技能工具集（skill 目录 + 沙箱 runtime + 两个 toolset）。"""
    repository = create_default_skill_repository(
        str(SKILLS_ROOT),
        workspace_runtime=create_tenant_workspace(tenant_config),
    )
    return (
        SkillToolSet(repository=repository),
        DynamicSkillToolSet(skill_repository=repository),
        repository,
    )
