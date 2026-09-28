"""Tenant-filtered access to the small platform Skill catalog."""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import cast

from trpc_agent_sdk.skills import BaseSkillRepository, FsSkillRepository, Skill, SkillSummary

from trpc_service.agent.contracts import AgentRuntimeConfig

BUILTIN_SKILL_ROOT = Path(__file__).with_name("builtins")


class GrantedSkillRepository(BaseSkillRepository):  # type: ignore[misc]
    """Expose only Skill names granted by an immutable Agent configuration."""

    def __init__(self, delegate: BaseSkillRepository, granted_names: frozenset[str]) -> None:
        super().__init__(delegate.workspace_runtime)
        self._delegate = delegate
        self._granted_names = granted_names

    def summaries(self) -> list[SkillSummary]:
        return [item for item in self._delegate.summaries() if item.name in self._granted_names]

    def get(self, name: str) -> Skill:
        if name not in self._granted_names:
            raise ValueError(f"skill {name!r} is not granted for this Agent")
        return self._delegate.get(name)

    def skill_list(self, mode: str = "all") -> list[str]:
        return [name for name in self._delegate.skill_list(mode) if name in self._granted_names]

    def path(self, name: str) -> str:
        if name not in self._granted_names:
            raise ValueError(f"skill {name!r} is not granted for this Agent")
        return cast(str, self._delegate.path(name))

    def refresh(self) -> None:
        self._delegate.refresh()


def _granted_skill_names(config: AgentRuntimeConfig) -> frozenset[str]:
    """Read typed Skill grants; legacy Tool allowlists never imply Skill access."""

    raw_grants = config.tools.get("grants", ())
    if not isinstance(raw_grants, Sequence) or isinstance(raw_grants, (str, bytes)):
        return frozenset()
    names: set[str] = set()
    for raw_grant in raw_grants:
        if not isinstance(raw_grant, Mapping):
            continue
        actions = raw_grant.get("actions", ())
        name = raw_grant.get("name")
        if (raw_grant.get("kind") == "skill" and isinstance(name, str)
                and isinstance(actions, Sequence) and not isinstance(actions, (str, bytes))
                and "load" in actions):
            names.add(name)
    return frozenset(names)


class BuiltinSkillCatalog:
    """Index the two reviewed platform Skills through the upstream SDK."""

    def __init__(self, root: Path = BUILTIN_SKILL_ROOT) -> None:
        self._repository = FsSkillRepository(str(root))

    def summaries(self) -> list[SkillSummary]:
        return cast(list[SkillSummary], self._repository.summaries())

    def repository_for(self, config: AgentRuntimeConfig) -> GrantedSkillRepository:
        return GrantedSkillRepository(self._repository, _granted_skill_names(config))
