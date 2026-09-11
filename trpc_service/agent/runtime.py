"""Process-local Runner selection for one immutable tenant configuration version."""

from __future__ import annotations

from collections.abc import Iterable

from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.tools import BaseTool

from trpc_service.tenant import ToolPolicy


RuntimeKey = tuple[str, str, int]


class RunnerRegistrationError(ValueError):
    """Raised when a Runner key has already been registered."""


class RunnerNotFoundError(LookupError):
    """Raised when no Runner exactly matches a trusted route."""


class UnknownToolError(ValueError):
    """Raised when a ToolPolicy names a tool missing from the runtime catalog."""


def tenant_app_name(tenant_id: str, agent_app_id: str) -> str:
    """Return the stable tRPC-Agent session namespace for a tenant application."""

    return f"{tenant_id}:{agent_app_id}"


def select_tools_for_runner(available_tools: Iterable[BaseTool], policy: ToolPolicy) -> tuple[BaseTool, ...]:
    """Apply one application's ToolPolicy before constructing its Agent and Runner."""

    tools = tuple(available_tools)
    from trpc_service.governance.action_tools import ProposalTool
    tools_by_name = {tool.name: tool for tool in tools}
    configured_names = policy.allow | policy.deny | policy.require_confirmation
    unknown_names = configured_names - tools_by_name.keys()
    if unknown_names:
        raise UnknownToolError(f"tool policy references unknown tools: {sorted(unknown_names)}")

    return tuple(
        tool for tool in tools
        if tool.name not in policy.deny
        and (tool.name not in policy.require_confirmation or isinstance(tool, ProposalTool))
        and (not policy.default_deny or tool.name in policy.allow)
    )


class RunnerRegistry:
    """Exact in-memory mapping from tenant/app/config version to Runner."""

    def __init__(self) -> None:
        self._runners: dict[RuntimeKey, Runner] = {}
        self.run_configs = {}
        self.protected_sessions = {}

    def register(
        self,
        tenant_id: str,
        agent_app_id: str,
        config_version: int,
        runner: Runner,
    ) -> None:
        key = (tenant_id, agent_app_id, config_version)
        if key in self._runners:
            raise RunnerRegistrationError(f"runner already registered for runtime key: {key}")
        self._runners[key] = runner

    def resolve(self, tenant_id: str, agent_app_id: str, config_version: int) -> Runner:
        key = (tenant_id, agent_app_id, config_version)
        try:
            return self._runners[key]
        except KeyError as error:
            raise RunnerNotFoundError(f"runner not found for runtime key: {key}") from error
