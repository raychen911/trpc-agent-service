"""Static tool allowlist for tenant agent runtimes.

Stage 6A1: ``build_tools`` injects a fresh tenant-private governance filter
into every constructed tool (instance injection; the global SDK filter
registry is never used).

Stage 6A2: tools whose decision is ``review`` are built as
``LongRunningFunctionTool`` so the SDK emits a real ``LongRunningEvent`` after
the 6A1 filter blocks the underlying function with zero calls.  Approved
execution goes exclusively through :class:`ApprovedToolExecutor` (signature
validation, thread offloading, explicit non-support for tool-context tools) —
never through the filtered wrapper.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from trpc_agent_sdk.server.knowledge.tools import LangchainKnowledgeSearchTool
from trpc_agent_sdk.tools import BaseTool, FunctionTool, LongRunningFunctionTool

from trpc_service.agent.errors import TenantAgentConfigurationError
from trpc_service.agent.tools import get_current_time
from trpc_service.config.tenant import ToolDecision
from trpc_service.governance.approved_execution import ApprovedToolExecutor
from trpc_service.governance.tool_filter import TenantToolGovernanceFilter
from trpc_service.telemetry.runtime import TelemetryRuntime
from trpc_service.telemetry.tool import traced_tool_function, tracer_for

_VALID_DECISIONS = frozenset({"allow", "deny", "review"})


class AllowedToolRegistry:
    """Resolves tool names to fresh tool instances from a static factory map."""

    def __init__(
        self,
        factories: Mapping[str, Callable[[list[TenantToolGovernanceFilter], bool], BaseTool]],
        originals: Mapping[str, Callable[..., Any]] | None = None,
        *,
        tracer: object | None = None,
    ) -> None:
        self._factories = dict(factories)
        # Stage 6B1: one tracer serves BOTH tool boundaries — the SDK-facing
        # wrapped function (normal execution) and the approved-execution
        # executor — so every tool run emits exactly one fixed tool.execute.
        self._tracer = tracer
        self._executor = ApprovedToolExecutor(originals or {}, tracer=tracer)

    @classmethod
    def default(cls, telemetry: TelemetryRuntime | None = None) -> AllowedToolRegistry:
        tracer = tracer_for(telemetry, "trpc-service.tools")
        traced_time_tool = traced_tool_function(tracer, get_current_time)

        def _build_get_current_time(
            filters: list[TenantToolGovernanceFilter],
            long_running: bool,
        ) -> BaseTool:
            tool_class = LongRunningFunctionTool if long_running else FunctionTool
            return tool_class(traced_time_tool, filters=list(filters))

        return cls(
            factories={"get_current_time": _build_get_current_time},
            originals={"get_current_time": get_current_time},
            tracer=tracer,
        )

    def build_tools(
        self,
        allowed_tools: tuple[str, ...],
        tool_decisions: Mapping[str, ToolDecision],
        *,
        knowledge_base: object | None = None,
    ) -> list[BaseTool]:
        """Build tenant-private tools; ``review`` tools become long-running.

        A single immutable filter instance is shared across this call's tools
        only; every Runtime call constructs new tool and filter objects, so
        policies can never leak across tenants or config versions.
        """
        decisions = dict(tool_decisions)
        for name, decision in decisions.items():
            if decision not in _VALID_DECISIONS:
                raise TenantAgentConfigurationError()
        if set(decisions) - set(allowed_tools):
            raise TenantAgentConfigurationError()

        governance_filter = TenantToolGovernanceFilter(decisions)
        result: list[BaseTool] = []
        for name in allowed_tools:
            if name == "knowledge_search":
                # Knowledge is tenant-bound by the capabilities resolver.  It
                # is intentionally not a global/static tool, because sharing
                # one instance could cross tenant data boundaries.  Search is
                # read-only; a "review" policy has no approved-execution
                # implementation and therefore fails closed at configuration
                # time instead of silently bypassing the policy.
                if knowledge_base is None or decisions.get(name, "allow") == "review":
                    raise TenantAgentConfigurationError()
                result.append(LangchainKnowledgeSearchTool(rag=knowledge_base, filters=[governance_filter]))
                continue
            factory = self._factories.get(name)
            if factory is None:
                raise TenantAgentConfigurationError()
            long_running = decisions.get(name, "allow") == "review"
            result.append(factory([governance_filter], long_running))
        return result

    async def execute_approved(self, name: str, args: Any) -> Any:
        """The single approved-execution boundary (delegates to the executor).

        Callers must have re-validated config version/allowed_tools/decision
        before invoking; the executor itself performs argument validation and
        rejects unsupported context-dependent tools.
        """
        return await self._executor.execute(name, args)


__all__ = ["AllowedToolRegistry"]
