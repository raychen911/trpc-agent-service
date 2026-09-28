"""Explicit Tool routing keeps implementations independently replaceable."""

from collections.abc import Mapping

from trpc_service.agent.contracts import AgentExecutionContext, AgentToolCall, AgentToolResult
from trpc_service.agent.ports import AgentToolInvoker


class CompositeToolInvoker(AgentToolInvoker):
    """Route a registered Tool name to one cohesive concrete implementation."""

    def __init__(
        self,
        routes: Mapping[str, AgentToolInvoker],
        *,
        fallback: AgentToolInvoker | None = None,
    ) -> None:
        self._routes = dict(routes)
        self._fallback = fallback

    async def invoke(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> AgentToolResult:
        """Fail closed when a policy references an implementation not installed here."""

        try:
            invoker = self._routes[call.name]
        except KeyError as error:
            if self._fallback is None:
                raise PermissionError(f"tool is not registered: {call.name}") from error
            invoker = self._fallback
        return await invoker.invoke(context, call)
