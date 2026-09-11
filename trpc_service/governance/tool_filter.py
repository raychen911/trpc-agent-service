"""Real SDK tool filter enforcing per-tenant allow/deny/review decisions.

SDK contract evidence (installed editable ``trpc_agent_sdk`` == local repo):

- ``filter/_base_filter.py`` ``BaseFilter.run``: awaits ``_before`` **before**
  invoking the handle; with ``is_continue=False`` set, the tool function body
  never runs and the FilterResult is returned up the chain.
- ``filter/_run_filter.py`` ``run_filters``: a set ``error`` would be logged
  and raised as a runtime failure, so governance blocks set only
  ``rsp`` + ``is_continue=False`` and keep ``error=None`` — the same pattern
  the SDK's own ``ToolCallbackFilter`` uses (``agents/_callback.py``).
- ``tools/_base_tool.py``: the running tool is exposed to filters via the
  ``get_tool_var()`` contextvar, which is set before the filter chain.

Instances are per-Runtime (never the global ``register_tool_filter``
registry), and the captured decisions mapping is immutable.
"""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Mapping

from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.filter import BaseFilter, FilterResult, FilterType
from trpc_agent_sdk.tools import get_tool_var

from trpc_service.config.tenant import ToolDecision

FILTER_NAME = "tenant_tool_governance"

_BLOCKED_RESPONSES: dict[str, dict[str, str]] = {
    "deny": {
        "status": "denied"
    },
    "review": {
        "status": "approval_required"
    },
}


class TenantToolGovernanceFilter(BaseFilter):
    """Blocks tool execution before the function runs, per tenant policy.

    ``review`` is an honest *blocked pending human approval* verdict in this
    stage (6A2 adds real HITL resume); it must never read as "approved".
    """

    def __init__(self, tool_decisions: Mapping[str, ToolDecision]) -> None:
        super().__init__()
        self._decisions = MappingProxyType(dict(tool_decisions))
        self._type = FilterType.TOOL
        self._name = FILTER_NAME

    def _block(self, rsp: FilterResult, verdict: dict[str, str]) -> None:
        # Stable structured result without arguments or free text; no error
        # so the SDK treats this as a clean governance verdict, not a crash.
        rsp.rsp = dict(verdict)
        rsp.error = None
        rsp.is_continue = False

    async def _before(self, ctx: AgentContext, req: Any, rsp: FilterResult) -> None:
        name: str | None = None
        try:
            tool = get_tool_var()
            candidate = getattr(tool, "name", None)
            if isinstance(candidate, str) and candidate.strip():
                name = candidate.strip()
        except Exception:
            name = None
        if name is None:
            # Fail CLOSED: an unresolvable tool context must never let the
            # function run unjudged. Fixed verdict, no parameters or
            # internals leak.
            self._block(rsp, _BLOCKED_RESPONSES["deny"])
            return
        blocked = _BLOCKED_RESPONSES.get(self._decisions.get(name, "allow"))
        if blocked is None:
            return
        self._block(rsp, blocked)


__all__ = ["FILTER_NAME", "TenantToolGovernanceFilter"]
