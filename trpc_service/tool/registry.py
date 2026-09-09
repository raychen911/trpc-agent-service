# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Fail-closed registry used to materialize tenant-approved Agent tools."""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any

from trpc_agent_sdk.tools import BaseTool
from trpc_agent_sdk.tools import FunctionTool
from trpc_agent_sdk.tools.safety import ToolSafetyFilter

from trpc_service.config import ToolPolicy
from trpc_service.tenant.filters import ToolConfirmationFilter
from trpc_service.tool.execution import InMemoryToolExecutionStore
from trpc_service.tool.execution_filter import ToolExecutionFilter
from trpc_service.tool.observability import ToolObservabilityFilter
from trpc_service.tenant.context import get_current_tenant


def current_utc_time() -> str:
    """Return the current UTC time in ISO 8601 format."""
    return datetime.now(timezone.utc).isoformat()


class ToolRegistry:
    """Map stable configuration names to SDK tool objects or Python callables."""

    def __init__(self,
                 tools: dict[str, Any] | None = None,
                 execution_store: Any = None,
                 knowledge_provider: Any = None,
                 metrics: Any = None,
                 audit: Any = None) -> None:
        self._execution_store = execution_store or InMemoryToolExecutionStore()
        self._metrics = metrics
        self._audit = audit
        self._tools: dict[str, Any] = {"current_utc_time": current_utc_time}
        self._tools.update(tools or {})
        if knowledge_provider is not None:

            async def knowledge_search(query: str, limit: int = 5) -> list[dict]:
                """Search only the current tenant/app's knowledge documents."""
                context = get_current_tenant()
                hits = await knowledge_provider.search(context.tenant_id, context.app_id, query, max(1, min(limit, 20)))
                return [hit.model_dump() for hit in hits]

            self._tools["knowledge_search"] = knowledge_search

    def register(self, name: str, tool: Any) -> None:
        if not name or name in self._tools:
            raise ValueError(f"tool is already registered or has an invalid name: {name!r}")
        if not callable(tool) and not hasattr(tool, "run_async"):
            raise TypeError("tool must be callable or implement the SDK tool contract")
        self._tools[name] = tool

    def resolve(self, policy: ToolPolicy) -> list[Any]:
        """Return only explicitly allowed tools; unknown names fail publication."""
        denied = set(policy.denied)
        names = [name for name in policy.allowed if name not in denied]
        missing = sorted(set(names) - self._tools.keys())
        if missing:
            raise ValueError(f"tenant references unknown tools: {missing}")
        resolved: list[Any] = []
        for name in names:
            tool = self._tools[name]
            if callable(tool) and not isinstance(tool, BaseTool):
                filters = [ToolSafetyFilter(block_on_review=True)]
                if name in policy.confirmation_required:
                    filters.insert(0, ToolExecutionFilter(name, self._execution_store))
                    filters.insert(0, ToolConfirmationFilter(name))
                filters.append(ToolObservabilityFilter(name, self._metrics, self._audit))
                resolved.append(FunctionTool(tool, filters=filters))
            elif name in policy.confirmation_required:
                raise ValueError(
                    f"confirmation-required tool must be registered as a callable or preconfigured factory: {name}")
            else:
                resolved.append(tool)
        return resolved

    def names(self) -> list[str]:
        return sorted(self._tools)
