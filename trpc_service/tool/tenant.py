# mypy: disable-error-code="import-untyped"
"""Tenant-scoped, fail-closed tool exposure for tRPC-Agent invocations."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from trpc_agent_sdk.abc import ToolSetABC
from trpc_agent_sdk.context import InvocationContext
from trpc_agent_sdk.tools import BaseTool, FunctionTool

from trpc_service.tenant.context import TenantContext
from trpc_service.tenant.models import ToolPolicy

TENANT_CONTEXT_METADATA_KEY = "trpc_service.tenant_context"
_TOOL_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


class ToolConfigurationError(ValueError):
    """A published tool policy cannot be resolved safely."""


class ToolAuthorizationError(PermissionError):
    """A tool set was resolved outside its trusted tenant context."""


class TenantToolSet(ToolSetABC):
    """Immutable SDK ToolSet that exposes only one tenant's authorized tools.

    The allow-list is enforced both when the model request is built and when a
    function call is resolved for execution, because tRPC-Agent calls ``get_tools``
    in both paths.  Unknown or approval-gated tools fail closed.
    """

    def __init__(
        self,
        *,
        tenant_id: str,
        policy: ToolPolicy,
        registered_tools: Mapping[str, BaseTool | Callable[..., Any]]
        | Iterable[BaseTool | Callable[..., Any]],
        approved_tools: Iterable[str] = (),
    ) -> None:
        if not tenant_id:
            raise ToolConfigurationError("tenant_id must not be empty")
        super().__init__(name=f"tenant-tools:{tenant_id}")
        registry = _normalize_registry(registered_tools)
        approved = frozenset(approved_tools)
        unexpected_approvals = approved - policy.requires_approval
        if unexpected_approvals:
            raise ToolConfigurationError(
                "approved_tools contains tools that do not require approval: "
                f"{sorted(unexpected_approvals)}"
            )

        missing = policy.allowed - registry.keys()
        if missing:
            raise ToolConfigurationError(
                f"tenant policy references unregistered tools: {sorted(missing)}"
            )

        allowed = policy.allowed - policy.requires_approval | approved
        if policy.max_calls_per_turn == 0:
            allowed = frozenset()
        self._tenant_id = tenant_id
        self._tools = tuple(registry[name] for name in sorted(allowed))

    @property
    def tenant_id(self) -> str:
        """Tenant to which this immutable tool view belongs."""

        return self._tenant_id

    @property
    def tool_names(self) -> tuple[str, ...]:
        """Deterministic names visible to the model."""

        return tuple(tool.name for tool in self._tools)

    async def get_tools(
        self,
        invocation_context: InvocationContext | None = None,
    ) -> list[BaseTool]:
        """Resolve authorized tools and verify trusted invocation metadata."""

        if invocation_context is not None:
            tenant_context = invocation_context.agent_context.get_metadata(
                TENANT_CONTEXT_METADATA_KEY
            )
            if not isinstance(tenant_context, TenantContext):
                raise ToolAuthorizationError("trusted TenantContext metadata is missing")
            if tenant_context.tenant_id != self._tenant_id:
                raise ToolAuthorizationError("tool set tenant does not match the invocation tenant")
        return list(self._tools)

    def add_tools(self, tools: list[BaseTool | Callable[..., Any]]) -> None:
        """Reject mutation so one request cannot widen another tenant's view."""

        del tools
        raise TypeError("TenantToolSet is immutable; build a new scoped instance")


def _normalize_registry(
    registered_tools: Mapping[str, BaseTool | Callable[..., Any]]
    | Iterable[BaseTool | Callable[..., Any]],
) -> dict[str, BaseTool]:
    entries = (
        registered_tools.items()
        if isinstance(registered_tools, Mapping)
        else ((_tool_name(tool), tool) for tool in registered_tools)
    )
    normalized: dict[str, BaseTool] = {}
    for declared_name, tool_or_callable in entries:
        if not _TOOL_NAME.fullmatch(declared_name):
            raise ToolConfigurationError(f"invalid tool name: {declared_name!r}")
        if isinstance(tool_or_callable, BaseTool):
            tool = tool_or_callable
        else:
            callable_name = _tool_name(tool_or_callable)
            if callable_name != declared_name:
                raise ToolConfigurationError(
                    f"registry key {declared_name!r} does not match tool name {callable_name!r}"
                )
            tool = FunctionTool(tool_or_callable)
        if tool.name != declared_name:
            raise ToolConfigurationError(
                f"registry key {declared_name!r} does not match tool name {tool.name!r}"
            )
        if declared_name in normalized:
            raise ToolConfigurationError(f"duplicate tool name: {declared_name}")
        normalized[declared_name] = tool
    return normalized


def _tool_name(tool: BaseTool | Callable[..., Any]) -> str:
    if isinstance(tool, BaseTool):
        return tool.name
    name = getattr(tool, "__name__", "")
    if not name:
        raise ToolConfigurationError("callable tools must have a stable __name__")
    return name
