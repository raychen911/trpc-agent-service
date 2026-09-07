# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Tenant governance filters built on the framework filter system.

These filters plug into the existing TOOL filter chain (same mechanism as
``ToolSafetyFilter``) and enforce per-tenant policy:

* ``ToolAllowlistFilter`` — whitelist / denylist / dangerous-tool gating.
* ``ToolOutputRedactionFilter`` — redact sensitive data from tool output.

Tenant resolution is delegated to a resolver callable so a single filter
instance can serve many tenants; the ``tenant_id`` is read from the
:class:`AgentContext` metadata injected at request time.
"""

from __future__ import annotations

import inspect
import threading
from typing import Any
from typing import Callable
from typing import Optional

from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.abc import FilterType
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.context import get_invocation_ctx
from trpc_agent_sdk.filter import BaseFilter
from trpc_agent_sdk.tools import get_tool_var

from trpc_service.log import AuditLogEntry
from trpc_service.tenant import Tenant
from trpc_service.tenant import ToolPermissions
from ._exceptions import ToolConfirmationRequired
from ._hitl import ConfirmationManager
from ._redactor import SensitiveDataRedactor

TenantResolver = Callable[[str], Optional[Tenant]]
"""Resolves a ``tenant_id`` to a :class:`Tenant` (or ``None``)."""


def _resolve_tool_name(req: Any) -> str:
    if isinstance(req, dict):
        name = req.get("tool_name")
        if isinstance(name, str) and name.strip():
            return name
    tool = get_tool_var()
    name = getattr(tool, "name", "")
    if isinstance(name, str) and name.strip():
        return name
    return "unknown_tool"


def _evaluate_tool_permission(perms: ToolPermissions, tool_name: str) -> tuple[str, Optional[str]]:
    """Return ``(decision, message)`` for a tool under the given permissions."""
    if tool_name in perms.tool_denylist:
        return "deny", f"tool '{tool_name}' is denied for this tenant"
    if tool_name in perms.dangerous_tools:
        return "confirm", f"tool '{tool_name}' requires confirmation before execution"
    if perms.tool_whitelist and tool_name not in perms.tool_whitelist:
        return "deny", f"tool '{tool_name}' is not in the tenant tool whitelist"
    return "allow", None


class ToolAllowlistFilter(BaseFilter):
    """Blocks tool calls not permitted for the current tenant."""

    def __init__(
        self,
        *,
        permissions: Optional[ToolPermissions] = None,
        resolver: Optional[TenantResolver] = None,
        audit_logger: Any = None,
        confirmation_manager: Optional[ConfirmationManager] = None,
    ) -> None:
        super().__init__()
        self._type = FilterType.TOOL
        self._name = "tenant_tool_allowlist"
        self._permissions = permissions
        self._resolver = resolver
        self._audit_logger = audit_logger
        self._confirmation_manager = confirmation_manager

    def _resolve(self, tenant_id: str) -> Optional[ToolPermissions]:
        if self._resolver is not None:
            tenant = self._resolver(tenant_id)
            return tenant.tool_permissions if tenant is not None else None
        return self._permissions

    async def _before(self, ctx: AgentContext, req: Any, rsp: FilterResult):
        tenant_id = ctx.get_metadata("tenant_id")
        perms = self._resolve(tenant_id)
        if perms is None:
            return None
        tool_name = _resolve_tool_name(req)
        decision, message = _evaluate_tool_permission(perms, tool_name)

        if decision == "deny":
            rsp.error = PermissionError(message)
            rsp.is_continue = False
        elif decision == "confirm":
            confirmed = set(ctx.get_metadata("confirmed_tools") or [])
            if tool_name in confirmed:
                # Already confirmed earlier in this turn — allow execution.
                decision = "allow"
            elif self._confirmation_manager is not None:
                tool_args = req if isinstance(req, dict) else {}
                try:
                    invocation = get_invocation_ctx()
                except Exception:  # pragma: no cover - defensive outside Runner
                    invocation = None
                pending = self._confirmation_manager.request(
                    tenant_id or "",
                    tool_name,
                    tool_args=tool_args,
                    user_id=invocation.user_id if invocation is not None else None,
                    session_id=invocation.session_id if invocation is not None else None,
                )
                if inspect.isawaitable(pending):
                    pending = await pending
                rsp.error = ToolConfirmationRequired(pending.token, tool_name)
                rsp.is_continue = False
            else:
                rsp.error = PermissionError(message)
                rsp.is_continue = False

        if self._audit_logger is not None:
            await self._audit_logger.log(self._build_audit_entry(tenant_id, tool_name, decision))
        return None

    def _build_audit_entry(self, tenant_id: str, tool_name: str, decision: str) -> AuditLogEntry:
        entry = AuditLogEntry(
            tenant_id=tenant_id or "",
            tool_name=tool_name,
            decision=decision,
        )
        try:
            inv = get_invocation_ctx()
        except Exception:  # pragma: no cover - not inside an invocation
            inv = None
        if inv is not None:
            entry.user_id = inv.user_id
            entry.session_id = inv.session_id
            entry.agent_name = inv.agent_name
        return entry


class ToolOutputRedactionFilter(BaseFilter):
    """Redacts sensitive data from tool output after execution."""

    def __init__(
        self,
        *,
        redactor: Optional[SensitiveDataRedactor] = None,
        rules: Optional[list] = None,
        resolver: Optional[TenantResolver] = None,
    ) -> None:
        super().__init__()
        self._type = FilterType.TOOL
        self._name = "tenant_tool_output_redaction"
        self._redactor = redactor or SensitiveDataRedactor()
        self._rules = rules
        self._resolver = resolver

    def _resolve_rules(self, ctx: AgentContext) -> Optional[list]:
        if self._resolver is not None:
            tenant = self._resolver(ctx.get_metadata("tenant_id"))
            if tenant is not None:
                return tenant.audit_policy.desensitize_rules
        return self._rules

    async def _after(self, ctx: AgentContext, req: Any, rsp: FilterResult):
        if rsp.error:
            return None
        rsp.rsp = self._redactor.redact_any(rsp.rsp, self._resolve_rules(ctx))
        return None


class ToolCallLimitFilter(BaseFilter):
    """Enforce the tenant's maximum successful tool attempts per Agent turn.

    A new governed Agent/filter chain is created for every Worker invocation,
    so the counter is naturally scoped to one turn. The lock keeps parallel
    tool calls from racing past the configured ceiling.
    """

    def __init__(self, max_calls: int) -> None:
        super().__init__()
        self._type = FilterType.TOOL
        self._name = "tenant_tool_call_limit"
        self._max_calls = max(0, max_calls)
        self._calls = 0
        self._lock = threading.Lock()

    async def _before(self, ctx: AgentContext, req: Any, rsp: FilterResult):
        if self._max_calls == 0:
            return None
        with self._lock:
            if self._calls >= self._max_calls:
                rsp.error = PermissionError(f"tool call limit exceeded for this turn ({self._max_calls})")
                rsp.is_continue = False
                return None
            self._calls += 1
        return None


def build_governance_filters(tenant: Tenant, *, audit_logger: Any = None) -> list[BaseFilter]:
    """Build the default tool governance filter chain for a tenant.

    Returns ``[ToolAllowlistFilter, ToolOutputRedactionFilter]`` bound to the
    tenant's tool permissions and desensitization rules.
    """
    redactor = SensitiveDataRedactor()
    return [
        ToolAllowlistFilter(permissions=tenant.tool_permissions, audit_logger=audit_logger),
        ToolOutputRedactionFilter(redactor=redactor, rules=tenant.audit_policy.desensitize_rules),
    ]


def build_governance(
    tenant: Tenant,
    *,
    tracker: Any = None,
    confirmation_manager: Optional[ConfirmationManager] = None,
    audit_logger: Any = None,
) -> dict[str, list[BaseFilter]]:
    """Build the complete governance filter chains (tool + model) for a tenant.

    Returns ``{"tool_filters": [...], "model_filters": [...]}``. The tool
    filters attach to tools (or the agent's tool processor); the model filters
    attach to the LLM model. ``tracker`` (a :class:`BudgetTracker`) enables the
    budget-enforcement model filter.
    """
    tool_filters = [
        ToolAllowlistFilter(
            permissions=tenant.tool_permissions,
            audit_logger=audit_logger,
            confirmation_manager=confirmation_manager,
        ),
        ToolCallLimitFilter(tenant.tool_permissions.max_tool_calls_per_turn),
        ToolOutputRedactionFilter(
            redactor=SensitiveDataRedactor(),
            rules=tenant.audit_policy.desensitize_rules,
        ),
    ]
    model_filters: list[BaseFilter] = []
    if tracker is not None:
        from ._budget import ModelBudgetFilter

        model_filters.append(ModelBudgetFilter(tracker, tenant=tenant))
    return {"tool_filters": tool_filters, "model_filters": model_filters}


def apply_tenant_governance(
    agent: Any,
    tenant: Tenant,
    *,
    confirmation_manager: Optional[ConfirmationManager] = None,
    audit_logger: Any = None,
) -> Any:
    """Attach tenant tool policies to an agent produced by any factory.

    The deployment-level ``create_agent`` factory owns the agent-level channel-user
    authorization filter. Applying governance here closes the tool-policy bypass:
    every ``BaseTool`` receives the same allow/deny/HITL and redaction chain before
    the Runner can execute it. Plain callables are wrapped as ``FunctionTool`` with
    the same filters.
    """
    from trpc_agent_sdk.tools import BaseTool
    from trpc_agent_sdk.tools import FunctionTool

    tools = getattr(agent, "tools", None)
    if not isinstance(tools, list):
        return agent

    tool_filters = build_governance(
        tenant,
        confirmation_manager=confirmation_manager,
        audit_logger=audit_logger,
    )["tool_filters"]
    governed_tools = []
    for tool in tools:
        if isinstance(tool, BaseTool):
            tool.add_filters(tool_filters)
            governed_tools.append(tool)
        elif callable(tool):
            governed_tools.append(FunctionTool(tool, filters=tool_filters.copy()))
        else:
            # Toolsets resolve their tools asynchronously inside the upstream SDK.
            # They remain supported, but should return pre-filtered BaseTool objects.
            governed_tools.append(tool)
    agent.tools = governed_tools
    return agent
