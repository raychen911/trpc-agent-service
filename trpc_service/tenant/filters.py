# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""SDK Filter implementations enforcing platform-owned tenant decisions."""

from __future__ import annotations

from typing import Any
import json

from trpc_agent_sdk.abc import FilterResult
from trpc_agent_sdk.abc import FilterType
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.filter import BaseFilter

from trpc_service.tenant.context import get_current_tenant
from trpc_service.tenant.approval import InMemoryApprovalStore


class TenantBoundaryAgentFilter(BaseFilter):
    """Fail closed if context-local and SDK metadata tenant identities differ."""

    def __init__(self, tenant_id: str, app_id: str) -> None:
        super().__init__()
        self._type = FilterType.AGENT
        self._name = "tenant_boundary"
        self._tenant_id = tenant_id
        self._app_id = app_id

    async def _before(self, ctx: AgentContext, req: Any, rsp: FilterResult) -> None:
        del req
        try:
            trusted = get_current_tenant()
        except RuntimeError as error:
            rsp.error = error
            rsp.is_continue = False
            return
        matches = (trusted.tenant_id == self._tenant_id and trusted.app_id == self._app_id
                   and ctx.get_metadata("tenant_id") == self._tenant_id and ctx.get_metadata("app_id") == self._app_id)
        if not matches:
            rsp.error = PermissionError("tenant execution context mismatch")
            rsp.is_continue = False


class ToolConfirmationFilter(BaseFilter):
    """Block a confirmation-required tool until trusted metadata approves it."""

    def __init__(self, tool_name: str) -> None:
        super().__init__()
        self._type = FilterType.TOOL
        self._name = "tool_confirmation"
        self._tool_name = tool_name

    async def _before(self, ctx: AgentContext, req: Any, rsp: FilterResult) -> None:
        approved = ctx.get_metadata("approved_arguments", {})
        actual = InMemoryApprovalStore.arguments_hash(json.dumps(req or {}))
        if approved.get(self._tool_name) != actual:
            rsp.error = PermissionError(f"tool requires confirmation: {self._tool_name}")
            rsp.is_continue = False
        else:
            # Consume before any await: parallel tool calls cannot reuse the grant.
            approved.pop(self._tool_name)
