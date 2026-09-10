"""预算限流过滤器（TOOL 层）。"""
from __future__ import annotations

from trpc_agent_sdk.filter import BaseFilter
from trpc_agent_sdk.filter import register_tool_filter

from trpc_service.tenant.governance.context import resolve_tenant
from trpc_service.tenant.budget import BudgetExceeded
from trpc_service.tenant.budget import BudgetManager

budget_manager = BudgetManager()


@register_tool_filter("budget_limit")
class BudgetLimitFilter(BaseFilter):
    """工具执行前的租户预算校验。"""

    async def _before(self, ctx, req, rsp):
        tenant = resolve_tenant(ctx)
        if tenant is None:
            return
        try:
            budget_manager.check(tenant.tenant_id)
        except BudgetExceeded as ex:
            rsp.rsp = {
                "error": "budget_exceeded",
                "message": str(ex),
                "status": "blocked",
            }
            rsp.is_continue = False
