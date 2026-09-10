"""危险工具二次确认过滤器（TOOL 层）。"""
from __future__ import annotations

from trpc_agent_sdk.filter import BaseFilter
from trpc_agent_sdk.filter import register_tool_filter

from trpc_service.tenant.governance.context import current_tool_name
from trpc_service.tenant.governance.context import resolve_tenant
from trpc_service.tool.functions import is_dangerous


@register_tool_filter("dangerous_confirm")
class DangerousConfirmFilter(BaseFilter):
    """危险操作需用户确认后执行。"""

    async def _before(self, ctx, req, rsp):
        tenant = resolve_tenant(ctx)
        if tenant is None:
            return
        name = current_tool_name()
        if not is_dangerous(name):
            return
        confirmed = False
        if isinstance(req, dict):
            confirmed = bool(req.get("confirm", False))
        if not confirmed and ctx is not None and ctx.get_metadata("tool_confirmed", False):
            confirmed = True
        if not confirmed:
            rsp.rsp = {
                "error": "confirmation_required",
                "message": (
                    f"工具 {name} 属于危险操作，需先向用户说明将执行的内容，"
                    "并在用户明确同意后携带 confirm=true 重新调用。"
                ),
                "status": "pending_confirm",
            }
            rsp.is_continue = False
