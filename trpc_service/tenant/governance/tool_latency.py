"""工具耗时过滤器（TOOL 层）。"""
from __future__ import annotations

import time

from trpc_agent_sdk.filter import BaseFilter
from trpc_agent_sdk.filter import register_tool_filter

from trpc_service.metrics.collector import metrics_collector
from trpc_service.tenant.governance.context import current_tool_name
from trpc_service.tenant.governance.context import resolve_tenant

META_TOOL_TS = "tool_started_monotonic"


@register_tool_filter("tool_latency")
class ToolLatencyFilter(BaseFilter):
    """工具调用耗时采集（OTel 直方图 + 每租户聚合）。"""

    async def _before(self, ctx, req, rsp):
        ctx.with_metadata(META_TOOL_TS, time.monotonic())

    async def _after(self, ctx, req, rsp):
        start = ctx.get_metadata(META_TOOL_TS)
        if start is None:
            return
        latency_ms = int((time.monotonic() - start) * 1000)
        tenant = resolve_tenant(ctx)
        tenant_id = tenant.tenant_id if tenant else "unknown"
        tool_name = current_tool_name() or "unknown"
        metrics_collector.inc_tool_latency(tenant_id, tool_name, latency_ms)
