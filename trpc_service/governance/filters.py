"""Immutable per-application Filter instances; no singleton tenant or request state."""

from trpc_agent_sdk.filter import BaseFilter, FilterType, FilterResult


class TenantBoundaryFilter(BaseFilter):

    def __init__(self, tenant_id, app_id, *, filter_type=FilterType.AGENT, authorize=None):
        super().__init__()
        self.tenant_id, self.app_id, self.authorize = tenant_id, app_id, authorize
        self.type, self.name = filter_type, "tenant_boundary"

    async def check(self, ctx):
        metadata = ctx.metadata if ctx else {}
        if (metadata.get("tenant_id"), metadata.get("agent_app_id")) != (self.tenant_id, self.app_id):
            raise PermissionError("tenant execution boundary mismatch")
        if self.authorize:
            await self.authorize(metadata)

    async def run(self, ctx, req, handle):
        await self.check(ctx)
        return await handle()

    async def run_stream(self, ctx, req, handle):
        await self.check(ctx)
        async for result in handle():
            yield result


class ToolGovernanceFilter(TenantBoundaryFilter):
    """The tool's registered name is fixed by construction, never supplied in arguments."""

    def __init__(self, tenant_id, app_id, tool_name, policy, *, authorize=None, confirmation_check=None):
        super().__init__(tenant_id, app_id, filter_type=FilterType.TOOL, authorize=authorize)
        self.tool_name, self.policy = tool_name, policy
        self.name = "tool_governance"
        self.confirmation_check = confirmation_check

    async def run(self, ctx, req, handle):
        await self.check(ctx)
        if self.tool_name not in self.policy.allow or self.tool_name in self.policy.deny:
            return FilterResult(error=PermissionError("tool is not allowed"), is_continue=False)
        if self.tool_name in self.policy.require_confirmation:
            # W06 owns the durable action executor. A model-supplied confirmation flag
            # never authorizes a side effect in this execution path.
            if self.confirmation_check is None or not await self.confirmation_check(ctx, req):
                return FilterResult(error=PermissionError("tool requires a durable approval"), is_continue=False)
        return await handle()
