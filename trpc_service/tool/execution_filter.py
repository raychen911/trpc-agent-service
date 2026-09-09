"""SDK Tool Filter for conservative at-most-once side-effect execution."""

from trpc_agent_sdk.abc import FilterResult, FilterType
from trpc_agent_sdk.filter import BaseFilter

from trpc_service.tenant.context import get_current_tenant
from trpc_service.tool.execution import ToolExecutionState


class ToolExecutionFilter(BaseFilter):

    def __init__(self, name, store):
        super().__init__()
        self._type = FilterType.TOOL
        self._name = "tool_execution_idempotency"
        self._tool_name = name
        self._store = store

    async def run(self, ctx, req, handle):
        trusted = get_current_tenant()
        record, created = await self._store.reserve(trusted.tenant_id, trusted.request_id, self._tool_name, req)
        if not created:
            if record.state == ToolExecutionState.SUCCEEDED:
                return FilterResult(rsp=record.result["value"])
            return FilterResult(error=PermissionError("tool_outcome_unknown_requires_review"), is_continue=False)
        await self._store.finish(record.execution_id, ToolExecutionState.RUNNING)
        try:
            result = await handle()
            if isinstance(result, FilterResult):
                if result.error:
                    await self._store.finish(record.execution_id,
                                             ToolExecutionState.UNKNOWN,
                                             error_code=type(result.error).__name__)
                    return result
                value = result.rsp
            elif isinstance(result, tuple) and len(result) == 2:
                value, error = result
                if error:
                    raise RuntimeError("tool_execution_failed")
            else:
                value = result
            await self._store.finish(record.execution_id, ToolExecutionState.SUCCEEDED, result={"value": value})
            return FilterResult(rsp=value)
        except BaseException as error:
            await self._store.finish(record.execution_id, ToolExecutionState.UNKNOWN, error_code=type(error).__name__)
            raise
