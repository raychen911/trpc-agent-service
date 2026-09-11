"""Public SDK proposal tools. Their model-visible function never executes a write."""

import functools
import os

from sqlalchemy import select
from trpc_agent_sdk.filter import FilterType
from trpc_agent_sdk.tools import FunctionTool

from trpc_service.persistence.models import PendingActionRow
from .filters import TenantBoundaryFilter
from .model_filter import database_call


class ConfirmationBoundaryFilter(TenantBoundaryFilter):
    """Install on the model and ordinary tools to stop the original turn at approval."""

    def __init__(self, service, tenant_id, app_id, *, filter_type=FilterType.MODEL):
        super().__init__(tenant_id, app_id, filter_type=filter_type)
        self.service = service

    async def check(self, ctx):
        await super().check(ctx)
        if self.type == FilterType.MODEL and ctx.metadata.get("execution_kind") == "summary":
            return

        def pending():
            with self.service.database.sessions() as session:
                return session.scalar(
                    select(PendingActionRow.action_id).where(
                        PendingActionRow.tenant_id == self.tenant_id,
                        PendingActionRow.execution_id == ctx.metadata.get("execution_id"))) is not None

        if await database_call(pending):
            raise PermissionError("execution stopped at its confirmation boundary")


class ProposalTool(FunctionTool):
    """Schema from a registered function; execution is only an atomic proposal insert.

    The supplied function is a declaration and is never called. Its signature must
    include the SDK's public tool_context parameter. prepare is a trusted, pure
    validator which selects immutable resource preconditions and a public summary.
    """

    def __init__(self, declaration, definition, service, *, tenant_id, app_id):
        if declaration.__name__ != definition.name:
            raise ValueError("action schema name must match its registered definition")

        @functools.wraps(declaration)
        async def propose(**kwargs):
            context = kwargs["tool_context"]
            metadata = context.agent_context.metadata
            if ((metadata.get("tenant_id"), metadata.get("agent_app_id")) != (tenant_id, app_id)
                    or not metadata.get("execution_id") or not context.function_call_id):
                raise PermissionError("action requires a trusted SDK invocation")
            arguments = {key: value for key, value in kwargs.items() if key != "tool_context"}
            normalized, preconditions, summary = definition.prepare(arguments)
            result = await database_call(service.propose, tenant_id, metadata["execution_id"], context.function_call_id,
                                         definition.name, definition.revision, normalized, preconditions, summary,
                                         ttl_seconds=int(os.environ.get("TRPC_ACCEPTANCE_ACTION_TTL_SECONDS", "300")))
            # This is the SDK's supported stop-after-tool contract, not a private
            # runner mutation or a coroutine waiting for an IM response.
            context.event_actions.skip_summarization = True
            return {"status": "awaiting_confirmation", "action_id": result["action_id"]}

        super().__init__(propose)
