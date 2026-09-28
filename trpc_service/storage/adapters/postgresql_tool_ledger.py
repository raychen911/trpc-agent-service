"""PostgreSQL-backed governed Tool call ledger."""

from datetime import datetime, timezone

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.agent.contracts import AgentExecutionContext, AgentToolCall, AgentToolResult
from trpc_service.agent.ledger import (
    ToolLedger,
    ToolLedgerClaim,
    ToolLedgerConflict,
    ToolLedgerStatus,
    tool_call_fingerprint,
)
from trpc_service.storage.runtime_orm import ToolCallLedgerRow


class PostgreSQLToolLedger(ToolLedger):
    """Persist logical Tool outcomes across retries and Worker nodes."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions

    async def prepare(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> ToolLedgerClaim:
        tenant = context.request.tenant
        fingerprint = tool_call_fingerprint(call)
        async with self._sessions.begin() as database:
            identity = select(ToolCallLedgerRow).where(
                ToolCallLedgerRow.tenant_id == tenant.tenant_id,
                ToolCallLedgerRow.agent_app_id == tenant.agent_app_id,
                or_(
                    ToolCallLedgerRow.call_id == call.call_id,
                    (ToolCallLedgerRow.request_id == tenant.request_id)
                    & (ToolCallLedgerRow.logical_call_index == call.logical_call_index),
                ),
            )
            row = await database.scalar(identity.with_for_update())
            if row is None:
                candidate = ToolCallLedgerRow(
                    tenant_id=tenant.tenant_id,
                    agent_app_id=tenant.agent_app_id,
                    call_id=call.call_id,
                    request_id=tenant.request_id,
                    logical_call_index=call.logical_call_index,
                    name=call.name,
                    kind=call.kind.value,
                    action=call.action,
                    resource=call.resource,
                    intent_hash=fingerprint,
                    status=ToolLedgerStatus.PREPARED.value,
                )
                try:
                    # The savepoint keeps the transaction usable when another
                    # Worker inserts the same logical call concurrently.
                    async with database.begin_nested():
                        database.add(candidate)
                        await database.flush()
                except IntegrityError:
                    row = await database.scalar(identity.with_for_update())
                    if row is None:
                        raise
                else:
                    return ToolLedgerClaim(ToolLedgerStatus.PREPARED, should_execute=True)
            if row.call_id != call.call_id or row.intent_hash != fingerprint:
                raise ToolLedgerConflict("Tool call identity was reused with a different intent")
            result = self._result(row.result_payload)
            return ToolLedgerClaim(
                ToolLedgerStatus(row.status),
                should_execute=False,
                result=result,
            )

    async def complete(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        result: AgentToolResult,
    ) -> None:
        await self._transition(
            context,
            call,
            ToolLedgerStatus.SUCCEEDED,
            result_payload={
                "call_id": result.call_id,
                "content": result.content,
                "artifact_refs": list(result.artifact_refs),
                "attributes": dict(result.attributes),
            },
        )

    async def fail(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        summary: str,
    ) -> None:
        await self._transition(context, call, ToolLedgerStatus.FAILED, error_summary=summary)

    async def mark_unknown(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        summary: str,
    ) -> None:
        await self._transition(context, call, ToolLedgerStatus.UNKNOWN, error_summary=summary)

    async def _transition(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
        status: ToolLedgerStatus,
        *,
        result_payload: dict[str, object] | None = None,
        error_summary: str | None = None,
    ) -> None:
        tenant = context.request.tenant
        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(ToolCallLedgerRow).where(
                    ToolCallLedgerRow.tenant_id == tenant.tenant_id,
                    ToolCallLedgerRow.agent_app_id == tenant.agent_app_id,
                    ToolCallLedgerRow.call_id == call.call_id,
                ).with_for_update())
            if row is None or row.intent_hash != tool_call_fingerprint(call):
                raise ToolLedgerConflict("Tool call was not prepared with this intent")
            if row.status == ToolLedgerStatus.SUCCEEDED.value:
                if status is ToolLedgerStatus.SUCCEEDED and row.result_payload == result_payload:
                    return
                raise ToolLedgerConflict("completed Tool call cannot change outcome")
            if (row.status == ToolLedgerStatus.UNKNOWN.value
                    and status is not ToolLedgerStatus.UNKNOWN):
                raise ToolLedgerConflict("unknown Tool outcome requires operator reconciliation")
            row.status = status.value
            row.result_payload = result_payload
            row.error_summary = error_summary[:1000] if error_summary is not None else None
            row.completed_at = datetime.now(timezone.utc)

    @staticmethod
    def _result(payload: dict[str, object] | None) -> AgentToolResult | None:
        if payload is None:
            return None
        artifact_refs = payload.get("artifact_refs", [])
        attributes = payload.get("attributes", {})
        if not isinstance(artifact_refs, list) or not isinstance(attributes, dict):
            raise RuntimeError("stored Tool result is invalid")
        return AgentToolResult(
            call_id=str(payload["call_id"]),
            content=None if payload.get("content") is None else str(payload["content"]),
            artifact_refs=tuple(str(value) for value in artifact_refs),
            attributes=attributes,
        )
