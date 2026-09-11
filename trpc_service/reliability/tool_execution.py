"""SDK FunctionTool integration with durable result recovery and explicit ownership."""

import asyncio
import functools
import json
import uuid

from sqlalchemy import select, func
from sqlalchemy.exc import IntegrityError
from trpc_agent_sdk.tools import FunctionTool

from trpc_service.persistence.models import AuditLogRow, InboundMessageRow, ToolInvocationRow, ToolResultRow
from trpc_service.storage import ConflictError
from .tool_invocation import _hash
from trpc_service.telemetry.runtime import measured, count


class ToolOutcomeUnknown(ConflictError):
    pass


class DurableTools:

    def __init__(self, database):
        self.database = database

    @measured("tool.invoke")
    async def invoke(self,
                     tenant,
                     execution,
                     call_id,
                     name,
                     arguments,
                     operation,
                     *,
                     recover=None,
                     max_calls=20,
                     confirmed_action_id=None):
        key, digest = (execution, call_id), _hash(arguments)
        created = False
        try:
            with self.database.sessions.begin() as session:
                # Serialize the call budget across parallel tools of one input.
                parent = session.scalar(
                    select(InboundMessageRow).where(InboundMessageRow.execution_id == execution,
                                                    InboundMessageRow.tenant_id == tenant).with_for_update())
                if parent is None:
                    raise PermissionError("tool execution requires a trusted input")
                from trpc_service.persistence.models import PendingActionRow
                action = session.scalar(
                    select(PendingActionRow).where(PendingActionRow.tenant_id == tenant,
                                                   PendingActionRow.execution_id == execution))
                if action is not None:
                    if (confirmed_action_id != action.action_id or action.status != "executing"
                            or name != action.tool_name or call_id != "action:" + action.action_id):
                        raise PermissionError("execution stopped at its confirmation boundary")
                elif confirmed_action_id is not None:
                    raise PermissionError("confirmed action is missing")
                existing = session.get(ToolInvocationRow, key)
                if existing is None:
                    call_count = session.scalar(
                        select(func.count()).select_from(ToolInvocationRow).where(
                            ToolInvocationRow.execution_id == execution))
                    if call_count >= max_calls:
                        raise PermissionError("tool call budget exhausted")
                    session.add(
                        ToolInvocationRow(tenant_id=tenant,
                                          execution_id=execution,
                                          tool_call_id=call_id,
                                          tool_name=name,
                                          idempotency_key=f"{execution}:{call_id}",
                                          request_hash=digest,
                                          status="running"))
                    session.add(
                        AuditLogRow(audit_id=uuid.uuid4().hex,
                                    tenant_id=tenant,
                                    decision="tool_started",
                                    tool_name=name,
                                    metadata_json={"execution_id": execution},
                                    redacted=True))
                    created = True
        except IntegrityError:
            created = False
        if not created:
            with self.database.sessions() as session:
                row = session.get(ToolInvocationRow, key)
                if row is None or (row.tenant_id, row.tool_name, row.request_hash) != (tenant, name, digest):
                    raise ConflictError("tool identity conflicts with trusted input")
                if row.status == "succeeded":
                    result = session.get(ToolResultRow, key)
                    if result is None or result.tenant_id != tenant:
                        raise ToolOutcomeUnknown("completed tool result unavailable")
                    return result.result_json["value"]
            if recover is None:
                count("tool.unknown_outcome")
                raise ToolOutcomeUnknown("tool outcome requires reconciliation; execution is not replayed")
            found, value = await recover(f"{execution}:{call_id}", arguments)
            if not found:
                raise ToolOutcomeUnknown("external operation outcome not confirmed")
        else:
            try:
                value = await operation(f"{execution}:{call_id}")
                json.dumps(value)  # A result must be persistable before completion is claimed.
            except BaseException:
                with self.database.sessions.begin() as session:
                    row = session.get(ToolInvocationRow, key, with_for_update=True)
                    if row.status == "running":
                        row.status = "unknown_outcome"
                raise
        with self.database.sessions.begin() as session:
            row = session.get(ToolInvocationRow, key, with_for_update=True)
            result = session.get(ToolResultRow, key)
            if result is not None:
                if row.result_hash != _hash(value):
                    raise ConflictError("recovered result differs from committed result")
                return result.result_json["value"]
            session.add(
                ToolResultRow(tenant_id=tenant,
                              execution_id=execution,
                              tool_call_id=call_id,
                              result_json={"value": value}))
            row.status, row.result_hash = "succeeded", _hash(value)
            session.add(
                AuditLogRow(audit_id=uuid.uuid4().hex,
                            tenant_id=tenant,
                            decision="tool_completed",
                            tool_name=name,
                            metadata_json={"execution_id": execution},
                            redacted=True))
        return value

    def wrap(self, function, *, tenant_id, timeout_seconds=30, max_calls=20, recover=None):
        """Preserve the function signature used by the public FunctionTool schema builder."""

        @functools.wraps(function)
        async def invoke(**kwargs):
            context = kwargs["tool_context"]
            metadata = context.agent_context.metadata
            if metadata.get("tenant_id") != tenant_id or not context.function_call_id:
                raise PermissionError("tool context is outside its tenant binding")
            execution = metadata.get("execution_id")
            if not execution:
                raise PermissionError("tool execution identity missing")
            arguments = {key: value for key, value in kwargs.items() if key != "tool_context"}

            async def operation(idempotency_key):
                # Operators can read this key from AgentContext inside their tool.
                # function_call_id is per invocation and remains the stable business key suffix.
                return await asyncio.wait_for(function(**kwargs), timeout_seconds)

            return await self.invoke(tenant_id,
                                     execution,
                                     context.function_call_id,
                                     function.__name__,
                                     arguments,
                                     operation,
                                     recover=recover,
                                     max_calls=max_calls)

        return FunctionTool(invoke)
