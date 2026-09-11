"""Side-effect tool invocation idempotency ledger."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

from trpc_service.persistence import Database
from trpc_service.persistence.models import ToolInvocationRow
from trpc_service.storage import ConflictError


def _hash(value) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True)
class ToolInvocation:
    tenant_id: str
    execution_id: str
    tool_call_id: str
    tool_name: str
    idempotency_key: str
    request_hash: str
    result_hash: str | None
    external_operation_id: str | None
    status: str


class ToolInvocationRepository:
    def __init__(self, database: Database) -> None:
        self._database = database

    def begin(self, tenant_id, execution_id, tool_call_id, tool_name, arguments) -> ToolInvocation:
        request_hash = _hash(arguments)
        with self._database.sessions.begin() as session:
            key = (execution_id, tool_call_id)
            row = session.get(ToolInvocationRow, key)
            if row is None:
                row = ToolInvocationRow(
                    tenant_id=tenant_id,
                    execution_id=execution_id,
                    tool_call_id=tool_call_id,
                    tool_name=tool_name,
                    idempotency_key=f"{execution_id}:{tool_call_id}",
                    request_hash=request_hash,
                    status="running",
                )
                session.add(row)
                session.flush()
            elif row.tenant_id != tenant_id or row.tool_name != tool_name or row.request_hash != request_hash:
                raise ConflictError("tool invocation idempotency key was reused with different input")
            return self._to_value(row)

    def complete(self, execution_id, tool_call_id, result, *, external_operation_id=None) -> None:
        with self._database.sessions.begin() as session:
            row = session.get(ToolInvocationRow, (execution_id, tool_call_id))
            if row is None:
                raise LookupError("tool invocation not found")
            row.result_hash = _hash(result)
            row.external_operation_id = external_operation_id
            row.status = "succeeded"

    def mark_unknown(self, execution_id, tool_call_id) -> None:
        with self._database.sessions.begin() as session:
            row = session.get(ToolInvocationRow, (execution_id, tool_call_id))
            if row is None:
                raise LookupError("tool invocation not found")
            row.status = "unknown_outcome"

    @staticmethod
    def _to_value(row) -> ToolInvocation:
        return ToolInvocation(
            row.tenant_id,
            row.execution_id,
            row.tool_call_id,
            row.tool_name,
            row.idempotency_key,
            row.request_hash,
            row.result_hash,
            row.external_operation_id,
            row.status,
        )
