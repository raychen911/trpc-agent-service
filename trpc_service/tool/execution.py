"""Idempotency records for tools with external side effects."""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from typing import Any

from pydantic import BaseModel

from trpc_service._compat import StrEnum


class ToolExecutionState(StrEnum):
    RESERVED = "reserved"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNKNOWN = "unknown"


class ToolExecutionRecord(BaseModel):
    execution_id: str
    tenant_id: str
    request_id: str
    tool_name: str
    arguments_sha256: str
    state: ToolExecutionState = ToolExecutionState.RESERVED
    result: dict[str, Any] | None = None
    error_code: str = ""


def canonical_arguments_hash(arguments: dict[str, Any]) -> str:
    encoded = json.dumps(arguments, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


class InMemoryToolExecutionStore:
    """Reserve one request/tool/argument tuple before invoking a side effect."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str, str, str], ToolExecutionRecord] = {}
        self._lock = asyncio.Lock()

    async def reserve(self, tenant_id: str, request_id: str, tool_name: str,
                      arguments: dict[str, Any]) -> tuple[ToolExecutionRecord, bool]:
        arguments_hash = canonical_arguments_hash(arguments)
        key = (tenant_id, request_id, tool_name, arguments_hash)
        async with self._lock:
            existing = self._records.get(key)
            if existing:
                return existing.model_copy(deep=True), False
            record = ToolExecutionRecord(execution_id=uuid.uuid4().hex,
                                         tenant_id=tenant_id,
                                         request_id=request_id,
                                         tool_name=tool_name,
                                         arguments_sha256=arguments_hash)
            self._records[key] = record
            return record.model_copy(deep=True), True

    async def finish(self,
                     execution_id: str,
                     state: ToolExecutionState,
                     *,
                     result: dict[str, Any] | None = None,
                     error_code: str = "") -> ToolExecutionRecord:
        async with self._lock:
            record = next((item for item in self._records.values() if item.execution_id == execution_id), None)
            if record is None:
                raise KeyError(f"tool execution not found: {execution_id}")
            record.state = state
            record.result = result
            record.error_code = error_code
            return record.model_copy(deep=True)


class PostgresToolExecutionStore:
    """Unique tuple reservation; RUNNING after a crash is treated as unknown."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def reserve(self, tenant_id: str, request_id: str, tool_name: str,
                      arguments: dict[str, Any]) -> tuple[ToolExecutionRecord, bool]:
        execution_id = uuid.uuid4().hex
        digest = canonical_arguments_hash(arguments)
        await self._pool.execute(
            "INSERT INTO tool_execution (execution_id,tenant_id,request_id,tool_name,arguments_sha256,state) "
            "VALUES ($1,$2,$3,$4,$5,'reserved') "
            "ON CONFLICT (tenant_id,request_id,tool_name,arguments_sha256) DO NOTHING", execution_id, tenant_id,
            request_id, tool_name, digest)
        row = await self._pool.fetchrow(
            "SELECT * FROM tool_execution WHERE tenant_id=$1 AND request_id=$2 "
            "AND tool_name=$3 AND arguments_sha256=$4", tenant_id, request_id, tool_name, digest)
        record = self._record(row)
        return record, record.execution_id == execution_id

    @staticmethod
    def _record(row) -> ToolExecutionRecord:
        value = row["result_json"]
        if isinstance(value, str):
            value = json.loads(value)
        return ToolExecutionRecord(execution_id=row["execution_id"],
                                   tenant_id=row["tenant_id"],
                                   request_id=row["request_id"],
                                   tool_name=row["tool_name"],
                                   arguments_sha256=row["arguments_sha256"],
                                   state=row["state"],
                                   result=value,
                                   error_code=row["error_code"] or "")

    async def finish(self,
                     execution_id: str,
                     state: ToolExecutionState,
                     *,
                     result: dict[str, Any] | None = None,
                     error_code: str = "") -> ToolExecutionRecord:
        row = await self._pool.fetchrow(
            "UPDATE tool_execution SET state=$2,result_json=$3::jsonb,error_code=$4,updated_at=now() "
            "WHERE execution_id=$1 RETURNING *", execution_id, state.value,
            json.dumps(result) if result is not None else None, error_code)
        if row is None:
            raise KeyError("tool execution not found")
        return self._record(row)
