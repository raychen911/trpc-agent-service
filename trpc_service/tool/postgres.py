"""PostgreSQL tool execution ledger."""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

from .confirmation import ConfirmationScope
from .execution import ExecutionRecord, ExecutionStatus


class PostgresExecutionLedger:
    def __init__(self, repository: Any, tenant_id: str, session_id: str, turn_id: str, tool_name: str, arguments_hash: str) -> None:
        self.repository = repository
        self.tenant_id = tenant_id
        self.session_id = session_id
        self.turn_id = turn_id
        self.tool_name = tool_name
        self.arguments_hash = arguments_hash

    async def begin(self, key: str) -> ExecutionRecord:
        async with self.repository._scoped(self.tenant_id) as connection:
            inserted = await connection.fetchval(
                """
                INSERT INTO tool_executions(tenant_id,execution_key,session_id,turn_id,tool_name,arguments_hash,status)
                VALUES($1,$2,$3,$4,$5,$6,'started') ON CONFLICT DO NOTHING RETURNING execution_key
                """,
                self.tenant_id,
                key,
                self.session_id,
                self.turn_id,
                self.tool_name,
                self.arguments_hash,
            )
            if inserted:
                return ExecutionRecord(key, ExecutionStatus.STARTED, fresh=True)
            row = await connection.fetchrow(
                "SELECT status,result_json FROM tool_executions WHERE tenant_id=$1 AND execution_key=$2",
                self.tenant_id,
                key,
            )
        result = row["result_json"]
        if isinstance(result, str):
            result = json.loads(result)
        return ExecutionRecord(key, ExecutionStatus(row["status"]), result)

    async def finish(self, key: str, status: ExecutionStatus, result: Any = None) -> None:
        async with self.repository._scoped(self.tenant_id) as connection:
            await connection.execute(
                """
                UPDATE tool_executions SET status=$3,result_json=$4::jsonb,updated_at=now()
                WHERE tenant_id=$1 AND execution_key=$2
                """,
                self.tenant_id,
                key,
                status.value,
                json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=str),
            )


class PostgresConfirmationStore:
    def __init__(self, repository: Any) -> None:
        self.repository = repository

    async def issue(self, token_id: str, expires_at: int, scope: ConfirmationScope) -> None:
        async with self.repository._scoped(scope.tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO tool_confirmations(
                    tenant_id,token_id,user_id,session_id,tool_name,arguments_hash,expires_at
                ) VALUES($1,$2,$3,$4,$5,$6,to_timestamp($7))
                """,
                scope.tenant_id,
                token_id,
                scope.user_id,
                scope.session_id,
                scope.tool_name,
                scope.arguments_hash,
                expires_at,
            )

    async def consume(self, token_id: str, scope: ConfirmationScope) -> bool:
        values = asdict(scope)
        async with self.repository._scoped(scope.tenant_id) as connection:
            result = await connection.fetchval(
                """
                UPDATE tool_confirmations SET consumed_at=now()
                WHERE tenant_id=$1 AND token_id=$2 AND user_id=$3 AND session_id=$4
                  AND tool_name=$5 AND arguments_hash=$6 AND consumed_at IS NULL AND expires_at >= now()
                RETURNING token_id
                """,
                values["tenant_id"],
                token_id,
                values["user_id"],
                values["session_id"],
                values["tool_name"],
                values["arguments_hash"],
            )
        return result is not None
