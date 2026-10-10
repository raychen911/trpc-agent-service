"""PostgreSQL ledgers for budgets, confirmation tokens, and tool execution."""

from __future__ import annotations

import hashlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import asyncpg

from trpc_service.tenant.models import TenantConfig, TenantContext, ToolRisk
from trpc_service.tool.confirmation import ConfirmationScope
from trpc_service.tool.execution import ExecutionRecord, ExecutionStatus
from trpc_service.tool.governance import Decision


class _TenantStore:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    @asynccontextmanager
    async def _transaction(self, tenant_id: str) -> AsyncIterator[asyncpg.Connection]:
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            yield connection


class PostgresBudgetLedger(_TenantStore):
    """Atomically reserve monthly tenant usage across all worker processes."""

    async def reserve(
        self,
        tenant_id: str,
        *,
        token_units: int,
        cost_units: int,
        monthly_limit: int,
    ) -> bool:
        if token_units < 0 or cost_units < 0:
            raise ValueError("budget reservation cannot be negative")
        async with self._transaction(tenant_id) as connection:
            reserved = await connection.fetchval(
                """
                INSERT INTO tenant_budget_usage (
                    tenant_id,usage_month,token_units,cost_units
                )
                SELECT $1,date_trunc('month',now())::date,$2::bigint,$3::bigint
                 WHERE $3::bigint <= $4::bigint
                ON CONFLICT (tenant_id,usage_month)
                DO UPDATE SET token_units=tenant_budget_usage.token_units+excluded.token_units,
                              cost_units=tenant_budget_usage.cost_units+excluded.cost_units,
                              updated_at=now()
                 WHERE tenant_budget_usage.cost_units+excluded.cost_units <= $4::bigint
                RETURNING true
                """,
                tenant_id,
                token_units,
                cost_units,
                monthly_limit,
            )
        return bool(reserved)


class PostgresConfirmationLedger(_TenantStore):
    """Store only a hash of the one-time token identifier."""

    async def issue(
        self,
        token_id: str,
        expires_at: int,
        scope: ConfirmationScope,
    ) -> None:
        token_hash = _token_hash(token_id)
        expires = datetime.fromtimestamp(expires_at, tz=UTC)
        async with self._transaction(scope.tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO confirmation_challenges (
                    tenant_id,principal_id,session_id,tool_name,arguments_hash,
                    token_hash,expires_at
                ) VALUES ($1,$2,$3,$4,$5,$6,$7)
                """,
                scope.tenant_id,
                scope.principal_id,
                scope.session_id,
                scope.tool_name,
                scope.arguments_hash,
                token_hash,
                expires,
            )

    async def consume(self, token_id: str, scope: ConfirmationScope) -> bool:
        async with self._transaction(scope.tenant_id) as connection:
            challenge_id = await connection.fetchval(
                """
                UPDATE confirmation_challenges
                   SET consumed_at=now()
                 WHERE tenant_id=$1 AND token_hash=$2
                   AND principal_id=$3 AND session_id=$4 AND tool_name=$5
                   AND arguments_hash=$6 AND consumed_at IS NULL AND expires_at>=now()
                RETURNING challenge_id
                """,
                scope.tenant_id,
                _token_hash(token_id),
                scope.principal_id,
                scope.session_id,
                scope.tool_name,
                scope.arguments_hash,
            )
        return challenge_id is not None


class PostgresExecutionLedger(_TenantStore):
    """Persist tool outcome state without storing arguments or result content."""

    async def _assert_active_turn(
        self,
        connection: asyncpg.Connection,
        *,
        tenant_id: str,
        turn_id: UUID,
        owner_id: str,
        fencing_token: int,
    ) -> None:
        """Lock and validate the authoritative session lease for a tool turn.

        ``tool_executions.lease_*`` is only a copy of the worker fence.  The
        session and turn rows are authoritative, so every operation that can
        create or finish a side effect checks them in the same transaction.
        Locking both rows also makes a lease hand-off serialize with this
        check; the final UPDATE below repeats the predicates as a last CAS.
        """

        # Do not lock a joined pair here.  The session mailbox commit path
        # locks ``sessions`` before ``session_turns``; explicitly acquire the
        # same order to avoid a planner-dependent turn/session deadlock.
        turn_ref = await connection.fetchrow(
            """
            SELECT session_id
              FROM session_turns
             WHERE tenant_id=$1 AND turn_id=$2::uuid
            """,
            tenant_id,
            turn_id,
        )
        if turn_ref is None:
            raise ToolExecutionConflict(
                "tool execution session lease is no longer current",
                requires_confirmation=True,
            )
        session = await connection.fetchrow(
            """
            SELECT session_id,lease_owner,lease_epoch,lease_expires_at
              FROM sessions
             WHERE tenant_id=$1 AND session_id=$2
             FOR UPDATE
            """,
            tenant_id,
            turn_ref["session_id"],
        )
        turn = await connection.fetchrow(
            """
            SELECT session_id,status,fencing_token
              FROM session_turns
             WHERE tenant_id=$1 AND turn_id=$2::uuid
             FOR UPDATE
            """,
            tenant_id,
            turn_id,
        )
        current = await connection.fetchrow(
            """
            SELECT lease_owner,lease_epoch,lease_expires_at,
                   lease_expires_at > clock_timestamp() AS lease_valid
              FROM sessions
             WHERE tenant_id=$1 AND session_id=$2
            """,
            tenant_id,
            turn_ref["session_id"],
        )
        if (
            session is None
            or turn is None
            or current is None
            or turn["session_id"] != turn_ref["session_id"]
            or turn["status"] != "processing"
            or int(turn["fencing_token"]) != fencing_token
            or current["lease_owner"] != owner_id
            or int(current["lease_epoch"]) != fencing_token
            or not bool(current["lease_valid"])
        ):
            raise ToolExecutionConflict(
                "tool execution session lease is no longer current",
                requires_confirmation=True,
            )

    @staticmethod
    def _require_fencing(owner_id: str | None, fencing_token: int | None) -> None:
        """Reject a tool ledger operation that cannot be fenced.

        The database predicates intentionally retain the nullable SQL form
        for compatibility with old rows, but production callers must never
        use that bypass.  The worker's session lease owner and epoch are the
        values that must be supplied for both begin and finish.
        """

        if not isinstance(owner_id, str) or not owner_id.strip():
            raise ValueError("tool execution owner_id is required for fencing")
        if (
            not isinstance(fencing_token, int)
            or isinstance(fencing_token, bool)
            or fencing_token < 1
        ):
            raise ValueError("tool execution fencing_token must be a positive integer")

    async def begin(
        self,
        execution_key: str,
        *,
        tenant_id: str,
        turn_id: str,
        tool_name: str,
        risk: ToolRisk,
        arguments_hash: str,
        owner_id: str | None = None,
        fencing_token: int | None = None,
    ) -> ExecutionRecord:
        self._require_fencing(owner_id, fencing_token)
        turn_uuid = UUID(turn_id)
        assert owner_id is not None
        assert fencing_token is not None
        async with self._transaction(tenant_id) as connection:
            existing = await connection.fetchrow(
                """
                SELECT turn_id,tool_name,classification,arguments_hash,status,
                       lease_owner,lease_epoch
                  FROM tool_executions
                 WHERE tenant_id=$1 AND execution_key=$2
                 FOR UPDATE
                """,
                tenant_id,
                execution_key,
            )
            if existing is None:
                await self._assert_active_turn(
                    connection,
                    tenant_id=tenant_id,
                    turn_id=turn_uuid,
                    owner_id=owner_id,
                    fencing_token=fencing_token,
                )
            else:
                status = _execution_status(existing["status"])
                # A terminal success is authoritative and must never require
                # a live lease merely to answer a duplicate non-idempotent
                # request.  An idempotent success without a stored result is
                # recoverable only while the current processing fence is
                # still valid; the caller may rebuild the result, but must
                # not rewrite the terminal ledger row.
                if status == ExecutionStatus.SUCCEEDED:
                    if risk == ToolRisk.IDEMPOTENT:
                        await self._assert_active_turn(
                            connection,
                            tenant_id=tenant_id,
                            turn_id=UUID(str(existing["turn_id"])),
                            owner_id=owner_id,
                            fencing_token=fencing_token,
                        )
                        return ExecutionRecord(
                            execution_key,
                            status,
                            replay_terminal=True,
                        )
                    return ExecutionRecord(execution_key, status)
                existing_turn_id = UUID(str(existing["turn_id"]))
                await self._assert_active_turn(
                    connection,
                    tenant_id=tenant_id,
                    turn_id=existing_turn_id,
                    owner_id=owner_id,
                    fencing_token=fencing_token,
                )
                same_fence = (
                    existing["lease_owner"] == owner_id
                    and int(existing["lease_epoch"]) == fencing_token
                )
                if not same_fence:
                    if risk != ToolRisk.IDEMPOTENT:
                        raise ToolExecutionConflict(
                            "non-idempotent tool execution crossed a session fence",
                            requires_confirmation=True,
                        )
                    claimed = await connection.fetchval(
                        """
                        UPDATE tool_executions
                           SET lease_owner=$3,lease_epoch=$4,
                               status='started',completed_at=NULL
                         WHERE tenant_id=$1 AND execution_key=$2
                           AND status IN ('started','failed','ambiguous')
                           AND (
                               lease_owner IS DISTINCT FROM $3
                               OR lease_epoch IS DISTINCT FROM $4
                           )
                        RETURNING execution_key
                        """,
                        tenant_id,
                        execution_key,
                        owner_id,
                        fencing_token,
                    )
                    if claimed is None:
                        raise ToolExecutionConflict(
                            "tool execution was changed while taking its fence",
                            requires_confirmation=True,
                        )
                    return ExecutionRecord(execution_key, ExecutionStatus.STARTED, fresh=True)
                return ExecutionRecord(execution_key, status)

            inserted = await connection.fetchval(
                """
                INSERT INTO tool_executions (
                    tenant_id,execution_key,turn_id,tool_name,classification,
                    arguments_hash,status,lease_owner,lease_epoch
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
                ON CONFLICT (tenant_id,execution_key) DO NOTHING
                RETURNING execution_key
                """,
                tenant_id,
                execution_key,
                turn_uuid,
                tool_name,
                risk.value,
                arguments_hash,
                ExecutionStatus.STARTED.value,
                owner_id,
                fencing_token,
            )
            if inserted is not None:
                return ExecutionRecord(execution_key, ExecutionStatus.STARTED, fresh=True)
            # A concurrent insert can win after the initial SELECT.  Re-read
            # its locked row and apply exactly the same fence rules.
            existing = await connection.fetchrow(
                """
                SELECT turn_id,status,lease_owner,lease_epoch
                  FROM tool_executions
                 WHERE tenant_id=$1 AND execution_key=$2
                 FOR UPDATE
                """,
                tenant_id,
                execution_key,
            )
            if existing is None:
                raise RuntimeError("tool execution disappeared during conflict resolution")
            status = _execution_status(existing["status"])
            if status == ExecutionStatus.SUCCEEDED:
                if risk == ToolRisk.IDEMPOTENT:
                    await self._assert_active_turn(
                        connection,
                        tenant_id=tenant_id,
                        turn_id=UUID(str(existing["turn_id"])),
                        owner_id=owner_id,
                        fencing_token=fencing_token,
                    )
                    return ExecutionRecord(
                        execution_key,
                        status,
                        replay_terminal=True,
                    )
                return ExecutionRecord(execution_key, status)
            await self._assert_active_turn(
                connection,
                tenant_id=tenant_id,
                turn_id=UUID(str(existing["turn_id"])),
                owner_id=owner_id,
                fencing_token=fencing_token,
            )
            if existing["lease_owner"] != owner_id or int(existing["lease_epoch"]) != fencing_token:
                if risk != ToolRisk.IDEMPOTENT:
                    raise ToolExecutionConflict(
                        "tool execution fencing token is no longer current",
                        requires_confirmation=True,
                    )
                claimed = await connection.fetchval(
                    """
                    UPDATE tool_executions
                       SET lease_owner=$3,lease_epoch=$4,
                           status='started',completed_at=NULL
                     WHERE tenant_id=$1 AND execution_key=$2
                       AND status IN ('started','failed','ambiguous')
                    RETURNING execution_key
                    """,
                    tenant_id,
                    execution_key,
                    owner_id,
                    fencing_token,
                )
                if claimed is None:
                    raise ToolExecutionConflict(
                        "tool execution was changed while taking its fence",
                        requires_confirmation=True,
                    )
                return ExecutionRecord(execution_key, ExecutionStatus.STARTED, fresh=True)
            return ExecutionRecord(execution_key, status)

    async def finish(
        self,
        execution_key: str,
        *,
        tenant_id: str,
        status: ExecutionStatus,
        result: Any = None,
        owner_id: str | None = None,
        fencing_token: int | None = None,
    ) -> None:
        if status == ExecutionStatus.STARTED:
            raise ValueError("tool execution finish requires a terminal status")
        self._require_fencing(owner_id, fencing_token)
        assert owner_id is not None
        assert fencing_token is not None
        async with self._transaction(tenant_id) as connection:
            existing = await connection.fetchrow(
                """
                SELECT turn_id,status,lease_owner,lease_epoch
                  FROM tool_executions
                 WHERE tenant_id=$1 AND execution_key=$2
                 FOR UPDATE
                """,
                tenant_id,
                execution_key,
            )
            if existing is None:
                raise RuntimeError("tool execution does not exist or is no longer owned")
            if _execution_status(existing["status"]) != ExecutionStatus.STARTED:
                raise RuntimeError("tool execution does not exist or is no longer owned")
            if existing["lease_owner"] != owner_id or int(existing["lease_epoch"]) != fencing_token:
                raise ToolExecutionConflict(
                    "tool execution finish crossed a session fence",
                    requires_confirmation=True,
                )
            await self._assert_active_turn(
                connection,
                tenant_id=tenant_id,
                turn_id=UUID(str(existing["turn_id"])),
                owner_id=owner_id,
                fencing_token=fencing_token,
            )
            updated = await connection.fetchval(
                """
                UPDATE tool_executions AS execution
                   SET status=$3,completed_at=clock_timestamp()
                 WHERE tenant_id=$1 AND execution_key=$2
                   AND status='started'
                   AND lease_owner=$4 AND lease_epoch=$5
                   AND EXISTS (
                       SELECT 1
                         FROM session_turns AS turn
                         JOIN sessions AS session
                           ON session.tenant_id=turn.tenant_id
                          AND session.session_id=turn.session_id
                        WHERE turn.tenant_id=execution.tenant_id
                          AND turn.turn_id=execution.turn_id
                          AND turn.status='processing'
                          AND turn.fencing_token=$5
                          AND session.lease_owner=$4
                          AND session.lease_epoch=$5
                          AND session.lease_expires_at > clock_timestamp()
                   )
                RETURNING execution_key
                """,
                tenant_id,
                execution_key,
                status.value,
                owner_id,
                fencing_token,
            )
        if updated is None:
            raise RuntimeError("tool execution does not exist or is no longer owned")


class ToolExecutionConflict(RuntimeError):
    """A tool operation crossed the authoritative session fence."""

    def __init__(self, message: str, *, requires_confirmation: bool = False) -> None:
        super().__init__(message)
        self.requires_confirmation = requires_confirmation


def _execution_status(value: object) -> ExecutionStatus:
    try:
        return ExecutionStatus(str(value))
    except ValueError as exc:
        raise RuntimeError("tool execution has an invalid status") from exc


class PostgresGovernanceAuditSink(_TenantStore):
    """Persist policy decisions without arguments, results, or message content."""

    async def record(
        self,
        *,
        context: TenantContext,
        config: TenantConfig,
        tool_name: str,
        decision: Decision,
        reason: str,
    ) -> None:
        async with self._transaction(context.tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO audit_logs (
                    tenant_id,user_id,session_id,tool_name,decision,trace_id,
                    config_version,policy_version,redaction_applied,metadata_json
                ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,true,$9::jsonb)
                """,
                context.tenant_id,
                context.principal_id,
                context.session_id,
                tool_name,
                decision.value,
                context.trace_id,
                config.version,
                config.policy_version,
                json.dumps({"reason": reason}, separators=(",", ":")),
            )


def _token_hash(token_id: str) -> str:
    return hashlib.sha256(token_id.encode()).hexdigest()


__all__ = [
    "PostgresBudgetLedger",
    "PostgresConfirmationLedger",
    "PostgresExecutionLedger",
    "PostgresGovernanceAuditSink",
    "ToolExecutionConflict",
]
