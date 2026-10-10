from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Self
from uuid import uuid4

import pytest

from tests.conftest import envelope, repository, tenant_config
from trpc_service.runtime import TenantRuntime
from trpc_service.tenant.models import ToolRisk
from trpc_service.tool.confirmation import ConfirmationScope
from trpc_service.tool.execution import ExecutionStatus
from trpc_service.tool.governance import Decision
from trpc_service.tool.postgres import (
    PostgresBudgetLedger,
    PostgresConfirmationLedger,
    PostgresExecutionLedger,
    PostgresGovernanceAuditSink,
)


class Connection:
    def __init__(
        self,
        fetchvals: Iterable[object] = (),
        fetchrows: Iterable[object] = (),
    ) -> None:
        self.fetchvals = list(fetchvals)
        self.fetchrows = list(fetchrows)
        self.calls: list[tuple[object, ...]] = []

    def transaction(self) -> Self:
        return self

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *args: object) -> None:
        return None

    async def execute(self, *args: object) -> str:
        self.calls.append(args)
        return "OK"

    async def fetchval(self, *args: object) -> object:
        self.calls.append(args)
        return self.fetchvals.pop(0)

    async def fetchrow(self, *args: object) -> object:
        self.calls.append(args)
        return self.fetchrows.pop(0)


class Acquire:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    async def __aenter__(self) -> Connection:
        return self.connection

    async def __aexit__(self, *args: object) -> None:
        return None


class Pool:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection

    def acquire(self) -> Acquire:
        return Acquire(self.connection)


@pytest.mark.asyncio
async def test_postgres_budget_reservation_is_atomic_and_validated() -> None:
    connection = Connection(fetchvals=[True, None])
    ledger = PostgresBudgetLedger(Pool(connection))
    assert await ledger.reserve("tenant", token_units=4, cost_units=2, monthly_limit=10)
    assert not await ledger.reserve("tenant", token_units=1, cost_units=9, monthly_limit=10)
    assert any("tenant_budget_usage" in str(call[0]) for call in connection.calls)
    with pytest.raises(ValueError, match="negative"):
        await ledger.reserve("tenant", token_units=-1, cost_units=0, monthly_limit=10)


@pytest.mark.asyncio
async def test_postgres_confirmation_stores_token_hash_and_consumes_exact_scope() -> None:
    connection = Connection(fetchvals=[uuid4(), None])
    ledger = PostgresConfirmationLedger(Pool(connection))
    scope = ConfirmationScope(
        tenant_id="tenant",
        principal_id="principal",
        session_id="session",
        tool_name="write",
        arguments_hash="a" * 64,
    )
    raw_id = "sensitive-token-id"
    expires = int(datetime.now(UTC).timestamp()) + 60
    await ledger.issue(raw_id, expires, scope)
    assert await ledger.consume(raw_id, scope)
    assert not await ledger.consume(raw_id, scope)
    rendered = repr(connection.calls)
    assert raw_id not in rendered
    assert scope.arguments_hash in rendered


@pytest.mark.asyncio
async def test_postgres_execution_ledger_new_existing_and_finish() -> None:
    turn_id = str(uuid4())
    connection = Connection(
        fetchvals=["execution", "execution"],
        fetchrows=[
            None,
            *_active_turn_rows(turn_id, owner="worker-a", epoch=1),
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.STARTED.value,
                "lease_owner": "worker-a",
                "lease_epoch": 1,
            },
            *_active_turn_rows(turn_id, owner="worker-a", epoch=1),
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.STARTED.value,
                "lease_owner": "worker-a",
                "lease_epoch": 1,
            },
            *_active_turn_rows(turn_id, owner="worker-a", epoch=1),
        ],
    )
    ledger = PostgresExecutionLedger(Pool(connection))
    fresh = await ledger.begin(
        "execution",
        tenant_id="tenant",
        turn_id=turn_id,
        tool_name="read",
        risk=ToolRisk.IDEMPOTENT,
        arguments_hash="h",
        owner_id="worker-a",
        fencing_token=1,
    )
    assert fresh.fresh and fresh.status == ExecutionStatus.STARTED
    existing = await ledger.begin(
        "execution",
        tenant_id="tenant",
        turn_id=turn_id,
        tool_name="read",
        risk=ToolRisk.IDEMPOTENT,
        arguments_hash="h",
        owner_id="worker-a",
        fencing_token=1,
    )
    assert not existing.fresh and existing.status == ExecutionStatus.STARTED
    await ledger.finish(
        "execution",
        tenant_id="tenant",
        status=ExecutionStatus.SUCCEEDED,
        result={"must": "not be stored"},
        owner_id="worker-a",
        fencing_token=1,
    )
    assert "must" not in repr(connection.calls)


@pytest.mark.asyncio
async def test_postgres_execution_ledger_rejects_missing_or_invalid_state() -> None:
    turn_id = str(uuid4())
    missing = PostgresExecutionLedger(Pool(Connection(fetchrows=[None, None])))
    with pytest.raises(RuntimeError, match="session lease"):
        await missing.begin(
            "key",
            tenant_id="tenant",
            turn_id=turn_id,
            tool_name="read",
            risk=ToolRisk.IDEMPOTENT,
            arguments_hash="h",
            owner_id="worker-a",
            fencing_token=1,
        )
    invalid = PostgresExecutionLedger(
        Pool(
            Connection(
                fetchrows=[
                    {
                        "status": "corrupt",
                    }
                ]
            )
        )
    )
    with pytest.raises(RuntimeError, match="invalid status"):
        await invalid.begin(
            "key",
            tenant_id="tenant",
            turn_id=turn_id,
            tool_name="read",
            risk=ToolRisk.IDEMPOTENT,
            arguments_hash="h",
            owner_id="worker-a",
            fencing_token=1,
        )
    with pytest.raises(RuntimeError, match="does not exist"):
        await PostgresExecutionLedger(Pool(Connection(fetchrows=[None]))).finish(
            "key",
            tenant_id="tenant",
            status=ExecutionStatus.FAILED,
            owner_id="worker-a",
            fencing_token=1,
        )


@pytest.mark.asyncio
async def test_postgres_execution_ledger_rejects_unfenced_calls() -> None:
    ledger = PostgresExecutionLedger(Pool(Connection()))
    with pytest.raises(ValueError, match="owner_id"):
        await ledger.begin(
            "key",
            tenant_id="tenant",
            turn_id=str(uuid4()),
            tool_name="read",
            risk=ToolRisk.IDEMPOTENT,
            arguments_hash="h",
            fencing_token=1,
        )
    with pytest.raises(ValueError, match="fencing_token"):
        await ledger.finish(
            "key",
            tenant_id="tenant",
            status=ExecutionStatus.FAILED,
            owner_id="worker-a",
        )


def _active_turn_rows(turn_id: str, *, owner: str, epoch: int) -> list[dict[str, object]]:
    return [
        {"session_id": "session"},
        {"session_id": "session", "lease_owner": owner, "lease_epoch": epoch},
        {"session_id": "session", "status": "processing", "fencing_token": epoch},
        {"lease_owner": owner, "lease_epoch": epoch, "lease_valid": True},
    ]


@pytest.mark.asyncio
async def test_postgres_execution_ledger_idempotent_takeover_requires_current_fence() -> None:
    turn_id = str(uuid4())
    connection = Connection(
        fetchvals=["execution"],
        fetchrows=[
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.STARTED.value,
                "lease_owner": "worker-a",
                "lease_epoch": 1,
            },
            *_active_turn_rows(turn_id, owner="worker-b", epoch=2),
        ],
    )
    record = await PostgresExecutionLedger(Pool(connection)).begin(
        "execution",
        tenant_id="tenant",
        turn_id=turn_id,
        tool_name="read",
        risk=ToolRisk.IDEMPOTENT,
        arguments_hash="h",
        owner_id="worker-b",
        fencing_token=2,
    )
    assert record.fresh and record.status == ExecutionStatus.STARTED
    assert any("SET lease_owner" in str(call[0]) for call in connection.calls)
    assert any("lease_epoch IS DISTINCT FROM" in str(call[0]) for call in connection.calls)


@pytest.mark.asyncio
async def test_postgres_succeeded_without_result_replays_only_under_current_fence() -> None:
    turn_id = str(uuid4())
    connection = Connection(
        fetchrows=[
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.SUCCEEDED.value,
                "lease_owner": "worker-a",
                "lease_epoch": 1,
            },
            *_active_turn_rows(turn_id, owner="worker-a", epoch=1),
        ]
    )
    record = await PostgresExecutionLedger(Pool(connection)).begin(
        "execution",
        tenant_id="tenant",
        turn_id=turn_id,
        tool_name="read",
        risk=ToolRisk.IDEMPOTENT,
        arguments_hash="h",
        owner_id="worker-a",
        fencing_token=1,
    )
    assert record.status == ExecutionStatus.SUCCEEDED
    assert record.replay_terminal
    lock_queries = [
        str(call[0])
        for call in connection.calls
        if isinstance(call[0], str)
        and (
            "FROM session_turns" in str(call[0])
            or "FROM sessions" in str(call[0])
            or "lease_valid" in str(call[0])
        )
    ]
    assert "FROM session_turns" in lock_queries[0] and "FOR UPDATE" not in lock_queries[0]
    assert "FROM sessions" in lock_queries[1] and "FOR UPDATE" in lock_queries[1]
    assert "FROM session_turns" in lock_queries[2] and "FOR UPDATE" in lock_queries[2]
    assert "lease_valid" in lock_queries[3] and "FOR UPDATE" not in lock_queries[3]


@pytest.mark.asyncio
async def test_postgres_execution_ledger_non_idempotent_takeover_requires_confirmation() -> None:
    turn_id = str(uuid4())
    connection = Connection(
        fetchrows=[
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.STARTED.value,
                "lease_owner": "worker-a",
                "lease_epoch": 1,
            },
            *_active_turn_rows(turn_id, owner="worker-b", epoch=2),
        ]
    )
    from trpc_service.tool.postgres import ToolExecutionConflict

    with pytest.raises(ToolExecutionConflict, match="crossed a session fence") as exc_info:
        await PostgresExecutionLedger(Pool(connection)).begin(
            "execution",
            tenant_id="tenant",
            turn_id=turn_id,
            tool_name="write",
            risk=ToolRisk.NON_IDEMPOTENT,
            arguments_hash="h",
            owner_id="worker-b",
            fencing_token=2,
        )
    assert exc_info.value.requires_confirmation


@pytest.mark.asyncio
async def test_postgres_execution_ledger_finish_rechecks_authoritative_session_fence() -> None:
    turn_id = str(uuid4())
    connection = Connection(
        fetchrows=[
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.STARTED.value,
                "lease_owner": "worker-a",
                "lease_epoch": 1,
            },
            {
                "session_id": "session",
            },
            {
                "session_id": "session",
                "status": ExecutionStatus.STARTED.value,
                "lease_owner": "worker-b",
                "lease_epoch": 2,
            },
            {
                "session_id": "session",
                "status": "processing",
                "fencing_token": 2,
            },
            {"lease_owner": "worker-b", "lease_epoch": 2, "lease_valid": True},
        ]
    )
    from trpc_service.tool.postgres import ToolExecutionConflict

    with pytest.raises(ToolExecutionConflict, match="session lease"):
        await PostgresExecutionLedger(Pool(connection)).finish(
            "execution",
            tenant_id="tenant",
            status=ExecutionStatus.SUCCEEDED,
            owner_id="worker-a",
            fencing_token=1,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "old_status",
    [ExecutionStatus.STARTED.value, ExecutionStatus.FAILED.value, ExecutionStatus.AMBIGUOUS.value],
)
async def test_postgres_idempotent_takeover_restarts_attempt_before_finish(old_status: str) -> None:
    turn_id = str(uuid4())
    connection = Connection(
        fetchvals=["execution", "execution"],
        fetchrows=[
            {
                "turn_id": turn_id,
                "status": old_status,
                "lease_owner": "worker-a",
                "lease_epoch": 1,
            },
            *_active_turn_rows(turn_id, owner="worker-b", epoch=2),
            {
                "turn_id": turn_id,
                "status": ExecutionStatus.STARTED.value,
                "lease_owner": "worker-b",
                "lease_epoch": 2,
            },
            *_active_turn_rows(turn_id, owner="worker-b", epoch=2),
        ],
    )
    ledger = PostgresExecutionLedger(Pool(connection))
    record = await ledger.begin(
        "execution",
        tenant_id="tenant",
        turn_id=turn_id,
        tool_name="read",
        risk=ToolRisk.IDEMPOTENT,
        arguments_hash="h",
        owner_id="worker-b",
        fencing_token=2,
    )
    assert record.fresh and record.status == ExecutionStatus.STARTED
    await ledger.finish(
        "execution",
        tenant_id="tenant",
        status=ExecutionStatus.SUCCEEDED,
        owner_id="worker-b",
        fencing_token=2,
    )


@pytest.mark.asyncio
async def test_postgres_governance_audit_contains_policy_metadata_only() -> None:
    accepted = await TenantRuntime(repository(), routing_key=b"a" * 32).accept(
        "binding-unpredictable-a", envelope()
    )
    connection = Connection()
    await PostgresGovernanceAuditSink(Pool(connection)).record(
        context=accepted.context,
        config=tenant_config(),
        tool_name="read",
        decision=Decision.ALLOW,
        reason="policy_passed",
    )
    rendered = repr(connection.calls)
    assert "policy_passed" in rendered
    assert "message-1" not in rendered and "hello" not in rendered
