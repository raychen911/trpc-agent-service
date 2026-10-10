from __future__ import annotations

import json
import os
from uuid import uuid4

import asyncpg
import pytest

from trpc_service.storage.vector import PgVectorKnowledgeStore
from trpc_service.tenant.models import ToolRisk
from trpc_service.tool.execution import ExecutionStatus
from trpc_service.tool.postgres import (
    PostgresBudgetLedger,
    PostgresExecutionLedger,
    ToolExecutionConflict,
)

pytestmark = pytest.mark.integration


def _postgres_dsn() -> str:
    value = os.getenv("TRPC_TEST_POSTGRES_DSN")
    if not value:
        pytest.skip("TRPC_TEST_POSTGRES_DSN is not set")
    return value


def _worker_postgres_dsn() -> str:
    value = os.getenv("TRPC_TEST_POSTGRES_WORKER_DSN")
    if not value:
        pytest.skip("TRPC_TEST_POSTGRES_WORKER_DSN is not set")
    return value


@pytest.mark.asyncio
async def test_runtime_role_enforces_rls_for_identical_session_ids() -> None:
    connection = await asyncpg.connect(_postgres_dsn())
    transaction = connection.transaction()
    await transaction.start()
    suffix = uuid4().hex
    tenants = (f"rls-a-{suffix}", f"rls-b-{suffix}")
    try:
        ownership = await connection.fetchrow(
            """
            SELECT current_user AS runtime_user,
                   pg_get_userbyid(relowner) AS table_owner
              FROM pg_class
             WHERE oid = 'public.sessions'::regclass
            """
        )
        assert ownership is not None
        assert ownership["runtime_user"] != ownership["table_owner"]

        for tenant_id in tenants:
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            await connection.execute(
                "INSERT INTO tenants (tenant_id,display_name) VALUES ($1,$2)",
                tenant_id,
                "RLS contract tenant",
            )
            await connection.execute(
                """
                INSERT INTO agent_apps (tenant_id,app_id,display_name)
                VALUES ($1,'same-app','RLS contract app')
                """,
                tenant_id,
            )
            await connection.execute(
                """
                INSERT INTO sessions (tenant_id,session_id,app_id,principal_id,state_json)
                VALUES ($1,'same-session','same-app','same-user',$2::jsonb)
                """,
                tenant_id,
                '{"owner":"' + tenant_id + '"}',
            )

        await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenants[0])
        rows = await connection.fetch(
            "SELECT tenant_id,state_json FROM sessions WHERE session_id='same-session'"
        )
        assert [row["tenant_id"] for row in rows] == [tenants[0]]
        assert json.loads(rows[0]["state_json"])["owner"] == tenants[0]
        assert (
            await connection.fetchval(
                "SELECT count(*) FROM sessions WHERE tenant_id=$1", tenants[1]
            )
            == 0
        )
    finally:
        await transaction.rollback()
        await connection.close()


@pytest.mark.asyncio
async def test_global_worker_functions_are_not_executable_by_runtime_role() -> None:
    """The live role split must reject global SQL from the tenant login."""

    runtime = await asyncpg.connect(_postgres_dsn())
    worker = await asyncpg.connect(_worker_postgres_dsn())
    functions = (
        "SELECT * FROM list_channel_bindings('feishu')",
        "SELECT * FROM claim_outbox_events('none','rls-role-test',1,1)",
        "SELECT * FROM sweep_expired_session_leases(1)",
        "SELECT * FROM schedule_session_mailbox_retries(1)",
        "SELECT * FROM reconcile_session_mailboxes(1)",
        "SELECT * FROM reconcile_session_mailboxes_v2(1,5)",
    )
    try:
        role = await worker.fetchrow(
            """
            SELECT current_user::text AS current_user,
                   session_user::text AS session_user,
                   rolsuper,
                   rolbypassrls
              FROM pg_roles
             WHERE rolname=current_user
            """
        )
        assert role is not None
        assert role["current_user"] == "trpc_worker"
        assert role["session_user"] == "trpc_worker"
        assert role["rolsuper"] is False
        assert role["rolbypassrls"] is True

        runtime_role = await runtime.fetchrow(
            "SELECT rolsuper,rolbypassrls FROM pg_roles WHERE rolname=current_user"
        )
        assert runtime_role is not None
        assert runtime_role["rolsuper"] is False
        assert runtime_role["rolbypassrls"] is False

        for statement in functions:
            with pytest.raises(asyncpg.InsufficientPrivilegeError):
                await runtime.fetch(statement)
            await worker.fetch(statement)
    finally:
        await runtime.close()
        await worker.close()


@pytest.mark.asyncio
async def test_pgvector_projection_is_tenant_scoped() -> None:
    pool = await asyncpg.create_pool(_postgres_dsn(), min_size=1, max_size=2)
    tenant_id = f"vector-{uuid4().hex}"
    item_id = "same-item"
    try:
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            await connection.execute(
                "INSERT INTO tenants (tenant_id,display_name) VALUES ($1,'Vector contract tenant')",
                tenant_id,
            )
            await connection.execute(
                """
                INSERT INTO storage_profiles (tenant_id,profile_id,profile_json)
                VALUES ($1,'default','{}'::jsonb)
                """,
                tenant_id,
            )
            await connection.execute(
                """
                INSERT INTO knowledge_items (
                    tenant_id,item_id,profile_id,content_checksum
                ) VALUES ($1,$2,'default',$3)
                """,
                tenant_id,
                item_id,
                "0" * 64,
            )

        store = PgVectorKnowledgeStore(pool)
        await store.upsert(
            tenant_id,
            item_id,
            [0.0] * 1536,
            {"chunk_id": "0", "source": "contract", "profile_id": "default"},
        )

        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            row = await connection.fetchrow(
                """
                SELECT vector_dims(embedding) AS dimensions,metadata_json
                  FROM knowledge_embeddings
                 WHERE tenant_id=$1 AND item_id=$2
                """,
                tenant_id,
                item_id,
            )
            assert row is not None
            assert row["dimensions"] == 1536
            assert json.loads(row["metadata_json"])["source"] == "contract"

            await connection.execute(
                "SELECT set_config('app.tenant_id', $1, true)", f"other-{tenant_id}"
            )
            assert (
                await connection.fetchval(
                    "SELECT count(*) FROM knowledge_embeddings WHERE item_id=$1", item_id
                )
                == 0
            )
    finally:
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            await connection.execute(
                "DELETE FROM knowledge_items WHERE tenant_id=$1 AND item_id=$2",
                tenant_id,
                item_id,
            )
            await connection.execute("DELETE FROM storage_profiles WHERE tenant_id=$1", tenant_id)
            await connection.execute("DELETE FROM tenants WHERE tenant_id=$1", tenant_id)
        await pool.close()


@pytest.mark.asyncio
async def test_postgres_budget_reservation_binds_cost_units_as_bigint() -> None:
    """The budget SQL must not infer its cost parameter as text.

    PostgreSQL rejects the pre-fix statement while preparing it because the
    third parameter appears both in an untyped comparison and in the bigint
    ``cost_units`` target column.  This test exercises the real asyncpg
    protocol so a fake connection cannot hide that parser/type inference
    regression.
    """

    pool = await asyncpg.create_pool(_postgres_dsn(), min_size=1, max_size=2)
    tenant_id = f"budget-{uuid4().hex}"
    try:
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            await connection.execute(
                "INSERT INTO tenants (tenant_id,display_name) VALUES ($1,'Budget contract tenant')",
                tenant_id,
            )

        ledger = PostgresBudgetLedger(pool)
        assert await ledger.reserve(
            tenant_id,
            token_units=4,
            cost_units=2,
            monthly_limit=10,
        )

        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            row = await connection.fetchrow(
                """
                SELECT token_units,cost_units
                  FROM tenant_budget_usage
                 WHERE tenant_id=$1
                """,
                tenant_id,
            )
            assert row is not None
            assert (row["token_units"], row["cost_units"]) == (4, 2)
    finally:
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            await connection.execute(
                "DELETE FROM tenant_budget_usage WHERE tenant_id=$1", tenant_id
            )
            await connection.execute("DELETE FROM tenants WHERE tenant_id=$1", tenant_id)
        await pool.close()


@pytest.mark.asyncio
async def test_postgres_tool_ledger_checks_current_session_fence() -> None:
    """The live SQL contract must fence tool side effects by session lease."""

    pool = await asyncpg.create_pool(_postgres_dsn(), min_size=1, max_size=2)
    suffix = uuid4().hex
    tenant_id = f"tool-fence-{suffix}"
    session_id = f"session-{suffix}"
    inbound_id = uuid4()
    turn_id = uuid4()
    execution_key = f"execution-{suffix}"
    try:
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            await connection.execute(
                "INSERT INTO tenants (tenant_id,display_name) VALUES ($1,'Tool fence tenant')",
                tenant_id,
            )
            await connection.execute(
                """
                INSERT INTO agent_apps (tenant_id,app_id,display_name)
                VALUES ($1,'app','Tool fence app')
                """,
                tenant_id,
            )
            await connection.execute(
                """
                INSERT INTO config_revisions (
                    tenant_id,app_id,version,config_json,checksum,created_by
                ) VALUES ($1,'app',1,'{}'::jsonb,$2,'integration-test')
                """,
                tenant_id,
                "0" * 64,
            )
            await connection.execute(
                """
                INSERT INTO channel_bindings (
                    tenant_id,binding_id,app_id,channel,account_id
                ) VALUES ($1,$2,'app','feishu',$3)
                """,
                tenant_id,
                f"binding-{suffix}",
                f"account-{suffix}",
            )
            await connection.execute(
                """
                INSERT INTO inbound_messages (
                    tenant_id,inbound_id,binding_id,app_id,config_version,
                    channel,account_id,external_message_id,principal_id,session_id,
                    request_id,trace_id,envelope_json,status
                ) VALUES (
                    $1,$2,$3,'app',1,'feishu',$4,$5,'principal',$6,$7,$8,
                    '{}'::jsonb,'processing'
                )
                """,
                tenant_id,
                inbound_id,
                f"binding-{suffix}",
                f"account-{suffix}",
                f"external-{suffix}",
                session_id,
                f"request-{suffix}",
                f"trace-{suffix}",
            )
            await connection.execute(
                """
                INSERT INTO sessions (
                    tenant_id,session_id,app_id,principal_id,
                    lease_owner,lease_epoch,lease_expires_at
                ) VALUES (
                    $1,$2,'app','principal','worker-a',1,
                    clock_timestamp()+interval '5 minutes'
                )
                """,
                tenant_id,
                session_id,
            )
            await connection.execute(
                """
                INSERT INTO session_turns (
                    tenant_id,turn_id,session_id,inbound_id,config_version,
                    status,fencing_token
                ) VALUES ($1,$2,$3,$4,1,'processing',1)
                """,
                tenant_id,
                turn_id,
                session_id,
                inbound_id,
            )

        ledger = PostgresExecutionLedger(pool)
        fresh = await ledger.begin(
            execution_key,
            tenant_id=tenant_id,
            turn_id=str(turn_id),
            tool_name="read",
            risk=ToolRisk.IDEMPOTENT,
            arguments_hash="h",
            owner_id="worker-a",
            fencing_token=1,
        )
        assert fresh.fresh

        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            await connection.execute(
                """
                UPDATE sessions
                   SET lease_owner='worker-b',lease_epoch=2,
                       lease_expires_at=clock_timestamp()+interval '5 minutes'
                 WHERE tenant_id=$1 AND session_id=$2
                """,
                tenant_id,
                session_id,
            )
            await connection.execute(
                "UPDATE session_turns SET fencing_token=2 WHERE tenant_id=$1 AND turn_id=$2",
                tenant_id,
                turn_id,
            )

        with pytest.raises(ToolExecutionConflict):
            await ledger.finish(
                execution_key,
                tenant_id=tenant_id,
                status=ExecutionStatus.SUCCEEDED,
                owner_id="worker-a",
                fencing_token=1,
            )
        takeover = await ledger.begin(
            execution_key,
            tenant_id=tenant_id,
            turn_id=str(turn_id),
            tool_name="read",
            risk=ToolRisk.IDEMPOTENT,
            arguments_hash="h",
            owner_id="worker-b",
            fencing_token=2,
        )
        assert takeover.fresh
        await ledger.finish(
            execution_key,
            tenant_id=tenant_id,
            status=ExecutionStatus.SUCCEEDED,
            owner_id="worker-b",
            fencing_token=2,
        )
    finally:
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            await connection.execute(
                "DELETE FROM tool_executions WHERE tenant_id=$1 AND execution_key=$2",
                tenant_id,
                execution_key,
            )
            await connection.execute(
                "DELETE FROM session_turns WHERE tenant_id=$1 AND turn_id=$2",
                tenant_id,
                turn_id,
            )
            await connection.execute(
                "DELETE FROM inbound_messages WHERE tenant_id=$1 AND inbound_id=$2",
                tenant_id,
                inbound_id,
            )
            await connection.execute(
                "DELETE FROM sessions WHERE tenant_id=$1 AND session_id=$2",
                tenant_id,
                session_id,
            )
            await connection.execute("DELETE FROM channel_bindings WHERE tenant_id=$1", tenant_id)
            await connection.execute("DELETE FROM config_revisions WHERE tenant_id=$1", tenant_id)
            await connection.execute("DELETE FROM agent_apps WHERE tenant_id=$1", tenant_id)
            await connection.execute("DELETE FROM tenants WHERE tenant_id=$1", tenant_id)
        await pool.close()
