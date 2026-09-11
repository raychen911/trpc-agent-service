"""Stage 6B2 Task 1 integration: append-only execution audit on real PostgreSQL.

Proves with Alembic-managed schema and fault injection that:
- migration 0006 applies and reverses cleanly;
- ``execution_audit_events`` schema rejects unknown enums, illegal trace ids,
  broken event/outcome pairs, and any UPDATE/DELETE (append-only trigger);
- the receipt FK is RESTRICTed;
- ``complete``/``fail`` write receipt + message audit + execution audit in ONE
  transaction: a trigger injected to abort the audit insert must leave the
  receipt in ``processing`` with no completed message audit (all-or-nothing);
- ``list_for_receipt``/``list_for_request`` are tenant-isolated, chronologically
  ordered, and limited, and ``list_for_request`` makes receipt-less delivery
  events queryable;
- migration 0006 backfills an explicit DISABLED content_policy over old head
  and history rows without touching versions, preserves already-explicit
  policies, and its downgrade is symmetric; migration objects match the
  ``storage/schema.py`` metadata.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timedelta, timezone

import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

from trpc_service.audit.models import ExecutionAuditEvent
from trpc_service.storage.execution_audit_repository import (
    ExecutionAuditRepositoryDataError,
    ExecutionAuditRepositoryUnavailableError,
    SqlExecutionAuditRepository,
)
from trpc_service.storage.schema import execution_audit_events
from trpc_service.storage.message_repository import (
    MessageReceiptRepositoryDataError,
    MessageReceiptRepositoryUnavailableError,
    ReceiptAction,
    SqlMessageReceiptRepository,
)
from trpc_service.transport.models import WorkerErrorCode, WorkerTask

from .pg_helpers import PostgreSQLContainer, docker_is_available, requires_docker, run_alembic

pytestmark = requires_docker


def _unique_tenant() -> str:
    return f"t{uuid.uuid4().hex[:12]}"


def _make_task(tenant_id: str, message_id: str = "msg-1") -> WorkerTask:
    return WorkerTask(
        protocol_version=1,
        request_id=uuid.uuid4(),
        tenant_id=tenant_id,
        app_id="app_demo",
        config_version=1,
        user_id="user-1",
        channel="web_console",
        session_id="sess-1",
        message_id=message_id,
        message="hello world",
    )


def _event(
    tenant_id: str,
    receipt_id: uuid.UUID,
    task: WorkerTask,
    *,
    event_type: str = "content_decision",
    outcome: str = "allow",
    category: str | None = "none",
    tool_name: str | None = None,
    error_code: str | None = None,
    occurred_at: datetime | None = None,
) -> ExecutionAuditEvent:
    return ExecutionAuditEvent(
        audit_id=uuid.uuid4(),
        tenant_id=tenant_id,
        receipt_id=receipt_id,
        request_id=task.request_id,
        config_version=task.config_version,
        trace_id="a" * 32,
        event_type=event_type,
        outcome=outcome,
        category=category,
        tool_name=tool_name,
        error_code=error_code,
        latency_ms=7,
        occurred_at=occurred_at or datetime.now(timezone.utc),
    )


@pytest.fixture(scope="module")
def migrated_pg():
    """One container for the whole module, Alembic-migrated to head.

    The container object is exposed alongside the URL so fault-injection
    triggers can be installed into *the same database* the repositories use.
    """
    if not docker_is_available():
        pytest.skip("Docker not available")
    pg = PostgreSQLContainer(name_prefix="trpc-6b2-pg")
    pg.start()
    try:
        result = run_alembic(pg.url, "upgrade", "head")
        assert result.returncode == 0, result.stderr
        yield pg
    finally:
        pg.stop()


@pytest.fixture(scope="module")
def migrated_url(migrated_pg):
    return migrated_pg.url


@pytest.fixture
def engine(migrated_url):
    eng = create_async_engine(migrated_url)
    yield eng
    asyncio.run(eng.dispose())


@pytest.fixture
def audit_repo(engine):
    repo = SqlExecutionAuditRepository(engine, owns_engine=False)
    yield repo


@pytest.fixture
def receipt_repo(engine):
    return SqlMessageReceiptRepository(engine, owns_engine=False)


async def _scalar(engine, sql: str, **params):
    async with engine.connect() as conn:
        return (await conn.execute(sa.text(sql), params)).scalar()


async def _insert_raw_event(engine, **overrides):
    values = {
        "audit_id": uuid.uuid4(),
        "tenant_id": "tenant_it",
        "receipt_id": None,
        "request_id": uuid.uuid4(),
        "config_version": 1,
        "trace_id": None,
        "event_type": "agent_result",
        "outcome": "success",
        "category": None,
        "tool_name": None,
        "error_code": None,
        "latency_ms": None,
        "occurred_at": datetime.now(timezone.utc),
    }
    values.update(overrides)
    async with engine.begin() as conn:
        await conn.execute(execution_audit_events.insert().values(values))


class TestMigrationShape:

    def test_table_and_columns_exist(self, migrated_url):

        async def _check():
            engine = create_async_engine(migrated_url)
            try:
                cols = await _scalar(
                    engine,
                    "SELECT string_agg(column_name, ',' ORDER BY column_name) "
                    "FROM information_schema.columns WHERE table_name = 'execution_audit_events'",
                )
                trig = await _scalar(
                    engine,
                    "SELECT COUNT(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid "
                    "WHERE c.relname = 'execution_audit_events' AND NOT t.tgisinternal",
                )
            finally:
                await engine.dispose()
            return cols, trig

        cols, trig = asyncio.run(_check())
        assert set(cols.split(",")) == {
            "audit_id",
            "category",
            "config_version",
            "error_code",
            "event_type",
            "latency_ms",
            "occurred_at",
            "outcome",
            "receipt_id",
            "request_id",
            "tenant_id",
            "tool_name",
            "trace_id",
        }
        assert int(trig) >= 1  # append-only trigger installed by migration

    def test_migration_objects_match_schema_metadata(self, migrated_url):
        """Every constraint/index 0006 creates exists in the live database
        with the same name as ``storage/schema.py`` declares — migration and
        metadata may never drift."""
        from trpc_service.storage.schema import execution_audit_events as ea
        from trpc_service.storage.schema import message_receipts as mr

        expected = set()
        for constraint in ea.constraints:
            name = getattr(constraint, "name", None)
            if name:
                expected.add(name)
        for index in ea.indexes:
            expected.add(index.name)
        expected.add(
            next(c.name for c in mr.constraints if getattr(c, "name", None) == "message_receipts_execution_identity"))

        async def _live():
            engine = create_async_engine(migrated_url)
            try:
                async with engine.connect() as conn:
                    cons = (await conn.execute(
                        sa.text("SELECT conname FROM pg_constraint WHERE contype IN ('c','f','u')"
                                " AND (conname LIKE 'execution_audit%'"
                                " OR conname = 'message_receipts_execution_identity')"), )).fetchall()
                    idxs = (await conn.execute(
                        sa.text("SELECT indexname FROM pg_indexes WHERE indexname LIKE 'execution_audit%'"
                                " AND indexname <> 'execution_audit_events_pkey'"), )).fetchall()
            finally:
                await engine.dispose()
            return {r[0] for r in cons} | {r[0] for r in idxs}

        assert asyncio.run(_live()) == expected

    def test_upgrade_backfills_legacy_policy_without_changing_versions(self, migrated_url, migrated_pg):
        down = run_alembic(migrated_url, "downgrade", "0005_add_tool_approvals")
        assert down.returncode == 0, down.stderr
        tenant = _unique_tenant()
        legacy_governance = '{"allowed_channels":["web_console"],"allowed_user_ids":[],"tool_decisions":{}}'
        inserted = migrated_pg.run_sql(
            "INSERT INTO tenant_configs "
            "(tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,governance) VALUES "
            f"('{tenant}',true,7,'app_demo','legacy','default','[]'::jsonb,'{legacy_governance}'::jsonb);"
            "INSERT INTO tenant_config_versions "
            "(tenant_id,version,enabled,app_id,instruction,model_profile,allowed_tools,governance) VALUES "
            f"('{tenant}',7,true,'app_demo','legacy','default','[]'::jsonb,'{legacy_governance}'::jsonb);")
        assert inserted.success, inserted.output
        up = run_alembic(migrated_url, "upgrade", "head")
        assert up.returncode == 0, up.stderr
        backfilled = migrated_pg.run_sql("SELECT h.governance->'content_policy'->>'enabled',"
                                         "v.governance->'content_policy'->>'enabled',h.version,v.version "
                                         "FROM tenant_configs h JOIN tenant_config_versions v USING (tenant_id) "
                                         f"WHERE h.tenant_id = '{tenant}'")
        assert backfilled.success, backfilled.output
        # head and history both gained an explicit *disabled* policy while the
        # version numbers stayed byte-identical (7/7) — no new version was cut
        assert backfilled.stdout == "false|false|7|7"

    def test_upgrade_preserves_explicit_policy_and_downgrade_is_symmetric(self, migrated_url, migrated_pg):
        """Rows that already carry an explicit content_policy keep it through
        the 0006 backfill, and the downgrade strips the key and drops every
        0006 database object again."""
        down = run_alembic(migrated_url, "downgrade", "0005_add_tool_approvals")
        assert down.returncode == 0, down.stderr
        legacy = _unique_tenant()
        explicit = _unique_tenant()
        legacy_governance = '{"allowed_channels":["web_console"],"allowed_user_ids":[],"tool_decisions":{}}'
        explicit_governance = ('{"allowed_channels":["web_console"],"allowed_user_ids":[],'
                               '"tool_decisions":{},"content_policy":{"enabled":true,'
                               '"input_action":"allow","output_action":"block"}}')
        inserted = migrated_pg.run_sql(
            "INSERT INTO tenant_configs "
            "(tenant_id,enabled,version,app_id,instruction,model_profile,allowed_tools,governance) VALUES "
            f"('{legacy}',true,4,'app_demo','legacy','default','[]'::jsonb,'{legacy_governance}'::jsonb),"
            f"('{explicit}',true,2,'app_demo','explicit','default','[]'::jsonb,'{explicit_governance}'::jsonb);"
            "INSERT INTO tenant_config_versions "
            "(tenant_id,version,enabled,app_id,instruction,model_profile,allowed_tools,governance) VALUES "
            f"('{legacy}',4,true,'app_demo','legacy','default','[]'::jsonb,'{legacy_governance}'::jsonb),"
            f"('{legacy}',3,true,'app_demo','older','default','[]'::jsonb,'{legacy_governance}'::jsonb),"
            f"('{explicit}',2,true,'app_demo','explicit','default','[]'::jsonb,'{explicit_governance}'::jsonb);")
        assert inserted.success, inserted.output

        up = run_alembic(migrated_url, "upgrade", "head")
        assert up.returncode == 0, up.stderr
        after = migrated_pg.run_sql("SELECT tenant_id, version, governance->'content_policy'->>'enabled',"
                                    "governance->'content_policy'->>'input_action', governance ? 'content_policy' "
                                    "FROM tenant_config_versions WHERE tenant_id IN "
                                    f"('{legacy}','{explicit}') ORDER BY tenant_id, version")
        assert after.success, after.output
        rows = {(r[0], r[1]): r[2:] for r in (line.split("|") for line in after.stdout.strip().splitlines())}
        assert rows[(legacy, "3")] == ["false", "block", "t"]  # history rows: disabled backfill
        assert rows[(legacy, "4")] == ["false", "block", "t"]
        assert rows[(explicit, "2")] == ["true", "allow", "t"]  # explicit policy survived untouched
        head = migrated_pg.run_sql("SELECT tenant_id, governance->'content_policy'->>'enabled' "
                                   "FROM tenant_configs WHERE tenant_id IN "
                                   f"('{legacy}','{explicit}') ORDER BY tenant_id, version")
        assert head.success, head.output
        head_rows = dict(line.split("|") for line in head.stdout.strip().splitlines())
        assert head_rows[legacy] == "false" and head_rows[explicit] == "true"

        down2 = run_alembic(migrated_url, "downgrade", "0005_add_tool_approvals")
        assert down2.returncode == 0, down2.stderr
        stripped = migrated_pg.run_sql(
            "SELECT (SELECT COUNT(*) FROM tenant_configs WHERE governance ? 'content_policy') +"
            "(SELECT COUNT(*) FROM tenant_config_versions WHERE governance ? 'content_policy')")
        assert stripped.success and int(stripped.stdout) == 0
        gone = migrated_pg.run_sql(
            "SELECT (SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'execution_audit_events') ||"
            "'|' || (SELECT COUNT(*) FROM pg_constraint WHERE conname = 'message_receipts_execution_identity') ||"
            "'|' || (SELECT COUNT(*) FROM pg_constraint"
            " WHERE conname = 'execution_audit_events_receipt_identity_fk')")
        assert gone.success and gone.stdout == "0|0|0", gone.stdout

        # leave the shared database at head for the remaining tests
        up2 = run_alembic(migrated_url, "upgrade", "head")
        assert up2.returncode == 0, up2.stderr


class TestSchemaConstraints:

    @staticmethod
    def _insert_sql() -> str:
        return ("INSERT INTO execution_audit_events (audit_id, tenant_id, receipt_id, request_id, "
                "config_version, trace_id, event_type, outcome, category, tool_name, error_code, latency_ms, "
                "occurred_at) VALUES (:audit_id, :tenant_id, NULL, :request_id, :config_version, :trace_id, "
                ":event_type, :outcome, :category, NULL, NULL, NULL, now())")

    @pytest.mark.parametrize(
        "field, bad_value",
        [
            ("event_type", "prompt"),
            ("outcome", "ok"),
            ("trace_id", "ABCDEF0123456789ABCDEF0123456789"),
            ("category", "phone_number"),
            ("tenant_id", "BAD TENANT"),
            ("config_version", 0),
        ],
    )
    def test_illegal_fixed_values_rejected(self, engine, field, bad_value):
        params = {
            "audit_id": uuid.uuid4(),
            "tenant_id": "tenant_it",
            "request_id": uuid.uuid4(),
            "config_version": 1,
            "trace_id": "a" * 32,
            "event_type": "content_decision",
            "outcome": "allow",
            "category": "none",
        }
        params[field] = bad_value

        async def _attempt():
            async with engine.begin() as conn:
                await conn.execute(sa.text(self._insert_sql()), params)

        with pytest.raises(DBAPIError):
            asyncio.run(_attempt())

    def test_broken_pairing_rejected_by_check(self, engine):
        """event_type/outcome pairing must be impossible at DB level too."""

        async def _attempt():
            async with engine.begin() as conn:
                await conn.execute(
                    sa.text(self._insert_sql()),
                    {
                        "audit_id": uuid.uuid4(),
                        "tenant_id": "tenant_it",
                        "request_id": uuid.uuid4(),
                        "config_version": 1,
                        "trace_id": "a" * 32,
                        "event_type": "content_decision",
                        "outcome": "delivered",
                        "category": "none",
                    },
                )

        with pytest.raises(DBAPIError):
            asyncio.run(_attempt())

    @pytest.mark.parametrize(
        "event_type,outcome,error_code",
        [
            ("agent_result", "error", None),
            ("delivery_result", "failed", None),
            ("agent_result", "error", "not_a_worker_error"),
            ("agent_result", "success", "model_runtime"),
        ],
    )
    def test_error_code_contract_is_enforced_by_database(self, engine, event_type, outcome, error_code):
        with pytest.raises(DBAPIError):
            asyncio.run(_insert_raw_event(
                engine,
                event_type=event_type,
                outcome=outcome,
                error_code=error_code,
            ))

    def test_tool_name_must_be_normalized_by_database(self, engine):
        with pytest.raises(DBAPIError):
            asyncio.run(
                _insert_raw_event(
                    engine,
                    event_type="tool_decision",
                    outcome="allow",
                    tool_name=" get_current_time ",
                ))

    @pytest.mark.parametrize(
        "event_type,outcome,category,tool_name",
        [
            ("content_decision", "allow", "none", None),
            ("agent_result", "success", None, None),
            ("tool_decision", "allow", None, "get_current_time"),
        ],
    )
    def test_only_delivery_events_may_have_null_receipt_in_database(
        self,
        engine,
        event_type,
        outcome,
        category,
        tool_name,
    ):
        with pytest.raises(DBAPIError):
            asyncio.run(
                _insert_raw_event(
                    engine,
                    receipt_id=None,
                    event_type=event_type,
                    outcome=outcome,
                    category=category,
                    tool_name=tool_name,
                ))


class TestAppendOnly:

    def test_update_and_delete_are_refused(self, engine, audit_repo, receipt_repo):
        tenant = _unique_tenant()
        task = _make_task(tenant)

        async def _scenario():
            claim = await receipt_repo.claim(task, task.message)
            assert claim.action == ReceiptAction.EXECUTE
            event = _event(tenant, claim.receipt_id, task)
            await audit_repo.append(event)
            with pytest.raises(sa.exc.DBAPIError) as upd:
                async with engine.begin() as conn:
                    await conn.execute(
                        sa.text("UPDATE execution_audit_events SET outcome = 'success' WHERE audit_id = :id"),
                        {"id": event.audit_id},
                    )
            assert upd.value.__class__ is not ExecutionAuditRepositoryDataError
            with pytest.raises(sa.exc.DBAPIError):
                async with engine.begin() as conn:
                    await conn.execute(
                        sa.text("DELETE FROM execution_audit_events WHERE audit_id = :id"),
                        {"id": event.audit_id},
                    )
            # the row survived both refused mutations
            events = await audit_repo.list_for_receipt(tenant, claim.receipt_id, 10)
            assert [e.audit_id for e in events] == [event.audit_id]

        asyncio.run(_scenario())

    def test_receipt_fk_is_restrict(self, engine, audit_repo, receipt_repo):
        tenant = _unique_tenant()
        task = _make_task(tenant)

        async def _scenario():
            claim = await receipt_repo.claim(task, task.message)
            await audit_repo.append(_event(tenant, claim.receipt_id, task))
            with pytest.raises(IntegrityError):
                async with engine.begin() as conn:
                    await conn.execute(
                        sa.text("DELETE FROM message_receipts WHERE receipt_id = :id"),
                        {"id": claim.receipt_id},
                    )

        asyncio.run(_scenario())

    def test_append_unknown_receipt_maps_to_data_error(self, engine, audit_repo):
        task = _make_task(_unique_tenant())
        event = _event(task.tenant_id, uuid.uuid4(), task)

        async def _attempt():
            await audit_repo.append(event)

        with pytest.raises(ExecutionAuditRepositoryDataError):
            asyncio.run(_attempt())

    @pytest.mark.parametrize("mismatch", ["tenant_id", "request_id", "config_version"])
    def test_database_rejects_receipt_identity_mismatch(self, mismatch, engine, receipt_repo):
        tenant = _unique_tenant()
        task = _make_task(tenant)

        async def _attempt():
            claim = await receipt_repo.claim(task, task.message)
            overrides = {
                "receipt_id": claim.receipt_id,
                "tenant_id": task.tenant_id,
                "request_id": task.request_id,
                "config_version": task.config_version,
            }
            overrides[mismatch] = {
                "tenant_id": _unique_tenant(),
                "request_id": uuid.uuid4(),
                "config_version": task.config_version + 1,
            }[mismatch]
            await _insert_raw_event(engine, **overrides)

        with pytest.raises(DBAPIError):
            asyncio.run(_attempt())


class TestTerminalAtomicity:

    def test_complete_writes_receipt_message_audit_and_execution_audit(self, engine, receipt_repo, audit_repo):
        tenant = _unique_tenant()
        task = _make_task(tenant)
        t0 = datetime.now(timezone.utc)

        async def _scenario():
            claim = await receipt_repo.claim(task, task.message)
            events = (
                _event(tenant, claim.receipt_id, task, occurred_at=t0),
                _event(
                    tenant,
                    claim.receipt_id,
                    task,
                    event_type="agent_result",
                    outcome="success",
                    category=None,
                    occurred_at=t0 + timedelta(seconds=1),
                ),
            )
            await receipt_repo.complete(claim.receipt_id, "the answer", 42, execution_events=events)
            state = await _scalar(engine,
                                  "SELECT state FROM message_receipts WHERE receipt_id = :r",
                                  r=claim.receipt_id)
            msg_audits = await _scalar(
                engine,
                "SELECT COUNT(*) FROM message_audit_events WHERE receipt_id = :r AND event_type = 'completed'",
                r=claim.receipt_id,
            )
            rows = await audit_repo.list_for_receipt(tenant, claim.receipt_id, 10)
            return state, msg_audits, rows

        state, msg_audits, rows = asyncio.run(_scenario())
        assert state == "completed"
        assert int(msg_audits) == 1
        assert [e.event_type for e in rows] == ["content_decision", "agent_result"]  # chronological order

    def test_fail_carries_execution_events_atomically(self, engine, receipt_repo, audit_repo):
        tenant = _unique_tenant()
        task = _make_task(tenant)

        async def _scenario():
            claim = await receipt_repo.claim(task, task.message)
            event = _event(
                tenant,
                claim.receipt_id,
                task,
                outcome="blocked",
                category="credential",
            )
            await receipt_repo.fail(
                claim.receipt_id,
                WorkerErrorCode.CONTENT_INPUT_BLOCKED,
                5,
                execution_events=(event, ),
            )
            code = await _scalar(engine,
                                 "SELECT error_code FROM message_receipts WHERE receipt_id = :r",
                                 r=claim.receipt_id)
            rows = await audit_repo.list_for_receipt(tenant, claim.receipt_id, 10)
            return code, rows

        code, rows = asyncio.run(_scenario())
        assert code == "content_input_blocked"
        assert len(rows) == 1
        assert rows[0].outcome == "blocked"
        assert rows[0].category == "credential"

    def test_required_audit_failure_aborts_terminal_state(self, migrated_pg, engine, receipt_repo):
        """Fault injection: an INSERT-time trigger abort on execution audit must
        roll the WHOLE complete() transaction back — the receipt may not be
        observable as completed without its required audit."""
        tenant = _unique_tenant()
        task = _make_task(tenant)

        injected = migrated_pg.run_sql("""
CREATE FUNCTION trpc_it_abort_audit() RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'injected audit failure';
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trpc_it_abort_audit BEFORE INSERT ON execution_audit_events
FOR EACH ROW EXECUTE FUNCTION trpc_it_abort_audit();
""")
        assert injected.success, injected.output
        try:

            async def _scenario():
                claim = await receipt_repo.claim(task, task.message)
                event = _event(tenant, claim.receipt_id, task)
                with pytest.raises(MessageReceiptRepositoryUnavailableError):
                    await receipt_repo.complete(claim.receipt_id, "answer", 1, execution_events=(event, ))
                state = await _scalar(engine,
                                      "SELECT state FROM message_receipts WHERE receipt_id = :r",
                                      r=claim.receipt_id)
                completed_audits = await _scalar(
                    engine,
                    "SELECT COUNT(*) FROM message_audit_events WHERE receipt_id = :r AND event_type = 'completed'",
                    r=claim.receipt_id,
                )
                return state, int(completed_audits)

            state, completed_audits = asyncio.run(_scenario())
            assert state == "processing"  # fail-closed: no fake terminal success
            assert completed_audits == 0
        finally:
            migrated_pg.run_sql("DROP TRIGGER trpc_it_abort_audit ON execution_audit_events;")
            migrated_pg.run_sql("DROP FUNCTION trpc_it_abort_audit();")

    @pytest.mark.parametrize("mismatch", ["tenant_id", "request_id", "config_version", "receipt_id"])
    def test_terminal_events_must_match_receipt_identity(self, mismatch, engine, receipt_repo):
        tenant = _unique_tenant()
        task = _make_task(tenant)

        async def _scenario():
            claim = await receipt_repo.claim(task, task.message)
            overrides = {
                "tenant_id": _unique_tenant(),
                "request_id": uuid.uuid4(),
                "config_version": task.config_version + 1,
                "receipt_id": uuid.uuid4(),
            }
            event = _event(tenant, claim.receipt_id, task).model_copy(update={mismatch: overrides[mismatch]})
            with pytest.raises(MessageReceiptRepositoryDataError):
                await receipt_repo.complete(claim.receipt_id, "answer", 1, execution_events=(event, ))
            return await _scalar(engine, "SELECT state FROM message_receipts WHERE receipt_id = :r", r=claim.receipt_id)

        assert asyncio.run(_scenario()) == "processing"

    def test_terminal_event_may_not_have_null_receipt(self, engine, receipt_repo):
        tenant = _unique_tenant()
        task = _make_task(tenant)

        async def _scenario():
            claim = await receipt_repo.claim(task, task.message)
            event = _event(tenant, claim.receipt_id, task).model_copy(update={"receipt_id": None})
            with pytest.raises(MessageReceiptRepositoryDataError):
                await receipt_repo.fail(
                    claim.receipt_id,
                    WorkerErrorCode.MODEL_RUNTIME,
                    1,
                    execution_events=(event, ),
                )
            return await _scalar(engine, "SELECT state FROM message_receipts WHERE receipt_id = :r", r=claim.receipt_id)

        assert asyncio.run(_scenario()) == "processing"

    def test_execution_events_must_be_immutable_tuple(self, engine, receipt_repo):
        tenant = _unique_tenant()
        task = _make_task(tenant)

        async def _scenario():
            claim = await receipt_repo.claim(task, task.message)
            event = _event(tenant, claim.receipt_id, task)
            with pytest.raises(MessageReceiptRepositoryDataError):
                await receipt_repo.complete(
                    claim.receipt_id,
                    "answer",
                    1,
                    execution_events=[event],  # type: ignore[arg-type]
                )
            return await _scalar(engine, "SELECT state FROM message_receipts WHERE receipt_id = :r", r=claim.receipt_id)

        assert asyncio.run(_scenario()) == "processing"

    def test_complete_without_events_keeps_stage4c_semantics(self, engine, receipt_repo):
        tenant = _unique_tenant()
        task = _make_task(tenant)

        async def _scenario():
            claim = await receipt_repo.claim(task, task.message)
            await receipt_repo.complete(claim.receipt_id, "answer", 3)
            return await _scalar(engine, "SELECT state FROM message_receipts WHERE receipt_id = :r", r=claim.receipt_id)

        assert asyncio.run(_scenario()) == "completed"


class TestListing:

    def test_tenant_isolation_order_and_limit(self, engine, audit_repo, receipt_repo):
        tenant_a = _unique_tenant()
        tenant_b = _unique_tenant()
        task = _make_task(tenant_a)
        t0 = datetime.now(timezone.utc)

        async def _scenario():
            claim = await receipt_repo.claim(task, task.message)
            for offset in range(4):
                await audit_repo.append(
                    _event(
                        tenant_a,
                        claim.receipt_id,
                        task,
                        event_type="agent_result",
                        outcome="success",
                        category=None,
                        occurred_at=t0 + timedelta(seconds=offset),
                    ))
            # same receipt id cannot exist for another tenant (FK), so isolation
            # is proven by querying with the wrong tenant id
            mine = await audit_repo.list_for_receipt(tenant_a, claim.receipt_id, 10)
            theirs = await audit_repo.list_for_receipt(tenant_b, claim.receipt_id, 10)
            capped = await audit_repo.list_for_receipt(tenant_a, claim.receipt_id, 2)
            return mine, theirs, capped

        mine, theirs, capped = asyncio.run(_scenario())
        assert len(mine) == 4
        assert theirs == ()
        assert len(capped) == 2
        assert [e.occurred_at for e in mine] == sorted(e.occurred_at for e in mine)

    def test_list_for_request_covers_null_receipt_delivery_events(self, engine, audit_repo, receipt_repo):
        """Events written after the receipt went terminal (receipt_id NULL)
        must be enumerable by (tenant_id, request_id) — no write-only audit."""
        tenant_a = _unique_tenant()
        tenant_b = _unique_tenant()
        task = _make_task(tenant_a)
        t0 = datetime.now(timezone.utc)

        async def _scenario():
            claim = await receipt_repo.claim(task, task.message)
            await receipt_repo.complete(
                claim.receipt_id,
                "the answer",
                42,
                execution_events=(
                    _event(tenant_a, claim.receipt_id, task, occurred_at=t0),
                    _event(
                        tenant_a,
                        claim.receipt_id,
                        task,
                        event_type="agent_result",
                        outcome="success",
                        category=None,
                        occurred_at=t0 + timedelta(seconds=1),
                    ),
                ),
            )
            # delivery happens after the terminal transition: no receipt link
            await audit_repo.append(
                _event(
                    tenant_a,
                    None,
                    task,
                    event_type="delivery_result",
                    outcome="delivered",
                    category=None,
                    occurred_at=t0 + timedelta(seconds=2),
                ))
            mine = await audit_repo.list_for_request(tenant_a, task.request_id, 10)
            theirs = await audit_repo.list_for_request(tenant_b, task.request_id, 10)
            capped = await audit_repo.list_for_request(tenant_a, task.request_id, 2)
            return mine, theirs, capped

        mine, theirs, capped = asyncio.run(_scenario())
        assert [e.event_type for e in mine] == ["content_decision", "agent_result", "delivery_result"]
        assert [e.occurred_at for e in mine] == sorted(e.occurred_at for e in mine)
        assert mine[-1].receipt_id is None  # the delivery event is queryable
        assert {e.receipt_id for e in mine[:2]} != {None}
        assert theirs == ()  # tenant-isolated even though request_id is known
        assert [e.event_type for e in capped] == ["content_decision", "agent_result"]

    def test_check_ready_and_close_lifecycle(self, migrated_url):

        async def _scenario():
            engine = create_async_engine(migrated_url)
            repo = SqlExecutionAuditRepository(engine)
            await repo.check_ready()
            await repo.close()
            await repo.close()
            with pytest.raises(ExecutionAuditRepositoryUnavailableError):
                await repo.check_ready()

        asyncio.run(_scenario())
