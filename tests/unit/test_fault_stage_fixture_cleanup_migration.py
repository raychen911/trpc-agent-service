from __future__ import annotations

import ast
import importlib.util
import re
from io import StringIO
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import create_engine, event, text

ROOT = Path(__file__).parents[2]
MIGRATION = ROOT / "migrations" / "versions" / "0020_fault_stage_fixture_cleanup.py"
PERFORMANCE_MIGRATION = ROOT / "migrations" / "versions" / "0018_performance_fixture_cleanup.py"


def _source() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def _sql() -> str:
    return re.sub(r"\s+", " ", _source()).lower()


def _upgrade_sql() -> str:
    tree = ast.parse(_source())
    upgrade = next(
        node for node in tree.body if isinstance(node, ast.FunctionDef) and node.name == "upgrade"
    )
    execute = next(
        node
        for node in ast.walk(upgrade)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "op"
        and node.func.attr == "execute"
    )
    statement = ast.literal_eval(execute.args[0])
    assert isinstance(statement, str)
    return statement


def test_fault_cleanup_migration_follows_current_revision_chain() -> None:
    source = _source()
    ast.parse(source)

    assert 'revision = "0020_fault_stage_fixture_cleanup"' in source
    assert 'down_revision = "0019_migration_protected_target_counts"' in source


def test_fault_cleanup_function_is_bounded_and_ownership_proven() -> None:
    sql = _sql()

    assert "returns jsonb" in sql
    assert "security definer" in sql
    assert "set search_path = pg_catalog, public" in sql
    assert "owner to trpc_migration" in sql
    assert "pg_catalog.current_setting('app.tenant_id', true)" in sql
    assert "^case-[0-9a-f]{32}$" in sql
    assert "pg_catalog.length(p_run_id) not between 1 and 128" in sql
    assert "public.digest(" in sql
    assert "tenant.display_name = 'fault-stage-acceptance'" in sql
    assert "audit.user_id = 'fault-stage-acceptance'" in sql
    assert "audit.decision = 'tenant_created'" in sql
    assert "audit.idempotency_key = p_case_id || '\\:tenant'" in sql
    assert "audit.trace_id = 'admin:' || p_case_id || '\\:tenant'" in sql
    assert "join public.admin_idempotency as idempotency" in sql
    assert "idempotency.operation = 'create_tenant'" in sql
    assert "idempotency.request_hash" in sql
    assert "idempotency.response_status = 201" in sql
    assert "idempotency.response_json ->> 'tenant_id' = p_tenant_id" in sql
    assert "pg_catalog.pg_advisory_xact_lock" in sql
    assert "pg_catalog.hashtextextended" in sql
    assert "barrier.mode = 'active'" in sql
    assert "execute format" not in sql
    assert "execute immediate" not in sql


def test_fault_cleanup_upgrade_sql_has_no_sqlalchemy_bind_parameters() -> None:
    assert text(_upgrade_sql()).compile().params == {}


def _load_migration_module() -> object:
    spec = importlib.util.spec_from_file_location("fault_stage_fixture_cleanup", MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_fault_cleanup_upgrade_renders_literal_tenant_suffix_offline() -> None:
    output = StringIO()
    context = MigrationContext.configure(
        url="postgresql://",
        opts={"as_sql": True, "output_buffer": output},
    )
    module = _load_migration_module()
    module.op = Operations(context)  # type: ignore[attr-defined]

    module.upgrade()  # type: ignore[attr-defined]

    rendered = output.getvalue()
    assert "%(tenant)s" not in rendered
    assert rendered.count("':tenant'") == 3


def test_fault_cleanup_upgrade_reaches_online_driver_without_bind_parameters() -> None:
    class ExecutionObserved(RuntimeError):
        pass

    captured: list[tuple[str, object]] = []
    engine = create_engine("sqlite://")

    def observe(
        _connection: object,
        _cursor: object,
        statement: str,
        parameters: object,
        _context: object,
        _executemany: bool,
    ) -> None:
        captured.append((statement, parameters))
        raise ExecutionObserved

    event.listen(engine, "before_cursor_execute", observe)
    with engine.connect() as connection:
        module = _load_migration_module()
        module.op = Operations(MigrationContext.configure(connection))  # type: ignore[attr-defined]
        with pytest.raises(ExecutionObserved):
            module.upgrade()  # type: ignore[attr-defined]

    assert len(captured) == 1
    statement, parameters = captured[0]
    assert "%(tenant)s" not in statement
    assert statement.count("':tenant'") == 3
    assert parameters in ({}, ())


def test_fault_cleanup_function_uses_reviewed_child_before_parent_order() -> None:
    deleted_tables = re.findall(r"delete from public\.([a-z_]+)", _sql())
    performance_sql = re.sub(r"\s+", " ", PERFORMANCE_MIGRATION.read_text(encoding="utf-8")).lower()
    reviewed_tables = re.findall(r"delete from public\.([a-z_]+)", performance_sql)

    assert deleted_tables == reviewed_tables
    assert deleted_tables.index("im_acceptance_evidence_events") < deleted_tables.index(
        "channel_bindings"
    )
    assert deleted_tables.index("wecom_connection_state") < deleted_tables.index("channel_bindings")
    assert deleted_tables.index("migration_write_barriers") < deleted_tables.index(
        "migration_scope_manifests"
    )
    assert deleted_tables[-1] == "tenants"
    assert _sql().count("get diagnostics deleted_count = row_count") == len(deleted_tables)
    assert _sql().count("pg_catalog.jsonb_build_object(") == len(deleted_tables)


def test_fault_cleanup_function_exposes_only_runtime_execute_privilege() -> None:
    sql = _sql()
    signature = "public.cleanup_fault_stage_fixture(text, text, text)"

    assert f"revoke all on function {signature} from public, trpc_worker, trpc_metrics" in sql
    assert f"grant execute on function {signature} to trpc_runtime" in sql
    assert "grant delete" not in sql
    assert "grant all" not in sql
    assert (
        f"revoke all on function {signature} from public, trpc_runtime, trpc_worker, trpc_metrics"
    ) in sql
    assert f"drop function if exists {signature}" in sql


def test_fault_cleanup_migration_module_imports_without_running_ddl() -> None:
    module = _load_migration_module()

    assert module.revision == "0020_fault_stage_fixture_cleanup"
    assert module.down_revision == "0019_migration_protected_target_counts"
