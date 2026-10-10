from __future__ import annotations

import ast
import importlib.util
import re
from pathlib import Path

ROOT = Path(__file__).parents[2]
MIGRATION = ROOT / "migrations" / "versions" / "0018_performance_fixture_cleanup.py"
FIXTURE = ROOT / "scripts" / "performance_fixture.py"


def _source() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def _sql() -> str:
    return re.sub(r"\s+", " ", _source()).lower()


def _fixture_cleanup_tables() -> list[str]:
    tree = ast.parse(FIXTURE.read_text(encoding="utf-8"))
    for node in tree.body:
        if not isinstance(node, ast.AnnAssign):
            continue
        if not isinstance(node.target, ast.Name) or node.target.id != "CLEANUP_TABLES":
            continue
        assert isinstance(node.value, (ast.List, ast.Tuple))
        return [
            element.value
            for element in node.value.elts
            if isinstance(element, ast.Constant) and isinstance(element.value, str)
        ]
    raise AssertionError("CLEANUP_TABLES literal was not found")


def test_cleanup_migration_follows_current_revision_chain() -> None:
    source = _source()
    ast.parse(source)

    assert 'revision = "0018_performance_fixture_cleanup"' in source
    assert 'down_revision = "0017_im_acceptance_evidence"' in source


def test_cleanup_function_is_bounded_and_ownership_proven() -> None:
    sql = _sql()

    assert "returns jsonb" in sql
    assert "security definer" in sql
    assert "set search_path = pg_catalog, public" in sql
    assert "owner to trpc_migration" in sql
    assert "^perf-[0-9a-f]{32}$" in sql
    assert "perf-fixture-" in sql
    assert "^[0-9a-f]{64}$" in sql
    assert "synthetic performance fixture" in sql
    assert "performance-fixture" in sql
    assert "tenant_created" in sql
    assert "audit.idempotency_key" in sql
    assert "audit.trace_id" in sql
    assert "pg_catalog.pg_advisory_xact_lock" in sql
    assert "pg_catalog.hashtextextended" in sql
    assert "barrier.mode = 'active'" in sql
    assert "execute format" not in sql
    assert "execute immediate" not in sql


def test_cleanup_function_uses_reviewed_child_before_parent_table_order() -> None:
    deleted_tables = re.findall(r"delete from public\.([a-z_]+)", _sql())
    expected = _fixture_cleanup_tables()
    if "migration_write_barriers" not in expected:
        expected.insert(expected.index("migration_scope_manifests"), "migration_write_barriers")

    assert deleted_tables == expected
    assert deleted_tables.index("migration_write_barriers") < deleted_tables.index(
        "migration_scope_manifests"
    )
    assert deleted_tables[-1] == "tenants"
    assert _sql().count("get diagnostics deleted_count = row_count") == len(expected)
    assert _sql().count("pg_catalog.jsonb_build_object(") == len(expected)


def test_cleanup_function_exposes_only_runtime_execute_privilege() -> None:
    sql = _sql()
    signature = "public.cleanup_performance_fixture(text, text, text)"

    assert f"revoke all on function {signature} from public, trpc_worker, trpc_metrics" in sql
    assert f"grant execute on function {signature} to trpc_runtime" in sql
    assert "grant delete" not in sql
    assert "grant all" not in sql
    assert (
        f"revoke all on function {signature} from public, trpc_runtime, trpc_worker, trpc_metrics"
    ) in sql
    assert f"drop function if exists {signature}" in sql


def test_cleanup_migration_module_imports_without_running_ddl() -> None:
    spec = importlib.util.spec_from_file_location(
        "performance_fixture_cleanup_migration", MIGRATION
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.revision == "0018_performance_fixture_cleanup"
    assert module.down_revision == "0017_im_acceptance_evidence"
