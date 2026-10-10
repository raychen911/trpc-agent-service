from __future__ import annotations

import ast
import importlib.util
import re
from pathlib import Path

ROOT = Path(__file__).parents[2]
MIGRATION = ROOT / "migrations" / "versions" / "0019_migration_protected_target_counts.py"
GUARD = ROOT / "trpc_service" / "storage" / "migration.py"


def _source() -> str:
    return MIGRATION.read_text(encoding="utf-8")


def _sql() -> str:
    return re.sub(r"\s+", " ", _source()).lower()


def test_protected_count_migration_follows_current_revision_chain() -> None:
    source = _source()
    ast.parse(source)

    assert 'revision = "0019_migration_protected_target_counts"' in source
    assert 'down_revision = "0018_performance_fixture_cleanup"' in source


def test_protected_count_function_is_current_tenant_only() -> None:
    sql = _sql()

    assert "returns table(table_name text, row_count bigint)" in sql
    assert "security definer" in sql
    assert "set search_path = pg_catalog, public" in sql
    assert "owner to trpc_migration" in sql
    assert "pg_catalog.current_setting('app.tenant_id', true)" in sql
    assert "p_tenant_id is distinct from" in sql
    assert "pg_catalog.length(p_tenant_id) not between 1 and 256" in sql
    assert "using errcode = '42501'" in sql
    assert "execute format" not in sql
    assert "execute immediate" not in sql


def test_protected_count_function_exposes_only_reviewed_counts_and_privilege() -> None:
    sql = _sql()
    signature = "public.migration_protected_target_counts(text)"

    counted_tables = re.findall(r"from public\.([a-z_]+)", sql)
    assert counted_tables == [
        "wecom_connection_state",
        "im_acceptance_evidence_events",
    ]
    assert (
        f"revoke all on function {signature} from public, trpc_runtime, trpc_worker, trpc_metrics"
    ) in sql
    assert f"grant execute on function {signature} to trpc_runtime" in sql
    assert "grant select" not in sql
    assert "grant all" not in sql
    assert f"drop function if exists {signature}" in sql


def test_runtime_preflight_uses_count_function_not_worker_owned_tables() -> None:
    source = GUARD.read_text(encoding="utf-8")
    start = source.index("    async def _target_empty_preflight(")
    end = source.index("    async def target_empty_preflight(", start)
    method = source[start:end]

    assert "public.migration_protected_target_counts($1)" in method
    assert "FROM public.wecom_connection_state" not in method
    assert "FROM public.im_acceptance_evidence_events" not in method
    assert "unexpected = observed_tables - expected_tables" in method
    assert "duplicates =" in method


def test_protected_count_migration_module_imports_without_running_ddl() -> None:
    spec = importlib.util.spec_from_file_location("migration_protected_target_counts", MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.revision == "0019_migration_protected_target_counts"
    assert module.down_revision == "0018_performance_fixture_cleanup"
