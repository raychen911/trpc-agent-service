from __future__ import annotations

import importlib.util
import re
from pathlib import Path

ROOT = Path(__file__).parents[2]
MIGRATION = ROOT / "migrations" / "versions" / "0016_session_ready_backlog_metric.py"


def _sql() -> str:
    return re.sub(r"\s+", " ", MIGRATION.read_text(encoding="utf-8")).lower()


def test_backlog_metric_migration_follows_worker_role_contract() -> None:
    source = MIGRATION.read_text(encoding="utf-8")

    assert 'revision = "0016_session_ready_backlog_metric"' in source
    assert 'down_revision = "0015_worker_database_role_contract"' in source


def test_backlog_function_is_authoritative_and_retry_aware() -> None:
    sql = _sql()

    assert "create index if not exists ix_session_mailboxes_ready_backlog" in sql
    assert "on public.session_mailboxes (retry_at)" in sql
    assert "where status='queued'" in sql
    assert "accepted_sequence > resolved_sequence" in sql
    assert "retry_at is null or retry_at <= clock_timestamp()" in sql
    assert "create or replace function public.count_session_ready_backlog()" in sql
    assert "returns bigint" in sql
    assert "language sql" in sql
    assert "volatile" in sql
    assert "security definer" in sql
    assert "set search_path = pg_catalog, public" in sql


def test_backlog_function_has_only_metrics_execute_privilege() -> None:
    sql = _sql()

    assert "trpc_metrics role must be provisioned before migration 0016" in sql
    assert "metrics_can_login" in sql
    assert "metrics_is_superuser" in sql
    assert "metrics_can_create_database" in sql
    assert "metrics_can_create_role" in sql
    assert "metrics_inherits" in sql
    assert "metrics_bypasses_rls" in sql
    assert "revoke all on function public.count_session_ready_backlog() from public" in sql
    assert (
        "revoke all on function public.count_session_ready_backlog() from trpc_runtime, trpc_worker"
    ) in sql
    assert "grant execute on function public.count_session_ready_backlog() to trpc_metrics" in sql
    assert "revoke all on table public.session_mailboxes from trpc_metrics" in sql


def test_backlog_migration_module_imports_without_running_ddl() -> None:
    spec = importlib.util.spec_from_file_location("session_ready_backlog_migration", MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.revision == "0016_session_ready_backlog_metric"
    assert module.down_revision == "0015_worker_database_role_contract"
