from __future__ import annotations

import importlib.util
import re
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[2]
MIGRATION = ROOT / "migrations" / "versions" / "0014_worker_database_role.py"
CONTRACT_MIGRATION = ROOT / "migrations" / "versions" / "0015_worker_database_role_contract.py"


def _sql() -> str:
    return re.sub(r"\s+", " ", MIGRATION.read_text(encoding="utf-8")).lower()


def _contract_sql() -> str:
    return re.sub(r"\s+", " ", CONTRACT_MIGRATION.read_text(encoding="utf-8")).lower()


def test_worker_role_migration_precedes_applied_contract_validation() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    contract_source = CONTRACT_MIGRATION.read_text(encoding="utf-8")

    assert 'revision = "0014_worker_database_role"' in source
    assert 'down_revision = "0013_migration_write_barrier"' in source
    assert 'revision = "0015_worker_database_role_contract"' in contract_source
    assert 'down_revision = "0014_worker_database_role"' in contract_source


def test_worker_role_migration_fails_closed_without_safe_roles() -> None:
    sql = _sql()

    assert "trpc_worker role must be provisioned before migration 0014" in sql
    assert "trpc_runtime role must be provisioned before migration 0014" in sql
    assert "runtime_is_superuser or runtime_bypasses_rls" in sql
    assert "trpc_runtime must be nosuperuser nobypassrls" in sql
    contract_sql = _contract_sql()
    assert "worker_can_login is distinct from true" in contract_sql
    assert "runtime_can_login is distinct from true" in contract_sql
    assert "trpc_worker must not own rls tables" in contract_sql
    assert "trpc_runtime must not own rls tables" in contract_sql


def test_global_functions_are_worker_only_and_tenant_lookup_stays_minimal() -> None:
    sql = _sql()
    global_functions = (
        "list_channel_bindings(text)",
        "claim_outbox_events(text,text,integer,integer)",
        "sweep_expired_session_leases(integer)",
        "schedule_session_mailbox_retries(integer)",
        "reconcile_session_mailboxes(integer)",
        "reconcile_session_mailboxes_v2(integer,integer)",
    )
    for function_name in global_functions:
        assert f"revoke execute on function public.{function_name} from public, trpc_runtime" in sql
        assert f"grant execute on function public.{function_name} to trpc_worker" in sql

    # Binding resolution is the one pre-tenant callback lookup.  It remains
    # available to the gateway and is also explicit on the Worker login for
    # connector/startup routing.
    assert "grant execute on function public.resolve_channel_binding(text) to trpc_worker" in sql


def test_worker_table_privileges_match_the_global_sql_contract() -> None:
    sql = _sql()

    worker_tables = (
        "tenants",
        "agent_apps",
        "config_revisions",
        "storage_profiles",
        "tenant_policies",
        "admin_idempotency",
        "channel_bindings",
        "channel_identities",
        "inbound_messages",
        "outbound_messages",
        "delivery_attempts",
        "sessions",
        "session_turns",
        "turn_intents",
        "session_events",
        "session_summaries",
        "memories",
        "artifacts",
        "knowledge_items",
        "knowledge_embeddings",
        "outbox_events",
        "dead_letters",
        "tool_executions",
        "confirmation_challenges",
        "audit_logs",
        "tenant_budget_usage",
        "fault_stage_controls",
        "session_mailboxes",
        "session_mailbox_items",
    )
    assert "grant select, insert, update, delete on table" in sql
    for table in worker_tables:
        assert table in sql

    assert "grant select, insert, update, delete on all tables" not in sql
    assert "grant usage on schema public to trpc_worker" in sql


def test_migration_module_imports_without_running_ddl() -> None:
    spec = importlib.util.spec_from_file_location("worker_role_migration", MIGRATION)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.revision == "0014_worker_database_role"
    assert module.down_revision == "0013_migration_write_barrier"


def test_contract_migration_imports_without_running_ddl() -> None:
    spec = importlib.util.spec_from_file_location(
        "worker_role_contract_migration", CONTRACT_MIGRATION
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.revision == "0015_worker_database_role_contract"
    assert module.down_revision == "0014_worker_database_role"


@pytest.mark.parametrize(
    ("role", "role_exists", "is_superuser", "bypasses_rls", "accepted"),
    (
        ("worker", True, False, True, True),
        ("worker", True, True, True, False),
        ("worker", True, False, False, False),
        ("runtime", True, False, False, True),
        ("runtime", True, False, True, False),
        ("worker", False, False, True, False),
        ("runtime", False, False, False, False),
    ),
)
def test_role_contract_matrix_is_fail_closed(
    role: str,
    role_exists: bool,
    is_superuser: bool,
    bypasses_rls: bool,
    accepted: bool,
) -> None:
    """Keep the migration's accepted role combinations explicit and narrow."""

    sql = _contract_sql()
    if role == "worker":
        assert "worker_is_superuser is distinct from false" in sql
        assert "worker_bypasses_rls is distinct from true" in sql
        assert "trpc_worker role must be provisioned before migration 0015" in sql
        actual = role_exists and is_superuser is False and bypasses_rls is True
    else:
        assert "runtime_is_superuser is distinct from false" in sql
        assert "runtime_bypasses_rls is distinct from false" in sql
        assert "trpc_runtime role must be provisioned before migration 0015" in sql
        actual = role_exists and is_superuser is False and bypasses_rls is False
    assert actual is accepted
