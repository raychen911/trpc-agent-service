from __future__ import annotations

import re
from pathlib import Path

MIGRATION = (
    Path(__file__).resolve().parents[2] / "migrations" / "versions" / "0008_session_mailboxes.py"
)


def _sql() -> str:
    """Return migration SQL with formatting removed for stable contract checks."""

    value = re.sub(r"\s+", " ", MIGRATION.read_text(encoding="utf-8")).lower()
    return re.sub(r"\(\s+|\s+\)", lambda match: match.group(0).strip(), value)


def _table_body(sql: str, table_name: str) -> str:
    match = re.search(rf"create table {re.escape(table_name)} \((.*?)\);", sql)
    assert match is not None, f"missing CREATE TABLE {table_name}"
    return match.group(1)


def _function_body(sql: str, function_name: str) -> str:
    marker = f"create or replace function {function_name}(p_limit integer)"
    start = sql.find(marker)
    assert start >= 0, f"missing recovery function {function_name}"
    end = sql.find("create or replace function ", start + len(marker))
    return sql[start:] if end < 0 else sql[start:end]


def test_session_mailbox_schema_has_tenant_ordering_and_lease_contracts() -> None:
    sql = _sql()
    mailboxes = _table_body(sql, "session_mailboxes")
    items = _table_body(sql, "session_mailbox_items")

    assert "tenant_id text not null references tenants(tenant_id)" in mailboxes
    assert "primary key (tenant_id, session_id)" in mailboxes
    assert "status text not null default 'idle'" in mailboxes
    assert "status in ('idle','queued','running','retry_wait')" in mailboxes
    assert "accepted_sequence bigint not null default 0 check (accepted_sequence >= 0)" in mailboxes
    assert (
        "resolved_sequence bigint not null default 0 check (resolved_sequence >= 0 "
        "and resolved_sequence <= accepted_sequence)"
    ) in mailboxes
    assert (
        "processing_sequence bigint check (processing_sequence is null or ("
        "processing_sequence > resolved_sequence and processing_sequence <= accepted_sequence))"
    ) in mailboxes
    assert "queue_generation bigint not null default 0 check (queue_generation >= 0)" in mailboxes
    assert "lease_epoch bigint not null default 0 check (lease_epoch >= 0)" in mailboxes
    assert "check ((lease_owner is null) = (lease_expires_at is null))" in mailboxes
    assert "status = 'running'" in mailboxes
    assert "status <> 'running'" in mailboxes

    assert "tenant_id text not null references tenants(tenant_id)" in items
    assert "sequence bigint not null check (sequence >= 1)" in items
    assert "primary key (tenant_id, session_id, sequence)" in items
    assert "unique (tenant_id, session_id, inbound_id)" in items
    assert (
        "foreign key (tenant_id, inbound_id) references inbound_messages(tenant_id, inbound_id)"
    ) in items


def test_mailbox_tables_are_rls_protected_with_tenant_first_access() -> None:
    sql = _sql()

    for table_name in ("session_mailboxes", "session_mailbox_items"):
        assert f"alter table {table_name} enable row level security" in sql
        assert "revoke all on table session_mailboxes, session_mailbox_items from public" in sql
        assert f"create policy tenant_isolation_{table_name} on {table_name}" in sql

    tenant_setting = "nullif(current_setting('app.tenant_id', true), '')"
    assert sql.count(f"tenant_id = {tenant_setting}") >= 4
    assert "with check (tenant_id = " + tenant_setting + ")" in sql


def test_session_ready_v2_generation_is_validated_and_unique_per_session() -> None:
    sql = _sql()

    assert "event_type <> 'session.ready.v2'" in sql
    assert "payload_json ? 'generation'" in sql
    assert "jsonb_typeof(payload_json->'generation') = 'number'" in sql
    assert "payload_json->>'generation'" in sql
    assert "create unique index uq_outbox_session_ready_v2_generation on outbox_events" in sql
    assert "event_type, tenant_id, aggregate_id" in sql
    assert "where event_type = 'session.ready.v2'" in sql


def test_each_recovery_function_uses_database_clock_and_skip_locked() -> None:
    sql = _sql()
    recovery_functions = (
        "sweep_expired_session_leases",
        "schedule_session_mailbox_retries",
        "reconcile_session_mailboxes",
    )

    bodies = [_function_body(sql, function_name) for function_name in recovery_functions]
    assert len({body.split("$function$")[0] for body in bodies}) == len(recovery_functions)
    for body in bodies:
        assert "returns integer" in body
        assert "language plpgsql" in body
        assert "security definer" in body
        assert "clock_timestamp()" in body
        assert "for update of m skip locked" in body


def test_mailbox_schema_retains_retry_and_permanent_resolution_fields() -> None:
    sql = _sql()
    mailboxes = _table_body(sql, "session_mailboxes")
    items = _table_body(sql, "session_mailbox_items")

    for body in (mailboxes, items):
        assert "retry_count integer not null default 0 check (retry_count >= 0)" in body
        assert "attempt integer not null default 0 check (attempt >= 0)" in body
        assert "priority integer not null default 0 check (priority >= 0)" in body
        assert "retry_at timestamptz" in body

    assert "resolved_at timestamptz" in items
    assert "next_status='retry_wait'" in sql
    assert "next_status='queued'" in sql
    assert "queue_generation=queue_generation+1" in sql
