from __future__ import annotations

import ast
from pathlib import Path

MIGRATION = (
    Path(__file__).resolve().parents[2]
    / "migrations"
    / "versions"
    / "0017_im_acceptance_evidence.py"
)


def test_wecom_evidence_migration_is_tenant_scoped_and_worker_only() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    ast.parse(source)
    assert 'down_revision = "0016_session_ready_backlog_metric"' in source
    assert "CREATE TABLE public.wecom_connection_state" in source
    assert "CREATE TABLE public.im_acceptance_evidence_events" in source
    assert "migration_write_barrier_wecom_connection_state" in source
    assert "migration_write_barrier_im_acceptance_evidence_events" in source
    assert source.count("migration_write_barrier_guard()") == 2
    assert source.count("ENABLE ROW LEVEL SECURITY") == 2
    assert source.count("current_setting('app.tenant_id', true)") == 4
    assert "TO trpc_worker" in source
    assert "TO trpc_runtime" not in source
    assert "UPDATE, DELETE" not in source


def test_wecom_evidence_schema_never_persists_raw_provider_or_owner_values() -> None:
    source = MIGRATION.read_text(encoding="utf-8")
    assert "owner_hash" in source
    assert "provider_event_hash" in source
    assert "owner_id" not in source
    assert "provider_event_id" not in source
    assert "provider_message_id" not in source
    assert "payload_json" not in source
