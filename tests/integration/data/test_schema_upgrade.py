from __future__ import annotations

from pathlib import Path

from trpc_service.storage.postgres import database
from trpc_service.storage.postgres.models import Base


def test_schema_gate_requires_v6_and_forward_migration() -> None:
    # Phase eight upgrades the gate to v7; v6 capabilities must remain intact.
    assert database.SUPPORTED_SCHEMA_VERSION >= 6
    migration = Path("trpc_service/storage/postgres/migrations/006_memory_summary.sql")
    assert migration.exists()
    sql = migration.read_text(encoding="utf-8").lower()
    assert "create table if not exists" in sql
    assert "session_events" in sql
    assert "memory_records" in sql
    assert "summary_records" in sql
    assert "migration_states" in sql
    assert "drop table" not in sql


def test_schema_model_contains_v6_entities_without_removing_v5_tables() -> None:
    expected = {"session_streams", "session_events", "memory_records", "summary_records", "artifact_metadata", "artifact_uploads", "knowledge_documents", "migration_states"}
    assert expected <= set(Base.metadata.tables)
    assert {"tenants", "persistent_audit_records", "delivery_records"} <= set(Base.metadata.tables)
