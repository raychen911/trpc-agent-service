"""Regression checks for the reference relational data model."""

from pathlib import Path

SCHEMA_FILE = Path(__file__).resolve().parents[2] / "data/schema.mysql.sql"


def test_schema_covers_core_relational_vector_and_object_metadata():
    schema = SCHEMA_FILE.read_text(encoding="utf-8")

    for table in (
            "tenant",
            "agent_app",
            "agent_session",
            "message_event",
            "memory",
            "summary",
            "channel_binding",
            "inbound_receipt",
            "artifact",
            "knowledge_document",
            "knowledge_chunk",
            "storage_outbox",
            "audit_log",
    ):
        assert f"CREATE TABLE IF NOT EXISTS {table} (" in schema

    for table in ("tenant_config_draft", "delivery_outbox", "delivery_attempt"):
        assert f"CREATE TABLE IF NOT EXISTS {table} (" in schema

    assert "PRIMARY KEY (tenant_id, channel, message_id)" in schema
    assert "UNIQUE KEY uq_artifact_object (tenant_id, storage_backend, object_key)" in schema
    assert "UNIQUE KEY uq_vector_record (tenant_id, vector_backend, vector_id)" in schema
    assert "KEY idx_storage_outbox_pending (status, available_at)" in schema
    assert "KEY idx_delivery_pending (status, available_at)" in schema
    assert "fencing_token BIGINT NOT NULL DEFAULT 0" in schema
    assert "config_revision BIGINT" in schema
    audit_migration = (SCHEMA_FILE.parent / "migrations/0003_audit_correlation.sql").read_text(encoding="utf-8")
    assert "CREATE INDEX idx_audit_message ON audit_log" in audit_migration
