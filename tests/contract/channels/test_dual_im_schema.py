from __future__ import annotations

from pathlib import Path

from trpc_service.storage.postgres.database import SUPPORTED_SCHEMA_VERSION


MIGRATION = (
    Path(__file__).parents[3]
    / "trpc_service"
    / "storage"
    / "postgres"
    / "migrations"
    / "003_dual_im.sql"
)


def test_dual_im_migration_adds_identity_and_delivery_schema() -> None:
    sql = MIGRATION.read_text(encoding="utf-8").lower()

    assert "add column if not exists provider_tenant_key" in sql
    assert "add column if not exists provider_app_or_bot_id" in sql
    assert "add column if not exists channel_identity_digest" in sql
    assert "create unique index if not exists uq_channel_bindings_provider_identity" in sql
    assert "create table if not exists delivery_records" in sql
    assert "create table if not exists delivery_attempts" in sql
    assert "references channel_bindings(binding_id)" in sql
    assert "references delivery_records(delivery_id)" in sql


def test_dual_im_migration_is_additive_and_versioned() -> None:
    sql = MIGRATION.read_text(encoding="utf-8").lower()

    assert "drop table" not in sql
    assert "drop column" not in sql
    assert "delete from" not in sql
    assert SUPPORTED_SCHEMA_VERSION >= 5
