"""T011 RED: forward-only PostgreSQL schema upgrade from v6 to v7."""

from __future__ import annotations

from pathlib import Path

import pytest

from trpc_service.storage.postgres import database
from trpc_service.storage.postgres.models import Base

pytestmark = pytest.mark.shared_backend

EXPECTED_V7_TABLES: tuple[str, ...] = (
    "configuration_snapshots",
    "configuration_releases",
    "release_targets",
    "tenant_config_routes",
    "execution_config_pins",
    "release_gate_signals",
    "release_transition_events",
    "alert_incidents",
    "rollback_decisions",
)


def test_schema_gate_requires_v7_and_forward_migration() -> None:
    assert database.SUPPORTED_SCHEMA_VERSION == 7, (
        "schema gate must be upgraded to v7 for observability operations"
    )
    migration = Path("trpc_service/storage/postgres/migrations/007_observability_operations.sql")
    assert migration.exists(), "007_observability_operations.sql must exist"
    sql = migration.read_text(encoding="utf-8").lower()
    assert "create table if not exists" in sql
    for table in EXPECTED_V7_TABLES:
        assert table in sql, f"migration must create {table}"
    assert "drop table" not in sql, "forward-only upgrade must not drop existing tables"


def test_schema_model_contains_v7_entities_without_removing_v6_tables() -> None:
    assert set(EXPECTED_V7_TABLES) <= set(Base.metadata.tables)
    v6_tables = {
        "session_streams", "session_events", "memory_records", "summary_records",
        "artifact_metadata", "artifact_uploads", "knowledge_documents", "migration_states",
    }
    assert v6_tables <= set(Base.metadata.tables)
    assert {"tenants", "persistent_audit_records", "delivery_records"} <= set(Base.metadata.tables)


async def test_shared_backend_upgrades_to_v7_idempotently(
    ops_namespace: str, shared_database_url: str
) -> None:
    from trpc_service.storage.postgres.database import PostgresDatabase

    database_instance = PostgresDatabase(shared_database_url)
    try:
        await database_instance.initialize_schema()
        assert await database_instance.verify_schema() == 7, (
            "initialization must be idempotent at v7"
        )
        from sqlalchemy import text

        engine = database_instance.engine
        async with engine.connect() as connection:
            version = await connection.scalar(text("select max(version) from schema_migrations"))
            assert version == 7
            for table in EXPECTED_V7_TABLES:
                exists = await connection.scalar(
                    text("select to_regclass(:name) is not null"), {"name": f"public.{table}"}
                )
                assert exists, f"{table} must exist after upgrade"
    finally:
        await database_instance.close()
