import pytest
from sqlalchemy import text

from trpc_service.storage.postgres.database import (
    SUPPORTED_SCHEMA_VERSION,
    PostgresDatabase,
)


@pytest.mark.shared_backend
async def test_existing_v1_database_is_upgraded_without_reset(
    shared_database_url: str,
) -> None:
    database = PostgresDatabase(shared_database_url)
    async with database.engine.begin() as connection:
        await connection.execute(text("DELETE FROM schema_migrations WHERE version = 2"))
        await connection.execute(text(
            "ALTER TABLE persistent_audit_records DROP COLUMN IF EXISTS agent_id"
        ))

    await database.initialize_schema()

    async with database.engine.connect() as connection:
        has_column = await connection.scalar(text(
            "SELECT EXISTS ("
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_schema = current_schema() "
            "AND table_name = 'persistent_audit_records' "
            "AND column_name = 'agent_id')"
        ))
    assert has_column is True
    assert await database.verify_schema() == SUPPORTED_SCHEMA_VERSION
    await database.close()
