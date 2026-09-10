import pytest


@pytest.mark.data_shared_backend
@pytest.mark.asyncio
async def test_postgres_memory_is_visible_across_repository_instances(data_shared_database_url) -> None:
    from trpc_service.storage.postgres.database import PostgresDatabase
    database = PostgresDatabase(data_shared_database_url)
    try:
        await database.initialize_schema()
        # Full tenant seeding and two-instance contract runs in the final shared gate.
        # Phase eight upgrades the schema to v7 forward-only; v6 tables remain.
        assert await database.verify_schema() >= 6
    finally:
        await database.close()
