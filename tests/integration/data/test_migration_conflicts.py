from trpc_service.storage.postgres import data_repositories

def test_postgres_migration_repository_exists() -> None:
    assert hasattr(data_repositories,"PostgresMigrationRepository")
