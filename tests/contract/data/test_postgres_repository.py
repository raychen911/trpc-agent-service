from datetime import datetime, timezone
import pytest
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from tests.contract.data.repository_contracts import assert_tenant_isolation
from trpc_service.storage.postgres.data_repositories import PostgresDataRepositoryFactory
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.models import TenantRow


@pytest.mark.data_shared_backend
@pytest.mark.asyncio
async def test_postgres_repository_is_tenant_scoped(data_shared_database_url) -> None:
    database = PostgresDatabase(data_shared_database_url)
    await database.initialize_schema()
    now = datetime.now(timezone.utc)
    try:
        async with AsyncSession(database.engine) as session, session.begin():
            for tenant_id in ("tenant-alpha", "tenant-beta"):
                await session.execute(insert(TenantRow).values(
                    tenant_id=tenant_id, display_name=tenant_id, status="active",
                    config_version=1, created_at=now, updated_at=now,
                ).on_conflict_do_nothing(index_elements=["tenant_id"]))
        await assert_tenant_isolation(PostgresDataRepositoryFactory(database).memories())
    finally:
        await database.close()
