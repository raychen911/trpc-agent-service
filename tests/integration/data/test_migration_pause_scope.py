from uuid import UUID
import pytest
from trpc_service.storage.contracts import MigrationWritePaused
from trpc_service.storage.data_models import DataScope
from trpc_service.storage.migration import MigrationCoordinator
from trpc_service.storage.sync import InMemoryMigrationRepository

@pytest.mark.asyncio
async def test_pause_is_scoped_to_one_tenant_stream() -> None:
    repo=InMemoryMigrationRepository(); coordinator=MigrationCoordinator(repo)
    a=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1)); b=DataScope(tenant_id="tenant-beta",trace_id=UUID(int=2))
    await coordinator.pause(a,"s")
    with pytest.raises(MigrationWritePaused): await coordinator.assert_write_allowed(a,"s")
    await coordinator.assert_write_allowed(b,"s")
