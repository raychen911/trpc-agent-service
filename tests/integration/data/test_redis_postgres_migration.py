from uuid import UUID
import pytest
from trpc_service.storage.data_models import DataScope
from trpc_service.storage.migration import MigrationCoordinator
from trpc_service.storage.sync import InMemoryMigrationRepository

@pytest.mark.asyncio
async def test_migration_snapshot_checkpoint_verify_cutover() -> None:
    scope=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1)); coordinator=MigrationCoordinator(InMemoryMigrationRepository())
    state=await coordinator.pause(scope,"s"); state=await coordinator.lock_snapshot(scope,"s",state.generation,watermark=2,digest="a"*64)
    state=await coordinator.checkpoint(scope,"s",state.generation,watermark=2,digest="a"*64)
    state=await coordinator.cutover(scope,"s",state.generation)
    assert state.authority.value == "POSTGRES" and state.rollback_eligible
