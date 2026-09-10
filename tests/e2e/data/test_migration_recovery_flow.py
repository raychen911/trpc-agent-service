from uuid import UUID
import pytest
from trpc_service.storage.data_models import DataScope
from trpc_service.storage.migration import MigrationCoordinator
from trpc_service.storage.sync import InMemoryMigrationRepository

@pytest.mark.asyncio
async def test_migration_recovery_resumes_from_checkpoint():
    scope=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1)); c=MigrationCoordinator(InMemoryMigrationRepository())
    state=await c.pause(scope,"s"); state=await c.lock_snapshot(scope,"s",state.generation,watermark=1,digest="a"*64); state=await c.checkpoint(scope,"s",state.generation,watermark=1,digest="a"*64)
    assert state.state == "VERIFYING"
