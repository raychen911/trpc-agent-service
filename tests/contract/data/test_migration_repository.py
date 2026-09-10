from uuid import UUID
import pytest
from trpc_service.storage.data_models import DataScope
from trpc_service.storage.sync import InMemoryMigrationRepository

@pytest.mark.asyncio
async def test_migration_repository_cas_checkpoint_and_first_write() -> None:
    repo=InMemoryMigrationRepository(); scope=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1))
    state=await repo.get(scope,"s"); assert state.state == "PLANNED"
    state=await repo.transition(scope,"s",expected_state="PLANNED",expected_generation=1,target_state="PAUSING")
    state=await repo.transition(scope,"s",expected_state="PAUSING",expected_generation=state.generation,target_state="SNAPSHOT_LOCKED",fields={"source_watermark":2})
    state=await repo.checkpoint(scope,"s",expected_generation=state.generation,copied_watermark=2,target_digest="a"*64)
    state=await repo.activate(scope,"s",expected_generation=state.generation,verified_watermark=2,verified_digest="a"*64)
    state=await repo.mark_first_authoritative_write(scope,"s",expected_generation=state.generation)
    assert state.state == "ACTIVE_FORWARD_ONLY" and not state.rollback_eligible

def test_postgres_migration_repository_is_exposed():
    from trpc_service.storage.postgres import data_repositories
    assert hasattr(data_repositories, "PostgresMigrationRepository")
