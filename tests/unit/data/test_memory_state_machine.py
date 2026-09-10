from uuid import UUID
import pytest
from trpc_service.storage.contracts import ContentTooLarge, VersionConflict
from trpc_service.storage.data_models import DataScope, MemoryRecord
from trpc_service.storage.memory import InMemoryDataRepository

@pytest.mark.asyncio
async def test_memory_cas_and_size_boundary() -> None:
    repo = InMemoryDataRepository(max_memory_bytes=32); scope = DataScope(tenant_id="tenant-alpha", trace_id=UUID(int=1))
    first = MemoryRecord(tenant_id="tenant-alpha", namespace="n", memory_key="k", content={"x": 1})
    assert (await repo.compare_and_set(scope, first, expected_version=None)).outcome == "CREATED"
    assert (await repo.compare_and_set(scope, first, expected_version=1)).outcome == "REPLAYED"
    with pytest.raises(VersionConflict):
        await repo.compare_and_set(scope, MemoryRecord(tenant_id="tenant-alpha", namespace="n", memory_key="k", content={"x": 2}, version=2), expected_version=0)
    with pytest.raises(ContentTooLarge):
        await repo.compare_and_set(scope, MemoryRecord(tenant_id="tenant-alpha", namespace="n", memory_key="big", content={"x": "z" * 100}), expected_version=None)
