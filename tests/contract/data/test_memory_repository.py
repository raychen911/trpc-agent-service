from uuid import UUID
import pytest

from trpc_service.storage.contracts import TenantScopeInvalid, VersionConflict
from trpc_service.storage.data_models import DataScope, MemoryRecord
from trpc_service.storage.memory import InMemoryDataRepository


@pytest.mark.asyncio
async def test_memory_cas_metadata_and_tenant_isolation() -> None:
    repo = InMemoryDataRepository(); a = DataScope(tenant_id="tenant-alpha", trace_id=UUID(int=1)); b = DataScope(tenant_id="tenant-beta", trace_id=UUID(int=2))
    one = MemoryRecord(tenant_id=a.tenant_id, namespace="n", memory_key="k", content={"v": 1})
    await repo.compare_and_set(a, one, expected_version=None)
    assert (await repo.get_metadata(a, "n", "k")).content_digest == one.content_digest
    assert await repo.read_content(b, "n", "k") is None
    with pytest.raises(TenantScopeInvalid):
        await repo.compare_and_set(b, one, expected_version=None)
    with pytest.raises(VersionConflict):
        await repo.compare_and_set(a, one.model_copy(update={"version": 2, "content": {"v": 2}, "content_digest": "a" * 64}), expected_version=0)
