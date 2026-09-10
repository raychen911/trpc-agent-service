from uuid import UUID
import pytest
from trpc_service.storage.data_models import DataScope

@pytest.mark.asyncio
async def test_deterministic_vector_store_prefilters_tenant() -> None:
    from trpc_service.storage.vector_store import DeterministicVectorStore
    store=DeterministicVectorStore(); a=DataScope(tenant_id="tenant-alpha", trace_id=UUID(int=1)); b=DataScope(tenant_id="tenant-beta", trace_id=UUID(int=2))
    await store.upsert(a, "doc", "a"*64, [1.0, 0.0]); await store.upsert(b, "doc", "b"*64, [1.0, 0.0])
    hits=await store.search(a, [1.0,0.0], 10)
    assert store.supports_tenant_prefilter and store.is_deterministic_fixture
    assert [hit.digest for hit in hits] == ["a"*64]
