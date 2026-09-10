from uuid import UUID
import pytest
from trpc_service.storage.data_models import DataScope

@pytest.mark.asyncio
async def test_vector_candidates_are_filtered_before_scoring() -> None:
    from trpc_service.storage.vector_store import DeterministicVectorStore
    store=DeterministicVectorStore(); a=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1)); b=DataScope(tenant_id="tenant-beta",trace_id=UUID(int=2))
    await store.upsert(a,"a","a"*64,[1.0]); await store.upsert(b,"b","b"*64,[1.0])
    assert [x.document_id for x in await store.search(a,[1.0],10)] == ["a"]
    assert store.last_filter_tenant == "tenant-alpha"
