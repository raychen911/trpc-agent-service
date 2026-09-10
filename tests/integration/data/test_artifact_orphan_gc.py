from uuid import UUID
import pytest
from trpc_service.storage.data_models import DataScope

@pytest.mark.asyncio
async def test_orphan_delete_is_tenant_scoped_and_idempotent() -> None:
    from trpc_service.storage.object_store import DeterministicObjectStore
    store=DeterministicObjectStore(); a=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1)); b=DataScope(tenant_id="tenant-beta",trace_id=UUID(int=2))
    item=await store.put_temporary(a,"u",b"x")
    with pytest.raises(Exception): await store.delete_temporary(b,item.storage_ref)
    assert await store.delete_temporary(a,item.storage_ref)
    assert not await store.delete_temporary(a,item.storage_ref)
