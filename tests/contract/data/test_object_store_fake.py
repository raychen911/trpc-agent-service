from uuid import UUID
import pytest
from trpc_service.storage.data_models import DataScope

@pytest.mark.asyncio
async def test_deterministic_object_store_is_tenant_scoped_and_idempotent() -> None:
    from trpc_service.storage.object_store import DeterministicObjectStore
    store = DeterministicObjectStore(); a=DataScope(tenant_id="tenant-alpha", trace_id=UUID(int=1)); b=DataScope(tenant_id="tenant-beta", trace_id=UUID(int=2))
    temporary = await store.put_temporary(a, "upload", b"content")
    assert store.is_deterministic_fixture is True
    assert await store.read(a, temporary.storage_ref) == b"content"
    with pytest.raises(Exception): await store.read(b, temporary.storage_ref)
    assert await store.delete_temporary(a, temporary.storage_ref) is True
    assert await store.delete_temporary(a, temporary.storage_ref) is False
