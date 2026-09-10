from uuid import UUID
import pytest
from trpc_service.storage.data_models import DataScope, MemoryRecord, SessionEvent
from trpc_service.storage.memory import InMemoryDataRepository

@pytest.mark.asyncio
async def test_public_data_objects_map_without_vendor_types():
    repo=InMemoryDataRepository(); scope=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1))
    event=SessionEvent(tenant_id=scope.tenant_id,session_key="s",event_id="e",sequence=1,payload={"text":"hello"})
    await repo.append(scope,event,expected_watermark=0)
    record=MemoryRecord(tenant_id=scope.tenant_id,namespace="session",memory_key="s",content={"last":1})
    await repo.compare_and_set(scope,record,expected_version=None)
    assert (await repo.read_content(scope,"session","s")).content == {"last":1}
