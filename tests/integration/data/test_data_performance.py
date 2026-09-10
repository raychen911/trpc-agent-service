import time
import pytest
from uuid import UUID
from trpc_service.storage.data_models import DataScope, MemoryRecord
from trpc_service.storage.memory import InMemoryDataRepository

@pytest.mark.asyncio
async def test_inmemory_data_operations_are_deterministically_fast():
    repo=InMemoryDataRepository(); scope=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1)); samples=[]
    for i in range(50):
        start=time.perf_counter(); await repo.compare_and_set(scope,MemoryRecord(tenant_id=scope.tenant_id,namespace="n",memory_key=str(i),content={"i":i}),expected_version=None); samples.append((time.perf_counter()-start)*1000)
    assert sorted(samples)[int(len(samples)*.95)] < 50
