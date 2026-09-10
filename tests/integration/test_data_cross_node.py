import asyncio
from datetime import datetime, timezone
from trpc_service.storage.memory import InMemoryDataRepository
from trpc_service.storage.data_models import MemoryRecord

def test_shared_repository_instance_exposes_memory_cross_node():
    async def run():
        r=InMemoryDataRepository(); await r.put_memory(MemoryRecord(tenant_id='t',namespace='n',key='k',value={'v':1},version=1,updated_at=datetime.now(timezone.utc))); return await r.get_memory(tenant_id='t',namespace='n',key='k')
    assert asyncio.run(run()).value['v'] == 1
