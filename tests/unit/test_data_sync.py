import asyncio
from datetime import datetime, timezone
from trpc_service.storage.data_models import MemoryRecord
from trpc_service.storage.memory import InMemoryDataRepository
from trpc_service.storage.sync import DualReadMigrator, MigrationWatermark

def test_migration_watermark_is_immutable():
    w=MigrationWatermark('t','memory',3); assert w.sequence == 3

def test_dual_read_verifies_matching_versions():
    async def run():
        a=InMemoryDataRepository(); b=InMemoryDataRepository(); x=MemoryRecord(tenant_id='t',namespace='default',key='k',value={'x':1},version=1,updated_at=datetime.now(timezone.utc)); await a.put_memory(x); await b.put_memory(x); return await DualReadMigrator(a,b).verify(tenant_id='t',key='k')
    assert asyncio.run(run())
