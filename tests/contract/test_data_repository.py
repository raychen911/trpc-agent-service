import asyncio
from datetime import datetime, timezone
from trpc_service.storage.memory import InMemoryDataRepository
from trpc_service.storage.data_models import SessionEvent, SummaryRecord

def test_event_append_is_idempotent_and_ordered():
    async def run():
        r=InMemoryDataRepository(); now=datetime.now(timezone.utc)
        a=SessionEvent(tenant_id='t',key='s',event_id='e1',sequence=1,value={},updated_at=now); await r.append_event(a); await r.append_event(a); return await r.get_events(tenant_id='t',session_key='s')
    assert len(asyncio.run(run())) == 1

def test_summary_watermark_cannot_regress():
    async def run():
        r=InMemoryDataRepository(); now=datetime.now(timezone.utc); await r.put_summary(SummaryRecord(tenant_id='t',key='s',event_sequence=2,value={},version=1,updated_at=now))
        try: await r.put_summary(SummaryRecord(tenant_id='t',key='s',event_sequence=1,value={},version=2,updated_at=now)); return False
        except ValueError: return True
    assert asyncio.run(run())
