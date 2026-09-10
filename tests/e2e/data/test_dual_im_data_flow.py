from uuid import UUID
import pytest
from trpc_service.storage.data_models import DataScope, SessionEvent
from trpc_service.storage.memory import InMemoryDataRepository

@pytest.mark.asyncio
async def test_two_tenant_dual_channel_data_flow_isolated():
    repo=InMemoryDataRepository(); a=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1)); b=DataScope(tenant_id="tenant-beta",trace_id=UUID(int=2))
    await repo.append(a,SessionEvent(tenant_id=a.tenant_id,session_key="im",event_id="a",sequence=1,payload={"channel":"feishu"}),expected_watermark=0)
    await repo.append(b,SessionEvent(tenant_id=b.tenant_id,session_key="im",event_id="b",sequence=1,payload={"channel":"wecom"}),expected_watermark=0)
    assert (await repo.read_event_content(a,"im"))[0].payload["channel"] == "feishu"
    assert (await repo.read_event_content(b,"im"))[0].payload["channel"] == "wecom"
