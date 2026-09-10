from uuid import UUID
import pytest
from trpc_service.storage.data_models import DataRecoveryMarker, DataScope
from trpc_service.recovery.repository import InMemoryDataRecoveryRepository

@pytest.mark.asyncio
async def test_recovery_marker_create_once_claim_and_complete():
    repo=InMemoryDataRecoveryRepository(); scope=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1)); marker=DataRecoveryMarker(marker_id="m",tenant_id=scope.tenant_id,operation="EVENT",stage="COMMITTED")
    assert await repo.create_once(scope,marker) == await repo.create_once(scope,marker)
    result=await repo.mark_complete(scope,"m",generation=1,result_digest="a"*64)
    assert result.confirmed
