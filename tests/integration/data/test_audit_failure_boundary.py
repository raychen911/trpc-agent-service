from uuid import UUID
import pytest
from trpc_service.storage.contracts import AuditUnavailable
from trpc_service.storage.data_models import DataScope, MemoryRecord
from trpc_service.storage.data_service import AuditedDataAccess

class AuditDown:
    async def ensure_ready(self, scope): raise AuditUnavailable()

@pytest.mark.asyncio
async def test_raw_read_is_not_returned_when_audit_is_down():
    service=AuditedDataAccess(object(),AuditDown()); scope=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1))
    with pytest.raises(AuditUnavailable): await service.read_memory_content(scope,"n","k")
