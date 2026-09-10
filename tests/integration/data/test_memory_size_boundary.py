from uuid import UUID
import pytest

from trpc_service.storage.contracts import AuditUnavailable
from trpc_service.storage.data_models import DataScope, MemoryRecord
from trpc_service.storage.data_service import AuditedDataAccess


class CountingRepository:
    calls = 0
    async def compare_and_set(self, *args, **kwargs):
        self.calls += 1


class FailedAudit:
    async def ensure_ready(self, scope):
        raise AuditUnavailable()


@pytest.mark.asyncio
async def test_audit_gate_failure_makes_zero_memory_writes() -> None:
    repo = CountingRepository(); service = AuditedDataAccess(repo, FailedAudit())
    scope = DataScope(tenant_id="tenant-alpha", trace_id=UUID(int=1))
    record = MemoryRecord(tenant_id=scope.tenant_id, namespace="n", memory_key="k", content={"x": 1})
    with pytest.raises(AuditUnavailable):
        await service.put_memory(scope, record)
    assert repo.calls == 0
