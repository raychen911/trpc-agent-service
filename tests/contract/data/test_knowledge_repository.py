from uuid import UUID
import pytest
from trpc_service.storage.contracts import TenantFilterUnsupported
from trpc_service.storage.data_models import DataScope
from trpc_service.storage.data_service import AuditedDataAccess

class NoFilter:
    supports_tenant_prefilter=False
    calls=0
    async def search(self,*args): self.calls+=1

@pytest.mark.asyncio
async def test_knowledge_search_fails_before_vector_call_without_prefilter() -> None:
    vector=NoFilter(); service=AuditedDataAccess(object(), vector_store=vector)
    with pytest.raises(TenantFilterUnsupported): await service.search_knowledge(DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1)), [1.0], limit=5)
    assert vector.calls == 0

def test_postgres_knowledge_metadata_repository_is_available() -> None:
    from trpc_service.storage.postgres import data_repositories
    assert hasattr(data_repositories, "PostgresKnowledgeRepository")
