from uuid import UUID
import pytest
from trpc_service.storage.contracts import AuditUnavailable
from trpc_service.storage.data_models import DataScope
from trpc_service.storage.data_service import AuditedDataAccess

class FailedAudit:
    async def ensure_ready(self, scope): raise AuditUnavailable()
class CountingStore:
    calls=0
    async def put_temporary(self,*args): self.calls+=1

@pytest.mark.asyncio
async def test_artifact_audit_gate_precedes_object_side_effect() -> None:
    store=CountingStore(); service=AuditedDataAccess(object(), FailedAudit(), object_store=store)
    with pytest.raises(AuditUnavailable): await service.publish_artifact(DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1)), artifact_id="a", upload_id="u", content=b"x")
    assert store.calls == 0

def test_postgres_artifact_metadata_repository_is_available() -> None:
    from trpc_service.storage.postgres import data_repositories
    assert hasattr(data_repositories, "PostgresArtifactRepository")
