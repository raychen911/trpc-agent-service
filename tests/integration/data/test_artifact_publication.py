from uuid import UUID
import pytest
from trpc_service.storage.data_models import DataScope
from trpc_service.storage.data_service import AuditedDataAccess

class ReadyAudit:
    async def ensure_ready(self, scope): return None

@pytest.mark.asyncio
async def test_artifact_publication_verifies_digest_before_metadata() -> None:
    from trpc_service.storage.object_store import DeterministicObjectStore, InMemoryArtifactRepository
    scope=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1)); metadata=InMemoryArtifactRepository(); store=DeterministicObjectStore()
    service=AuditedDataAccess(metadata,ReadyAudit(),object_store=store)
    result=await service.publish_artifact(scope,artifact_id="a",upload_id="u",content=b"abc")
    assert result.content_digest
    assert (await metadata.get_metadata(scope,"a")).storage_ref == result.storage_ref
