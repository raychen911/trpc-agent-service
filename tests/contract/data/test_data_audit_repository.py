from uuid import UUID
from trpc_service.storage.data_models import DataScope
from uuid import UUID
from trpc_service.audit.models import DataAuditRecord
from datetime import datetime, timezone

def test_audit_diagnostics_are_metadata_only():
    scope=DataScope(tenant_id="tenant-alpha",trace_id=UUID(int=1)); value=scope.diagnostic()
    assert set(value) <= {"tenant_digest","trace_id"} and "tenant-alpha" not in str(value)

def test_data_audit_record_rejects_raw_content_fields():
    record=DataAuditRecord(audit_id=UUID(int=2),tenant_id="tenant-alpha",trace_id=UUID(int=1),operation="PUT_MEMORY",resource_type="memory",resource_key_digest="a"*64,content_digest="b"*64,result="COMMITTED",created_at=datetime.now(timezone.utc))
    assert "content" not in record.model_dump()

def test_data_audit_repository_is_tenant_and_trace_scoped():
    from trpc_service.audit.repository import InMemoryDataAuditRepository
    repo=InMemoryDataAuditRepository(); record=DataAuditRecord(audit_id=UUID(int=2),tenant_id="tenant-alpha",trace_id=UUID(int=1),operation="PUT_MEMORY",resource_type="memory",resource_key_digest="a"*64,content_digest="b"*64,result="COMMITTED",created_at=datetime.now(timezone.utc))
    import asyncio
    asyncio.run(repo.append(record))
    assert len(asyncio.run(repo.list_by_trace("tenant-alpha",UUID(int=1)))) == 1
    assert asyncio.run(repo.list_by_trace("tenant-beta",UUID(int=1))) == []
