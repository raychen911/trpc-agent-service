"""Deterministic DataAudit repository used by local contract tests."""
from __future__ import annotations
from uuid import UUID
from .models import DataAuditRecord

class InMemoryDataAuditRepository:
    def __init__(self) -> None: self._records: dict[UUID, DataAuditRecord] = {}
    async def append(self, record: DataAuditRecord) -> DataAuditRecord:
        existing=self._records.get(record.audit_id)
        if existing is not None: return existing
        self._records[record.audit_id]=record; return record
    async def list_by_trace(self, tenant_id: str, trace_id: UUID) -> list[DataAuditRecord]:
        return [x for x in self._records.values() if x.tenant_id==tenant_id and x.trace_id==trace_id]
    async def list_metadata(self, tenant_id: str) -> list[dict[str, object]]:
        return [{"audit_id":str(x.audit_id),"tenant_id":x.tenant_id,"operation":x.operation,"resource_type":x.resource_type,"result":x.result,"version":x.version,"watermark":x.watermark} for x in self._records.values() if x.tenant_id==tenant_id]
