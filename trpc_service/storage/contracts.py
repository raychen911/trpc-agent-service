"""Stable storage boundaries used by tenant-selectable adapters."""

from __future__ import annotations

from typing import Protocol

from trpc_service.config.models import (
    ArtifactRecord,
    AuditLogRecord,
    KnowledgeRecord,
    MemoryRecord,
    SummaryRecord,
)


class StorageConflictError(RuntimeError):
    """Raised when a backend uniqueness rule rejects a duplicate record."""


class MemoryStore(Protocol):
    async def create(self, record: MemoryRecord) -> MemoryRecord: ...

    async def list_for_principal(
        self, tenant_id: str, principal_id: str, limit: int = 20
    ) -> list[MemoryRecord]: ...


class SummaryStore(Protocol):
    async def create(self, record: SummaryRecord) -> SummaryRecord: ...

    async def latest(self, tenant_id: str, session_id: str) -> SummaryRecord | None: ...


class KnowledgeStore(Protocol):
    async def create(self, record: KnowledgeRecord) -> KnowledgeRecord: ...

    async def search(
        self, tenant_id: str, query: str, app_id: str | None = None, limit: int = 20
    ) -> list[KnowledgeRecord]: ...


class ArtifactStore(Protocol):
    async def put(
        self,
        *,
        tenant_id: str,
        artifact_id: str,
        filename: str,
        data: bytes,
        media_type: str = "application/octet-stream",
        session_id: str | None = None,
    ) -> ArtifactRecord: ...

    async def read(self, tenant_id: str, artifact_id: str) -> bytes: ...


class AuditStore(Protocol):
    async def create(self, record: AuditLogRecord) -> AuditLogRecord: ...

    async def list_for_tenant(self, tenant_id: str, limit: int = 100) -> list[AuditLogRecord]: ...


__all__ = [
    "ArtifactStore",
    "AuditStore",
    "KnowledgeStore",
    "MemoryStore",
    "StorageConflictError",
    "SummaryStore",
]
