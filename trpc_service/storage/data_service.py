"""Backend-neutral facade used by Gateway/Worker data flows."""
from __future__ import annotations

from typing import Any

from .canonical import content_digest
from .contracts import DigestMismatch, TenantFilterUnsupported
from .data_models import ArtifactMetadata, DataScope, MemoryRecord, SummaryRecord


class AuditedDataAccess:
    """Small orchestration facade; adapters remain replaceable behind it.

    A production composition root may provide an audit repository.  If one is
    provided, mutation/read methods call it before exposing content; otherwise
    deterministic local tests can use the reference adapter directly.
    """

    def __init__(self, repository: Any, audit: Any | None = None, *, object_store: Any | None = None, vector_store: Any | None = None) -> None:
        self.repository = repository
        self.audit = audit
        self.object_store = object_store
        self.vector_store = vector_store

    async def _audit_ready(self, scope: DataScope) -> None:
        if self.audit is not None:
            await self.audit.ensure_ready(scope)

    async def append_event(self, scope: DataScope, event: Any, *, expected_watermark: int) -> Any:
        await self._audit_ready(scope)
        return await self.repository.append(scope, event, expected_watermark=expected_watermark)

    async def put_memory(self, scope: DataScope, record: MemoryRecord, *, expected_version: int | None = None) -> Any:
        await self._audit_ready(scope)
        return await self.repository.compare_and_set(scope, record, expected_version=expected_version)

    async def put_summary(self, scope: DataScope, record: SummaryRecord, *, expected_version: int | None = None) -> Any:
        await self._audit_ready(scope)
        return await self.repository.compare_and_set_summary(scope, record, expected_version=expected_version)

    async def get_memory_metadata(self, scope: DataScope, namespace: str, key: str) -> Any:
        return await self.repository.get_metadata(scope, namespace, key)

    async def read_memory_content(self, scope: DataScope, namespace: str, key: str) -> Any:
        await self._audit_ready(scope)
        return await self.repository.read_content(scope, namespace, key)

    async def publish_artifact(self, scope: DataScope, *, artifact_id: str, upload_id: str, content: bytes, media_type: str = "application/octet-stream", expected_version: int | None = None) -> ArtifactMetadata:
        await self._audit_ready(scope)
        temporary = await self.object_store.put_temporary(scope, upload_id, content)
        readback = await self.object_store.read(scope, temporary.storage_ref)
        digest = content_digest(readback)
        if digest != temporary.content_digest or digest != content_digest(content):
            raise DigestMismatch()
        request = ArtifactMetadata(
            tenant_id=scope.tenant_id, artifact_id=artifact_id,
            storage_ref=temporary.storage_ref, content_digest=digest,
            byte_size=len(content), media_type=media_type,
            version=(expected_version or 0) + 1,
        )
        return await self.repository.publish(scope, request, expected_version=expected_version)

    async def read_artifact(self, scope: DataScope, artifact_id: str) -> bytes | None:
        await self._audit_ready(scope)
        metadata = await self.repository.get_metadata(scope, artifact_id)
        if metadata is None:
            return None
        content = await self.object_store.read(scope, metadata.storage_ref)
        if content_digest(content) != metadata.content_digest:
            raise DigestMismatch()
        return content

    async def search_knowledge(self, scope: DataScope, query_vector: Any, *, limit: int = 10) -> Any:
        if self.vector_store is None or not self.vector_store.supports_tenant_prefilter:
            raise TenantFilterUnsupported()
        await self._audit_ready(scope)
        return await self.vector_store.search(scope, query_vector, limit)
