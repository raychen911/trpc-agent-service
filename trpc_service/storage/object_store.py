"""Tenant-scoped deterministic object-store fixture (not a production store)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from .canonical import content_digest
from .contracts import StateBackendUnavailable, TenantScopeInvalid, VersionConflict
from .data_models import ArtifactMetadata, DataScope
from .memory import WriteResult


@dataclass(frozen=True, slots=True)
class TemporaryObject:
    storage_ref: str
    content_digest: str
    byte_size: int
    created_at: datetime


class DeterministicObjectStore:
    is_deterministic_fixture = True

    def __init__(self) -> None:
        self._objects: dict[str, tuple[str, bytes]] = {}
        self.fail_operations: set[str] = set()
        self.calls: dict[str, int] = {"put": 0, "read": 0, "delete": 0}

    def _fail(self, operation: str) -> None:
        if operation in self.fail_operations:
            raise StateBackendUnavailable()

    async def put_temporary(self, scope: DataScope, upload_id: str, content: bytes) -> TemporaryObject:
        self._fail("put"); self.calls["put"] += 1
        digest = content_digest(content)
        tenant_digest = content_digest(scope.tenant_id)[:16]
        storage_ref = f"fixture://temporary/{tenant_digest}/{upload_id}/{digest}"
        existing = self._objects.get(storage_ref)
        if existing is not None and existing != (scope.tenant_id, bytes(content)):
            raise VersionConflict()
        self._objects[storage_ref] = (scope.tenant_id, bytes(content))
        return TemporaryObject(storage_ref, digest, len(content), datetime.now(timezone.utc))

    async def read(self, scope: DataScope, storage_ref: str) -> bytes:
        self._fail("read"); self.calls["read"] += 1
        value = self._objects.get(storage_ref)
        if value is None:
            raise StateBackendUnavailable()
        if value[0] != scope.tenant_id:
            raise TenantScopeInvalid()
        return value[1]

    async def delete_temporary(self, scope: DataScope, storage_ref: str) -> bool:
        self._fail("delete"); self.calls["delete"] += 1
        value = self._objects.get(storage_ref)
        if value is None:
            return False
        if value[0] != scope.tenant_id:
            raise TenantScopeInvalid()
        del self._objects[storage_ref]
        return True


class InMemoryArtifactRepository:
    is_deterministic_fixture = True

    def __init__(self) -> None:
        self._items: dict[tuple[str, str], ArtifactMetadata] = {}

    async def publish(self, scope: DataScope, request: ArtifactMetadata, *, expected_version: int | None) -> ArtifactMetadata:
        if request.tenant_id != scope.tenant_id:
            raise TenantScopeInvalid()
        key = (scope.tenant_id, request.artifact_id); current = self._items.get(key)
        if current is None and expected_version not in (None, 0):
            raise VersionConflict()
        if current is not None:
            if current.content_digest == request.content_digest and current.version == request.version:
                return current
            if expected_version != current.version or request.version != current.version + 1:
                raise VersionConflict()
        self._items[key] = request
        return request

    async def get_metadata(self, scope: DataScope, artifact_id: str) -> ArtifactMetadata | None:
        return self._items.get((scope.tenant_id, artifact_id))

    async def read_content(self, scope: DataScope, artifact_id: str) -> bytes | None:
        raise NotImplementedError("content is resolved through the object-store facade")

    async def collect_orphans(self, scope: DataScope, *, before: datetime, limit: int) -> list[str]:
        del scope, before, limit
        return []
