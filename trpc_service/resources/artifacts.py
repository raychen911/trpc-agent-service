"""Artifact stores with tenant namespaces and path-traversal protection."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import uuid
from pathlib import Path
from typing import Protocol
from typing import Any

from pydantic import BaseModel
from pydantic import Field


class ArtifactNotFoundError(KeyError):
    """Raised without revealing whether another tenant owns an object."""


class ArtifactMetadata(BaseModel):
    artifact_id: str
    tenant_id: str
    app_id: str
    original_name: str
    mime_type: str
    size_bytes: int = Field(ge=0)
    checksum_sha256: str
    object_uri: str


class ArtifactStore(Protocol):

    async def put(self, tenant_id: str, app_id: str, name: str, mime_type: str, content: bytes) -> ArtifactMetadata:
        ...

    async def get(self, tenant_id: str, artifact_id: str) -> tuple[ArtifactMetadata, bytes]:
        ...


def _safe_name(value: str) -> str:
    leaf = Path(value).name
    normalized = re.sub(r"[^A-Za-z0-9._-]", "_", leaf)[:128]
    return normalized or "artifact.bin"


class InMemoryArtifactStore:
    """Deterministic test implementation with tenant-scoped lookup."""

    def __init__(self, max_bytes: int = 10 * 1024 * 1024) -> None:
        self._max_bytes = max_bytes
        self._objects: dict[tuple[str, str], tuple[ArtifactMetadata, bytes]] = {}
        self._lock = asyncio.Lock()

    async def put(self, tenant_id: str, app_id: str, name: str, mime_type: str, content: bytes) -> ArtifactMetadata:
        if len(content) > self._max_bytes:
            raise ValueError("artifact exceeds configured size limit")
        artifact_id = uuid.uuid4().hex
        safe_name = _safe_name(name)
        metadata = ArtifactMetadata(
            artifact_id=artifact_id,
            tenant_id=tenant_id,
            app_id=app_id,
            original_name=safe_name,
            mime_type=mime_type or "application/octet-stream",
            size_bytes=len(content),
            checksum_sha256=hashlib.sha256(content).hexdigest(),
            object_uri=f"memory://{tenant_id}/{app_id}/{artifact_id}/{safe_name}",
        )
        async with self._lock:
            self._objects[(tenant_id, artifact_id)] = (metadata, bytes(content))
        return metadata.model_copy(deep=True)

    async def get(self, tenant_id: str, artifact_id: str) -> tuple[ArtifactMetadata, bytes]:
        async with self._lock:
            value = self._objects.get((tenant_id, artifact_id))
        if value is None:
            raise ArtifactNotFoundError("artifact not found")
        return value[0].model_copy(deep=True), bytes(value[1])


class LocalArtifactStore:
    """Local object store for the exercise; metadata can be mirrored to PostgreSQL."""

    def __init__(self, root: str | Path, max_bytes: int = 10 * 1024 * 1024) -> None:
        self._root = Path(root).resolve()
        self._root.mkdir(parents=True, exist_ok=True)
        self._max_bytes = max_bytes
        self._metadata: dict[tuple[str, str], ArtifactMetadata] = {}

    def _path(self, tenant_id: str, app_id: str, artifact_id: str) -> Path:
        if not all(
                re.fullmatch(r"[A-Za-z0-9_-][A-Za-z0-9_.-]*", part) and part not in {".", ".."}
                for part in (tenant_id, app_id, artifact_id)):
            raise ValueError("invalid artifact namespace")
        target = (self._root / tenant_id / app_id / artifact_id).resolve()
        if self._root != target and self._root not in target.parents:
            raise ValueError("artifact path escapes object-store root")
        return target

    async def put(self, tenant_id: str, app_id: str, name: str, mime_type: str, content: bytes) -> ArtifactMetadata:
        if len(content) > self._max_bytes:
            raise ValueError("artifact exceeds configured size limit")
        artifact_id = uuid.uuid4().hex
        safe_name = _safe_name(name)
        directory = self._path(tenant_id, app_id, artifact_id)
        directory.mkdir(parents=True, exist_ok=False)
        path = directory / safe_name
        await asyncio.to_thread(path.write_bytes, content)
        metadata = ArtifactMetadata(
            artifact_id=artifact_id,
            tenant_id=tenant_id,
            app_id=app_id,
            original_name=safe_name,
            mime_type=mime_type or "application/octet-stream",
            size_bytes=len(content),
            checksum_sha256=hashlib.sha256(content).hexdigest(),
            object_uri=path.as_uri(),
        )
        self._metadata[(tenant_id, artifact_id)] = metadata
        return metadata.model_copy(deep=True)

    async def get(self, tenant_id: str, artifact_id: str) -> tuple[ArtifactMetadata, bytes]:
        metadata = self._metadata.get((tenant_id, artifact_id))
        if metadata is None:
            raise ArtifactNotFoundError("artifact not found")
        path = self._path(tenant_id, metadata.app_id, artifact_id) / _safe_name(metadata.original_name)
        content = await asyncio.to_thread(path.read_bytes)
        if hashlib.sha256(content).hexdigest() != metadata.checksum_sha256:
            raise IOError("artifact checksum mismatch")
        return metadata.model_copy(deep=True), content


class PostgresLocalArtifactStore(LocalArtifactStore):
    """Local bytes plus PostgreSQL metadata so restarts preserve lookup."""

    def __init__(self, root: str | Path, pool: Any, max_bytes: int = 10 * 1024 * 1024) -> None:
        super().__init__(root, max_bytes)
        self._pool = pool

    async def put(self, tenant_id: str, app_id: str, name: str, mime_type: str, content: bytes) -> ArtifactMetadata:
        metadata = await super().put(tenant_id, app_id, name, mime_type, content)
        await self._pool.execute(
            """
            INSERT INTO artifact
                (artifact_id,tenant_id,app_id,object_uri,mime_type,size_bytes,
                 checksum_sha256,metadata)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
            """, metadata.artifact_id, tenant_id, app_id, metadata.object_uri, metadata.mime_type, metadata.size_bytes,
            metadata.checksum_sha256, json.dumps({"original_name": metadata.original_name}))
        return metadata

    async def get(self, tenant_id: str, artifact_id: str) -> tuple[ArtifactMetadata, bytes]:
        row = await self._pool.fetchrow("SELECT * FROM artifact WHERE tenant_id=$1 AND artifact_id=$2", tenant_id,
                                        artifact_id)
        if row is None:
            raise ArtifactNotFoundError("artifact not found")
        extra = row["metadata"]
        if isinstance(extra, str):
            extra = json.loads(extra)
        metadata = ArtifactMetadata(artifact_id=row["artifact_id"],
                                    tenant_id=row["tenant_id"],
                                    app_id=row["app_id"],
                                    original_name=extra.get("original_name", "artifact.bin"),
                                    mime_type=row["mime_type"],
                                    size_bytes=row["size_bytes"],
                                    checksum_sha256=row["checksum_sha256"],
                                    object_uri=row["object_uri"])
        path = self._path(tenant_id, metadata.app_id, artifact_id) / _safe_name(metadata.original_name)
        content = await asyncio.to_thread(path.read_bytes)
        if hashlib.sha256(content).hexdigest() != metadata.checksum_sha256:
            raise IOError("artifact checksum mismatch")
        return metadata, content
