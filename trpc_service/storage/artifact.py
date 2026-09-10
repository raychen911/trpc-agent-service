"""Tenant-isolated local Artifact store for the small-scale deployment."""

from __future__ import annotations

import asyncio
import hashlib
import re
from pathlib import Path

from trpc_service.config.models import ArtifactRecord
from trpc_service.storage.database import Database
from trpc_service.storage.repositories import ArtifactRepository

_SAFE_ID = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")


class ArtifactNotFoundError(LookupError):
    pass


class LocalArtifactStore:
    def __init__(self, database: Database, root: Path, max_bytes: int = 20 * 1024 * 1024) -> None:
        self._repository = ArtifactRepository(database)
        self._root = root.resolve()
        self._max_bytes = max_bytes

    async def put(
        self,
        *,
        tenant_id: str,
        artifact_id: str,
        filename: str,
        data: bytes,
        media_type: str = "application/octet-stream",
        session_id: str | None = None,
    ) -> ArtifactRecord:
        self._validate_id("tenant_id", tenant_id)
        self._validate_id("artifact_id", artifact_id)
        if not filename or Path(filename).name != filename:
            raise ValueError("filename must not contain a path")
        if len(data) > self._max_bytes:
            raise ValueError("artifact exceeds configured size limit")

        target = self._path(tenant_id, artifact_id)
        await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
        if target.exists():
            raise FileExistsError("artifact already exists")
        await asyncio.to_thread(target.write_bytes, data)
        record = ArtifactRecord(
            artifact_id=artifact_id,
            tenant_id=tenant_id,
            session_id=session_id,
            filename=filename,
            media_type=media_type,
            size_bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
            storage_uri=f"artifact://{tenant_id}/{artifact_id}",
        )
        try:
            return await self._repository.create(record)
        except Exception:
            await asyncio.to_thread(target.unlink, missing_ok=True)
            raise

    async def read(self, tenant_id: str, artifact_id: str) -> bytes:
        self._validate_id("tenant_id", tenant_id)
        self._validate_id("artifact_id", artifact_id)
        record = await self._repository.get(tenant_id, artifact_id)
        if record is None:
            raise ArtifactNotFoundError("artifact does not exist for tenant")
        target = self._path(tenant_id, artifact_id)
        try:
            data = await asyncio.to_thread(target.read_bytes)
        except FileNotFoundError as exc:
            raise ArtifactNotFoundError("artifact content is unavailable") from exc
        if hashlib.sha256(data).hexdigest() != record.sha256:
            raise OSError("artifact checksum mismatch")
        return data

    def _path(self, tenant_id: str, artifact_id: str) -> Path:
        target = (self._root / tenant_id / "artifacts" / artifact_id[:2] / artifact_id).resolve()
        if self._root not in target.parents:
            raise ValueError("artifact path escapes configured root")
        return target

    @staticmethod
    def _validate_id(field: str, value: str) -> None:
        if not _SAFE_ID.fullmatch(value):
            raise ValueError(f"{field} contains unsupported characters")


__all__ = ["ArtifactNotFoundError", "LocalArtifactStore"]
