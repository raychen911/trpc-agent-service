import asyncio
import hashlib
import io
import json
import os
from collections.abc import Mapping
from pathlib import Path, PurePosixPath
from typing import Any

from trpc_service.storage.contracts import ArtifactMetadata, ArtifactObject
from trpc_service.storage.exceptions import InvalidArtifactKeyError, StorageNotFoundError


def _safe_parts(value: str) -> tuple[str, ...]:
    path = PurePosixPath(value.replace("\\", "/"))
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        raise InvalidArtifactKeyError("artifact key must be a relative normalized path")
    return path.parts


class LocalArtifactStore:
    def __init__(self, root: str | Path) -> None:
        self._root = Path(root).resolve()

    def _paths(self, tenant_id: str, object_key: str) -> tuple[Path, Path]:
        tenant_parts = _safe_parts(tenant_id)
        key_parts = _safe_parts(object_key)
        content_path = self._root.joinpath(*tenant_parts, *key_parts)
        metadata_path = content_path.with_name(content_path.name + ".metadata.json")
        if self._root not in content_path.resolve().parents:
            raise InvalidArtifactKeyError("artifact key escapes configured root")
        return content_path, metadata_path

    async def put(
        self,
        tenant_id: str,
        object_key: str,
        content: bytes,
        mime_type: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> ArtifactMetadata:
        return await asyncio.to_thread(
            self._put_sync, tenant_id, object_key, content, mime_type, metadata
        )

    def _put_sync(
        self,
        tenant_id: str,
        object_key: str,
        content: bytes,
        mime_type: str,
        metadata: Mapping[str, Any] | None,
    ) -> ArtifactMetadata:
        content_path, metadata_path = self._paths(tenant_id, object_key)
        content_path.parent.mkdir(parents=True, exist_ok=True)
        checksum = hashlib.sha256(content).hexdigest()
        record = ArtifactMetadata(
            tenant_id=tenant_id,
            object_key=object_key,
            mime_type=mime_type,
            size_bytes=len(content),
            checksum=checksum,
            metadata=dict(metadata or {}),
        )
        temporary = content_path.with_name(f".{content_path.name}.{os.getpid()}.tmp")
        temporary.write_bytes(content)
        os.replace(temporary, content_path)
        metadata_path.write_text(
            json.dumps(
                {
                    "tenant_id": record.tenant_id,
                    "object_key": record.object_key,
                    "mime_type": record.mime_type,
                    "size_bytes": record.size_bytes,
                    "checksum": record.checksum,
                    "metadata": dict(record.metadata),
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        return record

    async def get(self, tenant_id: str, object_key: str) -> ArtifactObject:
        return await asyncio.to_thread(self._get_sync, tenant_id, object_key)

    def _get_sync(self, tenant_id: str, object_key: str) -> ArtifactObject:
        content_path, metadata_path = self._paths(tenant_id, object_key)
        if not content_path.is_file() or not metadata_path.is_file():
            raise StorageNotFoundError("artifact not found")
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        metadata = ArtifactMetadata(**payload)
        content = content_path.read_bytes()
        if hashlib.sha256(content).hexdigest() != metadata.checksum:
            raise OSError("artifact checksum mismatch")
        return ArtifactObject(metadata=metadata, content=content)

    async def delete(self, tenant_id: str, object_key: str) -> None:
        content_path, metadata_path = self._paths(tenant_id, object_key)
        await asyncio.to_thread(content_path.unlink, missing_ok=True)
        await asyncio.to_thread(metadata_path.unlink, missing_ok=True)


class MinioArtifactStore:
    """Async facade for the official synchronous MinIO client."""

    def __init__(self, client: Any, bucket: str) -> None:
        self._client = client
        self._bucket = bucket

    @staticmethod
    def _object_name(tenant_id: str, object_key: str) -> str:
        return "/".join((*_safe_parts(tenant_id), *_safe_parts(object_key)))

    async def put(
        self,
        tenant_id: str,
        object_key: str,
        content: bytes,
        mime_type: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> ArtifactMetadata:
        checksum = hashlib.sha256(content).hexdigest()
        headers = {
            "x-amz-meta-sha256": checksum,
            **{str(key): str(value) for key, value in (metadata or {}).items()},
        }

        def upload() -> None:
            if not self._client.bucket_exists(self._bucket):
                self._client.make_bucket(self._bucket)
            self._client.put_object(
                self._bucket,
                self._object_name(tenant_id, object_key),
                io.BytesIO(content),
                len(content),
                content_type=mime_type,
                metadata=headers,
            )

        await asyncio.to_thread(upload)
        return ArtifactMetadata(
            tenant_id, object_key, mime_type, len(content), checksum, dict(metadata or {})
        )

    async def get(self, tenant_id: str, object_key: str) -> ArtifactObject:
        def download() -> ArtifactObject:
            name = self._object_name(tenant_id, object_key)
            try:
                response = self._client.get_object(self._bucket, name)
                content = response.read()
                response.close()
                response.release_conn()
                stat = self._client.stat_object(self._bucket, name)
            except Exception as error:
                raise StorageNotFoundError("artifact not found") from error
            checksum = hashlib.sha256(content).hexdigest()
            metadata = ArtifactMetadata(
                tenant_id=tenant_id,
                object_key=object_key,
                mime_type=stat.content_type or "application/octet-stream",
                size_bytes=len(content),
                checksum=checksum,
                metadata=dict(stat.metadata or {}),
            )
            return ArtifactObject(metadata, content)

        return await asyncio.to_thread(download)

    async def delete(self, tenant_id: str, object_key: str) -> None:
        await asyncio.to_thread(
            self._client.remove_object,
            self._bucket,
            self._object_name(tenant_id, object_key),
        )
