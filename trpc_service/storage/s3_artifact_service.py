"""Tenant-bound S3-compatible implementation of the SDK artifact service."""

from __future__ import annotations

import asyncio
import hashlib
import io
import re
import uuid
from collections.abc import Mapping
from typing import Any, Protocol

from trpc_agent_sdk.abc import ArtifactEntry, ArtifactId, ArtifactServiceABC, ArtifactVersion
from trpc_agent_sdk.artifacts import create_artifact_uri
from trpc_agent_sdk.types import Part

from trpc_service.storage.artifact_repository import (
    ArtifactRepository,
    ArtifactRepositoryDataError,
    ArtifactRepositoryUnavailableError,
    ArtifactVersionRecord,
)

_TENANT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_INTERNAL_KIND_KEY = "__trpc_artifact_kind"
_MAX_ARTIFACT_BYTES = 10 * 1024 * 1024


class UnsupportedArtifactError(ValueError):
    """The installed SDK Part kind is outside this platform's artifact contract."""


class ArtifactStorageUnavailableError(RuntimeError):
    """S3 or metadata storage is unavailable without exposing its details."""


class _ObjectResponse(Protocol):

    def read(self) -> bytes:
        ...

    def close(self) -> None:
        ...

    def release_conn(self) -> None:
        ...


class _ObjectClient(Protocol):

    def put_object(
        self,
        bucket_name: str,
        object_name: str,
        data: io.BytesIO,
        length: int,
        content_type: str,
    ) -> object:
        ...

    def get_object(self, bucket_name: str, object_name: str) -> _ObjectResponse:
        ...

    def stat_object(self, bucket_name: str, object_name: str) -> object:
        ...

    def remove_object(self, bucket_name: str, object_name: str) -> None:
        ...


class S3ArtifactService(ArtifactServiceABC):
    """Store one tenant's artifact payloads in a shared S3/MinIO bucket.

    Logical artifact identity remains in PostgreSQL.  Physical object names
    use the tenant namespace plus opaque UUIDs, so filenames and IM user IDs
    never become object-store paths.
    """

    def __init__(
        self,
        *,
        tenant_id: str,
        bucket: str,
        client: _ObjectClient,
        repository: ArtifactRepository,
        max_artifact_bytes: int = _MAX_ARTIFACT_BYTES,
    ) -> None:
        if not isinstance(tenant_id, str) or _TENANT_ID_PATTERN.fullmatch(tenant_id) is None:
            raise ValueError("artifact tenant ID is invalid")
        if not isinstance(bucket, str) or not bucket.strip():
            raise ValueError("artifact bucket is required")
        if isinstance(max_artifact_bytes, bool) or not isinstance(max_artifact_bytes, int) or max_artifact_bytes < 1:
            raise ValueError("artifact size cap is invalid")
        self._tenant_id = tenant_id
        self._bucket = bucket
        self._client = client
        self._repository = repository
        self._max_artifact_bytes = max_artifact_bytes

    async def save_artifact(
        self,
        *,
        artifact_id: ArtifactId,
        artifact: Part,
        metadata: Mapping[str, Any] | None = None,
    ) -> int:
        artifact_path = _artifact_path(artifact_id)
        payload, mime_type, kind = _payload_from_part(artifact, self._max_artifact_bytes)
        user_metadata = _metadata(metadata)
        stored_metadata = {**user_metadata, _INTERNAL_KIND_KEY: kind}
        digest = hashlib.sha256(payload).hexdigest()
        object_key = f"{self._tenant_id}/{uuid.uuid4().hex}/{uuid.uuid4().hex}"
        try:
            record = await self._repository.reserve_version(
                tenant_id=self._tenant_id,
                artifact_path=artifact_path,
                object_key=object_key,
                digest=digest,
                size_bytes=len(payload),
                mime_type=mime_type,
                metadata=stored_metadata,
            )
        except ArtifactRepositoryDataError:
            raise UnsupportedArtifactError("artifact metadata is invalid") from None
        except ArtifactRepositoryUnavailableError:
            raise ArtifactStorageUnavailableError("artifact object storage unavailable") from None

        try:
            await asyncio.to_thread(
                self._client.put_object,
                self._bucket,
                record.object_key,
                io.BytesIO(payload),
                len(payload),
                mime_type,
            )
            stat = await asyncio.to_thread(self._client.stat_object, self._bucket, record.object_key)
            if getattr(stat, "size", None) != len(payload):
                raise RuntimeError("object size mismatch")
            await self._repository.mark_available(record)
        except ArtifactRepositoryDataError:
            await self._cleanup(record.object_key)
            raise ArtifactStorageUnavailableError("artifact object storage unavailable") from None
        except Exception:
            await self._cleanup(record.object_key)
            raise ArtifactStorageUnavailableError("artifact object storage unavailable") from None
        return record.version

    async def load_artifact(
        self,
        *,
        artifact_id: ArtifactId,
        version: int | None = None,
    ) -> ArtifactEntry | None:
        artifact_path = _artifact_path(artifact_id)
        try:
            record = (await self._repository.latest_available(tenant_id=self._tenant_id, artifact_path=artifact_path)
                      if version is None else await self._repository.get_available(
                          tenant_id=self._tenant_id,
                          artifact_path=artifact_path,
                          version=version,
                      ))
        except (ArtifactRepositoryDataError, ArtifactRepositoryUnavailableError):
            raise ArtifactStorageUnavailableError("artifact object storage unavailable") from None
        if record is None:
            return None
        payload = await self._read_and_verify(record)
        part = _part_from_payload(payload, record)
        return ArtifactEntry(data=part, version=_sdk_version(artifact_id, record))

    async def list_artifact_keys(self, *, artifact_id: ArtifactId) -> list[str]:
        _artifact_path(artifact_id)
        try:
            return list(await self._repository.list_keys(
                tenant_id=self._tenant_id,
                app_name=artifact_id.app_name,
                user_id=artifact_id.user_id,
                session_id=artifact_id.session_id,
            ))
        except (ArtifactRepositoryDataError, ArtifactRepositoryUnavailableError):
            raise ArtifactStorageUnavailableError("artifact object storage unavailable") from None

    async def delete_artifact(self, *, artifact_id: ArtifactId) -> None:
        artifact_path = _artifact_path(artifact_id)
        try:
            records = await self._repository.mark_deleted(tenant_id=self._tenant_id, artifact_path=artifact_path)
        except (ArtifactRepositoryDataError, ArtifactRepositoryUnavailableError):
            raise ArtifactStorageUnavailableError("artifact object storage unavailable") from None
        for record in records:
            await self._cleanup(record.object_key)

    async def list_versions(self, *, artifact_id: ArtifactId) -> list[int]:
        artifact_path = _artifact_path(artifact_id)
        try:
            return [
                record.version for record in await self._repository.list_available(
                    tenant_id=self._tenant_id,
                    artifact_path=artifact_path,
                )
            ]
        except (ArtifactRepositoryDataError, ArtifactRepositoryUnavailableError):
            raise ArtifactStorageUnavailableError("artifact object storage unavailable") from None

    async def list_artifact_versions(self, *, artifact_id: ArtifactId) -> list[ArtifactVersion]:
        artifact_path = _artifact_path(artifact_id)
        try:
            records = await self._repository.list_available(tenant_id=self._tenant_id, artifact_path=artifact_path)
        except (ArtifactRepositoryDataError, ArtifactRepositoryUnavailableError):
            raise ArtifactStorageUnavailableError("artifact object storage unavailable") from None
        return [_sdk_version(artifact_id, record) for record in records]

    async def get_artifact_version(
        self,
        artifact_id: ArtifactId,
        version: int | None = None,
    ) -> ArtifactVersion | None:
        artifact_path = _artifact_path(artifact_id)
        try:
            record = (await self._repository.latest_available(tenant_id=self._tenant_id, artifact_path=artifact_path)
                      if version is None else await self._repository.get_available(
                          tenant_id=self._tenant_id,
                          artifact_path=artifact_path,
                          version=version,
                      ))
        except (ArtifactRepositoryDataError, ArtifactRepositoryUnavailableError):
            raise ArtifactStorageUnavailableError("artifact object storage unavailable") from None
        return _sdk_version(artifact_id, record) if record is not None else None

    async def _read_and_verify(self, record: ArtifactVersionRecord) -> bytes:

        def read() -> bytes:
            response = self._client.get_object(self._bucket, record.object_key)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()

        try:
            payload = await asyncio.to_thread(read)
        except Exception:
            raise ArtifactStorageUnavailableError("artifact object storage unavailable") from None
        if len(payload) != record.size_bytes or hashlib.sha256(payload).hexdigest() != record.digest:
            raise ArtifactStorageUnavailableError("artifact object storage unavailable")
        return payload

    async def _cleanup(self, object_key: str) -> None:
        """Best-effort cleanup: pending rows are not readable even if it fails."""
        try:
            await asyncio.to_thread(self._client.remove_object, self._bucket, object_key)
        except Exception:
            return


def _artifact_path(artifact_id: ArtifactId) -> str:
    if not isinstance(artifact_id, ArtifactId):
        raise UnsupportedArtifactError("artifact ID is invalid")
    values = (artifact_id.app_name, artifact_id.user_id, artifact_id.filename)
    if any(not isinstance(value, str) or not value or len(value) > 255 for value in values):
        raise UnsupportedArtifactError("artifact ID is invalid")
    if artifact_id.session_id is not None and (not isinstance(artifact_id.session_id, str) or not artifact_id.session_id
                                               or len(artifact_id.session_id) > 255):
        raise UnsupportedArtifactError("artifact ID is invalid")
    if any("\x00" in value
           for value in values) or (artifact_id.session_id is not None and "\x00" in artifact_id.session_id):
        raise UnsupportedArtifactError("artifact ID is invalid")
    return "/".join((artifact_id.app_name, artifact_id.user_id, artifact_id.session_id or "user", artifact_id.filename))


def _payload_from_part(part: Part, max_bytes: int) -> tuple[bytes, str, str]:
    if not isinstance(part, Part):
        raise UnsupportedArtifactError("artifact must be a Part")
    if part.inline_data is not None:
        payload = part.inline_data.data
        mime_type = part.inline_data.mime_type or "application/octet-stream"
        kind = "inline"
    elif part.text is not None:
        payload = part.text.encode("utf-8")
        mime_type = "text/plain"
        kind = "text"
    elif part.file_data is not None:
        raise UnsupportedArtifactError("remote artifact URLs are not supported")
    else:
        raise UnsupportedArtifactError("artifact part type is not supported")
    if not isinstance(payload, bytes) or len(payload) == 0 or len(payload) > max_bytes:
        raise UnsupportedArtifactError("artifact payload size is not supported")
    if not isinstance(mime_type, str) or not mime_type.strip() or len(mime_type) > 255:
        raise UnsupportedArtifactError("artifact MIME type is invalid")
    return payload, mime_type, kind


def _metadata(metadata: Mapping[str, Any] | None) -> dict[str, Any]:
    if metadata is None:
        return {}
    if not isinstance(metadata, Mapping) or _INTERNAL_KIND_KEY in metadata:
        raise UnsupportedArtifactError("artifact metadata is invalid")
    return dict(metadata)


def _part_from_payload(payload: bytes, record: ArtifactVersionRecord) -> Part:
    if record.metadata.get(_INTERNAL_KIND_KEY) == "text":
        try:
            return Part(text=payload.decode("utf-8"))
        except UnicodeDecodeError:
            raise ArtifactStorageUnavailableError("artifact object storage unavailable") from None
    from google.genai.types import Blob

    return Part(inline_data=Blob(data=payload, mime_type=record.mime_type))


def _sdk_version(artifact_id: ArtifactId, record: ArtifactVersionRecord) -> ArtifactVersion:
    metadata = dict(record.metadata)
    metadata.pop(_INTERNAL_KIND_KEY, None)
    return ArtifactVersion(
        version=record.version,
        canonical_uri=create_artifact_uri(artifact_id, record.version),
        custom_metadata=metadata,
        create_time=record.created_at.timestamp(),
        mime_type=record.mime_type,
    )


__all__ = [
    "ArtifactStorageUnavailableError",
    "S3ArtifactService",
    "UnsupportedArtifactError",
]
