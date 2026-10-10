"""Tenant-scoped staged artifact writes and safe media ingestion."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Protocol
from uuid import uuid4

DEFAULT_MAX_ARTIFACT_SIZE = 25 * 1024 * 1024
_STAGING_KEY_RE = re.compile(r"^tenants/[0-9a-f]{64}/staging/[0-9a-f-]{36}$")


class S3Client(Protocol):
    def put_object(self, **kwargs: Any) -> Any: ...

    def head_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def copy_object(self, **kwargs: Any) -> Any: ...

    def delete_object(self, **kwargs: Any) -> Any: ...

    def get_object(self, **kwargs: Any) -> Mapping[str, Any]: ...

    def list_objects_v2(self, **kwargs: Any) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class ArtifactMetadata:
    """Committed artifact metadata; bytes and provider keys are not stored."""

    tenant_id: str
    artifact_id: str
    idempotency_key: str
    channel: str
    external_message_id: str
    provider_media_id: str
    object_key: str
    checksum: str
    size_bytes: int
    filename: str | None = None
    content_type: str | None = None
    status: str = "committed"

    @property
    def metadata(self) -> Mapping[str, object]:
        return asdict(self)

    def model_dump(self) -> dict[str, object]:
        return dict(self.metadata)


ArtifactRecord = ArtifactMetadata


def _media_key_parts(
    tenant_id: str,
    channel: str | Enum,
    external_message_id: str,
    provider_media_id: str,
) -> bytes:
    channel_value = channel.value if isinstance(channel, Enum) else str(channel)
    return json.dumps(
        [tenant_id, channel_value, external_message_id, provider_media_id],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def media_idempotency_key(
    tenant_id: str,
    channel: str | Enum,
    external_message_id: str,
    provider_media_id: str,
) -> str:
    """Return an opaque key stable for one tenant/provider media tuple."""

    return hashlib.sha256(
        _media_key_parts(tenant_id, channel, external_message_id, provider_media_id)
    ).hexdigest()


build_media_idempotency_key = media_idempotency_key


class _ArtifactStoreBase:
    def __init__(self, *, max_size_bytes: int) -> None:
        if max_size_bytes <= 0:
            raise ValueError("max_size_bytes must be positive")
        self._max_size_bytes = max_size_bytes
        self._records: dict[tuple[str, str], ArtifactMetadata] = {}
        self._ingest_lock = asyncio.Lock()

    @staticmethod
    def _scope(tenant_id: str) -> str:
        return hashlib.sha256(tenant_id.encode()).hexdigest()

    @classmethod
    def _staging_prefix(cls, tenant_id: str) -> str:
        return f"tenants/{cls._scope(tenant_id)}/staging/"

    @classmethod
    def _artifact_prefix(cls, tenant_id: str) -> str:
        return f"tenants/{cls._scope(tenant_id)}/artifacts/"

    @classmethod
    def _target_key(cls, tenant_id: str, artifact_id: str, checksum: str) -> str:
        artifact_hash = hashlib.sha256(artifact_id.encode()).hexdigest()
        return f"{cls._artifact_prefix(tenant_id)}{artifact_hash}/{checksum}"

    @staticmethod
    def _required(value: str, field: str) -> str:
        if not isinstance(value, str) or not value:
            raise ValueError(f"{field} is required")
        if len(value) > 4096:
            raise ValueError(f"{field} is too long")
        return value

    @staticmethod
    def _channel_value(channel: str | Enum) -> str:
        return channel.value if isinstance(channel, Enum) else str(channel)

    def _validate_content(self, content: bytes, checksum: str | None) -> tuple[bytes, str]:
        if not isinstance(content, bytes):
            raise TypeError("artifact content must be bytes")
        if len(content) > self._max_size_bytes:
            raise ValueError("artifact exceeds maximum size")
        actual = hashlib.sha256(content).hexdigest()
        if checksum is not None and (
            not isinstance(checksum, str) or not hmac.compare_digest(actual, checksum.lower())
        ):
            raise ValueError("artifact checksum mismatch")
        return content, actual

    def _media_inputs(
        self,
        tenant_id: str,
        channel: str | Enum,
        external_message_id: str,
        provider_media_id: str,
        content: bytes,
        checksum: str | None,
    ) -> tuple[str, str, str, str, bytes, str]:
        tenant = self._required(tenant_id, "tenant_id")
        channel_value = self._required(self._channel_value(channel), "channel")
        external = self._required(external_message_id, "external_message_id")
        provider = self._required(provider_media_id, "provider_media_id")
        checked_content, checked_sum = self._validate_content(content, checksum)
        return tenant, channel_value, external, provider, checked_content, checked_sum

    @staticmethod
    def _optional(value: str | None, field: str) -> str | None:
        if value is not None and not isinstance(value, str):
            raise TypeError(f"{field} must be a string")
        if value is not None and len(value) > 1024:
            raise ValueError(f"{field} is too long")
        return value

    @staticmethod
    def _object_metadata(
        artifact_id: str,
        checksum: str,
        *,
        channel: str | None = None,
        external_message_id: str | None = None,
        provider_media_id: str | None = None,
    ) -> dict[str, str]:
        values = {
            "sha256": checksum,
            "artifact-id-hash": hashlib.sha256(artifact_id.encode()).hexdigest(),
        }
        for name, value in (
            ("channel", channel),
            ("external-message-id", external_message_id),
            ("provider-media-id", provider_media_id),
        ):
            if value is not None:
                values[f"{name}-hash"] = hashlib.sha256(value.encode()).hexdigest()
        return values

    @staticmethod
    def _merge_metadata(target: dict[str, str], extra: Mapping[str, str] | None) -> None:
        if extra is None:
            return
        forbidden = (
            "body",
            "content",
            "secret",
            "token",
            "password",
            "credential",
            "authorization",
            "encryption-key",
        )
        for name, value in extra.items():
            if not isinstance(name, str) or not isinstance(value, str):
                raise TypeError("artifact metadata must contain strings")
            lowered = name.lower()
            if lowered in {"sha256", "artifact-id-hash"} or any(
                marker in lowered for marker in forbidden
            ):
                raise ValueError("artifact metadata contains prohibited content")
            target[name] = value

    @staticmethod
    def _checksum(metadata: Mapping[str, Any]) -> str:
        checksum = metadata.get("sha256")
        if not isinstance(checksum, str) or len(checksum) != 64:
            raise ValueError("staged artifact has no valid checksum metadata")
        try:
            int(checksum, 16)
        except ValueError as exc:
            raise ValueError("staged artifact has no valid checksum metadata") from exc
        return checksum.lower()

    @staticmethod
    def _record(
        tenant_id: str,
        artifact_id: str,
        idempotency_key: str,
        channel: str,
        external_message_id: str,
        provider_media_id: str,
        object_key: str,
        checksum: str,
        size_bytes: int,
        filename: str | None,
        content_type: str | None,
    ) -> ArtifactMetadata:
        return ArtifactMetadata(
            tenant_id=tenant_id,
            artifact_id=artifact_id,
            idempotency_key=idempotency_key,
            channel=channel,
            external_message_id=external_message_id,
            provider_media_id=provider_media_id,
            object_key=object_key,
            checksum=checksum,
            size_bytes=size_bytes,
            filename=filename,
            content_type=content_type,
        )

    async def ingest_media(
        self,
        tenant_id: str,
        channel: str | Enum,
        external_message_id: str,
        provider_media_id: str,
        content: bytes,
        *,
        checksum: str | None = None,
        filename: str | None = None,
        content_type: str | None = None,
    ) -> ArtifactMetadata:
        """Upload, verify, commit, and register downloaded provider media."""

        tenant, channel_value, external, provider, body, digest = self._media_inputs(
            tenant_id,
            channel,
            external_message_id,
            provider_media_id,
            content,
            checksum,
        )
        filename = self._optional(filename, "filename")
        content_type = self._optional(content_type, "content_type")
        idem = media_idempotency_key(tenant, channel_value, external, provider)
        artifact_id = idem
        record_key = (tenant, idem)
        async with self._ingest_lock:
            existing = self._records.get(record_key)
            if existing is not None:
                if existing.checksum != digest or existing.size_bytes != len(body):
                    raise ValueError("artifact idempotency key conflicts with content")
                return existing

            target = self._target_key(tenant, artifact_id, digest)
            existing_head = await self._head_if_present(target)
            if existing_head is not None:
                if self._checksum(existing_head.get("Metadata", {})) != digest:
                    raise ValueError("artifact idempotency key conflicts with content")
                record = self._record(
                    tenant,
                    artifact_id,
                    idem,
                    channel_value,
                    external,
                    provider,
                    target,
                    digest,
                    len(body),
                    filename,
                    content_type,
                )
                self._records[record_key] = record
                return record

            staging_key: str | None = None
            committed_key: str | None = None
            try:
                extra = self._object_metadata(
                    artifact_id,
                    digest,
                    channel=channel_value,
                    external_message_id=external,
                    provider_media_id=provider,
                )
                extra.pop("sha256", None)
                extra.pop("artifact-id-hash", None)
                staging_key = await self.stage(
                    tenant, artifact_id, body, checksum=digest, metadata=extra
                )
                staged_head = await self._head_if_present(staging_key)
                if staged_head is None or self._checksum(staged_head.get("Metadata", {})) != digest:
                    raise ValueError("staged artifact checksum mismatch")
                size = staged_head.get("ContentLength")
                if size is not None and size != len(body):
                    raise ValueError("staged artifact size mismatch")
                committed_key = await self.commit(
                    tenant, artifact_id, staging_key, expected_checksum=digest
                )
                record = self._record(
                    tenant,
                    artifact_id,
                    idem,
                    channel_value,
                    external,
                    provider,
                    committed_key,
                    digest,
                    len(body),
                    filename,
                    content_type,
                )
                self._records[record_key] = record
                return record
            except BaseException:
                if staging_key is not None:
                    await self._best_effort_delete(staging_key)
                if committed_key is not None:
                    await self._best_effort_delete(committed_key)
                raise

    async def ingest(self, *args: Any, **kwargs: Any) -> ArtifactMetadata:
        return await self.ingest_media(*args, **kwargs)

    async def _best_effort_delete(self, key: str) -> None:
        try:
            await self._delete_any(key)
        except Exception:
            return

    async def stage(
        self,
        tenant_id: str,
        artifact_id: str,
        content: bytes,
        *,
        checksum: str,
        metadata: Mapping[str, str] | None = None,
    ) -> str:
        raise NotImplementedError

    async def commit(
        self,
        tenant_id: str,
        artifact_id: str,
        staged_key: str,
        *,
        expected_checksum: str | None = None,
    ) -> str:
        raise NotImplementedError

    async def discard(self, staged_key: str, *, tenant_id: str | None = None) -> None:
        raise NotImplementedError

    async def read(self, tenant_id: str, object_key: str) -> bytes:
        raise NotImplementedError

    async def _head_if_present(self, key: str) -> Mapping[str, Any] | None:
        raise NotImplementedError

    async def _delete_any(self, key: str) -> None:
        raise NotImplementedError


class S3ArtifactStore(_ArtifactStoreBase):
    """S3-compatible artifact store preserving the original staged API."""

    def __init__(
        self,
        client: S3Client,
        *,
        bucket: str,
        max_size_bytes: int = DEFAULT_MAX_ARTIFACT_SIZE,
    ) -> None:
        super().__init__(max_size_bytes=max_size_bytes)
        self._client = client
        self._bucket = bucket

    media_idempotency_key = staticmethod(media_idempotency_key)
    idempotency_key_for_media = staticmethod(media_idempotency_key)

    async def stage(
        self,
        tenant_id: str,
        artifact_id: str,
        content: bytes,
        *,
        checksum: str,
        metadata: Mapping[str, str] | None = None,
    ) -> str:
        tenant = self._required(tenant_id, "tenant_id")
        artifact = self._required(artifact_id, "artifact_id")
        body, digest = self._validate_content(content, checksum)
        key = f"{self._staging_prefix(tenant)}{uuid4()}"
        safe_metadata = self._object_metadata(artifact, digest)
        self._merge_metadata(safe_metadata, metadata)
        try:
            await asyncio.to_thread(
                self._client.put_object,
                Bucket=self._bucket,
                Key=key,
                Body=body,
                Metadata=safe_metadata,
            )
        except BaseException:
            await self._best_effort_delete(key)
            raise
        return key

    async def commit(
        self,
        tenant_id: str,
        artifact_id: str,
        staged_key: str,
        *,
        expected_checksum: str | None = None,
    ) -> str:
        tenant = self._required(tenant_id, "tenant_id")
        artifact = self._required(artifact_id, "artifact_id")
        if not staged_key.startswith(self._staging_prefix(tenant)):
            raise ValueError("staged artifact does not belong to tenant")
        head = await asyncio.to_thread(
            self._client.head_object, Bucket=self._bucket, Key=staged_key
        )
        digest = self._checksum(head.get("Metadata", {}))
        if expected_checksum is not None and not hmac.compare_digest(
            digest, expected_checksum.lower()
        ):
            raise ValueError("staged artifact checksum mismatch")
        target = self._target_key(tenant, artifact, digest)
        await asyncio.to_thread(
            self._client.copy_object,
            Bucket=self._bucket,
            Key=target,
            CopySource={"Bucket": self._bucket, "Key": staged_key},
            MetadataDirective="COPY",
        )
        await asyncio.to_thread(self._client.delete_object, Bucket=self._bucket, Key=staged_key)
        return target

    async def discard(self, staged_key: str, *, tenant_id: str | None = None) -> None:
        if tenant_id is not None and not staged_key.startswith(self._staging_prefix(tenant_id)):
            raise ValueError("staged artifact does not belong to tenant")
        await self._delete_any(staged_key)

    async def list_staged(
        self,
        *,
        older_than: datetime,
        limit: int,
        continuation_token: str | None = None,
    ) -> tuple[tuple[str, ...], str | None]:
        """Return one bounded S3 page of expired staging keys for orphan cleanup."""

        if older_than.tzinfo is None or older_than.utcoffset() is None:
            raise ValueError("older_than must be timezone-aware")
        if limit < 1 or limit > 1_000:
            raise ValueError("staging list limit must be between 1 and 1000")
        options: dict[str, Any] = {
            "Bucket": self._bucket,
            "Prefix": "tenants/",
            "MaxKeys": limit,
        }
        if continuation_token:
            options["ContinuationToken"] = continuation_token
        response = await asyncio.to_thread(self._client.list_objects_v2, **options)
        keys: list[str] = []
        for item in response.get("Contents", ()):
            if not isinstance(item, Mapping):
                continue
            key = item.get("Key")
            modified = item.get("LastModified")
            if (
                isinstance(key, str)
                and _STAGING_KEY_RE.fullmatch(key) is not None
                and isinstance(modified, datetime)
                and modified.tzinfo is not None
                and modified <= older_than
            ):
                keys.append(key)
        next_token = response.get("NextContinuationToken")
        return tuple(keys), str(next_token) if isinstance(next_token, str) else None

    async def discard_unreferenced_staged(self, staged_key: str) -> None:
        """Delete only a structurally valid staging key discovered by bucket scan."""

        if _STAGING_KEY_RE.fullmatch(staged_key) is None:
            raise ValueError("object is not a valid staged artifact key")
        await self._delete_any(staged_key)

    async def read(self, tenant_id: str, object_key: str) -> bytes:
        tenant = self._required(tenant_id, "tenant_id")
        if not object_key.startswith(self._artifact_prefix(tenant)):
            raise ValueError("artifact does not belong to tenant")
        head = await asyncio.to_thread(
            self._client.head_object, Bucket=self._bucket, Key=object_key
        )
        expected = self._checksum(head.get("Metadata", {}))
        response = await asyncio.to_thread(
            self._client.get_object, Bucket=self._bucket, Key=object_key
        )
        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise ValueError("artifact object has no readable body")
        content = await asyncio.to_thread(body.read)
        if not isinstance(content, bytes) or not hmac.compare_digest(
            hashlib.sha256(content).hexdigest(), expected
        ):
            raise ValueError("artifact checksum mismatch")
        if len(content) > self._max_size_bytes:
            raise ValueError("artifact exceeds maximum size")
        return content

    async def _head_if_present(self, key: str) -> Mapping[str, Any] | None:
        try:
            return await asyncio.to_thread(self._client.head_object, Bucket=self._bucket, Key=key)
        except (FileNotFoundError, KeyError):
            return None
        except Exception as exc:
            response = getattr(exc, "response", None)
            error = response.get("Error") if isinstance(response, Mapping) else None
            code = error.get("Code") if isinstance(error, Mapping) else None
            if str(code) in {"404", "NoSuchKey", "NotFound"}:
                return None
            raise

    async def _delete_any(self, key: str) -> None:
        await asyncio.to_thread(self._client.delete_object, Bucket=self._bucket, Key=key)


class InMemoryArtifactStore(_ArtifactStoreBase):
    """Offline artifact implementation with the same staged semantics."""

    def __init__(self, *, max_size_bytes: int = DEFAULT_MAX_ARTIFACT_SIZE) -> None:
        super().__init__(max_size_bytes=max_size_bytes)
        self.objects: dict[str, tuple[bytes, dict[str, str]]] = {}

    media_idempotency_key = staticmethod(media_idempotency_key)
    idempotency_key_for_media = staticmethod(media_idempotency_key)

    async def stage(
        self,
        tenant_id: str,
        artifact_id: str,
        content: bytes,
        *,
        checksum: str,
        metadata: Mapping[str, str] | None = None,
    ) -> str:
        tenant = self._required(tenant_id, "tenant_id")
        artifact = self._required(artifact_id, "artifact_id")
        body, digest = self._validate_content(content, checksum)
        safe_metadata = self._object_metadata(artifact, digest)
        self._merge_metadata(safe_metadata, metadata)
        key = f"{self._staging_prefix(tenant)}{uuid4()}"
        self.objects[key] = (body, safe_metadata)
        return key

    async def commit(
        self,
        tenant_id: str,
        artifact_id: str,
        staged_key: str,
        *,
        expected_checksum: str | None = None,
    ) -> str:
        tenant = self._required(tenant_id, "tenant_id")
        artifact = self._required(artifact_id, "artifact_id")
        if not staged_key.startswith(self._staging_prefix(tenant)):
            raise ValueError("staged artifact does not belong to tenant")
        try:
            body, metadata = self.objects[staged_key]
        except KeyError as exc:
            raise LookupError("staged artifact does not exist") from exc
        digest = self._checksum(metadata)
        if expected_checksum is not None and not hmac.compare_digest(
            digest, expected_checksum.lower()
        ):
            raise ValueError("staged artifact checksum mismatch")
        target = self._target_key(tenant, artifact, digest)
        self.objects[target] = (body, dict(metadata))
        self.objects.pop(staged_key, None)
        return target

    async def discard(self, staged_key: str, *, tenant_id: str | None = None) -> None:
        if tenant_id is not None and not staged_key.startswith(self._staging_prefix(tenant_id)):
            raise ValueError("staged artifact does not belong to tenant")
        self.objects.pop(staged_key, None)

    async def read(self, tenant_id: str, object_key: str) -> bytes:
        tenant = self._required(tenant_id, "tenant_id")
        if not object_key.startswith(self._artifact_prefix(tenant)):
            raise ValueError("artifact does not belong to tenant")
        try:
            body, metadata = self.objects[object_key]
        except KeyError as exc:
            raise LookupError("artifact does not exist") from exc
        if hashlib.sha256(body).hexdigest() != self._checksum(metadata):
            raise ValueError("artifact checksum mismatch")
        return body

    async def _head_if_present(self, key: str) -> Mapping[str, Any] | None:
        value = self.objects.get(key)
        if value is None:
            return None
        body, metadata = value
        return {"Metadata": metadata, "ContentLength": len(body)}

    async def _delete_any(self, key: str) -> None:
        self.objects.pop(key, None)


__all__ = [
    "DEFAULT_MAX_ARTIFACT_SIZE",
    "ArtifactMetadata",
    "ArtifactRecord",
    "InMemoryArtifactStore",
    "S3ArtifactStore",
    "build_media_idempotency_key",
    "media_idempotency_key",
]
