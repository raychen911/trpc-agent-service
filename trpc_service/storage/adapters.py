"""Projection-only vector and artifact adapters with tenant-bound APIs.

PostgreSQL remains canonical for memory intents.  These adapters are deliberately
rebuildable projections: the local vector store supports a working demo without
a model/embedding service and the S3 adapter is compatible with MinIO.
"""

from __future__ import annotations

import hashlib
import re
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Protocol


class StorageError(RuntimeError):
    """Artifact/vector invariant failed before a cross-tenant write occurred."""


def _require_tenant(tenant_id: str) -> str:
    if not tenant_id or tenant_id.strip() != tenant_id or "/" in tenant_id or "\\" in tenant_id:
        raise StorageError("a canonical tenant_id is required")
    return tenant_id


def _terms(text: str) -> Counter[str]:
    return Counter(re.findall(r"[\w-]+", text.lower()))


@dataclass(frozen=True, slots=True)
class VectorMatch:
    tenant_id: str
    document_id: str
    text: str
    score: float
    metadata: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class _VectorRecord:
    tenant_id: str
    document_id: str
    text: str
    terms: Counter[str]
    metadata: Mapping[str, object]
    version: int


class VectorStore(Protocol):
    def upsert(
        self,
        tenant_id: str,
        document_id: str,
        text: str,
        *,
        metadata: Mapping[str, object],
        version: int,
    ) -> None: ...

    def search(self, tenant_id: str, query: str, *, limit: int = 8) -> list[VectorMatch]: ...


class LocalVectorStore:
    """Functional lexical vector-style store whose query cannot omit tenant_id."""

    def __init__(self) -> None:
        self._lock = RLock()
        self._records: dict[tuple[str, str], _VectorRecord] = {}

    def upsert(
        self,
        tenant_id: str,
        document_id: str,
        text: str,
        *,
        metadata: Mapping[str, object],
        version: int,
    ) -> None:
        _require_tenant(tenant_id)
        if not document_id or version < 1:
            raise StorageError("document_id and version >= 1 are required")
        record = _VectorRecord(tenant_id, document_id, text, _terms(text), dict(metadata), version)
        with self._lock:
            known = self._records.get((tenant_id, document_id))
            if known and known.version > version:
                return
            self._records[(tenant_id, document_id)] = record

    def search(self, tenant_id: str, query: str, *, limit: int = 8) -> list[VectorMatch]:
        _require_tenant(tenant_id)
        if limit < 1:
            return []
        query_terms = _terms(query)
        with self._lock:
            matches = []
            for (record_tenant, _), record in self._records.items():
                if record_tenant != tenant_id:
                    continue
                intersection = sum(
                    min(count, record.terms.get(term, 0)) for term, count in query_terms.items()
                )
                if intersection:
                    score = intersection / max(1, sum(query_terms.values()))
                    matches.append(
                        VectorMatch(
                            tenant_id, record.document_id, record.text, score, record.metadata
                        )
                    )
        return sorted(matches, key=lambda item: (-item.score, item.document_id))[:limit]


class MemoryProjectionWorker:
    """Projects canonical memory into a vector index and tracks requested versions."""

    def __init__(self, vector_store: VectorStore) -> None:
        self._vector = vector_store
        self._versions: dict[tuple[str, str], tuple[int, int]] = {}

    def project(
        self,
        *,
        tenant_id: str,
        memory_id: str,
        content: str,
        version: int,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        _require_tenant(tenant_id)
        key = (tenant_id, memory_id)
        requested, projected = self._versions.get(key, (0, 0))
        requested = max(requested, version)
        self._versions[key] = (requested, projected)
        self._vector.upsert(tenant_id, memory_id, content, metadata=metadata or {}, version=version)
        self._versions[key] = (requested, max(projected, version))

    def projection_versions(self, tenant_id: str, memory_id: str) -> tuple[int, int]:
        _require_tenant(tenant_id)
        return self._versions.get((tenant_id, memory_id), (0, 0))

    def read_your_write(
        self,
        tenant_id: str,
        query: str,
        canonical_recent: Iterable[tuple[str, str, Mapping[str, object]]],
        *,
        limit: int = 8,
    ) -> list[VectorMatch]:
        """Merge fresh canonical rows with an eventually consistent projection."""

        _require_tenant(tenant_id)
        projected = self._vector.search(tenant_id, query, limit=limit)
        merged = {match.document_id: match for match in projected}
        query_terms = _terms(query)
        for memory_id, content, metadata in canonical_recent:
            score = sum(
                min(count, _terms(content).get(term, 0)) for term, count in query_terms.items()
            )
            if score:
                merged[memory_id] = VectorMatch(
                    tenant_id, memory_id, content, float(score), dict(metadata)
                )
        return sorted(merged.values(), key=lambda item: (-item.score, item.document_id))[:limit]


@dataclass(frozen=True, slots=True)
class ArtifactMetadata:
    tenant_id: str
    artifact_id: str
    object_uri: str
    checksum: str
    content_type: str
    size_bytes: int


class ArtifactStore(Protocol):
    def put(
        self, tenant_id: str, artifact_id: str, data: bytes, *, content_type: str
    ) -> ArtifactMetadata: ...

    def get(self, tenant_id: str, artifact_id: str) -> bytes: ...


class LocalArtifactStore:
    """Controlled local object store, useful when MinIO is unavailable."""

    _allowed_prefixes = (
        "text/",
        "application/pdf",
        "application/json",
        "image/",
        "audio/",
        "video/",
    )

    def __init__(self, root: str | Path = "data/artifacts", *, max_bytes: int = 25_000_000) -> None:
        self._root = Path(root)
        self._max_bytes = max_bytes
        self._metadata: dict[tuple[str, str], ArtifactMetadata] = {}

    @classmethod
    def _validate_type(cls, content_type: str) -> str:
        if not content_type or not content_type.startswith(cls._allowed_prefixes):
            raise StorageError("artifact content type is not allowed")
        return content_type

    def put(
        self, tenant_id: str, artifact_id: str, data: bytes, *, content_type: str
    ) -> ArtifactMetadata:
        _require_tenant(tenant_id)
        if not artifact_id or "/" in artifact_id or "\\" in artifact_id:
            raise StorageError("artifact_id must be a single opaque identifier")
        self._validate_type(content_type)
        if len(data) > self._max_bytes:
            raise StorageError("artifact exceeds configured size limit")
        checksum = hashlib.sha256(data).hexdigest()
        target = self._root / tenant_id / artifact_id
        target.parent.mkdir(mode=0o750, parents=True, exist_ok=True)
        # The content-address checksum in metadata makes a read-time mismatch
        # detectable even though local demo mode has no separate object database.
        target.write_bytes(data)
        metadata = ArtifactMetadata(
            tenant_id,
            artifact_id,
            f"local://{tenant_id}/{artifact_id}",
            checksum,
            content_type,
            len(data),
        )
        self._metadata[(tenant_id, artifact_id)] = metadata
        return metadata

    def get(self, tenant_id: str, artifact_id: str) -> bytes:
        _require_tenant(tenant_id)
        try:
            metadata = self._metadata[(tenant_id, artifact_id)]
        except KeyError as exc:
            raise StorageError("artifact does not exist in this tenant") from exc
        data = (self._root / tenant_id / artifact_id).read_bytes()
        if hashlib.sha256(data).hexdigest() != metadata.checksum:
            raise StorageError("artifact checksum mismatch")
        return data


class S3ArtifactStore:
    """S3/MinIO adapter; database rows retain its returned URI/checksum only."""

    def __init__(
        self,
        *,
        endpoint_url: str,
        bucket: str,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
    ) -> None:
        self._endpoint_url = endpoint_url
        self._bucket = bucket
        self._access_key = access_key
        self._secret_key = secret_key
        self._region = region

    def _client(self):
        import boto3

        return boto3.client(
            "s3",
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._access_key,
            aws_secret_access_key=self._secret_key,
            region_name=self._region,
        )

    def put(
        self, tenant_id: str, artifact_id: str, data: bytes, *, content_type: str
    ) -> ArtifactMetadata:
        _require_tenant(tenant_id)
        LocalArtifactStore._validate_type(content_type)
        checksum = hashlib.sha256(data).hexdigest()
        key = f"tenants/{tenant_id}/artifacts/{artifact_id}"
        self._client().put_object(
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
            Metadata={"sha256": checksum},
        )
        return ArtifactMetadata(
            tenant_id, artifact_id, f"s3://{self._bucket}/{key}", checksum, content_type, len(data)
        )

    def signed_download(
        self, tenant_id: str, artifact_id: str, *, expires_seconds: int = 300
    ) -> str:
        _require_tenant(tenant_id)
        if expires_seconds < 1 or expires_seconds > 3600:
            raise StorageError("signed download expiry must be between 1 and 3600 seconds")
        key = f"tenants/{tenant_id}/artifacts/{artifact_id}"
        return str(
            self._client().generate_presigned_url(
                "get_object", Params={"Bucket": self._bucket, "Key": key}, ExpiresIn=expires_seconds
            )
        )
