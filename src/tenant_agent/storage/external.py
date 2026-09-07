"""Filesystem, S3, Qdrant, and external-memory adapters."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import struct
import uuid
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import Any

import httpx

from tenant_agent.models import ArtifactRecord, KnowledgeRecord, MemoryRecord
from tenant_agent.storage.base import ConcurrentWriteError, same_artifact_payload


def _tenant_segment(tenant_id: str) -> str:
    return hashlib.sha256(tenant_id.encode()).hexdigest()[:20]


def _encode_artifact_bundle(record: ArtifactRecord, content: bytes) -> bytes:
    metadata = record.model_dump_json().encode()
    return struct.pack(">Q", len(metadata)) + metadata + content


def _decode_artifact_bundle(payload: bytes) -> tuple[ArtifactRecord, bytes]:
    if len(payload) < 8:
        raise ValueError("artifact bundle is truncated")
    metadata_size = struct.unpack(">Q", payload[:8])[0]
    metadata_end = 8 + metadata_size
    if metadata_end > len(payload):
        raise ValueError("artifact bundle metadata is truncated")
    record = ArtifactRecord.model_validate_json(payload[8:metadata_end])
    return record, payload[metadata_end:]


class FilesystemArtifactRepository:
    backend_name = "filesystem"

    def __init__(self, root: Path) -> None:
        self.root = root.resolve()

    async def initialize(self) -> None:
        await asyncio.to_thread(self.root.mkdir, parents=True, exist_ok=True)

    async def close(self) -> None:
        return None

    async def healthcheck(self) -> bool:
        return self.root.is_dir()

    def _paths(self, tenant_id: str, artifact_id: str) -> tuple[Path, Path]:
        safe_id = hashlib.sha256(artifact_id.encode()).hexdigest()
        directory = self.root / _tenant_segment(tenant_id) / safe_id[:2]
        return directory / f"{safe_id}.bin", directory / f"{safe_id}.json"

    def _bundle_path(self, tenant_id: str, artifact_id: str, version: int) -> Path:
        content_path, _ = self._paths(tenant_id, artifact_id)
        return content_path.with_name(f"{content_path.stem}.v{version:020d}.bundle")

    async def put_artifact(self, record: ArtifactRecord, content: bytes) -> None:
        if len(content) != record.size_bytes or hashlib.sha256(content).hexdigest() != record.checksum_sha256:
            raise ValueError("artifact size or checksum does not match its content")
        current = await self.get_artifact(record.tenant_id, record.artifact_id)
        if current and current[0].version > record.version:
            return
        if current and current[0].version == record.version:
            if not same_artifact_payload(current[0], current[1], record, content):
                raise ConcurrentWriteError("artifact version is immutable")
            return
        target = self._bundle_path(record.tenant_id, record.artifact_id, record.version)
        bundle = _encode_artifact_bundle(record, content)

        def write() -> None:
            target.parent.mkdir(parents=True, exist_ok=True)
            nonce = uuid.uuid4().hex
            temporary = target.with_name(f"{target.name}.{nonce}.tmp")
            try:
                with temporary.open("xb") as handle:
                    handle.write(bundle)
                    handle.flush()
                    os.fsync(handle.fileno())
                try:
                    os.link(temporary, target)
                except FileExistsError as exc:
                    existing_record, existing_content = _decode_artifact_bundle(target.read_bytes())
                    if not same_artifact_payload(
                        existing_record,
                        existing_content,
                        record,
                        content,
                    ):
                        raise ConcurrentWriteError("artifact version is immutable") from exc
            finally:
                temporary.unlink(missing_ok=True)

        await asyncio.to_thread(write)

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> tuple[ArtifactRecord, bytes] | None:
        content_path, metadata_path = self._paths(tenant_id, artifact_id)
        bundle_pattern = f"{content_path.stem}.v*.bundle"
        bundles = await asyncio.to_thread(
            lambda: sorted(content_path.parent.glob(bundle_pattern)) if content_path.parent.exists() else []
        )
        if bundles:
            record, content = _decode_artifact_bundle(await asyncio.to_thread(bundles[-1].read_bytes))
        else:
            content_exists = content_path.exists()
            metadata_exists = metadata_path.exists()
            if not content_exists and not metadata_exists:
                return None
            if content_exists != metadata_exists:
                raise ValueError("legacy artifact pair is incomplete")
            record_raw, content = await asyncio.gather(
                asyncio.to_thread(metadata_path.read_text, encoding="utf-8"),
                asyncio.to_thread(content_path.read_bytes),
            )
            record = ArtifactRecord.model_validate_json(record_raw)
        if record.tenant_id != tenant_id or record.artifact_id != artifact_id:
            raise ValueError("artifact metadata scope mismatch")
        if len(content) != record.size_bytes or hashlib.sha256(content).hexdigest() != record.checksum_sha256:
            raise ValueError("stored artifact failed size or checksum verification")
        return record, content

    async def iter_artifacts(self, tenant_id: str) -> AsyncIterator[ArtifactRecord]:
        directory = self.root / _tenant_segment(tenant_id)
        if not directory.exists():
            return
        bundle_files = await asyncio.to_thread(lambda: tuple(directory.rglob("*.bundle")))
        latest: dict[str, Path] = {}
        for path in bundle_files:
            base = path.name.rsplit(".v", 1)[0]
            latest[base] = max(path, latest.get(base, path))
        for path in sorted(latest.values()):
            record, content = _decode_artifact_bundle(await asyncio.to_thread(path.read_bytes))
            if (
                len(content) != record.size_bytes
                or hashlib.sha256(content).hexdigest() != record.checksum_sha256
            ):
                raise ValueError("stored artifact failed size or checksum verification")
            if record.tenant_id == tenant_id:
                yield record
        files = await asyncio.to_thread(lambda: tuple(directory.rglob("*.json")))
        for path in files:
            if path.stem in latest:
                continue
            raw = await asyncio.to_thread(path.read_text, encoding="utf-8")
            record = ArtifactRecord.model_validate_json(raw)
            legacy_content = path.with_suffix(".bin")
            if not legacy_content.exists():
                raise ValueError("legacy artifact pair is incomplete")
            if record.tenant_id == tenant_id:
                yield record


class S3ArtifactRepository:
    """S3-compatible artifact storage with tenant-prefixed object keys."""

    backend_name = "s3"

    def __init__(self, connection_json: str, *, namespace: str = "artifacts") -> None:
        config = json.loads(connection_json)
        self.bucket = config["bucket"]
        self.prefix = f"{config.get('prefix', namespace).strip('/')}/"
        self._client_kwargs = {
            key: value
            for key, value in config.items()
            if key
            in {
                "endpoint_url",
                "aws_access_key_id",
                "aws_secret_access_key",
                "aws_session_token",
                "region_name",
                "use_ssl",
                "verify",
            }
        }
        try:
            import aioboto3
        except ImportError as exc:
            raise RuntimeError("install the 'production' extra to use S3") from exc
        self._session = aioboto3.Session()

    async def initialize(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def healthcheck(self) -> bool:
        async with self._session.client("s3", **self._client_kwargs) as client:
            await client.head_bucket(Bucket=self.bucket)
        return True

    def _base_key(self, tenant_id: str, artifact_id: str) -> str:
        safe_id = hashlib.sha256(artifact_id.encode()).hexdigest()
        return f"{self.prefix}{_tenant_segment(tenant_id)}/{safe_id}"

    @staticmethod
    def _bundle_key(base: str, version: int) -> str:
        return f"{base}.v{version:020d}.bundle"

    @staticmethod
    def _encode_bundle(record: ArtifactRecord, content: bytes) -> bytes:
        return _encode_artifact_bundle(record, content)

    @staticmethod
    def _decode_bundle(payload: bytes) -> tuple[ArtifactRecord, bytes]:
        return _decode_artifact_bundle(payload)

    @staticmethod
    def _error_code(exc: Exception) -> str:
        response = getattr(exc, "response", {})
        error = response.get("Error", {}) if isinstance(response, dict) else {}
        return str(error.get("Code", "")) if isinstance(error, dict) else ""

    @classmethod
    def _precondition_failed(cls, exc: Exception) -> bool:
        return cls._error_code(exc) in {"PreconditionFailed", "412"} or (
            exc.__class__.__name__ == "PreconditionFailed"
        )

    @classmethod
    def _conditional_conflict(cls, exc: Exception) -> bool:
        return cls._error_code(exc) in {"ConditionalRequestConflict", "409"} or (
            exc.__class__.__name__ == "ConditionalRequestConflict"
        )

    async def _load_bundle(self, client: Any, key: str) -> tuple[ArtifactRecord, bytes]:
        response = await client.get_object(Bucket=self.bucket, Key=key)
        return self._decode_bundle(await response["Body"].read())

    async def put_artifact(self, record: ArtifactRecord, content: bytes) -> None:
        if len(content) != record.size_bytes or hashlib.sha256(content).hexdigest() != record.checksum_sha256:
            raise ValueError("artifact size or checksum does not match its content")
        current = await self.get_artifact(record.tenant_id, record.artifact_id)
        if current and current[0].version > record.version:
            return
        if current and current[0].version == record.version:
            if not same_artifact_payload(current[0], current[1], record, content):
                raise ConcurrentWriteError("artifact version is immutable")
            return
        base = self._base_key(record.tenant_id, record.artifact_id)
        key = self._bundle_key(base, record.version)
        bundle = self._encode_bundle(record, content)
        async with self._session.client("s3", **self._client_kwargs) as client:
            for attempt in range(5):
                try:
                    await client.put_object(
                        Bucket=self.bucket,
                        Key=key,
                        Body=bundle,
                        ContentType="application/octet-stream",
                        ServerSideEncryption="AES256",
                        IfNoneMatch="*",
                    )
                    break
                except Exception as exc:
                    if self._conditional_conflict(exc) and attempt < 4:
                        await asyncio.sleep(0.01 * (2**attempt))
                        continue
                    if not self._precondition_failed(exc):
                        raise
                    existing_record, existing_content = await self._load_bundle(client, key)
                    if not same_artifact_payload(existing_record, existing_content, record, content):
                        raise ConcurrentWriteError("artifact version is immutable") from exc
                    break

    async def get_artifact(self, tenant_id: str, artifact_id: str) -> tuple[ArtifactRecord, bytes] | None:
        base = self._base_key(tenant_id, artifact_id)
        async with self._session.client("s3", **self._client_kwargs) as client:
            bundle_keys: list[str] = []
            token: str | None = None
            while True:
                kwargs = {"Bucket": self.bucket, "Prefix": f"{base}.v"}
                if token:
                    kwargs["ContinuationToken"] = token
                page = await client.list_objects_v2(**kwargs)
                bundle_keys.extend(
                    item["Key"] for item in page.get("Contents", []) if item["Key"].endswith(".bundle")
                )
                if not page.get("IsTruncated"):
                    break
                token = page.get("NextContinuationToken")
            bundle_keys.sort()
            if bundle_keys:
                record, content = await self._load_bundle(client, bundle_keys[-1])
            else:
                # Backward-compatible read for pre-bundle deployments.
                metadata_response: Any | None = None
                content_response: Any | None = None
                try:
                    metadata_response = await client.get_object(Bucket=self.bucket, Key=f"{base}.json")
                except client.exceptions.NoSuchKey:
                    pass
                try:
                    content_response = await client.get_object(Bucket=self.bucket, Key=f"{base}.bin")
                except client.exceptions.NoSuchKey:
                    pass
                if metadata_response is None and content_response is None:
                    return None
                if metadata_response is None or content_response is None:
                    raise ValueError("legacy artifact pair is incomplete")
                metadata_raw = await metadata_response["Body"].read()
                content = await content_response["Body"].read()
                record = ArtifactRecord.model_validate_json(metadata_raw)
        if record.tenant_id != tenant_id or record.artifact_id != artifact_id:
            raise ValueError("artifact metadata scope mismatch")
        if len(content) != record.size_bytes or hashlib.sha256(content).hexdigest() != record.checksum_sha256:
            raise ValueError("stored artifact failed size or checksum verification")
        return record, content

    async def iter_artifacts(self, tenant_id: str) -> AsyncIterator[ArtifactRecord]:
        prefix = f"{self.prefix}{_tenant_segment(tenant_id)}/"
        token: str | None = None
        latest_bundles: dict[str, str] = {}
        legacy_metadata: list[str] = []
        async with self._session.client("s3", **self._client_kwargs) as client:
            while True:
                kwargs = {"Bucket": self.bucket, "Prefix": prefix}
                if token:
                    kwargs["ContinuationToken"] = token
                page = await client.list_objects_v2(**kwargs)
                for item in page.get("Contents", []):
                    key = item["Key"]
                    if key.endswith(".bundle") and ".v" in key:
                        base = key.rsplit(".v", 1)[0]
                        latest_bundles[base] = max(key, latest_bundles.get(base, key))
                    elif key.endswith(".json"):
                        legacy_metadata.append(key)
                if not page.get("IsTruncated"):
                    break
                token = page.get("NextContinuationToken")
            for key in sorted(latest_bundles.values()):
                record, _ = await self._load_bundle(client, key)
                if record.tenant_id == tenant_id:
                    yield record
            bundled_bases = set(latest_bundles)
            for key in sorted(legacy_metadata):
                if key.removesuffix(".json") in bundled_bases:
                    continue
                response = await client.get_object(Bucket=self.bucket, Key=key)
                record = ArtifactRecord.model_validate_json(await response["Body"].read())
                try:
                    content_response = await client.get_object(
                        Bucket=self.bucket,
                        Key=f"{key.removesuffix('.json')}.bin",
                    )
                except client.exceptions.NoSuchKey as exc:
                    raise ValueError("legacy artifact pair is incomplete") from exc
                content = await content_response["Body"].read()
                if (
                    len(content) != record.size_bytes
                    or hashlib.sha256(content).hexdigest() != record.checksum_sha256
                ):
                    raise ValueError("legacy artifact failed size or checksum verification")
                if record.tenant_id == tenant_id:
                    yield record


class ExternalMemoryRepository:
    """HTTP memory service adapter with explicit tenant scoping."""

    backend_name = "external_memory"

    def __init__(self, connection_json: str) -> None:
        config = json.loads(connection_json)
        self.base_url = config["base_url"].rstrip("/")
        self.headers = {"Authorization": config["authorization"]} if config.get("authorization") else {}
        self.timeout = float(config.get("timeout_seconds", 10.0))
        self.healthcheck_path = str(config.get("healthcheck_path", "/health/ready"))
        self._client: httpx.AsyncClient | None = None

    async def initialize(self) -> None:
        self._client = httpx.AsyncClient(base_url=self.base_url, headers=self.headers, timeout=self.timeout)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()

    async def healthcheck(self) -> bool:
        response = await self._require_client().get(self.healthcheck_path)
        return response.is_success

    def _require_client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("external memory adapter is not initialized")
        return self._client

    async def put_memory(self, memory: MemoryRecord) -> None:
        response = await self._require_client().put(
            f"/v1/tenants/{memory.tenant_id}/users/{memory.user_id}/memories/{memory.memory_id}",
            json=memory.model_dump(mode="json"),
        )
        response.raise_for_status()

    async def search_memory(
        self, tenant_id: str, user_id: str, query: str, *, limit: int = 10
    ) -> Sequence[MemoryRecord]:
        response = await self._require_client().get(
            f"/v1/tenants/{tenant_id}/users/{user_id}/memories",
            params={"q": query, "limit": limit},
        )
        response.raise_for_status()
        records = tuple(MemoryRecord.model_validate(item) for item in response.json().get("items", []))
        if any(item.tenant_id != tenant_id or item.user_id != user_id for item in records):
            raise ValueError("external memory service returned cross-scope data")
        return records

    async def iter_memories(self, tenant_id: str) -> AsyncIterator[MemoryRecord]:
        cursor: str | None = None
        while True:
            response = await self._require_client().get(
                f"/v1/tenants/{tenant_id}/memories/export",
                params={"cursor": cursor} if cursor else {},
            )
            response.raise_for_status()
            payload = response.json()
            for item in payload.get("items", []):
                record = MemoryRecord.model_validate(item)
                if record.tenant_id != tenant_id:
                    raise ValueError("external memory export returned cross-tenant data")
                yield record
            cursor = payload.get("next_cursor")
            if not cursor:
                break


class QdrantKnowledgeRepository:
    """One collection per tenant plus a mandatory tenant payload filter."""

    backend_name = "qdrant"

    def __init__(self, connection_json: str, *, namespace: str = "tap_knowledge") -> None:
        config = json.loads(connection_json)
        self.url = config["url"]
        self.api_key = config.get("api_key")
        self.prefix = config.get("collection_prefix", namespace)
        self._client: Any = None
        self._models: Any = None

    async def initialize(self) -> None:
        try:
            from qdrant_client import AsyncQdrantClient, models
        except ImportError as exc:
            raise RuntimeError("install the 'production' extra to use Qdrant") from exc
        self._client = AsyncQdrantClient(url=self.url, api_key=self.api_key)
        self._models = models

    async def close(self) -> None:
        if self._client:
            await self._client.close()

    async def healthcheck(self) -> bool:
        await self._client.get_collections()
        return True

    def _collection(self, tenant_id: str) -> str:
        return f"{self.prefix}_{_tenant_segment(tenant_id)}"

    async def put_knowledge(self, record: KnowledgeRecord) -> None:
        if not record.embedding:
            raise ValueError("Qdrant records require a non-empty embedding")
        collection = self._collection(record.tenant_id)
        collections = await self._client.get_collections()
        if collection not in {item.name for item in collections.collections}:
            await self._client.create_collection(
                collection_name=collection,
                vectors_config=self._models.VectorParams(
                    size=len(record.embedding), distance=self._models.Distance.COSINE
                ),
            )
        point_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"{record.document_id}/{record.chunk_id}"))
        await self._client.upsert(
            collection_name=collection,
            points=[
                self._models.PointStruct(
                    id=point_id,
                    vector=list(record.embedding),
                    payload={
                        "tenant_scope": _tenant_segment(record.tenant_id),
                        "record": record.model_dump(mode="json", exclude={"embedding"}),
                    },
                )
            ],
            wait=True,
        )

    def _filter(self, tenant_id: str, metadata_filter: dict[str, Any] | None = None) -> Any:
        conditions = [
            self._models.FieldCondition(
                key="tenant_scope", match=self._models.MatchValue(value=_tenant_segment(tenant_id))
            )
        ]
        for key, value in (metadata_filter or {}).items():
            conditions.append(
                self._models.FieldCondition(
                    key=f"record.metadata.{key}", match=self._models.MatchValue(value=value)
                )
            )
        return self._models.Filter(must=conditions)

    async def search_knowledge(
        self,
        tenant_id: str,
        query_embedding: Sequence[float],
        *,
        limit: int = 10,
        metadata_filter: dict[str, Any] | None = None,
    ) -> Sequence[KnowledgeRecord]:
        response = await self._client.query_points(
            collection_name=self._collection(tenant_id),
            query=list(query_embedding),
            query_filter=self._filter(tenant_id, metadata_filter),
            limit=limit,
            with_payload=True,
            with_vectors=True,
        )
        records: list[KnowledgeRecord] = []
        for point in response.points:
            record = KnowledgeRecord.model_validate(
                {**point.payload["record"], "embedding": tuple(point.vector)}
            )
            if record.tenant_id != tenant_id:
                raise ValueError("Qdrant returned cross-tenant data")
            records.append(record)
        return tuple(records)

    async def iter_knowledge(self, tenant_id: str) -> AsyncIterator[KnowledgeRecord]:
        offset: Any = None
        while True:
            points, offset = await self._client.scroll(
                collection_name=self._collection(tenant_id),
                scroll_filter=self._filter(tenant_id),
                limit=256,
                offset=offset,
                with_payload=True,
                with_vectors=True,
            )
            for point in points:
                record = KnowledgeRecord.model_validate(
                    {**point.payload["record"], "embedding": tuple(point.vector)}
                )
                if record.tenant_id != tenant_id:
                    raise ValueError("Qdrant returned cross-tenant data")
                yield record
            if offset is None:
                break
