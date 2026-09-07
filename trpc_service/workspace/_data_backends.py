"""Tenant-scoped vector and object storage contracts and built-in adapters."""

from __future__ import annotations

import asyncio
import hashlib
import json
import math
import os
import time
from abc import ABC
from abc import abstractmethod
from pathlib import Path
from typing import Any
from typing import Optional
from uuid import NAMESPACE_URL
from uuid import uuid4
from uuid import uuid5

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field

from trpc_service.metrics import EnterpriseMetrics
from trpc_service.metrics import get_enterprise_metrics
from trpc_service.metrics import storage_span


class VectorRecord(BaseModel):
    """One knowledge chunk and its embedding."""

    model_config = ConfigDict(extra="forbid")

    record_id: str
    embedding: list[float] = Field(min_length=1)
    content: str = ""
    metadata: dict[str, Any] = Field(default_factory=dict)
    version: int = 1


class VectorMatch(BaseModel):
    """A vector search result."""

    record: VectorRecord
    score: float


class VectorStoreABC(ABC):
    """Backend-independent vector store used by the platform layer."""

    @abstractmethod
    async def upsert(self, namespace: str, records: list[VectorRecord]) -> None:
        """Insert or replace records in a namespace."""

    @abstractmethod
    async def search(self,
                     namespace: str,
                     embedding: list[float],
                     limit: int = 5,
                     metadata_filter: Optional[dict[str, Any]] = None) -> list[VectorMatch]:
        """Return nearest records, ordered by descending similarity."""

    @abstractmethod
    async def delete(self, namespace: str, record_ids: Optional[list[str]] = None) -> None:
        """Delete selected records, or the entire namespace when IDs are omitted."""

    @abstractmethod
    async def close(self) -> None:
        """Release backend resources."""


class InMemoryVectorStore(VectorStoreABC):
    """Cosine-similarity vector store for tests and single-process demos."""

    def __init__(self) -> None:
        self._records: dict[str, dict[str, VectorRecord]] = {}

    async def upsert(self, namespace: str, records: list[VectorRecord]) -> None:
        if not records:
            return
        dimensions = {len(record.embedding) for record in records}
        existing = self._records.get(namespace, {})
        dimensions.update(len(record.embedding) for record in existing.values())
        if len(dimensions) != 1:
            raise ValueError("all vectors in a namespace must have the same dimensions")
        bucket = self._records.setdefault(namespace, {})
        for record in records:
            bucket[record.record_id] = record.model_copy(deep=True)

    async def search(self,
                     namespace: str,
                     embedding: list[float],
                     limit: int = 5,
                     metadata_filter: Optional[dict[str, Any]] = None) -> list[VectorMatch]:
        if not embedding:
            raise ValueError("query embedding must not be empty")
        if limit < 1:
            return []
        matches = []
        for record in self._records.get(namespace, {}).values():
            if len(record.embedding) != len(embedding):
                raise ValueError("query embedding dimensions do not match the namespace")
            if metadata_filter and any(record.metadata.get(key) != value for key, value in metadata_filter.items()):
                continue
            matches.append(VectorMatch(record=record.model_copy(deep=True), score=_cosine(embedding, record.embedding)))
        return sorted(matches, key=lambda item: (-item.score, item.record.record_id))[:limit]

    async def delete(self, namespace: str, record_ids: Optional[list[str]] = None) -> None:
        if record_ids is None:
            self._records.pop(namespace, None)
            return
        bucket = self._records.get(namespace, {})
        for record_id in record_ids:
            bucket.pop(record_id, None)

    async def close(self) -> None:
        self._records.clear()


class QdrantVectorStore(VectorStoreABC):
    """Qdrant adapter with tenant namespaces stored as mandatory payload filters."""

    def __init__(self,
                 url: str,
                 collection: str,
                 dimensions: Optional[int] = None,
                 api_key: Optional[str] = None,
                 client: Any = None) -> None:
        try:
            from qdrant_client import AsyncQdrantClient
            from qdrant_client import models
        except ImportError as exc:
            raise RuntimeError("install trpc-agent-service[vector-storage] for Qdrant") from exc
        if not url and client is None:
            raise ValueError("qdrant vector backend requires a URL")
        if not collection:
            raise ValueError("qdrant collection must not be empty")
        self._client = client or AsyncQdrantClient(url=url, api_key=api_key)
        self._models = models
        self._collection = collection
        self._dimensions = dimensions

    @staticmethod
    def _point_id(namespace: str, record_id: str) -> str:
        return str(uuid5(NAMESPACE_URL, f"{namespace}:{record_id}"))

    def _filter(self, namespace: str, metadata_filter: Optional[dict[str, Any]] = None):
        conditions = [
            self._models.FieldCondition(
                key="tenant_namespace",
                match=self._models.MatchValue(value=namespace),
            )
        ]
        for key, value in (metadata_filter or {}).items():
            conditions.append(
                self._models.FieldCondition(
                    key=f"metadata.{key}",
                    match=self._models.MatchValue(value=value),
                ))
        return self._models.Filter(must=conditions)

    async def _ensure_collection(self, dimensions: int) -> None:
        if self._dimensions is not None and dimensions != self._dimensions:
            raise ValueError("vector dimensions do not match the configured Qdrant collection")
        if await self._client.collection_exists(self._collection):
            return
        try:
            await self._client.create_collection(
                collection_name=self._collection,
                vectors_config=self._models.VectorParams(size=dimensions, distance=self._models.Distance.COSINE),
            )
        except Exception:
            # Concurrent workers may race to create the same collection.
            if not await self._client.collection_exists(self._collection):
                raise

    async def upsert(self, namespace: str, records: list[VectorRecord]) -> None:
        if not records:
            return
        dimensions = {len(record.embedding) for record in records}
        if len(dimensions) != 1:
            raise ValueError("all vectors in a Qdrant batch must have the same dimensions")
        await self._ensure_collection(dimensions.pop())
        points = [
            self._models.PointStruct(
                id=self._point_id(namespace, record.record_id),
                vector=record.embedding,
                payload={
                    "tenant_namespace": namespace,
                    "record_id": record.record_id,
                    "content": record.content,
                    "metadata": record.metadata,
                    "version": record.version,
                },
            ) for record in records
        ]
        await self._client.upsert(collection_name=self._collection, points=points, wait=True)

    async def search(self,
                     namespace: str,
                     embedding: list[float],
                     limit: int = 5,
                     metadata_filter: Optional[dict[str, Any]] = None) -> list[VectorMatch]:
        if not embedding:
            raise ValueError("query embedding must not be empty")
        if limit < 1 or not await self._client.collection_exists(self._collection):
            return []
        if self._dimensions is not None and len(embedding) != self._dimensions:
            raise ValueError("query dimensions do not match the configured Qdrant collection")
        response = await self._client.query_points(
            collection_name=self._collection,
            query=embedding,
            query_filter=self._filter(namespace, metadata_filter),
            limit=limit,
            with_payload=True,
            with_vectors=True,
        )
        matches = []
        for point in response.points:
            payload = point.payload or {}
            vector = point.vector if isinstance(point.vector, list) else []
            matches.append(
                VectorMatch(
                    record=VectorRecord(
                        record_id=payload.get("record_id", str(point.id)),
                        embedding=vector,
                        content=payload.get("content", ""),
                        metadata=payload.get("metadata", {}),
                        version=int(payload.get("version", 1)),
                    ),
                    score=float(point.score),
                ))
        return matches

    async def delete(self, namespace: str, record_ids: Optional[list[str]] = None) -> None:
        if not await self._client.collection_exists(self._collection):
            return
        if record_ids is None:
            selector = self._models.FilterSelector(filter=self._filter(namespace))
        else:
            selector = self._models.PointIdsList(
                points=[self._point_id(namespace, record_id) for record_id in record_ids])
        await self._client.delete(collection_name=self._collection, points_selector=selector, wait=True)

    async def close(self) -> None:
        await self._client.close()


def _cosine(left: list[float], right: list[float]) -> float:
    numerator = sum(a * b for a, b in zip(left, right))
    denominator = math.sqrt(sum(value * value for value in left)) * math.sqrt(sum(value * value for value in right))
    return numerator / denominator if denominator else 0.0


class ObjectInfo(BaseModel):
    """Metadata returned for one immutable object payload."""

    model_config = ConfigDict(extra="forbid")

    key: str
    size: int
    checksum: str
    content_type: str = "application/octet-stream"
    metadata: dict[str, Any] = Field(default_factory=dict)


class ObjectStoreABC(ABC):
    """Small object-store contract for Agent artifacts."""

    @abstractmethod
    async def put(self,
                  key: str,
                  data: bytes,
                  content_type: str = "application/octet-stream",
                  metadata: Optional[dict[str, Any]] = None) -> ObjectInfo:
        """Persist bytes and return their checksum-bearing metadata."""

    @abstractmethod
    async def get(self, key: str) -> Optional[bytes]:
        """Load bytes, returning ``None`` when the key does not exist."""

    @abstractmethod
    async def head(self, key: str) -> Optional[ObjectInfo]:
        """Load metadata without loading the payload."""

    @abstractmethod
    async def delete(self, key: str) -> None:
        """Delete one object if present."""

    @abstractmethod
    async def close(self) -> None:
        """Release backend resources."""


class LocalObjectStore(ObjectStoreABC):
    """Atomic filesystem object store for local development."""

    def __init__(self, root: str) -> None:
        self._root = Path(root).expanduser().resolve()
        self._root.mkdir(parents=True, exist_ok=True)

    def _paths(self, key: str) -> tuple[Path, Path]:
        if not key or key.startswith(("/", "\\")) or "\\" in key:
            raise ValueError("object key must be a non-empty relative POSIX path")
        parts = Path(key).parts
        if ".." in parts:
            raise ValueError("object key must not escape the configured root")
        payload = self._root.joinpath(*parts).resolve()
        if self._root not in payload.parents:
            raise ValueError("object key must not escape the configured root")
        metadata = payload.with_name(f".{payload.name}.metadata.json")
        return payload, metadata

    async def put(self,
                  key: str,
                  data: bytes,
                  content_type: str = "application/octet-stream",
                  metadata: Optional[dict[str, Any]] = None) -> ObjectInfo:
        if not isinstance(data, bytes):
            raise TypeError("object data must be bytes")
        payload_path, metadata_path = self._paths(key)
        info = ObjectInfo(
            key=key,
            size=len(data),
            checksum=hashlib.sha256(data).hexdigest(),
            content_type=content_type,
            metadata=metadata or {},
        )
        metadata_bytes = info.model_dump_json().encode("utf-8")

        def write() -> None:
            payload_path.parent.mkdir(parents=True, exist_ok=True)
            suffix = uuid4().hex
            payload_tmp = payload_path.with_name(f".{payload_path.name}.{suffix}.tmp")
            metadata_tmp = metadata_path.with_name(f".{metadata_path.name}.{suffix}.tmp")
            payload_tmp.write_bytes(data)
            metadata_tmp.write_bytes(metadata_bytes)
            os.replace(payload_tmp, payload_path)
            # Metadata is committed last, so readers never observe a reference
            # before its payload has been durably renamed into place.
            os.replace(metadata_tmp, metadata_path)

        await asyncio.to_thread(write)
        return info

    async def get(self, key: str) -> Optional[bytes]:
        payload_path, metadata_path = self._paths(key)
        if not metadata_path.exists() or not payload_path.exists():
            return None
        return await asyncio.to_thread(payload_path.read_bytes)

    async def head(self, key: str) -> Optional[ObjectInfo]:
        payload_path, metadata_path = self._paths(key)
        if not payload_path.exists() or not metadata_path.exists():
            return None
        raw = await asyncio.to_thread(metadata_path.read_text, "utf-8")
        return ObjectInfo.model_validate_json(raw)

    async def delete(self, key: str) -> None:
        payload_path, metadata_path = self._paths(key)

        def remove() -> None:
            metadata_path.unlink(missing_ok=True)
            payload_path.unlink(missing_ok=True)

        await asyncio.to_thread(remove)

    async def close(self) -> None:
        return None


class S3CompatibleObjectStore(ObjectStoreABC):
    """S3 API adapter for AWS S3, MinIO and S3-compatible Tencent COS."""

    def __init__(self,
                 bucket: str,
                 endpoint_url: Optional[str] = None,
                 region: Optional[str] = None,
                 access_key: Optional[str] = None,
                 secret_key: Optional[str] = None,
                 client: Any = None) -> None:
        if not bucket:
            raise ValueError("object storage bucket must not be empty")
        self._bucket = bucket
        if client is None:
            try:
                import boto3
            except ImportError as exc:
                raise RuntimeError("install trpc-agent-service[object-storage] for S3 object storage") from exc
            client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                region_name=region,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
            )
        self._client = client

    @staticmethod
    def _missing(exc: Exception) -> bool:
        response = getattr(exc, "response", {})
        return response.get("Error", {}).get("Code") in {"404", "NoSuchKey", "NotFound"}

    async def put(self,
                  key: str,
                  data: bytes,
                  content_type: str = "application/octet-stream",
                  metadata: Optional[dict[str, Any]] = None) -> ObjectInfo:
        checksum = hashlib.sha256(data).hexdigest()
        custom = metadata or {}
        await asyncio.to_thread(
            self._client.put_object,
            Bucket=self._bucket,
            Key=key,
            Body=data,
            ContentType=content_type,
            Metadata={
                "sha256": checksum,
                "custom": json.dumps(custom, ensure_ascii=False, separators=(",", ":")),
            },
        )
        return ObjectInfo(
            key=key,
            size=len(data),
            checksum=checksum,
            content_type=content_type,
            metadata=custom,
        )

    async def get(self, key: str) -> Optional[bytes]:

        def load() -> bytes:
            return self._client.get_object(Bucket=self._bucket, Key=key)["Body"].read()

        try:
            return await asyncio.to_thread(load)
        except Exception as exc:
            if self._missing(exc):
                return None
            raise

    async def head(self, key: str) -> Optional[ObjectInfo]:
        try:
            response = await asyncio.to_thread(self._client.head_object, Bucket=self._bucket, Key=key)
        except Exception as exc:
            if self._missing(exc):
                return None
            raise
        metadata = response.get("Metadata", {})
        custom = json.loads(metadata.get("custom", "{}"))
        return ObjectInfo(
            key=key,
            size=int(response.get("ContentLength", 0)),
            checksum=metadata.get("sha256", ""),
            content_type=response.get("ContentType", "application/octet-stream"),
            metadata=custom,
        )

    async def delete(self, key: str) -> None:
        await asyncio.to_thread(self._client.delete_object, Bucket=self._bucket, Key=key)

    async def close(self) -> None:
        close = getattr(self._client, "close", None)
        if close is not None:
            await asyncio.to_thread(close)


class TenantVectorStore(VectorStoreABC):
    """Namespace a vector backend by tenant."""

    def __init__(self,
                 backend: VectorStoreABC,
                 tenant_id: str,
                 metrics: Optional[EnterpriseMetrics] = None,
                 backend_name: Optional[str] = None) -> None:
        self._backend = backend
        self._prefix = f"{tenant_id}:"
        self._tenant_id = tenant_id
        self._metrics = metrics or get_enterprise_metrics()
        self._backend_name = backend_name or type(backend).__name__

    def _scope(self, namespace: str) -> str:
        return namespace if namespace.startswith(self._prefix) else f"{self._prefix}{namespace}"

    async def _execute(self, operation: str, awaitable: Any) -> Any:
        started = time.perf_counter()
        outcome = "error"
        error_type = None
        try:
            with storage_span(self._tenant_id, "vector", operation):
                result = await awaitable
            outcome = "success"
            return result
        except Exception as exc:
            error_type = type(exc).__name__
            raise
        finally:
            attributes = {
                "tenant_id": self._tenant_id,
                "backend": self._backend_name,
                "data_type": "vector",
                "operation": operation,
                "outcome": outcome,
                "error_type": error_type,
            }
            self._metrics.increment("agent_storage_operation_total", **attributes)
            self._metrics.observe(
                "agent_storage_operation_duration_ms",
                (time.perf_counter() - started) * 1000,
                **attributes,
            )

    async def upsert(self, namespace: str, records: list[VectorRecord]) -> None:
        await self._execute("upsert", self._backend.upsert(self._scope(namespace), records))

    async def search(self,
                     namespace: str,
                     embedding: list[float],
                     limit: int = 5,
                     metadata_filter: Optional[dict[str, Any]] = None) -> list[VectorMatch]:
        return await self._execute(
            "search",
            self._backend.search(self._scope(namespace), embedding, limit, metadata_filter),
        )

    async def delete(self, namespace: str, record_ids: Optional[list[str]] = None) -> None:
        await self._execute("delete", self._backend.delete(self._scope(namespace), record_ids))

    async def close(self) -> None:
        await self._backend.close()


class TenantObjectStore(ObjectStoreABC):
    """Prefix every artifact key with the tenant id."""

    def __init__(self,
                 backend: ObjectStoreABC,
                 tenant_id: str,
                 metrics: Optional[EnterpriseMetrics] = None,
                 backend_name: Optional[str] = None) -> None:
        self._backend = backend
        self._prefix = f"{tenant_id}/"
        self._tenant_id = tenant_id
        self._metrics = metrics or get_enterprise_metrics()
        self._backend_name = backend_name or type(backend).__name__

    def _scope(self, key: str) -> str:
        return key if key.startswith(self._prefix) else f"{self._prefix}{key}"

    async def _execute(self, operation: str, awaitable: Any) -> Any:
        started = time.perf_counter()
        outcome = "error"
        error_type = None
        try:
            with storage_span(self._tenant_id, "object", operation):
                result = await awaitable
            outcome = "success"
            return result
        except Exception as exc:
            error_type = type(exc).__name__
            raise
        finally:
            attributes = {
                "tenant_id": self._tenant_id,
                "backend": self._backend_name,
                "data_type": "object",
                "operation": operation,
                "outcome": outcome,
                "error_type": error_type,
            }
            self._metrics.increment("agent_storage_operation_total", **attributes)
            self._metrics.observe(
                "agent_storage_operation_duration_ms",
                (time.perf_counter() - started) * 1000,
                **attributes,
            )

    async def put(self,
                  key: str,
                  data: bytes,
                  content_type: str = "application/octet-stream",
                  metadata: Optional[dict[str, Any]] = None) -> ObjectInfo:
        info = await self._execute("put", self._backend.put(self._scope(key), data, content_type, metadata))
        return info.model_copy(update={"key": key})

    async def get(self, key: str) -> Optional[bytes]:
        return await self._execute("get", self._backend.get(self._scope(key)))

    async def head(self, key: str) -> Optional[ObjectInfo]:
        info = await self._execute("head", self._backend.head(self._scope(key)))
        return info.model_copy(update={"key": key}) if info is not None else None

    async def delete(self, key: str) -> None:
        await self._execute("delete", self._backend.delete(self._scope(key)))

    async def close(self) -> None:
        await self._backend.close()
