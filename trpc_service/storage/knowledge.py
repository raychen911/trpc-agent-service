"""Tenant-filtered local and Qdrant knowledge indexes."""

from __future__ import annotations

import math
import json
import uuid
from dataclasses import dataclass

from .errors import TenantBoundaryError


@dataclass(frozen=True)
class KnowledgeResult:
    document_id: str
    chunk_id: str
    text: str
    score: float
    metadata: dict


@dataclass
class _Chunk:
    tenant_id: str
    knowledge_base_id: str
    index_version: int
    document_id: str
    chunk_id: str
    vector: tuple[float, ...]
    text: str
    metadata: dict
    deleted: bool = False


def _require_scope(tenant_id: str, knowledge_base_id: str, index_version: int) -> None:
    if not tenant_id or not knowledge_base_id or index_version < 1:
        raise TenantBoundaryError("knowledge queries require tenant, knowledge base, and index version scope")


class LocalVectorKnowledgeStore:
    """Deterministic development index with the same mandatory filters as remote stores."""

    def __init__(self) -> None:
        self._chunks: dict[tuple[str, str, int, str], _Chunk] = {}

    def upsert(
        self,
        tenant_id: str,
        knowledge_base_id: str,
        index_version: int,
        document_id: str,
        chunk_id: str,
        vector: list[float],
        text: str,
        metadata: dict | None = None,
    ) -> None:
        _require_scope(tenant_id, knowledge_base_id, index_version)
        if not vector:
            raise ValueError("knowledge vector must not be empty")
        key = (tenant_id, knowledge_base_id, index_version, chunk_id)
        self._chunks[key] = _Chunk(
            tenant_id,
            knowledge_base_id,
            index_version,
            document_id,
            chunk_id,
            tuple(vector),
            text,
            dict(metadata or {}),
        )

    def delete_document(self, tenant_id: str, knowledge_base_id: str, index_version: int, document_id: str) -> None:
        _require_scope(tenant_id, knowledge_base_id, index_version)
        for chunk in self._chunks.values():
            if (chunk.tenant_id, chunk.knowledge_base_id, chunk.index_version,
                    chunk.document_id) == (tenant_id, knowledge_base_id, index_version, document_id):
                chunk.deleted = True

    def search(self, tenant_id, knowledge_base_id, index_version, vector, limit=10) -> list[KnowledgeResult]:
        _require_scope(tenant_id, knowledge_base_id, index_version)
        norm = math.sqrt(sum(value * value for value in vector)) or 1.0
        matches = []
        for chunk in self._chunks.values():
            if chunk.deleted or (chunk.tenant_id, chunk.knowledge_base_id,
                                 chunk.index_version) != (tenant_id, knowledge_base_id, index_version):
                continue
            if len(chunk.vector) != len(vector):
                raise ValueError("query vector dimension does not match index")
            chunk_norm = math.sqrt(sum(value * value for value in chunk.vector)) or 1.0
            score = sum(left * right for left, right in zip(chunk.vector, vector)) / (chunk_norm * norm)
            matches.append(KnowledgeResult(chunk.document_id, chunk.chunk_id, chunk.text, score, chunk.metadata))
        return sorted(matches, key=lambda item: item.score, reverse=True)[:limit]

    def iter_index(self, tenant_id: str, knowledge_base_id: str, index_version: int):
        _require_scope(tenant_id, knowledge_base_id, index_version)
        return tuple(
            chunk for chunk in self._chunks.values()
            if (chunk.tenant_id, chunk.knowledge_base_id, chunk.index_version) == (
                tenant_id, knowledge_base_id, index_version)
        )


class QdrantKnowledgeStore:
    """Qdrant adapter that always emits tenant/kb/index equality filters."""

    def __init__(self, client, collection_name: str) -> None:
        self._client = client
        self._collection = collection_name

    @staticmethod
    def _filter(tenant_id, knowledge_base_id, index_version):
        _require_scope(tenant_id, knowledge_base_id, index_version)
        from qdrant_client.http import models

        return models.Filter(
            must=[
                models.FieldCondition(key="tenant_id", match=models.MatchValue(value=tenant_id)),
                models.FieldCondition(key="knowledge_base_id", match=models.MatchValue(value=knowledge_base_id)),
                models.FieldCondition(key="index_version", match=models.MatchValue(value=index_version)),
            ])

    def upsert(self, tenant_id, knowledge_base_id, index_version, document_id, chunk_id, vector, text,
               metadata=None):
        from qdrant_client.http import models

        _require_scope(tenant_id, knowledge_base_id, index_version)
        payload = dict(metadata or {})
        payload.update({
            "tenant_id": tenant_id,
            "knowledge_base_id": knowledge_base_id,
            "index_version": index_version,
            "document_id": document_id,
            "chunk_id": chunk_id,
            "text": text,
            "deleted": False,
        })
        self._client.upsert(
            collection_name=self._collection,
            points=[models.PointStruct(
                id=str(uuid.uuid5(uuid.NAMESPACE_URL, json.dumps(
                    [tenant_id, knowledge_base_id, index_version, chunk_id], ensure_ascii=False,
                    separators=(",", ":")))),
                vector=vector, payload=payload)],
            wait=True,
        )

    def search(self, tenant_id, knowledge_base_id, index_version, vector, limit=10):
        from qdrant_client.http import models
        query_filter = self._filter(tenant_id, knowledge_base_id, index_version)
        query_filter.must_not = [models.FieldCondition(key="deleted", match=models.MatchValue(value=True))]
        response = self._client.query_points(
            collection_name=self._collection,
            query=vector,
            query_filter=query_filter,
            limit=limit,
            with_payload=True,
        )
        points = getattr(response, "points", response)
        return [
            KnowledgeResult(
                point.payload["document_id"],
                point.payload["chunk_id"],
                point.payload["text"],
                point.score,
                {key: value for key, value in point.payload.items() if key not in {
                    "tenant_id", "knowledge_base_id", "index_version", "document_id", "chunk_id", "text"
                }},
            ) for point in points if not point.payload.get("deleted", False)
        ]

    def delete_document(self, tenant_id, knowledge_base_id, index_version, document_id) -> None:
        from qdrant_client.http import models

        scope = self._filter(tenant_id, knowledge_base_id, index_version)
        scope.must.append(models.FieldCondition(key="document_id", match=models.MatchValue(value=document_id)))
        self._client.set_payload(
            collection_name=self._collection,
            payload={"deleted": True},
            points=scope,
            wait=True,
        )

    def iter_index(self, tenant_id: str, knowledge_base_id: str, index_version: int):
        scope = self._filter(tenant_id, knowledge_base_id, index_version)
        chunks = []
        offset = None
        while True:
            points, offset = self._client.scroll(
                collection_name=self._collection,
                scroll_filter=scope,
                limit=256,
                offset=offset,
                with_payload=True,
                with_vectors=True,
            )
            for point in points:
                payload = point.payload
                vector = point.vector
                if isinstance(vector, dict):
                    if len(vector) != 1:
                        raise ValueError("named-vector knowledge indexes require exactly one vector")
                    vector = next(iter(vector.values()))
                chunks.append(
                    _Chunk(
                        tenant_id=payload["tenant_id"],
                        knowledge_base_id=payload["knowledge_base_id"],
                        index_version=payload["index_version"],
                        document_id=payload["document_id"],
                        chunk_id=payload["chunk_id"],
                        vector=tuple(vector),
                        text=payload["text"],
                        metadata={key: value for key, value in payload.items() if key not in {
                            "tenant_id", "knowledge_base_id", "index_version", "document_id", "chunk_id",
                            "text", "deleted"
                        }},
                        deleted=payload.get("deleted", False),
                    ))
            if offset is None:
                return tuple(chunks)
