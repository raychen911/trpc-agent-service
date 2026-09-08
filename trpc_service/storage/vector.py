import asyncio
import hashlib
import math
import re
import uuid
from collections import Counter
from collections.abc import Mapping, Sequence

import httpx

from trpc_service.storage.contracts import VectorDocument, VectorMatch, VectorStore

_token_pattern = re.compile(r"[\w\u4e00-\u9fff]+", re.UNICODE)


class HashingEmbedder:
    """Dependency-free deterministic embedding for development and tests."""

    def __init__(self, dimensions: int = 256) -> None:
        if dimensions < 8:
            raise ValueError("dimensions must be at least 8")
        self.dimensions = dimensions

    def embed(self, text: str) -> tuple[float, ...]:
        features = [0.0] * self.dimensions
        tokens = _token_pattern.findall(text.lower())
        for token, count in Counter(tokens).items():
            digest = hashlib.blake2b(token.encode("utf-8"), digest_size=8).digest()
            bucket = int.from_bytes(digest[:4], "big") % self.dimensions
            sign = 1.0 if digest[4] & 1 else -1.0
            features[bucket] += sign * (1.0 + math.log(count))
        norm = math.sqrt(sum(value * value for value in features))
        if norm:
            features = [value / norm for value in features]
        return tuple(features)


class InMemoryVectorStore(VectorStore):
    def __init__(self, embedder: HashingEmbedder | None = None) -> None:
        self._embedder = embedder or HashingEmbedder()
        self._documents: dict[tuple[str, str], tuple[VectorDocument, tuple[float, ...]]] = {}

    async def upsert(self, documents: Sequence[VectorDocument]) -> None:
        for document in documents:
            self._documents[(document.namespace, document.id)] = (
                document,
                self._embedder.embed(document.text),
            )

    async def delete(self, namespace: str, document_ids: Sequence[str]) -> None:
        for document_id in document_ids:
            self._documents.pop((namespace, document_id), None)

    async def search(self, namespace: str, query: str, limit: int = 5) -> Sequence[VectorMatch]:
        if limit < 1:
            raise ValueError("limit must be positive")
        query_vector = self._embedder.embed(query)
        matches = [
            VectorMatch(
                document=document,
                score=sum(a * b for a, b in zip(vector, query_vector, strict=True)),
            )
            for (stored_namespace, _), (document, vector) in self._documents.items()
            if stored_namespace == namespace
        ]
        matches.sort(key=lambda item: (-item.score, item.document.id))
        return tuple(matches[:limit])


class QdrantVectorStore(VectorStore):
    """Qdrant HTTP backend with tenant namespace isolation in payload filters."""

    def __init__(
        self,
        url: str,
        collection: str = "trpc_agent_vectors",
        api_key: str | None = None,
        embedder: HashingEmbedder | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._embedder = embedder or HashingEmbedder()
        self._collection = collection
        self._client = client or httpx.AsyncClient(
            base_url=url.rstrip("/"),
            headers={"api-key": api_key} if api_key else {},
            timeout=20,
        )
        self._owns_client = client is None
        self._ready = False
        self._ready_lock = asyncio.Lock()

    async def _ensure_collection(self) -> None:
        if self._ready:
            return
        async with self._ready_lock:
            if self._ready:
                return
            response = await self._client.get(f"/collections/{self._collection}")
            if response.status_code == 404:
                response = await self._client.put(
                    f"/collections/{self._collection}",
                    json={
                        "vectors": {
                            "size": self._embedder.dimensions,
                            "distance": "Cosine",
                        }
                    },
                )
            response.raise_for_status()
            self._ready = True

    @staticmethod
    def _point_id(namespace: str, document_id: str) -> str:
        return str(uuid.uuid5(uuid.NAMESPACE_URL, f"{namespace}:{document_id}"))

    async def upsert(self, documents: Sequence[VectorDocument]) -> None:
        if not documents:
            return
        await self._ensure_collection()
        response = await self._client.put(
            f"/collections/{self._collection}/points",
            params={"wait": "true"},
            json={
                "points": [
                    {
                        "id": self._point_id(item.namespace, item.id),
                        "vector": self._embedder.embed(item.text),
                        "payload": {
                            "namespace": item.namespace,
                            "document_id": item.id,
                            "text": item.text,
                            "metadata": dict(item.metadata),
                        },
                    }
                    for item in documents
                ]
            },
        )
        response.raise_for_status()

    async def delete(self, namespace: str, document_ids: Sequence[str]) -> None:
        if not document_ids:
            return
        await self._ensure_collection()
        response = await self._client.post(
            f"/collections/{self._collection}/points/delete",
            params={"wait": "true"},
            json={"points": [self._point_id(namespace, item) for item in document_ids]},
        )
        response.raise_for_status()

    async def search(self, namespace: str, query: str, limit: int = 5) -> Sequence[VectorMatch]:
        if limit < 1:
            raise ValueError("limit must be positive")
        await self._ensure_collection()
        response = await self._client.post(
            f"/collections/{self._collection}/points/search",
            json={
                "vector": self._embedder.embed(query),
                "limit": limit,
                "with_payload": True,
                "filter": {"must": [{"key": "namespace", "match": {"value": namespace}}]},
            },
        )
        response.raise_for_status()
        return tuple(
            VectorMatch(
                VectorDocument(
                    str(item["payload"]["document_id"]),
                    namespace,
                    str(item["payload"]["text"]),
                    dict(item["payload"].get("metadata", {})),
                ),
                float(item["score"]),
            )
            for item in response.json().get("result", [])
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


class SemanticStore:
    """Tenant-scoped Knowledge and Memory facade over a VectorStore."""

    def __init__(self, vector_store: VectorStore) -> None:
        self._vectors = vector_store

    @staticmethod
    def memory_namespace(tenant_id: str, agent_app_id: str, user_id: str) -> str:
        return f"tenant/{tenant_id}/app/{agent_app_id}/memory/{user_id}"

    @staticmethod
    def knowledge_namespace(tenant_id: str, agent_app_id: str) -> str:
        return f"tenant/{tenant_id}/app/{agent_app_id}/knowledge"

    async def upsert_memory(
        self,
        tenant_id: str,
        agent_app_id: str,
        user_id: str,
        memory_id: str,
        text: str,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        await self._vectors.upsert(
            [
                VectorDocument(
                    id=memory_id,
                    namespace=self.memory_namespace(tenant_id, agent_app_id, user_id),
                    text=text,
                    metadata=dict(metadata or {}),
                )
            ]
        )

    async def search_memories(
        self,
        tenant_id: str,
        agent_app_id: str,
        user_id: str,
        query: str,
        limit: int = 5,
    ) -> Sequence[VectorMatch]:
        return await self._vectors.search(
            self.memory_namespace(tenant_id, agent_app_id, user_id), query, limit
        )

    async def upsert_knowledge(
        self,
        tenant_id: str,
        agent_app_id: str,
        document_id: str,
        text: str,
        metadata: Mapping[str, object] | None = None,
    ) -> None:
        await self._vectors.upsert(
            [
                VectorDocument(
                    id=document_id,
                    namespace=self.knowledge_namespace(tenant_id, agent_app_id),
                    text=text,
                    metadata=dict(metadata or {}),
                )
            ]
        )

    async def search_knowledge(
        self,
        tenant_id: str,
        agent_app_id: str,
        query: str,
        limit: int = 5,
    ) -> Sequence[VectorMatch]:
        return await self._vectors.search(
            self.knowledge_namespace(tenant_id, agent_app_id), query, limit
        )

    async def close(self) -> None:
        close = getattr(self._vectors, "close", None)
        if close is not None:
            await close()
