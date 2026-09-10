"""Remote vector store adapters.

Qdrant REST is supported directly. The adapter also supports an
OpenAI-compatible embeddings endpoint, while retaining a deterministic local
embedding fallback for development and offline tests.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from trpc_service.security.secrets import SecretManager
from trpc_service.security.ssrf import validate_outbound_url
from trpc_service.storage.vector_store import KnowledgeChunk


def _hashed_embedding(text: str, dimension: int) -> list[float]:
    values = [0.0] * max(1, dimension)
    for token in text.lower().split():
        digest = hashlib.sha256(token.encode("utf-8")).digest()
        index = int.from_bytes(digest[:4], "big") % len(values)
        values[index] += 1.0
    norm = math.sqrt(sum(value * value for value in values))
    return [value / norm for value in values] if norm else values


class RemoteVectorStore:
    backend_name = "remote_vector"

    def __init__(
        self,
        base_url: str,
        token: str = "",
        provider: str = "qdrant",
        dimension: int = 64,
        collection_prefix: str = "trpc-agent",
        embedding_url: str = "",
        embedding_model: str = "",
        embedding_token: str = "",
        timeout: float = 15.0,
    ) -> None:
        if not base_url:
            raise ValueError("remote vector backend requires vector_url")
        self.base_url = validate_outbound_url(base_url).rstrip("/")
        self.token = token
        self.provider = provider.lower()
        self.dimension = dimension
        self.collection_prefix = collection_prefix
        self.embedding_url = validate_outbound_url(embedding_url).rstrip("/") if embedding_url else ""
        self.embedding_model = embedding_model
        self.embedding_token = embedding_token
        self.timeout = timeout
        self._collections: set[str] = set()

    def _embedding(self, text: str) -> list[float]:
        if not self.embedding_url:
            return _hashed_embedding(text, self.dimension)
        headers = {"Content-Type": "application/json"}
        if self.embedding_token:
            headers["Authorization"] = f"Bearer {self.embedding_token}"
        request = Request(
            self.embedding_url + "/embeddings",
            data=json.dumps({"model": self.embedding_model, "input": text}).encode("utf-8"),
            headers=headers,
            method="POST",
        )
        with urlopen(request, timeout=self.timeout) as response:
            body = json.loads(response.read().decode("utf-8"))
        vector = body.get("data", [{}])[0].get("embedding")
        if not isinstance(vector, list) or not vector:
            raise RuntimeError("embedding provider returned no vector")
        return [float(value) for value in vector]

    def _request(self, method: str, path: str, payload: dict | None = None) -> dict:
        headers = {"Content-Type": "application/json"}
        if self.token:
            headers["api-key"] = self.token
            headers["Authorization"] = f"Bearer {self.token}"
        request = Request(
            self.base_url + path,
            data=json.dumps(payload, ensure_ascii=False).encode("utf-8") if payload is not None else None,
            headers=headers,
            method=method,
        )
        with urlopen(request, timeout=self.timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def _collection(self, collection: str) -> str:
        return f"{self.collection_prefix}_{collection}"

    @staticmethod
    def _point_id(chunk: KnowledgeChunk) -> int:
        """Qdrant accepts UUIDs or uint64 IDs; preserve the original ID in payload."""
        digest = hashlib.sha256(f"{chunk.tenant_id}:{chunk.collection}:{chunk.chunk_id}".encode()).digest()
        return int.from_bytes(digest[:8], "big", signed=False)

    def _ensure_collection(self, collection: str) -> None:
        if self.provider != "qdrant":
            return
        name = self._collection(collection)
        if name in self._collections:
            return
        try:
            self._request(
                "PUT",
                f"/collections/{name}",
                {"vectors": {"size": self.dimension, "distance": "Cosine"}},
            )
        except HTTPError as exc:
            if exc.code != 409:
                raise
        self._collections.add(name)

    def upsert(self, chunk: KnowledgeChunk) -> None:
        if self.provider == "qdrant":
            self._ensure_collection(chunk.collection)
            self._request(
                "PUT",
                f"/collections/{self._collection(chunk.collection)}/points?wait=true",
                {
                    "points": [
                        {
                            "id": self._point_id(chunk),
                            "vector": self._embedding(chunk.text),
                            "payload": {
                                "tenant_id": chunk.tenant_id,
                                "collection": chunk.collection,
                                "chunk_id": chunk.chunk_id,
                                "text": chunk.text,
                                "metadata": chunk.metadata,
                            },
                        }
                    ]
                },
            )
            return
        self._request("POST", "/v1/knowledge/upsert", {"chunk": asdict(chunk)})

    def search(self, tenant_id: str, collection: str, query: str, limit: int = 5) -> list[KnowledgeChunk]:
        if self.provider == "qdrant":
            self._ensure_collection(collection)
            body = self._request(
                "POST",
                f"/collections/{self._collection(collection)}/points/search",
                {
                    "vector": self._embedding(query),
                    "limit": limit,
                    "with_payload": True,
                    "filter": {"must": [{"key": "tenant_id", "match": {"value": tenant_id}}]},
                },
            )
            points = body.get("result", [])
            return [
                KnowledgeChunk(
                    tenant_id=str(point.get("payload", {}).get("tenant_id", tenant_id)),
                    collection=str(point.get("payload", {}).get("collection", collection)),
                    chunk_id=str(point.get("payload", {}).get("chunk_id", point.get("id", ""))),
                    text=str(point.get("payload", {}).get("text", "")),
                    metadata=dict(point.get("payload", {}).get("metadata", {})),
                )
                for point in points
            ]
        body = self._request(
            "POST",
            "/v1/knowledge/search",
            {"tenant_id": tenant_id, "collection": collection, "query": query, "limit": limit},
        )
        items = body if isinstance(body, list) else body.get("items", [])
        return [KnowledgeChunk(**item) for item in items]

    def list_by_tenant(self, tenant_id: str) -> list[KnowledgeChunk]:
        if self.provider == "qdrant":
            result: list[KnowledgeChunk] = []
            collection_names = set(self._collections)
            try:
                body = self._request("GET", "/collections")
                collection_names.update(
                    str(item.get("name", "")) for item in body.get("result", {}).get("collections", [])
                )
            except Exception:
                # A restricted Qdrant token may allow point reads but not listing.
                pass
            prefix = f"{self.collection_prefix}_"
            collection_names = {name for name in collection_names if name.startswith(prefix)}
            for collection_name in sorted(collection_names):
                offset = None
                while True:
                    payload: dict[str, object] = {
                        "limit": 100,
                        "with_payload": True,
                        "filter": {"must": [{"key": "tenant_id", "match": {"value": tenant_id}}]},
                    }
                    if offset is not None:
                        payload["offset"] = offset
                    body = self._request(
                        "POST",
                        f"/collections/{collection_name}/points/scroll",
                        payload,
                    )
                    for point in body.get("result", {}).get("points", []):
                        point_payload = point.get("payload", {})
                        result.append(
                            KnowledgeChunk(
                                tenant_id=str(point_payload.get("tenant_id", tenant_id)),
                                collection=str(point_payload.get("collection", "default")),
                                chunk_id=str(point_payload.get("chunk_id", point.get("id", ""))),
                                text=str(point_payload.get("text", "")),
                                metadata=dict(point_payload.get("metadata", {})),
                            )
                        )
                    offset = body.get("result", {}).get("next_page_offset")
                    if offset is None:
                        break
            return result
        body = self._request("POST", "/v1/knowledge/list", {"tenant_id": tenant_id})
        items = body if isinstance(body, list) else body.get("items", [])
        return [KnowledgeChunk(**item) for item in items]

    def close(self) -> None:
        return None


def resolve_secret(reference: str) -> str:
    return SecretManager().resolve(reference) if reference else ""
