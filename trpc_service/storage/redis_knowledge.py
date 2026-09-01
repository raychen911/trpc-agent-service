"""Redis-backed tenant-scoped knowledge chunks."""

from __future__ import annotations

import json
import os
from dataclasses import asdict

from trpc_service.storage.vector_store import KnowledgeChunk, _cosine, _embed


class RedisKnowledgeStore:
    backend_name = "redis"

    def __init__(self, url: str | None = None, prefix: str = "trpc-agent") -> None:
        import redis

        self.client = redis.Redis.from_url(
            url or os.getenv("REDIS_URL", "redis://localhost:6379/0"),
            decode_responses=True,
        )
        self.prefix = prefix

    def _key(self, tenant_id: str, collection: str, chunk_id: str) -> str:
        return f"{self.prefix}:knowledge:{tenant_id}:{collection}:{chunk_id}"

    def _index(self, tenant_id: str, collection: str) -> str:
        return f"{self.prefix}:knowledge-index:{tenant_id}:{collection}"

    def upsert(self, chunk: KnowledgeChunk) -> None:
        self.client.set(
            self._key(chunk.tenant_id, chunk.collection, chunk.chunk_id), json.dumps(asdict(chunk), ensure_ascii=False)
        )
        self.client.sadd(self._index(chunk.tenant_id, chunk.collection), chunk.chunk_id)

    def search(self, tenant_id: str, collection: str, query: str, limit: int = 5) -> list[KnowledgeChunk]:
        query_vector = _embed(query)
        scored: list[tuple[float, KnowledgeChunk]] = []
        for chunk_id in self.client.smembers(self._index(tenant_id, collection)):
            raw = self.client.get(self._key(tenant_id, collection, chunk_id))
            if not raw:
                continue
            item = KnowledgeChunk(**json.loads(raw))
            scored.append((_cosine(query_vector, _embed(item.text)), item))
        scored.sort(key=lambda pair: pair[0], reverse=True)
        return [item for score, item in scored[:limit] if score > 0]

    def list_by_tenant(self, tenant_id: str) -> list[KnowledgeChunk]:
        prefix = f"{self.prefix}:knowledge-index:{tenant_id}:"
        chunks = []
        for index_key in self.client.scan_iter(f"{prefix}*"):
            collection = str(index_key)[len(prefix) :]
            for chunk_id in self.client.smembers(index_key):
                raw = self.client.get(self._key(tenant_id, collection, chunk_id))
                if raw:
                    chunks.append(KnowledgeChunk(**json.loads(raw)))
        return sorted(chunks, key=lambda chunk: (chunk.collection, chunk.chunk_id))

    def close(self) -> None:
        self.client.close()
