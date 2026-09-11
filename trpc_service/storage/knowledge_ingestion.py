"""Document to vector ingestion for tenant-scoped knowledge indexes."""

from __future__ import annotations

import hashlib
import asyncio
import inspect
import math
from dataclasses import dataclass

import httpx


@dataclass(frozen=True)
class IngestionResult:
    document_id: str
    chunk_count: int
    content_digest: str


class TextChunker:
    """Deterministic character chunking with bounded overlap."""

    def __init__(self, chunk_size: int = 1200, overlap: int = 120):
        if type(chunk_size) is not int or chunk_size < 1:
            raise ValueError("chunk_size must be positive")
        if type(overlap) is not int or overlap < 0 or overlap >= chunk_size:
            raise ValueError("overlap must be in [0, chunk_size)")
        self.chunk_size, self.overlap = chunk_size, overlap

    def split(self, text: str) -> tuple[str, ...]:
        if not isinstance(text, str) or not text.strip():
            raise ValueError("document text must not be empty")
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        chunks = []
        start = 0
        while start < len(text):
            end = min(len(text), start + self.chunk_size)
            if end < len(text):
                boundary = max(text.rfind("\n", start, end), text.rfind(" ", start, end))
                if boundary > start + self.chunk_size // 2:
                    end = boundary
            value = text[start:end].strip()
            if value:
                chunks.append(value)
            if end >= len(text):
                break
            start = max(start + 1, end - self.overlap)
        return tuple(chunks)


class KnowledgeIngestionService:
    """Embed and upsert one complete document under an immutable scope."""

    def __init__(self, store, *, tenant_id: str, knowledge_base_id: str, index_version: int,
                 embedder, chunker: TextChunker | None = None):
        if not tenant_id or not knowledge_base_id or type(index_version) is not int or index_version < 1:
            raise ValueError("knowledge ingestion requires a tenant, knowledge base and index version")
        if not callable(embedder):
            raise TypeError("embedder must be callable")
        self.store = store
        self.tenant_id, self.knowledge_base_id, self.index_version = tenant_id, knowledge_base_id, index_version
        self.embedder = embedder
        self.chunker = chunker or TextChunker()

    async def ingest_document(self, document_id: str, text: str, metadata: dict | None = None) -> IngestionResult:
        if not isinstance(document_id, str) or not document_id.strip() or len(document_id) > 255:
            raise ValueError("document_id is required")
        chunks = self.chunker.split(text)
        digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
        base_metadata = dict(metadata or {})
        embedded = []
        for offset, chunk in enumerate(chunks):
            vector = self.embedder(chunk)
            if inspect.isawaitable(vector):
                vector = await vector
            if not isinstance(vector, (list, tuple)) or not vector:
                raise ValueError("embedder must return a non-empty vector")
            if any(isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
                   for value in vector):
                raise ValueError("embedding vector contains an invalid value")
            chunk_id = hashlib.sha256(f"{document_id}:{digest}:{offset}".encode("utf-8")).hexdigest()[:32]
            embedded.append((chunk_id, chunk, list(vector), offset))
        if len({len(row[2]) for row in embedded}) != 1:
            raise ValueError("embedding dimensions differ between chunks")
        # Finish embedding before touching the index. Storage failures are
        # reported to the caller; retrying the same content uses stable IDs.
        await asyncio.to_thread(self.store.delete_document,
                                self.tenant_id, self.knowledge_base_id, self.index_version, document_id)
        for chunk_id, chunk, vector, offset in embedded:
            await asyncio.to_thread(self.store.upsert, self.tenant_id, self.knowledge_base_id, self.index_version,
                                    document_id, chunk_id, list(vector), chunk,
                                    {**base_metadata, "chunk_index": offset, "content_digest": digest})
        return IngestionResult(document_id, len(chunks), digest)

    def delete_document(self, document_id: str) -> None:
        self.store.delete_document(self.tenant_id, self.knowledge_base_id, self.index_version, document_id)


class OpenRouterEmbeddingClient:
    """OpenAI-compatible embedding endpoint with bounded response validation."""

    def __init__(self, *, api_key: str, model: str, base_url: str, timeout: float = 40, dimensions=None):
        if not api_key or not model or not base_url:
            raise ValueError("embedding client requires api key, model and endpoint")
        self.api_key, self.model, self.base_url, self.timeout = api_key, model, base_url.rstrip("/"), timeout
        self.dimensions = dimensions

    async def __call__(self, text: str) -> list[float]:
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=False) as client:
            response = await client.post(self.base_url + "/embeddings",
                                         headers={"Authorization": "Bearer " + self.api_key},
                                         json={"model": self.model, "input": text})
            if response.status_code != 200:
                raise RuntimeError("embedding provider request failed")
            payload = response.json()
        try:
            vector = payload["data"][0]["embedding"]
        except (KeyError, IndexError, TypeError) as error:
            raise RuntimeError("embedding provider response is invalid") from error
        if not isinstance(vector, list) or not vector:
            raise RuntimeError("embedding provider returned an empty vector")
        if (self.dimensions is not None and len(vector) != self.dimensions) or any(
                isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
                for value in vector):
            raise RuntimeError("embedding provider returned invalid dimensions or values")
        return vector


def configured_embedder(tenant):
    from .runtime_resources import resolve_env
    config = tenant.embedding
    if config is None:
        raise ValueError("tenant embedding is not configured")
    return OpenRouterEmbeddingClient(api_key=resolve_env(config.api_key_ref.uri), model=config.model_name,
                                     base_url=config.base_url, dimensions=config.dimensions)
