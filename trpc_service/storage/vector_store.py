"""Small tenant-scoped vector-store facade used by the local demo."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from math import sqrt
from pathlib import Path
from threading import RLock


@dataclass(slots=True)
class KnowledgeChunk:
    tenant_id: str
    collection: str
    chunk_id: str
    text: str
    metadata: dict = field(default_factory=dict)


def _embed(text: str) -> dict[str, float]:
    vector: dict[str, float] = {}
    for token in text.lower().split():
        vector[token] = vector.get(token, 0.0) + 1.0
    return vector


def _cosine(left: dict[str, float], right: dict[str, float]) -> float:
    numerator = sum(value * right.get(token, 0.0) for token, value in left.items())
    left_norm = sqrt(sum(value * value for value in left.values()))
    right_norm = sqrt(sum(value * value for value in right.values()))
    if left_norm == 0 or right_norm == 0:
        return 0.0
    return numerator / (left_norm * right_norm)


class LocalVectorStore:
    backend_name = "vector"

    def __init__(self, root: str | Path | None = None) -> None:
        self._chunks: dict[tuple[str, str, str], KnowledgeChunk] = {}
        self._vectors: dict[tuple[str, str, str], dict[str, float]] = {}
        self._lock = RLock()
        self._path = Path(root) / "chunks.json" if root else None
        if self._path:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._load()

    def upsert(self, chunk: KnowledgeChunk) -> None:
        key = (chunk.tenant_id, chunk.collection, chunk.chunk_id)
        with self._lock:
            self._chunks[key] = chunk
            self._vectors[key] = _embed(chunk.text)
            self._flush()

    def search(self, tenant_id: str, collection: str, query: str, limit: int = 5) -> list[KnowledgeChunk]:
        query_vector = _embed(query)
        with self._lock:
            scored = [
                (_cosine(query_vector, vector), self._chunks[key])
                for key, vector in self._vectors.items()
                if key[0] == tenant_id and key[1] == collection
            ]
        scored.sort(key=lambda pair: pair[0], reverse=True)
        return [chunk for score, chunk in scored[:limit] if score > 0]

    def list_by_tenant(self, tenant_id: str) -> list[KnowledgeChunk]:
        with self._lock:
            chunks = [chunk for key, chunk in self._chunks.items() if key[0] == tenant_id]
        return sorted(chunks, key=lambda chunk: (chunk.collection, chunk.chunk_id))

    def _load(self) -> None:
        if not self._path or not self._path.exists():
            return
        for item in json.loads(self._path.read_text(encoding="utf-8")):
            self._chunks[(item["tenant_id"], item["collection"], item["chunk_id"])] = KnowledgeChunk(**item)
            self._vectors[(item["tenant_id"], item["collection"], item["chunk_id"])] = _embed(item["text"])

    def _flush(self) -> None:
        if self._path:
            self._path.write_text(
                json.dumps([asdict(chunk) for chunk in self._chunks.values()], ensure_ascii=False),
                encoding="utf-8",
            )
