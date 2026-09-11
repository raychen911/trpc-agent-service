"""Tenant-scoped pgvector projection store."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class VectorMatch:
    item_id: str
    content: str
    score: float
    source_version: int


class PgVectorStore:
    def __init__(self, repository: Any, dimensions: int = 1536) -> None:
        self.repository = repository
        self.dimensions = dimensions

    def _literal(self, embedding: list[float]) -> str:
        if len(embedding) != self.dimensions:
            raise ValueError(f"embedding must contain {self.dimensions} values")
        return "[" + ",".join(format(float(value), ".10g") for value in embedding) + "]"

    async def upsert(
        self,
        tenant_id: str,
        item_id: str,
        content: str,
        embedding: list[float],
        source_version: int,
        collection: str = "default",
    ) -> None:
        async with self.repository._scoped(tenant_id) as connection:
            await connection.execute(
                """
                INSERT INTO knowledge_vectors(tenant_id,item_id,collection,content,embedding,source_version)
                VALUES($1,$2,$3,$4,$5::vector,$6)
                ON CONFLICT (tenant_id,item_id) DO UPDATE SET content=EXCLUDED.content,
                    collection=EXCLUDED.collection,embedding=EXCLUDED.embedding,
                    source_version=EXCLUDED.source_version,updated_at=now()
                WHERE knowledge_vectors.source_version <= EXCLUDED.source_version
                """,
                tenant_id,
                item_id,
                collection,
                content,
                self._literal(embedding),
                source_version,
            )

    async def search(
        self,
        tenant_id: str,
        embedding: list[float],
        limit: int = 10,
        collections: frozenset[str] | set[str] | None = None,
    ) -> list[VectorMatch]:
        selected = sorted(collections or {"default"})
        async with self.repository._scoped(tenant_id) as connection:
            rows = await connection.fetch(
                """
                SELECT item_id,content,source_version,1-(embedding <=> $2::vector) AS score
                FROM knowledge_vectors WHERE tenant_id=$1 AND collection=ANY($4::text[])
                ORDER BY embedding <=> $2::vector LIMIT $3
                """,
                tenant_id,
                self._literal(embedding),
                max(1, min(limit, 100)),
                selected,
            )
        return [VectorMatch(row["item_id"], row["content"], float(row["score"]), row["source_version"]) for row in rows]
