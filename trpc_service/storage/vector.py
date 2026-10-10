"""Tenant-scoped pgvector projection."""

from __future__ import annotations

import json
from typing import Any

import asyncpg


class PgVectorKnowledgeStore:
    def __init__(self, pool: asyncpg.Pool, *, dimension: int = 1536) -> None:
        self._pool = pool
        if dimension != 1536:
            raise ValueError("pgvector embedding dimension must be exactly 1536")
        self._dimension = 1536

    async def upsert(
        self,
        tenant_id: str,
        item_id: str,
        embedding: list[float],
        metadata: dict[str, Any],
    ) -> None:
        if len(embedding) != self._dimension:
            raise ValueError(f"embedding dimension must be {self._dimension}")
        profile_id = metadata.get("profile_id")
        if not isinstance(profile_id, str) or not profile_id:
            raise ValueError(
                "PgVectorKnowledgeStore compatibility entry requires an existing profile_id"
            )
        vector = "[" + ",".join(format(value, ".9g") for value in embedding) + "]"
        chunk_id = str(metadata.get("chunk_id", "0"))
        async with self._pool.acquire() as connection, connection.transaction():
            await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
            await connection.execute(
                """
                INSERT INTO knowledge_items (
                    tenant_id,item_id,profile_id,source_uri,content_checksum,metadata_json,
                    projection_status
                ) VALUES ($1,$2,$3,$4,$5,$6::jsonb,'projected')
                ON CONFLICT (tenant_id,item_id) DO UPDATE
                   SET profile_id=excluded.profile_id,
                       source_uri=excluded.source_uri,
                       content_checksum=excluded.content_checksum,
                       metadata_json=excluded.metadata_json,
                       projection_status='projected',updated_at=now()
                """,
                tenant_id,
                item_id,
                profile_id,
                metadata.get("source_uri"),
                str(metadata.get("content_checksum", "")) or "unknown",
                json.dumps(metadata, separators=(",", ":")),
            )
            await connection.execute(
                """
                INSERT INTO knowledge_embeddings (
                    tenant_id,item_id,chunk_id,embedding,metadata_json
                ) VALUES ($1,$2,$3,$4::vector,$5::jsonb)
                ON CONFLICT (tenant_id,item_id,chunk_id)
                DO UPDATE SET embedding=excluded.embedding,
                              metadata_json=excluded.metadata_json,
                              created_at=now()
                """,
                tenant_id,
                item_id,
                chunk_id,
                vector,
                json.dumps(metadata, separators=(",", ":")),
            )


__all__ = ["PgVectorKnowledgeStore"]
