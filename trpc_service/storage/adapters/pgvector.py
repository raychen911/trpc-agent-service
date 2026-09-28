"""pgvector implementation of tenant-scoped knowledge retrieval."""

import json
from collections.abc import Sequence
from typing import cast

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.storage.embedding import EmbeddingProvider
from trpc_service.storage.errors import EmbeddingDimensionError
from trpc_service.storage.ports import KnowledgeStore
from trpc_service.storage.types import KnowledgeDocument, KnowledgeHit
from trpc_service.tenant.context import TenantContext


def _vector_literal(values: Sequence[float]) -> str:
    """Serialize a validated vector for pgvector's explicit text cast."""

    return "[" + ",".join(str(float(value)) for value in values) + "]"


class PgVectorKnowledgeStore(KnowledgeStore):
    """Store embeddings in any configured PostgreSQL database with pgvector."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        embeddings: EmbeddingProvider,
    ) -> None:
        if embeddings.dimensions <= 0:
            raise ValueError("embedding dimensions must be positive")
        self._sessions = sessions
        self._embeddings = embeddings

    def _validate_vectors(self, vectors: Sequence[Sequence[float]], expected: int) -> None:
        """Reject partial provider results before opening a transaction."""

        if len(vectors) != expected:
            raise EmbeddingDimensionError(
                f"embedding provider returned {len(vectors)} vectors for {expected} inputs")
        for vector in vectors:
            if len(vector) != self._embeddings.dimensions:
                raise EmbeddingDimensionError(
                    f"expected {self._embeddings.dimensions} dimensions, got {len(vector)}")

    async def ensure_schema(self) -> None:
        """Create pgvector tables and the cosine HNSW index in the configured database."""

        dimensions = self._embeddings.dimensions
        # Dimensions come from the validated integer provider contract, not user SQL.
        statements = (
            # Every Gateway/Worker may initialize its configured adapters. A
            # transaction-scoped lock serializes extension/table/index DDL.
            "SELECT pg_advisory_xact_lock(hashtext('trpc-agent:pgvector-schema'))",
            "CREATE EXTENSION IF NOT EXISTS vector",
            f"""CREATE TABLE IF NOT EXISTS knowledge_chunk_vector (
            tenant_id uuid NOT NULL,
            document_id varchar(255) NOT NULL,
            knowledge_base_id varchar(255) NOT NULL,
            content text NOT NULL,
            attributes jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            embedding vector({dimensions}) NOT NULL,
            PRIMARY KEY (tenant_id, document_id)
        )""",
            """CREATE INDEX IF NOT EXISTS ix_knowledge_chunk_vector_scope_base
            ON knowledge_chunk_vector (tenant_id, knowledge_base_id)""",
            """CREATE INDEX IF NOT EXISTS ix_knowledge_chunk_vector_embedding_hnsw
            ON knowledge_chunk_vector USING hnsw (embedding vector_cosine_ops)""",
        )
        async with self._sessions.begin() as database:
            # Execute statements separately because asyncpg prepared statements
            # intentionally reject multi-command SQL batches.
            for statement in statements:
                await database.execute(text(statement))
        await self.validate_schema()

    async def validate_schema(self) -> None:
        """Verify runtime access and dimensions without requiring DDL privileges."""

        async with self._sessions() as database:
            column_type = await database.scalar(
                text("""
                    SELECT format_type(attribute.atttypid, attribute.atttypmod)
                    FROM pg_attribute AS attribute
                    WHERE attribute.attrelid = to_regclass('knowledge_chunk_vector')
                      AND attribute.attname = 'embedding'
                      AND NOT attribute.attisdropped
                """))
            expected_type = f"vector({self._embeddings.dimensions})"
            if column_type != expected_type:
                raise EmbeddingDimensionError(
                    f"configured embedding expects {expected_type}, "
                    f"existing column is {column_type}; "
                    "run storage provisioning with the migration identity")
            await database.execute(
                text("""
                SELECT tenant_id, document_id, knowledge_base_id, content, attributes, embedding
                FROM knowledge_chunk_vector LIMIT 0
            """))

    async def index(
        self,
        context: TenantContext,
        documents: Sequence[KnowledgeDocument],
    ) -> None:
        """Embed and idempotently upsert normalized knowledge documents."""

        if not documents:
            return
        vectors = await self._embeddings.embed_documents(
            [document.content for document in documents])
        self._validate_vectors(vectors, len(documents))
        statement = text("""
            INSERT INTO knowledge_chunk_vector (
                tenant_id, document_id, knowledge_base_id,
                content, attributes, embedding
            ) VALUES (
                :tenant_id, :document_id, :knowledge_base_id,
                :content, CAST(:attributes AS jsonb), CAST(:embedding AS vector)
            )
            ON CONFLICT (tenant_id, document_id) DO UPDATE SET
                knowledge_base_id = EXCLUDED.knowledge_base_id,
                content = EXCLUDED.content,
                attributes = EXCLUDED.attributes,
                embedding = EXCLUDED.embedding
        """)
        async with self._sessions.begin() as database:
            for document, vector in zip(documents, vectors, strict=True):
                await database.execute(
                    statement,
                    {
                        "tenant_id": context.tenant_id,
                        "document_id": document.document_id,
                        "knowledge_base_id": document.knowledge_base_id,
                        "content": document.content,
                        "attributes": json.dumps(dict(document.attributes)),
                        "embedding": _vector_literal(vector),
                    },
                )

    async def search(
        self,
        context: TenantContext,
        knowledge_base_id: str,
        query: str,
        limit: int,
    ) -> Sequence[KnowledgeHit]:
        """Return nearest cosine matches from one mandatory tenant scope."""

        vector = await self._embeddings.embed_query(query)
        self._validate_vectors([vector], 1)
        statement = text("""
            SELECT document_id, knowledge_base_id, content, attributes,
                   1 - (embedding <=> CAST(:embedding AS vector)) AS score
            FROM knowledge_chunk_vector
            WHERE tenant_id = :tenant_id
              AND knowledge_base_id = :knowledge_base_id
            ORDER BY embedding <=> CAST(:embedding AS vector)
            LIMIT :limit
        """)
        async with self._sessions() as database:
            result = await database.execute(
                statement,
                {
                    "tenant_id": context.tenant_id,
                    "knowledge_base_id": knowledge_base_id,
                    "embedding": _vector_literal(vector),
                    "limit": limit,
                },
            )
            rows = result.mappings().all()
        return [
            KnowledgeHit(
                document=KnowledgeDocument(
                    document_id=cast(str, row["document_id"]),
                    knowledge_base_id=cast(str, row["knowledge_base_id"]),
                    content=cast(str, row["content"]),
                    attributes=cast(dict[str, object], row["attributes"]),
                ),
                score=float(row["score"]),
            ) for row in rows
        ]

    async def delete(
        self,
        context: TenantContext,
        knowledge_base_id: str,
        document_ids: Sequence[str],
    ) -> None:
        """Delete indexed chunks without accepting a caller-supplied tenant."""

        if not document_ids:
            return
        statement = text("""
            DELETE FROM knowledge_chunk_vector
            WHERE tenant_id = :tenant_id
              AND knowledge_base_id = :knowledge_base_id
              AND document_id = ANY(CAST(:document_ids AS varchar[]))
        """)
        async with self._sessions.begin() as database:
            await database.execute(
                statement,
                {
                    "tenant_id": context.tenant_id,
                    "knowledge_base_id": knowledge_base_id,
                    "document_ids": list(document_ids),
                },
            )
