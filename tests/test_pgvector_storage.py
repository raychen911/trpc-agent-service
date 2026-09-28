from collections.abc import AsyncGenerator, Sequence
from contextlib import asynccontextmanager
from uuid import uuid4

import pytest

from trpc_service.storage import EmbeddingDimensionError, EmbeddingProvider, KnowledgeDocument
from trpc_service.storage.adapters.pgvector import PgVectorKnowledgeStore
from trpc_service.tenant import TenantContext


class WrongDimensionEmbedding(EmbeddingProvider):
    """Return malformed vectors so validation runs before database access."""

    @property
    def dimensions(self) -> int:
        return 3

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        return [[1.0, 2.0] for _ in texts]

    async def embed_query(self, text: str) -> Sequence[float]:
        return [1.0, 2.0]


class FixedEmbedding(EmbeddingProvider):
    """Provide deterministic vectors for adapter contract tests."""

    def __init__(self, *, document_count_delta: int = 0) -> None:
        self._document_count_delta = document_count_delta

    @property
    def dimensions(self) -> int:
        return 3

    async def embed_documents(self, texts: Sequence[str]) -> Sequence[Sequence[float]]:
        count = len(texts) + self._document_count_delta
        return [[1.0, 2.0, 3.0] for _ in range(max(count, 0))]

    async def embed_query(self, text: str) -> Sequence[float]:
        del text
        return [3.0, 2.0, 1.0]


class FakeResult:

    def mappings(self) -> "FakeResult":
        return self

    def all(self) -> list[dict[str, object]]:
        return [{
            "document_id": "doc-1",
            "knowledge_base_id": "kb-1",
            "content": "tenant handbook",
            "attributes": {
                "page": 1
            },
            "score": 0.875,
        }]


class FakeDatabase:

    def __init__(self, column_type: str = "vector(3)") -> None:
        self.column_type = column_type
        self.executions: list[tuple[str, object]] = []

    async def execute(self, statement: object, parameters: object = None) -> FakeResult:
        self.executions.append((str(statement), parameters))
        return FakeResult()

    async def scalar(self, statement: object) -> str:
        self.executions.append((str(statement), None))
        return self.column_type


class FakeSessions:

    def __init__(self, column_type: str = "vector(3)") -> None:
        self.database = FakeDatabase(column_type)

    @asynccontextmanager
    async def _scope(self) -> AsyncGenerator[FakeDatabase, None]:
        yield self.database

    def begin(self):  # type: ignore[no-untyped-def]
        return self._scope()

    def __call__(self):  # type: ignore[no-untyped-def]
        return self._scope()


@pytest.mark.anyio
async def test_pgvector_runtime_validation_never_provisions_infrastructure() -> None:
    sessions = FakeSessions()
    store = PgVectorKnowledgeStore(sessions, FixedEmbedding())  # type: ignore[arg-type]
    await store.validate_schema()
    assert all(sql.strip().startswith("SELECT") for sql, _ in sessions.database.executions)
    assert any("LIMIT 0" in sql for sql, _ in sessions.database.executions)
    sessions.database.column_type = "vector(7)"
    with pytest.raises(EmbeddingDimensionError, match="migration identity"):
        await store.validate_schema()


@pytest.mark.anyio
async def test_pgvector_rejects_embedding_dimension_mismatch_before_writing() -> None:
    store = PgVectorKnowledgeStore(object(), WrongDimensionEmbedding())  # type: ignore[arg-type]
    context = TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-1",
        trace_id="trace-1",
    )

    with pytest.raises(EmbeddingDimensionError):
        await store.index(
            context,
            [KnowledgeDocument("doc-1", "kb-1", "content")],
        )


@pytest.mark.anyio
async def test_pgvector_executes_tenant_scoped_index_search_and_delete() -> None:
    """The adapter binds tenant scope on every SQL data operation."""

    sessions = FakeSessions()
    store = PgVectorKnowledgeStore(sessions, FixedEmbedding())  # type: ignore[arg-type]
    context = TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-pgvector",
        trace_id="trace-pgvector",
    )
    document = KnowledgeDocument(
        "doc-1",
        "kb-1",
        "tenant handbook",
        {"page": 1},
    )

    await store.ensure_schema()
    await store.index(context, [])
    await store.index(context, [document])
    hits = await store.search(context, "kb-1", "handbook", 2)
    await store.delete(context, "kb-1", [])
    await store.delete(context, "kb-1", ["doc-1"])

    assert hits[0].document == document
    assert hits[0].score == 0.875
    bound_parameters = [
        parameters for _, parameters in sessions.database.executions
        if isinstance(parameters, dict) and "tenant_id" in parameters
    ]
    assert len(bound_parameters) == 3
    assert all(parameters["tenant_id"] == context.tenant_id for parameters in bound_parameters)
    assert any("CREATE EXTENSION IF NOT EXISTS vector" in sql
               for sql, _ in sessions.database.executions)


@pytest.mark.anyio
async def test_pgvector_rejects_invalid_provider_and_existing_schema() -> None:
    """Provider cardinality and deployed vector dimensions fail before data drift."""

    class InvalidEmbedding(FixedEmbedding):

        @property
        def dimensions(self) -> int:
            return 0

    with pytest.raises(ValueError, match="dimensions must be positive"):
        PgVectorKnowledgeStore(FakeSessions(), InvalidEmbedding())  # type: ignore[arg-type]

    mismatched = PgVectorKnowledgeStore(
        FakeSessions("vector(4)"),  # type: ignore[arg-type]
        FixedEmbedding(),
    )
    with pytest.raises(EmbeddingDimensionError, match=r"expects vector\(3\)"):
        await mismatched.ensure_schema()

    wrong_count = PgVectorKnowledgeStore(
        FakeSessions(),  # type: ignore[arg-type]
        FixedEmbedding(document_count_delta=-1),
    )
    context = TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-invalid",
        trace_id="trace-invalid",
    )
    with pytest.raises(EmbeddingDimensionError, match="0 vectors for 1 inputs"):
        await wrong_count.index(
            context,
            [KnowledgeDocument("doc-1", "kb-1", "content")],
        )
