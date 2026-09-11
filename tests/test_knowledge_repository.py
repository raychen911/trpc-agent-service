"""Unit contracts for the tenant-scoped SQL knowledge backend."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
from trpc_agent_sdk.knowledge import SearchParams, SearchRequest
from trpc_agent_sdk.types import Part

import trpc_service.storage.knowledge_repository as knowledge_repository
from trpc_service.storage.knowledge_repository import (
    KnowledgeRepositoryDataError,
    SqlTenantKnowledge,
)


def _run(coro):
    return asyncio.run(coro)


class _Rows:

    def __init__(self, rows):
        self._rows = rows

    def mappings(self):
        return self

    def all(self):
        return self._rows


class _Connection:

    def __init__(self, rows=()):
        self.rows = rows
        self.statements = []

    async def execute(self, statement):
        self.statements.append(statement)
        return _Rows(self.rows)


class _Engine:

    def __init__(self, rows=()):
        self.connection = _Connection(rows)

    @asynccontextmanager
    async def connect(self):
        yield self.connection

    @asynccontextmanager
    async def begin(self):
        yield self.connection


@pytest.fixture(autouse=True)
def _knowledge_table(monkeypatch):
    table = sa.table(
        "knowledge_documents",
        sa.column("tenant_id"),
        sa.column("document_id"),
        sa.column("content"),
        sa.column("metadata", postgresql.JSONB),
        sa.column("updated_at"),
    )
    monkeypatch.setattr(knowledge_repository, "_knowledge_documents", lambda: table)


def _request(query: str = "deployment policy", *, limit: int = 3, metadata=None) -> SearchRequest:
    return SearchRequest(
        query=Part(text=query),
        params=SearchParams(rank_top_k=limit, extra_params={} if metadata is None else {"metadata": metadata}),
    )


def test_search_returns_sdk_documents_in_deterministic_rank_then_document_order():
    engine = _Engine([
        {
            "document_id": "doc_a",
            "content": "policy A",
            "metadata": {
                "kind": "runbook"
            },
            "rank": 0.8
        },
        {
            "document_id": "doc_b",
            "content": "policy B",
            "metadata": {
                "kind": "runbook"
            },
            "rank": 0.8
        },
    ])
    knowledge = SqlTenantKnowledge(engine, "tenant_alpha")

    result = _run(knowledge.search(None, _request()))

    assert [item.document.id for item in result.documents] == ["doc_a", "doc_b"]
    assert [item.document.page_content for item in result.documents] == ["policy A", "policy B"]
    assert [item.score for item in result.documents] == [0.8, 0.8]
    compiled = engine.connection.statements[0].compile(dialect=postgresql.dialect())
    assert "knowledge_documents.tenant_id = %(tenant_id_1)s" in str(compiled)
    assert compiled.params["tenant_id_1"] == "tenant_alpha"
    assert "ORDER BY rank DESC, knowledge_documents.document_id ASC" in str(compiled)


def test_search_applies_tenant_scoped_metadata_filter_and_limit():
    engine = _Engine()
    knowledge = SqlTenantKnowledge(engine, "tenant_alpha")

    _run(knowledge.search(None, _request(limit=2, metadata={"kind": "runbook"})))

    compiled = engine.connection.statements[0].compile(dialect=postgresql.dialect())
    assert "knowledge_documents.tenant_id = %(tenant_id_1)s" in str(compiled)
    assert "knowledge_documents.metadata @>" in str(compiled)
    assert compiled.params["tenant_id_1"] == "tenant_alpha"
    assert compiled.params["param_1"] == 2


@pytest.mark.parametrize(
    "search_request",
    [
        SearchRequest(),
        SearchRequest(query=Part(text="   ")),
        SearchRequest(query=Part(inline_data={
            "data": b"x",
            "mime_type": "text/plain"
        })),
        _request(limit=0),
        _request(limit=21),
        _request(metadata={"bad key": "value"}),
        _request(metadata={"kind": ["not", "scalar"]}),
    ],
)
def test_search_rejects_invalid_query_limit_or_metadata_without_database_access(search_request):
    engine = _Engine()
    knowledge = SqlTenantKnowledge(engine, "tenant_alpha")

    with pytest.raises(KnowledgeRepositoryDataError):
        _run(knowledge.search(None, search_request))
    assert engine.connection.statements == []


def test_upsert_and_delete_always_scope_mutations_to_one_tenant():
    engine = _Engine()
    knowledge = SqlTenantKnowledge(engine, "tenant_alpha")

    _run(knowledge.upsert_document("doc_1", "deployment policy", {"kind": "runbook"}))
    _run(knowledge.delete_document("doc_1"))

    upsert = engine.connection.statements[0].compile(dialect=postgresql.dialect())
    delete = engine.connection.statements[1].compile(dialect=postgresql.dialect())
    assert "tenant_id" in str(upsert)
    assert "knowledge_documents.tenant_id = %(tenant_id_1)s" in str(delete)
    assert "tenant_alpha" in upsert.params.values()
    assert delete.params["tenant_id_1"] == "tenant_alpha"


@pytest.mark.parametrize(
    "document_id,text,metadata",
    [
        ("", "content", {}),
        ("doc_1", "   ", {}),
        ("doc_1", "content", {
            "bad key": "value"
        }),
        ("doc_1", "content", {
            "kind": {
                "nested": "value"
            }
        }),
    ],
)
def test_upsert_rejects_unsafe_document_values_without_database_access(document_id, text, metadata):
    engine = _Engine()
    knowledge = SqlTenantKnowledge(engine, "tenant_alpha")

    with pytest.raises(KnowledgeRepositoryDataError):
        _run(knowledge.upsert_document(document_id, text, metadata))
    assert engine.connection.statements == []
