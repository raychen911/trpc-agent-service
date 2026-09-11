"""Tenant-bound PostgreSQL full-text knowledge implementation."""

from __future__ import annotations

import math
import re
from collections.abc import Mapping
from typing import Any

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.exc import TimeoutError as SATimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine
from langchain_core.documents import Document
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.knowledge import KnowledgeBase, SearchDocument, SearchRequest, SearchResult

_DOCUMENT_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_METADATA_KEY_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_.-]{0,63}$")
_TENANT_ID_RE = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")
_MAX_SEARCH_RESULTS = 20


class KnowledgeRepositoryDataError(ValueError):
    """A knowledge document or search request is not safe to persist."""


class KnowledgeRepositoryUnavailableError(RuntimeError):
    """The knowledge database cannot service an operation."""


def _knowledge_documents():
    """Resolve the R1C table after schema metadata has been initialized."""
    from trpc_service.storage import schema

    table = getattr(schema, "knowledge_documents", None)
    if table is None:
        raise KnowledgeRepositoryUnavailableError("knowledge storage is not initialized")
    return table


class SqlTenantKnowledge(KnowledgeBase):
    """Minimal tenant-isolated PostgreSQL keyword/full-text knowledge base."""

    def __init__(self, engine: AsyncEngine, tenant_id: str) -> None:
        if not isinstance(tenant_id, str) or _TENANT_ID_RE.fullmatch(tenant_id) is None:
            raise KnowledgeRepositoryDataError("knowledge tenant ID is invalid")
        self._engine = engine
        self._tenant_id = tenant_id
        self._closed = False

    async def upsert_document(self, document_id: str, text: str, metadata: dict) -> None:
        """Create or replace one tenant-owned text document."""
        self._require_open()
        document_id = _require_document_id(document_id)
        text = _require_text(text)
        metadata = _require_metadata(metadata)
        documents = _knowledge_documents()
        statement = pg_insert(documents).values(
            tenant_id=self._tenant_id,
            document_id=document_id,
            content=text,
            metadata=metadata,
        ).on_conflict_do_update(
            index_elements=["tenant_id", "document_id"],
            set_={
                "content": text,
                "metadata": metadata,
                "updated_at": sa.text("now()"),
            },
        )
        try:
            async with self._engine.begin() as connection:
                await connection.execute(statement)
        except IntegrityError:
            raise KnowledgeRepositoryDataError("knowledge document violates storage rules") from None
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise KnowledgeRepositoryUnavailableError("knowledge database write failed") from None

    async def delete_document(self, document_id: str) -> None:
        """Delete one tenant-owned document; deletion of an absent row is idempotent."""
        self._require_open()
        document_id = _require_document_id(document_id)
        documents = _knowledge_documents()
        statement = sa.delete(documents).where(
            documents.c.tenant_id == self._tenant_id,
            documents.c.document_id == document_id,
        )
        try:
            async with self._engine.begin() as connection:
                await connection.execute(statement)
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise KnowledgeRepositoryUnavailableError("knowledge database delete failed") from None

    async def search(self, ctx: AgentContext, req: SearchRequest) -> SearchResult:
        """Search only this tenant's documents using PostgreSQL full-text ranking."""
        del ctx
        self._require_open()
        query, limit, metadata_filter = _validate_search_request(req)
        documents = _knowledge_documents()
        vector = sa.func.to_tsvector("simple", documents.c.content)
        tsquery = sa.func.websearch_to_tsquery("simple", query)
        rank = sa.func.ts_rank_cd(vector, tsquery).label("rank")
        conditions = [
            documents.c.tenant_id == self._tenant_id,
            vector.op("@@")(tsquery),
        ]
        if metadata_filter:
            conditions.append(documents.c.metadata.contains(metadata_filter))
        statement = sa.select(
            documents.c.document_id,
            documents.c.content,
            documents.c.metadata,
            rank,
        ).where(*conditions).order_by(
            rank.desc(),
            documents.c.document_id.asc(),
        ).limit(limit)
        try:
            async with self._engine.connect() as connection:
                rows = (await connection.execute(statement)).mappings().all()
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise KnowledgeRepositoryUnavailableError("knowledge database search failed") from None
        return SearchResult(documents=[
            SearchDocument(
                document=Document(
                    id=row["document_id"],
                    page_content=row["content"],
                    metadata=dict(row["metadata"]),
                ),
                score=float(row["rank"]),
            ) for row in rows
        ])

    async def check_ready(self) -> None:
        self._require_open()
        documents = _knowledge_documents()
        try:
            async with self._engine.connect() as connection:
                await connection.execute(sa.select(documents.c.document_id).limit(0))
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise KnowledgeRepositoryUnavailableError("knowledge database is not reachable") from None

    async def close(self) -> None:
        """Release this tenant binding; the shared Worker engine stays owned elsewhere."""
        self._closed = True

    def _require_open(self) -> None:
        if self._closed:
            raise KnowledgeRepositoryUnavailableError("knowledge repository is closed")


def _validate_search_request(req: SearchRequest) -> tuple[str, int, dict[str, Any]]:
    if not isinstance(req, SearchRequest):
        raise KnowledgeRepositoryDataError("knowledge search request is invalid")
    query_part = req.query
    query = getattr(query_part, "text", None)
    query = _require_text(query, message="knowledge search query is invalid")
    params = req.params
    limit = getattr(params, "rank_top_k", None)
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= _MAX_SEARCH_RESULTS:
        raise KnowledgeRepositoryDataError("knowledge search limit is invalid")
    extra_params = getattr(params, "extra_params", None) or {}
    if not isinstance(extra_params, Mapping) or set(extra_params) - {"metadata"}:
        raise KnowledgeRepositoryDataError("knowledge search metadata is invalid")
    metadata = extra_params.get("metadata", {})
    return query, limit, _require_metadata(metadata)


def _require_document_id(document_id: str) -> str:
    if not isinstance(document_id, str) or _DOCUMENT_ID_RE.fullmatch(document_id) is None:
        raise KnowledgeRepositoryDataError("knowledge document ID is invalid")
    return document_id


def _require_text(text: object, *, message: str = "knowledge document text is invalid") -> str:
    if not isinstance(text, str) or not text.strip():
        raise KnowledgeRepositoryDataError(message)
    return text


def _require_metadata(metadata: object) -> dict[str, Any]:
    if not isinstance(metadata, Mapping):
        raise KnowledgeRepositoryDataError("knowledge metadata is invalid")
    normalized: dict[str, Any] = {}
    for key, value in metadata.items():
        if not isinstance(key, str) or _METADATA_KEY_RE.fullmatch(key) is None:
            raise KnowledgeRepositoryDataError("knowledge metadata is invalid")
        if isinstance(value, bool) or value is None or isinstance(value, (str, int)):
            normalized[key] = value
            continue
        if isinstance(value, float) and math.isfinite(value):
            normalized[key] = value
            continue
        raise KnowledgeRepositoryDataError("knowledge metadata is invalid")
    return normalized


__all__ = [
    "KnowledgeRepositoryDataError",
    "KnowledgeRepositoryUnavailableError",
    "SqlTenantKnowledge",
]
