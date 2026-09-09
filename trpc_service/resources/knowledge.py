"""Small deterministic Knowledge provider whose source documents are truth."""

from __future__ import annotations

import asyncio
import re
import uuid
from typing import Protocol
from typing import Any

from pydantic import BaseModel
from pydantic import Field


class KnowledgeDocument(BaseModel):
    document_id: str = Field(default_factory=lambda: uuid.uuid4().hex)
    tenant_id: str
    app_id: str
    title: str
    text: str
    metadata: dict[str, str] = Field(default_factory=dict)


class KnowledgeHit(BaseModel):
    document_id: str
    title: str
    excerpt: str
    score: float


class KnowledgeProvider(Protocol):

    async def add(self, document: KnowledgeDocument) -> KnowledgeDocument:
        ...

    async def search(self, tenant_id: str, app_id: str, query: str, limit: int = 5) -> list[KnowledgeHit]:
        ...


class InMemoryKnowledgeProvider:
    """Token-overlap search used by tests; no claim of vector-search quality."""

    def __init__(self) -> None:
        self._documents: dict[tuple[str, str, str], KnowledgeDocument] = {}
        self._lock = asyncio.Lock()

    async def add(self, document: KnowledgeDocument) -> KnowledgeDocument:
        async with self._lock:
            self._documents[(document.tenant_id, document.app_id,
                             document.document_id)] = document.model_copy(deep=True)
        return document.model_copy(deep=True)

    async def search(self, tenant_id: str, app_id: str, query: str, limit: int = 5) -> list[KnowledgeHit]:
        terms = set(re.findall(r"[\w\u4e00-\u9fff]+", query.lower()))
        hits: list[KnowledgeHit] = []
        async with self._lock:
            documents = [
                value.model_copy(deep=True) for key, value in self._documents.items() if key[:2] == (tenant_id, app_id)
            ]
        for document in documents:
            haystack = set(re.findall(r"[\w\u4e00-\u9fff]+", f"{document.title} {document.text}".lower()))
            overlap = len(terms & haystack)
            if overlap:
                hits.append(
                    KnowledgeHit(document_id=document.document_id,
                                 title=document.title,
                                 excerpt=document.text[:300],
                                 score=overlap / max(1, len(terms))))
        return sorted(hits, key=lambda item: (-item.score, item.document_id))[:limit]


class PostgresKnowledgeProvider:
    """PostgreSQL source documents with deterministic local retrieval."""

    def __init__(self, pool: Any) -> None:
        self._pool = pool

    async def add(self, document: KnowledgeDocument) -> KnowledgeDocument:
        await self._pool.execute(
            """
            INSERT INTO knowledge_document
                (document_id,tenant_id,app_id,title,body,metadata)
            VALUES ($1,$2,$3,$4,$5,$6::jsonb)
            ON CONFLICT (tenant_id,document_id) DO NOTHING
            """, document.document_id, document.tenant_id, document.app_id, document.title, document.text,
            document.model_dump_json(include={"metadata"}))
        return document.model_copy(deep=True)

    async def search(self, tenant_id: str, app_id: str, query: str, limit: int = 5) -> list[KnowledgeHit]:
        rows = await self._pool.fetch(
            "SELECT document_id,title,body FROM knowledge_document WHERE tenant_id=$1 AND app_id=$2", tenant_id, app_id)
        terms = set(re.findall(r"[\w\u4e00-\u9fff]+", query.lower()))
        hits = []
        for row in rows:
            haystack = set(re.findall(r"[\w\u4e00-\u9fff]+", f"{row['title']} {row['body']}".lower()))
            overlap = len(terms & haystack)
            if overlap:
                hits.append(
                    KnowledgeHit(document_id=row["document_id"],
                                 title=row["title"],
                                 excerpt=row["body"][:300],
                                 score=overlap / max(1, len(terms))))
        return sorted(hits, key=lambda item: (-item.score, item.document_id))[:limit]
