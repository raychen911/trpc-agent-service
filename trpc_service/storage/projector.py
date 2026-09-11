"""Version-guarded summary, memory and vector projections."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from ..tenant import SessionEvent
from .projection import InMemoryProjectionStore, ProjectionStore, Summary


@dataclass(frozen=True)
class ProjectionResult:
    tenant_id: str
    session_id: str
    source_version: int
    summary_updated: bool
    vectors_written: int


class SessionProjector:
    def __init__(
        self,
        summary_store: Any,
        *,
        vector_store: Any | None = None,
        summarize: Callable[[list[SessionEvent]], Awaitable[str]] | None = None,
        embed: Callable[[str], Awaitable[list[float]]] | None = None,
    ) -> None:
        self.summary_store = summary_store
        self.vector_store = vector_store
        self.summarize = summarize or self._default_summary
        self.embed = embed

    async def project(
        self, tenant_id: str, session_id: str, events: list[SessionEvent], source_version: int
    ) -> ProjectionResult:
        if source_version < 0 or any(event.tenant_id != tenant_id or event.session_id != session_id for event in events):
            raise ValueError("projection scope is invalid")
        content = await self.summarize(events)
        updated = await self.summary_store.put_summary(Summary(tenant_id, session_id, source_version, content))
        vectors = 0
        if updated and self.vector_store is not None and self.embed is not None and content:
            embedding = await self.embed(content)
            await self.vector_store.upsert(
                tenant_id,
                f"summary:{session_id}",
                content,
                embedding,
                source_version,
                collection="__session_summaries__",
            )
            vectors = 1
        return ProjectionResult(tenant_id, session_id, source_version, updated, vectors)

    @staticmethod
    async def _default_summary(events: list[SessionEvent]) -> str:
        values = [str(event.payload.get("text", "")) for event in events[-8:] if event.payload.get("text")]
        return "\n".join(values)[:8000]


class ProjectionCoordinator:
    """Build versioned projections from the tenant's authoritative event store."""

    def __init__(
        self,
        router: Any,
        *,
        embed: Callable[[str], Awaitable[list[float]]] | None = None,
        cache_store: Any | None = None,
    ) -> None:
        self.router = router
        self.embed = embed
        self.cache_store = cache_store
        self._summary_stores: dict[int, Any] = {}

    async def project(self, config: Any, tenant_id: str, session_id: str, source_version: int) -> ProjectionResult:
        routed = self.router.route(config)
        events = [
            event
            for event in await routed.session.events(tenant_id, session_id)
            if event.sequence <= source_version
        ]
        backend_key = id(routed.session)
        summary_store = self._summary_stores.get(backend_key)
        if summary_store is None:
            summary_store = (
                routed.session
                if hasattr(routed.session, "put_summary") and hasattr(routed.session, "get_summary")
                else ProjectionStore(routed.session)
                if hasattr(routed.session, "_scoped")
                else InMemoryProjectionStore()
            )
            self._summary_stores[backend_key] = summary_store
        projector = SessionProjector(summary_store, vector_store=routed.vector, embed=self.embed)
        result = await projector.project(tenant_id, session_id, events, source_version)
        if result.summary_updated and self.cache_store is not None:
            summary = await summary_store.get_summary(tenant_id, session_id)
            if summary is not None:
                await self.cache_store.put(
                    tenant_id,
                    "summary",
                    session_id,
                    summary.source_version,
                    {"content": summary.content},
                )
        return result

    async def recall(
        self,
        config: Any,
        tenant_id: str,
        query: str,
        collections: frozenset[str],
        limit: int = 5,
    ) -> list[str]:
        routed = self.router.route(config)
        if not collections or routed.vector is None or self.embed is None or not query.strip():
            return []
        matches = await routed.vector.search(
            tenant_id,
            await self.embed(query),
            limit=max(1, min(limit, 20)),
            collections=collections,
        )
        return [match.content for match in matches]

    async def index_knowledge(
        self,
        config: Any,
        tenant_id: str,
        collection: str,
        item_id: str,
        content: str,
        source_version: int,
    ) -> None:
        routed = self.router.route(config)
        if routed.vector is None or self.embed is None:
            raise RuntimeError("knowledge vector backend and embedding model are required")
        await routed.vector.upsert(
            tenant_id,
            f"{collection}:{item_id}",
            content,
            await self.embed(content),
            source_version,
            collection=collection,
        )


class OpenAIEmbedder:
    """Small async adapter for an OpenAI-compatible embeddings endpoint."""

    def __init__(self, model: str, api_key: str, *, base_url: str | None = None, dimensions: int = 1536) -> None:
        from openai import AsyncOpenAI

        self.model = model
        self.dimensions = dimensions
        self.client = AsyncOpenAI(api_key=api_key, base_url=base_url or None)

    async def __call__(self, content: str) -> list[float]:
        response = await self.client.embeddings.create(
            model=self.model,
            input=content,
            dimensions=self.dimensions,
        )
        return [float(value) for value in response.data[0].embedding]
