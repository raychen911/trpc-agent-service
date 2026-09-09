# mypy: disable-error-code="import-untyped"
"""Read-only SDK MemoryService over the platform's durable memory projection."""

from __future__ import annotations

import re
import time
from collections.abc import Sequence

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.memory import BaseMemoryService
from trpc_agent_sdk.sessions import Session
from trpc_agent_sdk.types import Content, MemoryEntry, Part, SearchMemoryResponse

from trpc_service.metrics import METRICS
from trpc_service.storage.models import MemoryRecord
from trpc_service.tenant.context import TenantContext
from trpc_service.tool import TENANT_CONTEXT_METADATA_KEY

_WORD = re.compile(r"[\w\u4e00-\u9fff]+", re.UNICODE)
_MAX_CANDIDATES = 200


class MemoryAuthorizationError(PermissionError):
    """A memory lookup lacks a matching trusted tenant context."""


class ProjectedSqlMemoryService(BaseMemoryService):
    """Expose Projector-produced explicit memories to tRPC-Agent tools.

    Writes remain owned by the fenced Projector, so ``store_session`` is a no-op.
    This prevents the SDK's generic post-turn hook from creating a second writer.
    """

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        super().__init__(enabled=True)
        self._session_factory = session_factory

    async def store_session(
        self,
        session: Session,
        agent_context: AgentContext | None = None,
    ) -> None:
        del session, agent_context

    async def search_memory(
        self,
        key: str,
        query: str,
        limit: int = 10,
        agent_context: AgentContext | None = None,
    ) -> SearchMemoryResponse:
        context = _trusted_context(key, agent_context)
        bounded_limit = min(max(limit, 0), 20)
        if bounded_limit == 0 or not query.strip():
            return SearchMemoryResponse(memories=[])

        started = time.monotonic()
        outcome = "error"
        try:
            async with self._session_factory() as database, database.begin():
                await _set_tenant_scope(database, context.tenant_id)
                rows = (
                    await database.scalars(
                        select(MemoryRecord)
                        .where(
                            MemoryRecord.tenant_id == context.tenant_id,
                            MemoryRecord.principal_id == context.principal_id,
                        )
                        .order_by(
                            MemoryRecord.record_version.desc(),
                            MemoryRecord.created_at.desc(),
                        )
                        .limit(_MAX_CANDIDATES)
                    )
                ).all()
            ranked = _rank_memories(rows, query)[:bounded_limit]
            outcome = "success"
            return SearchMemoryResponse(
                memories=[
                    MemoryEntry(
                        content=Content(
                            role="user",
                            parts=[Part.from_text(text=row.content)],
                        ),
                        author="user",
                        timestamp=row.created_at.isoformat(),
                    )
                    for row in ranked
                ]
            )
        finally:
            METRICS.storage_duration_seconds.labels(
                "postgresql",
                "memory_search",
                outcome,
            ).observe(max(0.0, time.monotonic() - started))

    async def close(self) -> None:
        """The owning process closes the shared SQLAlchemy engine."""


def _trusted_context(key: str, agent_context: AgentContext | None) -> TenantContext:
    if agent_context is None:
        raise MemoryAuthorizationError("trusted tenant context is missing")
    context = agent_context.get_metadata(TENANT_CONTEXT_METADATA_KEY)
    if not isinstance(context, TenantContext):
        raise MemoryAuthorizationError("trusted tenant context is missing")
    _, separator, principal = key.rpartition("/")
    if not separator or principal != context.principal_id:
        raise MemoryAuthorizationError("memory key does not match the invocation principal")
    return context


def _rank_memories(rows: Sequence[MemoryRecord], query: str) -> list[MemoryRecord]:
    terms = {term.casefold() for term in _WORD.findall(query) if term.strip()}
    if not terms:
        return list(rows)

    def score(row: MemoryRecord) -> tuple[int, int]:
        content = row.content.casefold()
        overlap = sum(term in content for term in terms)
        return overlap, row.record_version

    return sorted((row for row in rows if score(row)[0] > 0), key=score, reverse=True)


async def _set_tenant_scope(database: AsyncSession, tenant_id: str) -> None:
    if database.get_bind().dialect.name == "postgresql":
        await database.execute(
            text("SELECT set_config('app.tenant_id', :tenant_id, true)"),
            {"tenant_id": tenant_id},
        )
