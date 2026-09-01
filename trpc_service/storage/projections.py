"""Monotonic SQL projections for Summary and long-term Memory."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.storage.database import Database
from trpc_service.storage.models import MemoryRecord, SessionSummary


class ProjectionConflictError(RuntimeError):
    """The same projection version was reused with different content."""


@dataclass(frozen=True, slots=True)
class SummaryProjection:
    tenant_id: str
    session_id: str
    through_seq: int
    content: str
    summarizer_version: str


@dataclass(frozen=True, slots=True)
class MemoryProjection:
    tenant_id: str
    principal_id: str
    session_id: str
    source_event_id: str
    extractor_version: str
    record_version: int
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)


class SqlProjectionStore:
    """Persist monotonic projections while canonical Events remain authoritative."""

    def __init__(self, database: Database) -> None:
        self._database = database

    async def put_summary_if_newer(self, projection: SummaryProjection) -> bool:
        """Write a summary only when its committed-event watermark advances."""

        async with self._database.tenant_transaction(projection.tenant_id) as session:
            return await self.put_summary_if_newer_in_session(session, projection)

    async def put_memory_once(self, projection: MemoryProjection) -> bool:
        """Idempotently store one extracted memory by event and extractor version."""

        async with self._database.tenant_transaction(projection.tenant_id) as session:
            return await self.put_memory_once_in_session(session, projection)

    @staticmethod
    async def put_summary_if_newer_in_session(
        session: AsyncSession,
        projection: SummaryProjection,
    ) -> bool:
        """Apply a summary inside a caller-owned atomic transaction."""

        if projection.through_seq < 0:
            raise ValueError("through_seq must not be negative")
        current = await session.get(
            SessionSummary,
            (projection.tenant_id, projection.session_id),
            with_for_update=True,
        )
        if current is None:
            session.add(
                SessionSummary(
                    tenant_id=projection.tenant_id,
                    session_id=projection.session_id,
                    through_seq=projection.through_seq,
                    content=projection.content,
                    summarizer_version=projection.summarizer_version,
                )
            )
            return True
        if projection.through_seq < current.through_seq:
            return False
        if projection.through_seq == current.through_seq:
            if (
                current.content != projection.content
                or current.summarizer_version != projection.summarizer_version
            ):
                raise ProjectionConflictError("summary watermark has conflicting content")
            return False
        current.through_seq = projection.through_seq
        current.content = projection.content
        current.summarizer_version = projection.summarizer_version
        return True

    @staticmethod
    async def put_memory_once_in_session(
        session: AsyncSession,
        projection: MemoryProjection,
    ) -> bool:
        """Apply one event-keyed memory inside a caller-owned transaction.

        Older event records may arrive after newer ones. They are appended under
        their own immutable key and never overwrite a later ``record_version``.
        """

        if projection.record_version < 0:
            raise ValueError("record_version must not be negative")
        current = await session.scalar(
            select(MemoryRecord).where(
                MemoryRecord.tenant_id == projection.tenant_id,
                MemoryRecord.source_event_id == projection.source_event_id,
                MemoryRecord.extractor_version == projection.extractor_version,
            )
        )
        if current is not None:
            matches = (
                current.principal_id == projection.principal_id
                and current.session_id == projection.session_id
                and current.record_version == projection.record_version
                and current.content == projection.content
                and current.metadata_json == projection.metadata
            )
            if not matches:
                raise ProjectionConflictError("memory idempotency key has conflicting content")
            return False
        session.add(
            MemoryRecord(
                tenant_id=projection.tenant_id,
                principal_id=projection.principal_id,
                session_id=projection.session_id,
                source_event_id=projection.source_event_id,
                extractor_version=projection.extractor_version,
                record_version=projection.record_version,
                content=projection.content,
                metadata_json=projection.metadata,
            )
        )
        return True
