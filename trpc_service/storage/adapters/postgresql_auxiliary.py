"""Small PostgreSQL adapters for independently selectable storage capabilities."""

from collections.abc import Sequence
from decimal import Decimal, InvalidOperation
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.storage.ports import AuditStore, MemoryStore, SummaryStore
from trpc_service.storage.scoring import lexical_score
from trpc_service.storage.runtime_orm import AuditLogRow, MemoryRecordRow, SessionSummaryRow
from trpc_service.storage.types import (
    AuditRecord,
    MemoryHit,
    MemoryRecord,
    SessionSummary,
)
from trpc_service.tenant.context import TenantContext


class _PostgreSQLAdapter:
    """Share only connection ownership; every public adapter keeps one responsibility."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        self._sessions = sessions


class PostgreSQLMemoryStore(_PostgreSQLAdapter, MemoryStore):
    """Persist and query scoped long-term Memory records."""

    async def upsert(
        self,
        context: TenantContext,
        records: Sequence[MemoryRecord],
    ) -> None:
        """Insert or replace stable Memory IDs inside the mandatory scope."""

        async with self._sessions.begin() as database:
            for record in records:
                row = await database.scalar(
                    select(MemoryRecordRow).where(
                        MemoryRecordRow.tenant_id == context.tenant_id,
                        MemoryRecordRow.agent_app_id == context.agent_app_id,
                        MemoryRecordRow.memory_id == record.memory_id,
                    ))
                if row is None:
                    database.add(
                        MemoryRecordRow(
                            tenant_id=context.tenant_id,
                            agent_app_id=context.agent_app_id,
                            memory_id=record.memory_id,
                            principal_id=record.principal_id,
                            content=record.content,
                            attributes=dict(record.attributes),
                        ))
                else:
                    row.principal_id = record.principal_id
                    row.content = record.content
                    row.attributes = dict(record.attributes)

    async def search(
        self,
        context: TenantContext,
        principal_id: str,
        query: str,
        limit: int,
    ) -> Sequence[MemoryHit]:
        """Rank scoped Memory using a deterministic provider-neutral fallback."""

        async with self._sessions() as database:
            rows = (await database.scalars(
                select(MemoryRecordRow).where(
                    MemoryRecordRow.tenant_id == context.tenant_id,
                    MemoryRecordRow.agent_app_id == context.agent_app_id,
                    MemoryRecordRow.principal_id == principal_id,
                ))).all()
        hits = [
            MemoryHit(
                record=MemoryRecord(
                    row.memory_id,
                    row.principal_id,
                    row.content,
                    row.attributes,
                ),
                score=lexical_score(row.content, query),
            ) for row in rows
        ]
        return sorted(hits, key=lambda hit: (-hit.score, hit.record.memory_id))[:limit]


class PostgreSQLSummaryStore(_PostgreSQLAdapter, SummaryStore):
    """Persist monotonic Session summaries without coupling them to execution."""

    async def put_if_newer(
        self,
        context: TenantContext,
        summary: SessionSummary,
    ) -> bool:
        """Update a Summary only when it covers a newer Event sequence."""

        async with self._sessions.begin() as database:
            row = await database.scalar(
                select(SessionSummaryRow).where(
                    SessionSummaryRow.tenant_id == context.tenant_id,
                    SessionSummaryRow.agent_app_id == context.agent_app_id,
                    SessionSummaryRow.session_id == summary.session_id,
                ).with_for_update())
            if row is not None and row.source_event_seq >= summary.source_event_seq:
                return False
            if row is None:
                database.add(
                    SessionSummaryRow(
                        tenant_id=context.tenant_id,
                        agent_app_id=context.agent_app_id,
                        session_id=summary.session_id,
                        source_event_seq=summary.source_event_seq,
                        content=summary.content,
                        attributes=dict(summary.attributes),
                    ))
            else:
                row.source_event_seq = summary.source_event_seq
                row.content = summary.content
                row.attributes = dict(summary.attributes)
            return True


def _audit_latency_ms(value: object) -> int | None:
    """Normalize measured milliseconds for the integer query column."""

    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value < 0:
        raise ValueError("audit latency_ms must be a non-negative number")
    return round(value)


def _audit_cost_amount(value: object) -> str | None:
    """Store monetary values as exact decimal text instead of binary floats."""

    if value is None:
        return None
    try:
        amount = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError("audit cost_amount must be a decimal number") from error
    if not amount.is_finite() or amount < 0:
        raise ValueError("audit cost_amount must be a non-negative finite number")
    return format(amount, "f")


class PostgreSQLAuditStore(_PostgreSQLAdapter, AuditStore):
    """Append immutable, queryable governance decisions."""

    async def append(self, context: TenantContext, record: AuditRecord) -> None:
        """Append one immutable Audit decision with explicit query dimensions."""

        attributes = dict(record.attributes)
        # UUID values remain typed in columns and become strings only in JSON.
        details_redacted = dict(attributes)
        if details_redacted.get("binding_id") is not None:
            details_redacted["binding_id"] = str(details_redacted["binding_id"])
        async with self._sessions.begin() as database:
            database.add(
                AuditLogRow(
                    audit_id=uuid4(),
                    tenant_id=context.tenant_id,
                    agent_app_id=context.agent_app_id,
                    binding_id=attributes.get("binding_id"),
                    principal_id=attributes.get("principal_id"),
                    session_id=attributes.get("session_id"),
                    request_id=context.request_id,
                    trace_id=context.trace_id,
                    action=record.action,
                    decision=record.decision,
                    policy_version=attributes.get("policy_version"),
                    tool_name=attributes.get("tool_name"),
                    latency_ms=_audit_latency_ms(attributes.get("latency_ms")),
                    error_type=attributes.get("error_type"),
                    cost_amount=_audit_cost_amount(attributes.get("cost_amount")),
                    occurred_at=record.occurred_at,
                    details_redacted=details_redacted,
                ))
