"""PostgreSQL unified audit projection, always tenant scoped."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Protocol
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine

from trpc_service.audit.query import UnifiedAuditRecord
from trpc_service.storage.schema import execution_audit_events, message_receipts, request_usage_records, tenant_configs


class AuditQueryRepositoryUnavailableError(RuntimeError):
    pass


class AuditQueryRepositoryDataError(RuntimeError):
    pass


class AuditQueryRepository(Protocol):

    async def list_for_tenant(self,
                              tenant_id: str,
                              *,
                              request_id: UUID | None = None,
                              trace_id: str | None = None,
                              before: datetime | None = None,
                              limit: int = 50) -> tuple[UnifiedAuditRecord, ...]:
        ...

    async def check_ready(self) -> None:
        ...

    async def close(self) -> None:
        ...


class SqlAuditQueryRepository:

    def __init__(self, engine: AsyncEngine, *, owns_engine: bool = True) -> None:
        self._engine, self._owns_engine, self._closed = engine, owns_engine, False

    async def list_for_tenant(self,
                              tenant_id: str,
                              *,
                              request_id: UUID | None = None,
                              trace_id: str | None = None,
                              before: datetime | None = None,
                              limit: int = 50) -> tuple[UnifiedAuditRecord, ...]:
        if self._closed:
            raise AuditQueryRepositoryUnavailableError("audit query repository is closed")
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise AuditQueryRepositoryDataError("limit must be between 1 and 100")
        # Audit policy lives on the tenant row: application never lets a
        # caller choose an older window than policy permits.
        try:
            async with self._engine.connect() as conn:
                policy = (await conn.execute(
                    sa.select(tenant_configs.c.audit_policy).where(tenant_configs.c.tenant_id == tenant_id)
                )).scalar_one_or_none()
                if policy is None:
                    return ()
                retention = int(policy["retention_days"])
                floor = datetime.now(timezone.utc) - timedelta(days=retention)
                stmt = sa.select(
                    execution_audit_events,
                    message_receipts.c.channel,
                    message_receipts.c.user_id,
                    message_receipts.c.session_id,
                    message_receipts.c.app_id,
                    request_usage_records.c.cost_microunits.label("usage_cost"),
                ).select_from(
                    execution_audit_events.outerjoin(
                        message_receipts,
                        sa.and_(
                            execution_audit_events.c.receipt_id == message_receipts.c.receipt_id,
                            execution_audit_events.c.tenant_id == message_receipts.c.tenant_id,
                        )).outerjoin(
                            request_usage_records,
                            sa.and_(
                                execution_audit_events.c.tenant_id == request_usage_records.c.tenant_id,
                                execution_audit_events.c.request_id == request_usage_records.c.request_id,
                            ))).where(
                                execution_audit_events.c.tenant_id == tenant_id,
                                execution_audit_events.c.occurred_at >= floor,
                            )
                if request_id is not None:
                    stmt = stmt.where(execution_audit_events.c.request_id == request_id)
                if trace_id is not None:
                    stmt = stmt.where(execution_audit_events.c.trace_id == trace_id)
                if before is not None:
                    stmt = stmt.where(execution_audit_events.c.occurred_at < before)
                rows = (await conn.execute(
                    stmt.order_by(execution_audit_events.c.occurred_at.desc(),
                                  execution_audit_events.c.audit_id.desc()).limit(limit))).fetchall()
        except (DBAPIError, OperationalError, OSError):
            raise AuditQueryRepositoryUnavailableError("audit query failed") from None
        records = []
        for row in rows:
            m = row._mapping
            records.append(
                UnifiedAuditRecord(
                    tenant_id=m["tenant_id"],
                    channel=m["channel"],
                    user_id=m["user_id"],
                    session_id=m["session_id"],
                    agent_name=m["app_id"],
                    tool_name=m["tool_name"],
                    decision=m["outcome"],
                    latency_ms=m["latency_ms"],
                    error_type=m["event_type"] if m["error_code"] else None,
                    error_code=m["error_code"],
                    cost_microunits=m["usage_cost"],
                    trace_id=m["trace_id"],
                    request_id=m["request_id"],
                    config_version=m["config_version"],
                    occurred_at=m["occurred_at"],
                ))
        return tuple(records)

    async def check_ready(self) -> None:
        if self._closed:
            raise AuditQueryRepositoryUnavailableError("audit query repository is closed")
        try:
            async with self._engine.connect() as conn:
                await conn.execute(sa.text("SELECT 1 FROM execution_audit_events LIMIT 0"))
        except (DBAPIError, OperationalError, OSError):
            raise AuditQueryRepositoryUnavailableError("audit query repository is unavailable") from None

    async def close(self) -> None:
        if not self._closed:
            self._closed = True
            if self._owns_engine:
                await self._engine.dispose()


__all__ = [
    "AuditQueryRepository",
    "AuditQueryRepositoryDataError",
    "AuditQueryRepositoryUnavailableError",
    "SqlAuditQueryRepository",
]
