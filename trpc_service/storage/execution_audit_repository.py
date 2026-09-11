"""Append-only repository for execution audit events (Stage 6B2 Task 1).

Writes go one way only: the table (and its PostgreSQL UPDATE/DELETE-rejecting
trigger installed by migration 0006) is immutable once written.  Two write
paths exist by design:

- ``append()`` for facts recorded after a receipt already reached its terminal
  state (e.g. channel delivery results);
- :func:`append_events_in_transaction` for the terminal-state-bound facts the
  Worker hands to ``SqlMessageReceiptRepository.complete()/fail()``, which
  writes them inside the *same* transaction — a required audit failure aborts
  the terminal transition (fail-closed; the user never sees a "success" whose
  governance trail could not be written).

Database failures map to exactly two fixed error types:
:class:`ExecutionAuditRepositoryDataError` (integrity violation — bad or
conflicting data) and :class:`ExecutionAuditRepositoryUnavailableError`
(connectivity/query failures, including the append-only trigger refusing a
mutation).  Neither ever carries driver detail.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Mapping, Sequence
from typing import Protocol

import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError, IntegrityError, OperationalError
from sqlalchemy.exc import TimeoutError as SATimeoutError
from sqlalchemy.ext.asyncio import AsyncEngine

from trpc_service.audit.models import ExecutionAuditEvent
from trpc_service.storage.database import (
    DatabaseSettings,
    check_database_readiness,
    create_database_engine,
)
from trpc_service.storage.schema import execution_audit_events

_TENANT_ID_PATTERN = re.compile(r"^[a-z][a-z0-9_-]{0,63}$")

# PostgreSQL integrity SQLSTATEs → data errors (FK, unique, check, not-null).
_INTEGRITY_SQLSTATES = frozenset({"23000", "23001", "23502", "23503", "23505", "23514"})


class ExecutionAuditRepositoryConfigurationError(ValueError):
    """Repository could not be configured."""


class ExecutionAuditRepositoryUnavailableError(RuntimeError):
    """Repository backend is not reachable, not ready, or refused a mutation."""


class ExecutionAuditRepositoryDataError(RuntimeError):
    """Event violates data/integrity rules or the backend returned corrupt rows."""


class ExecutionAuditRepository(Protocol):
    """Read/append protocol for the execution audit trail."""

    async def append(self, event: ExecutionAuditEvent) -> None:
        ...

    async def list_for_receipt(
        self,
        tenant_id: str,
        receipt_id: uuid.UUID,
        limit: int,
    ) -> tuple[ExecutionAuditEvent, ...]:
        ...

    async def list_for_request(
        self,
        tenant_id: str,
        request_id: uuid.UUID,
        limit: int,
    ) -> tuple[ExecutionAuditEvent, ...]:
        ...

    async def check_ready(self) -> None:
        ...

    async def close(self) -> None:
        ...


def _row_values(event: ExecutionAuditEvent) -> dict:
    return {
        "audit_id": event.audit_id,
        "tenant_id": event.tenant_id,
        "receipt_id": event.receipt_id,
        "request_id": event.request_id,
        "config_version": event.config_version,
        "trace_id": event.trace_id,
        "event_type": event.event_type,
        "outcome": event.outcome,
        "category": event.category,
        "tool_name": event.tool_name,
        "error_code": event.error_code,
        "latency_ms": event.latency_ms,
        "occurred_at": event.occurred_at,
    }


def _row_to_event(mapping: Mapping) -> ExecutionAuditEvent:
    return ExecutionAuditEvent(
        audit_id=mapping["audit_id"],
        tenant_id=mapping["tenant_id"],
        receipt_id=mapping["receipt_id"],
        request_id=mapping["request_id"],
        config_version=mapping["config_version"],
        trace_id=mapping["trace_id"],
        event_type=mapping["event_type"],
        outcome=mapping["outcome"],
        category=mapping["category"],
        tool_name=mapping["tool_name"],
        error_code=mapping["error_code"],
        latency_ms=mapping["latency_ms"],
        occurred_at=mapping["occurred_at"],
    )


def _sqlstate(exc: BaseException) -> str | None:
    orig = getattr(exc, "orig", exc)
    for attr in ("sqlstate", "pgcode"):
        code = getattr(orig, attr, None)
        if isinstance(code, str) and code:
            return code
    return None


def _raise_mapped(exc: DBAPIError | OSError) -> None:
    """Translate a driver error into the fixed audit error taxonomy."""
    if isinstance(exc, IntegrityError) and _sqlstate(exc) in _INTEGRITY_SQLSTATES:
        raise ExecutionAuditRepositoryDataError("execution audit row violates integrity rules") from None
    raise ExecutionAuditRepositoryUnavailableError("execution audit database query failed") from None


def validate_events_for_receipt(
    receipt_id: uuid.UUID,
    identity: Mapping,
    events: Sequence[ExecutionAuditEvent],
) -> None:
    """Reject non-tuple input or events whose identity deviates from the row.

    ``identity`` carries the receipt's authoritative ``tenant_id``,
    ``request_id`` and ``config_version``.  Raises
    :class:`ExecutionAuditRepositoryDataError`; callers in other repository
    boundaries map it to their own fixed error type.
    """
    if not isinstance(events, tuple):
        raise ExecutionAuditRepositoryDataError("execution_events must be an immutable tuple")
    expected = (
        receipt_id,
        identity["tenant_id"],
        identity["request_id"],
        identity["config_version"],
    )
    for event in events:
        if not isinstance(event, ExecutionAuditEvent):
            raise ExecutionAuditRepositoryDataError("execution_events contain an invalid event")
        actual = (event.receipt_id, event.tenant_id, event.request_id, event.config_version)
        if actual != expected:
            raise ExecutionAuditRepositoryDataError("execution event identity does not match receipt")


async def append_events_in_transaction(
    conn,
    events: Sequence[ExecutionAuditEvent],
) -> None:
    """Insert ``events`` on the caller's open connection (same transaction).

    Used by ``SqlMessageReceiptRepository.complete()/fail()`` so terminal
    state and its governance trail commit or abort together.  Validation and
    integrity problems raise :class:`ExecutionAuditRepositoryDataError`;
    connectivity or trigger aborts raise
    :class:`ExecutionAuditRepositoryUnavailableError`.
    """
    for event in events:
        if not isinstance(event, ExecutionAuditEvent):
            raise ExecutionAuditRepositoryDataError("execution_events must be ExecutionAuditEvent instances")
    if not events:
        return
    try:
        for event in events:
            await conn.execute(execution_audit_events.insert().values(_row_values(event)))
    except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
        _raise_mapped(exc)


class SqlExecutionAuditRepository:
    """PostgreSQL-backed append-only execution audit repository."""

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> "SqlExecutionAuditRepository":
        """Create a repository using ``TRPC_DATABASE_URL`` from the environment."""
        from trpc_service.storage.database import DatabaseConfigurationError

        try:
            settings = DatabaseSettings.from_env(environ)
        except DatabaseConfigurationError:
            # Fixed text, no cause: the underlying URL error must never chain
            # a DSN (even in a traceback) through this public boundary.
            raise ExecutionAuditRepositoryConfigurationError(
                "execution audit repository could not be configured") from None
        engine = create_database_engine(settings)
        return cls(engine)

    def __init__(self, engine: AsyncEngine, *, owns_engine: bool = True) -> None:
        self._engine = engine
        self._owns_engine = owns_engine
        self._closed = False

    async def append(self, event: ExecutionAuditEvent) -> None:
        """Append one audit event in its own transaction."""
        self._require_open()
        if not isinstance(event, ExecutionAuditEvent):
            raise ExecutionAuditRepositoryDataError("event must be an ExecutionAuditEvent")
        try:
            async with self._engine.begin() as conn:
                await conn.execute(execution_audit_events.insert().values(_row_values(event)))
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            _raise_mapped(exc)

    async def list_for_receipt(
        self,
        tenant_id: str,
        receipt_id: uuid.UUID,
        limit: int,
    ) -> tuple[ExecutionAuditEvent, ...]:
        """Events for one receipt, chronologically ordered, capped at ``limit``.

        The tenant filter is mandatory — rows are never enumerable across
        tenants even with a valid receipt id.
        """
        if not isinstance(receipt_id, uuid.UUID):
            raise ExecutionAuditRepositoryDataError("receipt_id must be a UUID")
        return await self._list_events(
            execution_audit_events.c.receipt_id == receipt_id,
            tenant_id=tenant_id,
            limit=limit,
        )

    async def list_for_request(
        self,
        tenant_id: str,
        request_id: uuid.UUID,
        limit: int,
    ) -> tuple[ExecutionAuditEvent, ...]:
        """Events for one request, chronologically ordered, capped at ``limit``.

        Covers events whose ``receipt_id`` is NULL (channel delivery results
        recorded after the receipt reached its terminal state), so no audited
        fact is write-only.  The tenant filter is mandatory.
        """
        if not isinstance(request_id, uuid.UUID):
            raise ExecutionAuditRepositoryDataError("request_id must be a UUID")
        return await self._list_events(
            execution_audit_events.c.request_id == request_id,
            tenant_id=tenant_id,
            limit=limit,
        )

    async def _list_events(
        self,
        *predicates,
        tenant_id: str,
        limit: int,
    ) -> tuple[ExecutionAuditEvent, ...]:
        self._require_open()
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ExecutionAuditRepositoryDataError("limit must be a positive integer")
        if _TENANT_ID_PATTERN.fullmatch(str(tenant_id)) is None:
            raise ExecutionAuditRepositoryDataError("invalid tenant ID")
        stmt = (sa.select(execution_audit_events).where(
            execution_audit_events.c.tenant_id == tenant_id,
            *predicates,
        ).order_by(execution_audit_events.c.occurred_at.asc(), execution_audit_events.c.audit_id.asc()).limit(limit))
        try:
            async with self._engine.connect() as conn:
                rows = (await conn.execute(stmt)).fetchall()
        except (DBAPIError, SATimeoutError, OperationalError, OSError) as exc:
            _raise_mapped(exc)
        try:
            return tuple(_row_to_event(row._mapping) for row in rows)
        except Exception:  # corrupt stored rows are a data problem, never a crash
            raise ExecutionAuditRepositoryDataError("stored execution audit rows are invalid") from None

    async def check_ready(self) -> None:
        """Verify the database is reachable and the audit table exists."""
        if self._closed:
            raise ExecutionAuditRepositoryUnavailableError("repository is closed")
        try:
            await check_database_readiness(self._engine)
            async with self._engine.connect() as conn:
                await conn.execute(sa.text("SELECT 1 FROM execution_audit_events LIMIT 0"))
        except (DBAPIError, SATimeoutError, OperationalError, OSError):
            raise ExecutionAuditRepositoryUnavailableError("execution audit database is not reachable") from None

    async def close(self) -> None:
        """Dispose the owned engine pool.  Idempotent."""
        if self._closed:
            return
        self._closed = True
        if self._owns_engine and self._engine is not None:
            await self._engine.dispose()

    def _require_open(self) -> None:
        if self._closed:
            raise ExecutionAuditRepositoryUnavailableError("repository is closed")


__all__ = [
    "ExecutionAuditRepository",
    "ExecutionAuditRepositoryConfigurationError",
    "ExecutionAuditRepositoryDataError",
    "ExecutionAuditRepositoryUnavailableError",
    "SqlExecutionAuditRepository",
    "append_events_in_transaction",
    "validate_events_for_receipt",
]
