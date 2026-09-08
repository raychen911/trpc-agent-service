# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Durable SQL audit sink built on the framework's :class:`SqlStorage`.

Writes each :class:`AuditLogEntry` into an append-only ``audit_log`` table so
audit history survives process restarts (satisfying the retention requirement).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any
from typing import Optional
from uuid import uuid4

from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import func
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.types import String

from trpc_agent_sdk.storage import DEFAULT_MAX_KEY_LENGTH
from trpc_agent_sdk.storage import DEFAULT_MAX_VARCHAR_LENGTH
from trpc_agent_sdk.storage import DynamicJSON
from trpc_agent_sdk.storage import PreciseTimestamp
from trpc_agent_sdk.storage import SqlCondition
from trpc_agent_sdk.storage import SqlKey
from trpc_agent_sdk.storage import SqlStorage

from ._models import AuditLogEntry


class AuditStorageData(DeclarativeBase):
    """Base class for audit storage tables."""


class AuditLogRecord(AuditStorageData):
    """A single persisted audit log row."""

    __tablename__ = "audit_log"

    id: Mapped[str] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH), primary_key=True, default=lambda: str(uuid4()))
    tenant_id: Mapped[str] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH), nullable=False, index=True)
    channel: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    user_id: Mapped[Optional[str]] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH), nullable=True)
    session_id: Mapped[Optional[str]] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH), nullable=True)
    message_id: Mapped[Optional[str]] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH), nullable=True)
    turn_id: Mapped[Optional[str]] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH), nullable=True)
    config_revision: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    agent_name: Mapped[Optional[str]] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH), nullable=True)
    tool_name: Mapped[Optional[str]] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH), nullable=True)
    decision: Mapped[str] = mapped_column(String(32), default="allow")
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    error_type: Mapped[Optional[str]] = mapped_column(String(DEFAULT_MAX_VARCHAR_LENGTH), nullable=True)
    cost: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    trace_id: Mapped[Optional[str]] = mapped_column(String(DEFAULT_MAX_KEY_LENGTH), nullable=True)
    detail: Mapped[Optional[dict[str, Any]]] = mapped_column(DynamicJSON, nullable=True)
    created_at: Mapped[PreciseTimestamp] = mapped_column(PreciseTimestamp, default=func.now())

    @classmethod
    def from_entry(cls, entry: AuditLogEntry) -> "AuditLogRecord":
        return cls(
            tenant_id=entry.tenant_id,
            channel=entry.channel,
            user_id=entry.user_id,
            session_id=entry.session_id,
            message_id=entry.message_id,
            turn_id=entry.turn_id,
            config_revision=entry.config_revision,
            agent_name=entry.agent_name,
            tool_name=entry.tool_name,
            decision=entry.decision,
            latency_ms=entry.latency_ms,
            error_type=entry.error_type,
            cost=entry.cost,
            trace_id=entry.trace_id,
            detail=entry.detail,
        )

    def to_entry(self) -> AuditLogEntry:
        """Convert a storage row back to the public audit model."""
        return AuditLogEntry(
            tenant_id=self.tenant_id,
            channel=self.channel,
            user_id=self.user_id,
            session_id=self.session_id,
            message_id=self.message_id,
            turn_id=self.turn_id,
            config_revision=self.config_revision,
            agent_name=self.agent_name,
            tool_name=self.tool_name,
            decision=self.decision,
            latency_ms=self.latency_ms,
            error_type=self.error_type,
            cost=self.cost,
            trace_id=self.trace_id,
            detail=self.detail or {},
            created_at=self.created_at,
        )


class SqlAuditSink:
    """An :class:`AuditLogger` sink that persists entries to SQL."""

    def __init__(self, db_url: str, is_async: bool = True) -> None:
        self._storage = SqlStorage(is_async=is_async, db_url=db_url, metadata=AuditStorageData.metadata)
        self._ready = False

    async def _ensure_ready(self) -> None:
        if not self._ready:
            await self._storage.create_sql_engine()
            self._ready = True

    async def __call__(self, entry: AuditLogEntry) -> None:
        await self._ensure_ready()
        async with self._storage.create_db_session() as db:
            await self._storage.add(db, AuditLogRecord.from_entry(entry))
            await self._storage.commit(db)

    async def query(self, tenant_id: Optional[str] = None, limit: int = 100) -> list[AuditLogRecord]:
        """Query recent audit records, optionally filtered by tenant."""
        await self._ensure_ready()
        filters = [AuditLogRecord.tenant_id == tenant_id] if tenant_id is not None else None
        conditions = SqlCondition(filters=filters, order_func=AuditLogRecord.created_at.desc, limit=limit)
        key = SqlKey(key=tuple(), storage_cls=AuditLogRecord)
        async with self._storage.create_db_session() as db:
            return await self._storage.query(db, key, conditions)

    async def query_entries(
        self,
        *,
        tenant_id: Optional[str] = None,
        tool_name: Optional[str] = None,
        decision: Optional[str] = None,
        since: Optional[datetime] = None,
        limit: int = 100,
    ) -> list[AuditLogEntry]:
        """Query persisted entries using the same filters as ``AuditLogger``."""
        await self._ensure_ready()
        filters = []
        if tenant_id is not None:
            filters.append(AuditLogRecord.tenant_id == tenant_id)
        if tool_name is not None:
            filters.append(AuditLogRecord.tool_name == tool_name)
        if decision is not None:
            filters.append(AuditLogRecord.decision == decision)
        if since is not None:
            filters.append(AuditLogRecord.created_at >= since)
        conditions = SqlCondition(
            filters=filters or None,
            order_func=AuditLogRecord.created_at.desc,
            limit=limit,
        )
        key = SqlKey(key=tuple(), storage_cls=AuditLogRecord)
        async with self._storage.create_db_session() as db:
            records = await self._storage.query(db, key, conditions)
        return [record.to_entry() for record in records]

    async def close(self) -> None:
        await self._storage.close()
