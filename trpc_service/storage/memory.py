"""Deterministic tenant-scoped reference adapter for unit/contract tests.

This adapter intentionally has no process-global fallback and is not a
production shared store.  Durable deployments use the PostgreSQL repository
behind the same protocol.
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from .canonical import safe_size_bytes
from .contracts import (
    ContentTooLarge,
    IdempotencyConflict,
    SequenceGap,
    SummaryConflict,
    VersionConflict,
)
from .data_models import (
    ArtifactMetadata,
    ArtifactRecord,
    DataScope,
    EventMetadata,
    KnowledgeDocument,
    MemoryMetadata,
    MemoryRecord,
    SessionEvent,
    SummaryMetadata,
    SummaryRecord,
)


@dataclass(frozen=True, slots=True)
class WriteResult:
    record: Any
    outcome: str = "CREATED"


class InMemoryDataRepository:
    """Reference implementation; use only for deterministic tests."""

    def __init__(self, *, max_memory_bytes: int = 65_536) -> None:
        self.max_memory_bytes = max_memory_bytes
        self._events: dict[tuple[str, str], dict[str, SessionEvent]] = {}
        self._watermarks: dict[tuple[str, str], int] = {}
        self._memory: dict[tuple[str, str, str], MemoryRecord] = {}
        self._summaries: dict[tuple[str, str], SummaryRecord] = {}
        self._artifacts: dict[tuple[str, str], ArtifactMetadata] = {}
        self._knowledge: dict[tuple[str, str], KnowledgeDocument] = {}
        self._lock = asyncio.Lock()

    @staticmethod
    def _check_scope(scope: DataScope) -> None:
        if not isinstance(scope, DataScope) or not scope.tenant_id.strip():
            from .contracts import TenantScopeInvalid
            raise TenantScopeInvalid()

    async def append(self, scope: DataScope, event: SessionEvent, *, expected_watermark: int) -> WriteResult:
        self._check_scope(scope)
        if event.tenant_id != scope.tenant_id:
            from .contracts import TenantScopeInvalid
            raise TenantScopeInvalid()
        key = (scope.tenant_id, event.session_key)
        async with self._lock:
            bucket = self._events.setdefault(key, {})
            existing = bucket.get(event.event_id)
            if existing is not None:
                if existing.content_digest != event.content_digest:
                    raise IdempotencyConflict()
                return WriteResult(existing, "REPLAYED")
            current = self._watermarks.get(key, 0)
            if expected_watermark != current or event.sequence != current + 1:
                raise SequenceGap()
            bucket[event.event_id] = event
            self._watermarks[key] = event.sequence
            return WriteResult(event)

    async def list_metadata(self, scope: DataScope, session_key: str, *, after_sequence: int = 0, limit: int = 100) -> list[EventMetadata]:
        self._check_scope(scope)
        values = sorted(self._events.get((scope.tenant_id, session_key), {}).values(), key=lambda x: x.sequence)
        return [EventMetadata(
            tenant_id=x.tenant_id, session_key=x.session_key, event_id=x.event_id,
            sequence=x.sequence, event_type=x.event_type,
            content_digest=x.content_digest or "", trace_id=x.trace_id,
            created_at=x.created_at,
        ) for x in values if x.sequence > after_sequence][:limit]

    async def read_event_content(self, scope: DataScope, session_key: str, *, after_sequence: int = 0, limit: int = 100) -> list[SessionEvent]:
        self._check_scope(scope)
        values = sorted(self._events.get((scope.tenant_id, session_key), {}).values(), key=lambda x: x.sequence)
        return [x for x in values if x.sequence > after_sequence][:limit]

    async def get_watermark(self, scope: DataScope, session_key: str) -> int:
        self._check_scope(scope)
        return self._watermarks.get((scope.tenant_id, session_key), 0)

    async def compare_and_set(self, scope: DataScope, record: MemoryRecord, *, expected_version: int | None) -> WriteResult:
        self._check_scope(scope)
        if record.tenant_id != scope.tenant_id:
            from .contracts import TenantScopeInvalid
            raise TenantScopeInvalid()
        if safe_size_bytes(record.content) > min(record.max_bytes, self.max_memory_bytes):
            raise ContentTooLarge()
        key = (scope.tenant_id, record.namespace, record.memory_key)
        async with self._lock:
            current = self._memory.get(key)
            if current is None:
                if expected_version not in (None, 0) or record.version != 1:
                    raise VersionConflict()
            elif record.version == current.version and record.content_digest == current.content_digest:
                return WriteResult(current, "REPLAYED")
            elif expected_version != current.version:
                raise VersionConflict()
            if current and record.version != current.version + 1:
                raise VersionConflict()
            self._memory[key] = record
            return WriteResult(record)

    async def get_metadata(self, scope: DataScope, namespace: str, key: str) -> MemoryMetadata | None:
        self._check_scope(scope)
        record = self._memory.get((scope.tenant_id, namespace, key))
        if record is None:
            return None
        return MemoryMetadata(tenant_id=record.tenant_id, namespace=record.namespace, memory_key=record.memory_key, version=record.version, content_digest=record.content_digest or "", byte_size=safe_size_bytes(record.content), source_event_watermark=record.source_event_watermark, updated_at=record.updated_at)

    async def read_content(self, scope: DataScope, namespace_or_session: str, key: str | None = None, *, after_sequence: int = 0, limit: int = 100) -> Any:
        self._check_scope(scope)
        if key is None:
            return await self.read_event_content(scope, namespace_or_session, after_sequence=after_sequence, limit=limit)
        return self._memory.get((scope.tenant_id, namespace_or_session, key))

    async def compare_and_set_summary(self, scope: DataScope, summary: SummaryRecord, *, expected_version: int | None) -> WriteResult:
        self._check_scope(scope)
        if summary.tenant_id != scope.tenant_id:
            from .contracts import TenantScopeInvalid
            raise TenantScopeInvalid()
        key = (scope.tenant_id, summary.session_key)
        async with self._lock:
            current = self._summaries.get(key)
            event_watermark = self._watermarks.get(key, 0)
            if summary.event_watermark > event_watermark:
                raise VersionConflict()
            if current:
                if summary.event_watermark < current.event_watermark:
                    raise VersionConflict()
                if summary.event_watermark == current.event_watermark:
                    if summary.content_digest == current.content_digest:
                        return WriteResult(current, "REPLAYED")
                    raise SummaryConflict()
                if expected_version != current.version or summary.version != current.version + 1:
                    raise VersionConflict()
            elif expected_version not in (None, 0) or summary.version != 1:
                raise VersionConflict()
            self._summaries[key] = summary
            return WriteResult(summary)

    async def get_summary_metadata(self, scope: DataScope, session_key: str) -> SummaryMetadata | None:
        self._check_scope(scope)
        record = self._summaries.get((scope.tenant_id, session_key))
        if record is None:
            return None
        return SummaryMetadata(tenant_id=record.tenant_id, session_key=record.session_key, version=record.version, event_watermark=record.event_watermark, content_digest=record.content_digest or "", updated_at=record.updated_at)

    async def read_summary_content(self, scope: DataScope, session_key: str) -> SummaryRecord | None:
        self._check_scope(scope)
        return self._summaries.get((scope.tenant_id, session_key))

    # Legacy prototype methods remain available for earlier phases.
    async def append_event(self, event: SessionEvent) -> SessionEvent:
        scope = DataScope(tenant_id=event.tenant_id, trace_id=event.trace_id or __import__("uuid").UUID(int=1))
        return (await self.append(scope, event, expected_watermark=event.sequence - 1)).record

    async def get_events(self, *, tenant_id: str, session_key: str, after_sequence: int = 0) -> list[SessionEvent]:
        scope = DataScope(tenant_id=tenant_id, trace_id=__import__("uuid").UUID(int=1))
        return await self.read_event_content(scope, session_key, after_sequence=after_sequence)

    async def put_memory(self, record: MemoryRecord) -> MemoryRecord:
        scope = DataScope(tenant_id=record.tenant_id, trace_id=record.trace_id or __import__("uuid").UUID(int=1))
        return (await self.compare_and_set(scope, record, expected_version=record.version - 1 if record.version > 1 else None)).record

    async def get_memory(self, *, tenant_id: str, namespace: str, key: str) -> MemoryRecord | None:
        scope = DataScope(tenant_id=tenant_id, trace_id=__import__("uuid").UUID(int=1))
        return await self.read_content(scope, namespace, key)

    async def put_summary(self, record: SummaryRecord) -> SummaryRecord:
        # Compatibility path for the pre-v7 prototype, which did not have an
        # authoritative Event watermark. New callers use compare_and_set_summary.
        async with self._lock:
            key = (record.tenant_id, record.session_key)
            current = self._summaries.get(key)
            if current and record.event_watermark < current.event_watermark:
                raise ValueError("summary watermark regressed")
            self._summaries[key] = record
            return record

    async def get_summary(self, *, tenant_id: str, session_key: str) -> SummaryRecord | None:
        scope = DataScope(tenant_id=tenant_id, trace_id=__import__("uuid").UUID(int=1))
        return await self.read_summary_content(scope, session_key)

    async def put_artifact(self, record: ArtifactRecord) -> ArtifactRecord:
        self._artifacts[(record.tenant_id, record.artifact_id)] = record
        return record

    async def get_artifact(self, *, tenant_id: str, key: str) -> ArtifactRecord | None:
        return self._artifacts.get((tenant_id, key))  # type: ignore[return-value]

    async def upsert_knowledge(self, document: KnowledgeDocument) -> KnowledgeDocument:
        self._knowledge[(document.tenant_id, document.document_id)] = document
        return document

    async def search_knowledge(self, *, tenant_id: str, query: str, limit: int = 10) -> list[KnowledgeDocument]:
        del query
        return [x for (tenant, _), x in self._knowledge.items() if tenant == tenant_id][:limit]
