"""PostgreSQL phase-seven repositories with mandatory tenant predicates."""
from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from trpc_service.storage.canonical import safe_size_bytes
from trpc_service.storage.contracts import ContentTooLarge, IdempotencyConflict, MigrationConflict, SequenceGap, StaleFence, StateBackendUnavailable, SummaryConflict, TenantScopeInvalid, VersionConflict
from trpc_service.storage.data_models import ArtifactMetadata, DataRecoveryMarker, DataScope, EventMetadata, KnowledgeDocument, KnowledgeStatus, MemoryMetadata, MemoryRecord, SessionEvent, SummaryMetadata, SummaryRecord
from trpc_service.storage.memory import WriteResult
from trpc_service.storage.postgres.database import PostgresDatabase
from trpc_service.storage.postgres.models import ArtifactMetadataRow, DataRecoveryMarkerRow, KnowledgeDocumentRow, MemoryRecordRow, MigrationStateRow, SessionEventRow, SessionStreamRow, SummaryRecordRow


class PostgresSessionEventRepository:
    """Authoritative event stream with Event/watermark/audit in one transaction."""

    def __init__(self, database: PostgresDatabase, session: AsyncSession | None = None, *, audit: object | None = None) -> None:
        self.database = database
        self.session = session
        self.audit = audit

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        if self.session is not None:
            yield self.session
            return
        async with AsyncSession(self.database.engine) as session, session.begin():
            yield session

    @staticmethod
    def _domain(row: SessionEventRow) -> SessionEvent:
        return SessionEvent(
            tenant_id=row.tenant_id, session_key=row.session_key, event_id=row.event_id,
            sequence=row.sequence, event_type=row.event_type, payload=row.payload,
            content_digest=row.content_digest, trace_id=row.trace_id,
            owner_trace_id=row.owner_trace_id, execution_trace_id=row.execution_trace_id,
            created_at=row.created_at,
        )

    async def append(self, scope: DataScope, event: SessionEvent, *, expected_watermark: int) -> WriteResult:
        PostgresMemoryRepository._scope(scope, event.tenant_id)
        try:
            async with self._transaction() as session:
                await session.execute(insert(SessionStreamRow).values(
                    tenant_id=scope.tenant_id, session_key=event.session_key,
                    watermark=0, authority="POSTGRES", rollback_eligible=True,
                    generation=max(scope.fence_generation or 1, 1), created_at=event.created_at,
                    updated_at=event.created_at,
                ).on_conflict_do_nothing(index_elements=["tenant_id", "session_key"]))
                stream = await session.scalar(select(SessionStreamRow).where(
                    SessionStreamRow.tenant_id == scope.tenant_id,
                    SessionStreamRow.session_key == event.session_key,
                ).with_for_update())
                assert stream is not None
                if scope.fence_generation is not None and scope.fence_generation < stream.generation:
                    raise StaleFence()
                existing = await session.scalar(select(SessionEventRow).where(
                    SessionEventRow.tenant_id == scope.tenant_id,
                    SessionEventRow.session_key == event.session_key,
                    SessionEventRow.event_id == event.event_id,
                ))
                if existing is not None:
                    if existing.content_digest != event.content_digest:
                        raise IdempotencyConflict()
                    return WriteResult(self._domain(existing), "REPLAYED")
                if stream.watermark != expected_watermark or event.sequence != stream.watermark + 1:
                    raise SequenceGap()
                row = SessionEventRow(
                    tenant_id=scope.tenant_id, session_key=event.session_key,
                    event_id=event.event_id, sequence=event.sequence,
                    event_type=event.event_type, payload=event.payload,
                    content_digest=event.content_digest or "", trace_id=str(event.trace_id) if event.trace_id else None,
                    owner_trace_id=str(event.owner_trace_id) if event.owner_trace_id else None,
                    execution_trace_id=str(event.execution_trace_id) if event.execution_trace_id else None,
                    created_at=event.created_at,
                )
                session.add(row)
                stream.watermark = event.sequence
                stream.updated_at = event.created_at
                await self.mark_first_authoritative_write(scope, event.session_key, session=session)
                if self.audit is not None:
                    await self.audit.append_mutation(scope, "append_event", event.content_digest, transaction=session)
                await session.flush()
                return WriteResult(self._domain(row))
        except (TenantScopeInvalid, IdempotencyConflict, SequenceGap, StaleFence):
            raise
        except Exception:
            raise StateBackendUnavailable() from None

    async def list_metadata(self, scope: DataScope, session_key: str, *, after_sequence: int = 0, limit: int = 100) -> list[EventMetadata]:
        PostgresMemoryRepository._scope(scope)
        try:
            async with self._transaction() as session:
                rows = (await session.scalars(select(SessionEventRow).where(
                    SessionEventRow.tenant_id == scope.tenant_id,
                    SessionEventRow.session_key == session_key,
                    SessionEventRow.sequence > after_sequence,
                ).order_by(SessionEventRow.sequence).limit(limit))).all()
                return [EventMetadata(tenant_id=x.tenant_id, session_key=x.session_key, event_id=x.event_id, sequence=x.sequence, event_type=x.event_type, content_digest=x.content_digest, trace_id=x.trace_id, created_at=x.created_at) for x in rows]
        except Exception:
            raise StateBackendUnavailable() from None

    async def read_content(self, scope: DataScope, session_key: str, *, after_sequence: int = 0, limit: int = 100) -> list[SessionEvent]:
        PostgresMemoryRepository._scope(scope)
        try:
            async with self._transaction() as session:
                rows = (await session.scalars(select(SessionEventRow).where(
                    SessionEventRow.tenant_id == scope.tenant_id,
                    SessionEventRow.session_key == session_key,
                    SessionEventRow.sequence > after_sequence,
                ).order_by(SessionEventRow.sequence).limit(limit))).all()
                return [self._domain(x) for x in rows]
        except Exception:
            raise StateBackendUnavailable() from None

    async def get_watermark(self, scope: DataScope, session_key: str) -> int:
        PostgresMemoryRepository._scope(scope)
        try:
            async with self._transaction() as session:
                value = await session.scalar(select(SessionStreamRow.watermark).where(
                    SessionStreamRow.tenant_id == scope.tenant_id,
                    SessionStreamRow.session_key == session_key,
                ))
                return int(value or 0)
        except Exception:
            raise StateBackendUnavailable() from None

    async def mark_first_authoritative_write(self, scope: DataScope, stream: str, *, session: AsyncSession | None = None) -> None:
        active = session or self.session
        if active is None:
            async with self._transaction() as current:
                await self.mark_first_authoritative_write(scope, stream, session=current)
            return
        await active.execute(update(MigrationStateRow).where(
            MigrationStateRow.tenant_id == scope.tenant_id,
            MigrationStateRow.stream == stream,
            MigrationStateRow.authority == "POSTGRES",
            MigrationStateRow.rollback_eligible.is_(True),
        ).values(rollback_eligible=False, state="ACTIVE_FORWARD_ONLY"))


class PostgresSummaryRepository:
    """Summary CAS which locks the authoritative Event stream first."""

    locks_event_stream = True

    def __init__(self, database: PostgresDatabase, session: AsyncSession | None = None, *, audit: object | None = None) -> None:
        self.database = database
        self.session = session
        self.audit = audit

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        if self.session is not None:
            yield self.session
            return
        async with AsyncSession(self.database.engine) as session, session.begin():
            yield session

    @staticmethod
    def _domain(row: SummaryRecordRow) -> SummaryRecord:
        return SummaryRecord(tenant_id=row.tenant_id, session_key=row.session_key, content=row.content, content_digest=row.content_digest, event_watermark=row.event_watermark, version=row.version, updated_at=row.updated_at)

    async def compare_and_set(self, scope: DataScope, summary: SummaryRecord, *, expected_version: int | None) -> WriteResult:
        PostgresMemoryRepository._scope(scope, summary.tenant_id)
        try:
            async with self._transaction() as session:
                stream = await session.scalar(select(SessionStreamRow).where(
                    SessionStreamRow.tenant_id == scope.tenant_id,
                    SessionStreamRow.session_key == summary.session_key,
                ).with_for_update())
                if stream is None or summary.event_watermark > stream.watermark:
                    raise VersionConflict()
                row = await session.scalar(select(SummaryRecordRow).where(
                    SummaryRecordRow.tenant_id == scope.tenant_id,
                    SummaryRecordRow.session_key == summary.session_key,
                ).with_for_update())
                if row is None:
                    if expected_version not in (None, 0) or summary.version != 1:
                        raise VersionConflict()
                    row = SummaryRecordRow(tenant_id=scope.tenant_id, session_key=summary.session_key, content=summary.content, content_digest=summary.content_digest or "", event_watermark=summary.event_watermark, version=summary.version, updated_at=summary.updated_at)
                    session.add(row)
                else:
                    if summary.event_watermark < row.event_watermark:
                        raise VersionConflict()
                    if summary.event_watermark == row.event_watermark:
                        if summary.content_digest == row.content_digest:
                            return WriteResult(self._domain(row), "REPLAYED")
                        raise SummaryConflict()
                    if expected_version != row.version or summary.version != row.version + 1:
                        raise VersionConflict()
                    row.content = summary.content; row.content_digest = summary.content_digest or ""
                    row.event_watermark = summary.event_watermark; row.version = summary.version; row.updated_at = summary.updated_at
                if self.audit is not None:
                    await self.audit.append_mutation(scope, "put_summary", summary.content_digest, transaction=session)
                await session.flush()
                return WriteResult(self._domain(row))
        except (TenantScopeInvalid, VersionConflict, SummaryConflict):
            raise
        except Exception:
            raise StateBackendUnavailable() from None

    async def get_metadata(self, scope: DataScope, session_key: str) -> SummaryMetadata | None:
        PostgresMemoryRepository._scope(scope)
        try:
            async with self._transaction() as session:
                row = await session.scalar(select(SummaryRecordRow).where(SummaryRecordRow.tenant_id == scope.tenant_id, SummaryRecordRow.session_key == session_key))
                return None if row is None else SummaryMetadata(tenant_id=row.tenant_id, session_key=row.session_key, version=row.version, event_watermark=row.event_watermark, content_digest=row.content_digest, updated_at=row.updated_at)
        except Exception:
            raise StateBackendUnavailable() from None

    async def read_content(self, scope: DataScope, session_key: str) -> SummaryRecord | None:
        PostgresMemoryRepository._scope(scope)
        try:
            async with self._transaction() as session:
                row = await session.scalar(select(SummaryRecordRow).where(SummaryRecordRow.tenant_id == scope.tenant_id, SummaryRecordRow.session_key == session_key))
                return None if row is None else self._domain(row)
        except Exception:
            raise StateBackendUnavailable() from None


class PostgresMemoryRepository:
    """Tenant-scoped Memory CAS repository; returns only domain projections."""

    def __init__(self, database: PostgresDatabase, session: AsyncSession | None = None, *, max_bytes: int = 65_536) -> None:
        self.database = database
        self.session = session
        self.max_bytes = max_bytes

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        if self.session is not None:
            yield self.session
            return
        async with AsyncSession(self.database.engine) as session, session.begin():
            yield session

    @staticmethod
    def _scope(scope: DataScope, tenant_id: str | None = None) -> None:
        if not isinstance(scope, DataScope) or not scope.tenant_id.strip() or (tenant_id is not None and tenant_id != scope.tenant_id):
            raise TenantScopeInvalid()

    @staticmethod
    def _domain(row: MemoryRecordRow) -> MemoryRecord:
        return MemoryRecord(
            tenant_id=row.tenant_id, namespace=row.namespace, memory_key=row.memory_key,
            content=row.content, content_digest=row.content_digest, version=row.version,
            source_event_watermark=row.source_event_watermark, updated_at=row.updated_at,
        )

    async def compare_and_set(self, scope: DataScope, record: MemoryRecord, *, expected_version: int | None) -> WriteResult:
        self._scope(scope, record.tenant_id)
        byte_size = safe_size_bytes(record.content)
        if byte_size > min(record.max_bytes, self.max_bytes):
            raise ContentTooLarge()
        try:
            async with self._transaction() as session:
                row = await session.scalar(
                    select(MemoryRecordRow).where(
                        MemoryRecordRow.tenant_id == scope.tenant_id,
                        MemoryRecordRow.namespace == record.namespace,
                        MemoryRecordRow.memory_key == record.memory_key,
                    ).with_for_update()
                )
                if row is None:
                    if expected_version not in (None, 0) or record.version != 1:
                        raise VersionConflict()
                    row = MemoryRecordRow(
                        tenant_id=scope.tenant_id, namespace=record.namespace,
                        memory_key=record.memory_key, content=record.content,
                        content_digest=record.content_digest, byte_size=byte_size,
                        version=record.version, source_event_watermark=record.source_event_watermark,
                        updated_at=record.updated_at,
                    )
                    session.add(row)
                    await session.flush()
                    return WriteResult(self._domain(row))
                if row.version == record.version and row.content_digest == record.content_digest:
                    return WriteResult(self._domain(row), "REPLAYED")
                if expected_version != row.version or record.version != row.version + 1:
                    raise VersionConflict()
                row.content = record.content
                row.content_digest = record.content_digest or ""
                row.byte_size = byte_size
                row.version = record.version
                row.source_event_watermark = record.source_event_watermark
                row.updated_at = record.updated_at
                await session.flush()
                return WriteResult(self._domain(row))
        except (TenantScopeInvalid, ContentTooLarge, VersionConflict):
            raise
        except Exception:
            raise StateBackendUnavailable() from None

    async def get_metadata(self, scope: DataScope, namespace: str, key: str) -> MemoryMetadata | None:
        self._scope(scope)
        try:
            async with self._transaction() as session:
                row = await session.scalar(select(MemoryRecordRow).where(
                    MemoryRecordRow.tenant_id == scope.tenant_id,
                    MemoryRecordRow.namespace == namespace,
                    MemoryRecordRow.memory_key == key,
                ))
                if row is None:
                    return None
                return MemoryMetadata(
                    tenant_id=row.tenant_id, namespace=row.namespace, memory_key=row.memory_key,
                    version=row.version, content_digest=row.content_digest, byte_size=row.byte_size,
                    source_event_watermark=row.source_event_watermark, updated_at=row.updated_at,
                )
        except Exception:
            raise StateBackendUnavailable() from None

    async def read_content(self, scope: DataScope, namespace: str, key: str) -> MemoryRecord | None:
        self._scope(scope)
        try:
            async with self._transaction() as session:
                row = await session.scalar(select(MemoryRecordRow).where(
                    MemoryRecordRow.tenant_id == scope.tenant_id,
                    MemoryRecordRow.namespace == namespace,
                    MemoryRecordRow.memory_key == key,
                ))
                return None if row is None else self._domain(row)
        except Exception:
            raise StateBackendUnavailable() from None


class _PostgresScopedRepository:
    def __init__(self, database: PostgresDatabase, session: AsyncSession | None = None, *, audit: object | None = None) -> None:
        self.database = database
        self.session = session
        self.audit = audit

    @asynccontextmanager
    async def _transaction(self) -> AsyncIterator[AsyncSession]:
        if self.session is not None:
            yield self.session
            return
        async with AsyncSession(self.database.engine) as session, session.begin():
            yield session


class PostgresArtifactRepository(_PostgresScopedRepository):
    @staticmethod
    def _domain(row: ArtifactMetadataRow) -> ArtifactMetadata:
        return ArtifactMetadata(
            tenant_id=row.tenant_id, artifact_id=row.artifact_id,
            storage_ref=row.storage_ref, content_digest=row.content_digest,
            byte_size=row.byte_size, media_type=row.media_type, status=row.status,
            version=row.version, created_at=row.created_at, updated_at=row.updated_at,
        )

    async def publish(self, scope: DataScope, request: ArtifactMetadata, *, expected_version: int | None) -> ArtifactMetadata:
        PostgresMemoryRepository._scope(scope, request.tenant_id)
        try:
            async with self._transaction() as session:
                row = await session.scalar(select(ArtifactMetadataRow).where(
                    ArtifactMetadataRow.tenant_id == scope.tenant_id,
                    ArtifactMetadataRow.artifact_id == request.artifact_id,
                ).with_for_update())
                if row is None:
                    if expected_version not in (None, 0) or request.version != 1:
                        raise VersionConflict()
                    row = ArtifactMetadataRow(
                        tenant_id=scope.tenant_id, artifact_id=request.artifact_id,
                        storage_ref=request.storage_ref, content_digest=request.content_digest,
                        byte_size=request.byte_size, media_type=request.media_type,
                        status=request.status.value, version=request.version,
                        created_at=request.created_at, updated_at=request.updated_at,
                    )
                    session.add(row)
                elif row.content_digest == request.content_digest and row.version == request.version:
                    return self._domain(row)
                else:
                    if expected_version != row.version or request.version != row.version + 1:
                        raise VersionConflict()
                    row.storage_ref = request.storage_ref; row.content_digest = request.content_digest
                    row.byte_size = request.byte_size; row.media_type = request.media_type
                    row.status = request.status.value; row.version = request.version
                    row.updated_at = request.updated_at
                if self.audit is not None:
                    await self.audit.append_mutation(scope, "publish_artifact", request.content_digest, transaction=session)
                await session.flush()
                return self._domain(row)
        except (TenantScopeInvalid, VersionConflict):
            raise
        except Exception:
            raise StateBackendUnavailable() from None

    async def get_metadata(self, scope: DataScope, artifact_id: str) -> ArtifactMetadata | None:
        PostgresMemoryRepository._scope(scope)
        try:
            async with self._transaction() as session:
                row = await session.scalar(select(ArtifactMetadataRow).where(
                    ArtifactMetadataRow.tenant_id == scope.tenant_id,
                    ArtifactMetadataRow.artifact_id == artifact_id,
                ))
                return None if row is None else self._domain(row)
        except Exception:
            raise StateBackendUnavailable() from None

    async def read_content(self, scope: DataScope, artifact_id: str) -> bytes | None:
        del scope, artifact_id
        raise RuntimeError("artifact content is resolved by AuditedDataAccess")

    async def collect_orphans(self, scope: DataScope, *, before: object, limit: int) -> list[str]:
        del scope, before, limit
        return []


class PostgresKnowledgeRepository(_PostgresScopedRepository):
    @staticmethod
    def _domain(row: KnowledgeDocumentRow) -> KnowledgeDocument:
        return KnowledgeDocument(
            tenant_id=row.tenant_id, document_id=row.document_id,
            metadata=row.metadata_json, content_digest=row.content_digest,
            embedding_ref=row.embedding_ref, index_status=row.index_status,
            version=row.version, updated_at=row.updated_at,
        )

    async def stage(self, scope: DataScope, document: KnowledgeDocument, *, expected_version: int | None) -> KnowledgeDocument:
        PostgresMemoryRepository._scope(scope, document.tenant_id)
        try:
            async with self._transaction() as session:
                row = await session.scalar(select(KnowledgeDocumentRow).where(
                    KnowledgeDocumentRow.tenant_id == scope.tenant_id,
                    KnowledgeDocumentRow.document_id == document.document_id,
                ).with_for_update())
                if row is None:
                    if expected_version not in (None, 0) or document.version != 1:
                        raise VersionConflict()
                    row = KnowledgeDocumentRow(
                        tenant_id=scope.tenant_id, document_id=document.document_id,
                        metadata_json=document.metadata, content_digest=document.content_digest,
                        embedding_ref=document.embedding_ref, index_status=document.index_status.value,
                        version=document.version, updated_at=document.updated_at,
                    )
                    session.add(row)
                elif row.content_digest == document.content_digest and row.version == document.version:
                    return self._domain(row)
                else:
                    if expected_version != row.version or document.version != row.version + 1:
                        raise VersionConflict()
                    row.metadata_json = document.metadata; row.content_digest = document.content_digest
                    row.embedding_ref = document.embedding_ref; row.index_status = document.index_status.value
                    row.version = document.version; row.updated_at = document.updated_at
                if self.audit is not None:
                    await self.audit.append_mutation(scope, "stage_knowledge", document.content_digest, transaction=session)
                await session.flush()
                return self._domain(row)
        except (TenantScopeInvalid, VersionConflict):
            raise
        except Exception:
            raise StateBackendUnavailable() from None

    async def mark_indexed(self, scope: DataScope, document_id: str, digest: str, *, expected_version: int) -> KnowledgeDocument:
        async with self._transaction() as session:
            row = await session.scalar(select(KnowledgeDocumentRow).where(
                KnowledgeDocumentRow.tenant_id == scope.tenant_id,
                KnowledgeDocumentRow.document_id == document_id,
            ).with_for_update())
            if row is None or row.version != expected_version or row.content_digest != digest:
                raise VersionConflict()
            row.index_status = KnowledgeStatus.INDEXED.value
            await session.flush()
            return self._domain(row)

    async def search(self, scope: DataScope, query: str, *, limit: int = 10) -> list[KnowledgeDocument]:
        del query
        async with self._transaction() as session:
            rows = (await session.scalars(select(KnowledgeDocumentRow).where(
                KnowledgeDocumentRow.tenant_id == scope.tenant_id,
                KnowledgeDocumentRow.index_status == KnowledgeStatus.INDEXED.value,
            ).limit(limit))).all()
            return [self._domain(row) for row in rows]


class PostgresMigrationRepository(_PostgresScopedRepository):
    """Durable migration state CAS boundary (data copy is coordinator-owned)."""
    async def get(self, scope: DataScope, stream: str):
        from trpc_service.storage.data_models import MigrationState
        PostgresMemoryRepository._scope(scope)
        async with self._transaction() as session:
            row=await session.scalar(select(MigrationStateRow).where(MigrationStateRow.tenant_id==scope.tenant_id,MigrationStateRow.stream==stream))
            if row is None: return MigrationState(tenant_id=scope.tenant_id,stream=stream)
            return MigrationState.model_validate({"tenant_id":row.tenant_id,"stream":row.stream,"state":row.state,"authority":row.authority,"source_watermark":row.source_watermark,"copied_watermark":row.copied_watermark,"source_digest":row.source_digest,"target_digest":row.target_digest,"rollback_eligible":row.rollback_eligible,"generation":row.generation,"lease_owner_digest":row.lease_owner_digest,"failure_code":row.failure_code,"created_at":row.created_at,"updated_at":row.updated_at})

    async def transition(self, scope, stream, *, expected_state, expected_generation, target_state, fields=None):
        state=await self.get(scope,stream)
        from trpc_service.storage.sync import transition_migration
        if state.state != expected_state: raise MigrationConflict()
        result=transition_migration(state,target_state,expected_generation=expected_generation)
        if fields: result=result.model_copy(update=fields)
        return result

    async def checkpoint(self, scope, stream, *, expected_generation, copied_watermark, target_digest=None):
        state=await self.get(scope,stream)
        if state.generation!=expected_generation: raise MigrationConflict()
        return state.model_copy(update={"copied_watermark":copied_watermark,"target_digest":target_digest,"generation":state.generation+1,"state":"VERIFYING"})

    async def activate(self, scope, stream, *, expected_generation, verified_watermark, verified_digest):
        state=await self.get(scope,stream)
        if state.generation!=expected_generation or state.copied_watermark!=verified_watermark or state.target_digest!=verified_digest: raise MigrationConflict()
        return state.model_copy(update={"authority":"POSTGRES","state":"ACTIVE_ROLLBACK_ELIGIBLE","generation":state.generation+1})

    async def mark_first_authoritative_write(self, scope, stream, *, expected_generation, transaction=None):
        state=await self.get(scope,stream)
        if state.generation!=expected_generation: raise MigrationConflict()
        return state.model_copy(update={"state":"ACTIVE_FORWARD_ONLY","rollback_eligible":False,"generation":state.generation+1})


class PostgresDataRecoveryRepository(_PostgresScopedRepository):
    """Create-once and generation-fenced recovery marker operations."""
    @staticmethod
    def _domain(row: DataRecoveryMarkerRow) -> DataRecoveryMarker:
        return DataRecoveryMarker(marker_id=row.marker_id, tenant_id=row.tenant_id, operation=row.operation, stage=row.stage, result_digest=row.result_digest, generation=row.generation, review_reason=row.review_reason, confirmed=row.confirmed, created_at=row.created_at)

    async def create_once(self, scope: DataScope, marker: DataRecoveryMarker) -> DataRecoveryMarker:
        PostgresMemoryRepository._scope(scope, marker.tenant_id)
        async with self._transaction() as session:
            row = await session.scalar(select(DataRecoveryMarkerRow).where(DataRecoveryMarkerRow.tenant_id==scope.tenant_id, DataRecoveryMarkerRow.marker_id==marker.marker_id))
            if row is None:
                row=DataRecoveryMarkerRow(marker_id=marker.marker_id,tenant_id=scope.tenant_id,operation=marker.operation,stage=marker.stage,result_digest=marker.result_digest,generation=marker.generation,review_reason=marker.review_reason,confirmed=marker.confirmed,created_at=marker.created_at); session.add(row); await session.flush()
            return self._domain(row)

    async def claim(self, scope: DataScope, marker_id: str, *, expected_generation: int) -> DataRecoveryMarker:
        PostgresMemoryRepository._scope(scope)
        async with self._transaction() as session:
            row=await session.scalar(select(DataRecoveryMarkerRow).where(DataRecoveryMarkerRow.tenant_id==scope.tenant_id,DataRecoveryMarkerRow.marker_id==marker_id).with_for_update())
            if row is None or row.generation != expected_generation: raise StaleFence()
            return self._domain(row)

    async def mark_complete(self, scope: DataScope, marker_id: str, *, generation: int, result_digest: str | None = None) -> DataRecoveryMarker:
        async with self._transaction() as session:
            row=await session.scalar(select(DataRecoveryMarkerRow).where(DataRecoveryMarkerRow.tenant_id==scope.tenant_id,DataRecoveryMarkerRow.marker_id==marker_id).with_for_update())
            if row is None or row.generation != generation: raise StaleFence()
            row.confirmed=True; row.result_digest=result_digest or row.result_digest; await session.flush(); return self._domain(row)

    async def mark_review(self, scope: DataScope, marker_id: str, *, generation: int, reason: str) -> DataRecoveryMarker:
        async with self._transaction() as session:
            row=await session.scalar(select(DataRecoveryMarkerRow).where(DataRecoveryMarkerRow.tenant_id==scope.tenant_id,DataRecoveryMarkerRow.marker_id==marker_id).with_for_update())
            if row is None or row.generation != generation: raise StaleFence()
            row.review_reason=reason; await session.flush(); return self._domain(row)


class PostgresDataRepositoryFactory:
    def __init__(self, database: PostgresDatabase) -> None:
        self.database = database

    def memories(self, session: AsyncSession | None = None) -> PostgresMemoryRepository:
        return PostgresMemoryRepository(self.database, session)

    def events(self, session: AsyncSession | None = None, *, audit: object | None = None) -> PostgresSessionEventRepository:
        return PostgresSessionEventRepository(self.database, session, audit=audit)

    def summaries(self, session: AsyncSession | None = None, *, audit: object | None = None) -> PostgresSummaryRepository:
        return PostgresSummaryRepository(self.database, session, audit=audit)

    def artifacts(self, session: AsyncSession | None = None, *, audit: object | None = None) -> PostgresArtifactRepository:
        return PostgresArtifactRepository(self.database, session, audit=audit)

    def knowledge(self, session: AsyncSession | None = None, *, audit: object | None = None) -> PostgresKnowledgeRepository:
        return PostgresKnowledgeRepository(self.database, session, audit=audit)
