"""Process-local backend used only for development and deterministic tests."""

from __future__ import annotations

import asyncio
import hashlib
from copy import deepcopy

from trpc_service.backends.contracts import (
    ArtifactObject,
    BackendConflictError,
    BackendRole,
    ConsistencyMetadata,
    DurabilityClass,
    KnowledgeDocument,
    MemoryProjection,
    ReadVisibility,
    ScopedStateProjection,
    SessionProjection,
    SummaryProjection,
    WatermarkRegressionError,
    WriteDisposition,
    WriteResult,
    validate_nonempty,
    validate_tenant_id,
)

_INMEMORY_CONSISTENCY = ConsistencyMetadata(
    backend_id="inmemory",
    role=BackendRole.PROJECTION,
    visibility=ReadVisibility.PROCESS_LOCAL,
    durability=DurabilityClass.PROCESS_LOCAL,
    supports_cas=True,
    supports_monotonic_watermark=True,
    development_only=True,
    notes="One Python process only; all data is lost on restart.",
)


class InMemoryBackend:
    """One lock protects all tenant-keyed maps and deterministic watermarks."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._sessions: dict[tuple[str, str], SessionProjection] = {}
        self._scoped: dict[tuple[str, str, str, str], ScopedStateProjection] = {}
        self._summaries: dict[tuple[str, str], SummaryProjection] = {}
        self._memories: dict[tuple[str, str, str], MemoryProjection] = {}
        self._memory_ids: dict[tuple[str, str], tuple[str, str, str]] = {}
        self._knowledge: dict[tuple[str, str], KnowledgeDocument] = {}
        self._artifacts: dict[tuple[str, str], ArtifactObject] = {}

    @property
    def consistency(self) -> ConsistencyMetadata:
        return _INMEMORY_CONSISTENCY

    async def get_session(
        self,
        tenant_id: str,
        session_id: str,
    ) -> SessionProjection | None:
        validate_tenant_id(tenant_id)
        validate_nonempty(session_id, "session_id", max_length=128)
        async with self._lock:
            current = self._sessions.get((tenant_id, session_id))
            return deepcopy(current)

    async def compare_and_set_session(
        self,
        projection: SessionProjection,
        *,
        expected_version: int | None,
    ) -> WriteResult:
        self._validate_session(projection, expected_version)
        key = (projection.tenant_id, projection.session_id)
        async with self._lock:
            current = self._sessions.get(key)
            if current is None:
                if expected_version is not None:
                    return WriteResult(WriteDisposition.CONFLICT, None, None)
                self._sessions[key] = deepcopy(projection)
                return WriteResult(
                    WriteDisposition.APPLIED,
                    projection.version,
                    projection.committed_through,
                )
            if expected_version != current.version:
                return WriteResult(
                    WriteDisposition.CONFLICT,
                    current.version,
                    current.committed_through,
                )
            if (
                projection.version < current.version
                or projection.committed_through < current.committed_through
            ):
                raise WatermarkRegressionError("Session projection cannot move backwards")
            if projection.version == current.version:
                if projection != current:
                    raise BackendConflictError("Session projection version has conflicting content")
                return WriteResult(
                    WriteDisposition.UNCHANGED,
                    current.version,
                    current.committed_through,
                )
            self._sessions[key] = deepcopy(projection)
            return WriteResult(
                WriteDisposition.APPLIED,
                projection.version,
                projection.committed_through,
            )

    @staticmethod
    def _validate_session(
        projection: SessionProjection,
        expected_version: int | None,
    ) -> None:
        validate_tenant_id(projection.tenant_id)
        validate_nonempty(projection.session_id, "session_id", max_length=128)
        if projection.version < 0:
            raise ValueError("Session projection version must not be negative")
        if not 0 <= projection.committed_through <= projection.version:
            raise ValueError("committed_through must be between zero and version")
        if expected_version is not None and expected_version < 0:
            raise ValueError("expected_version must be non-negative or None")

    async def get_scoped_state(
        self,
        tenant_id: str,
        app_id: str,
        scope: str,
        subject_id: str,
    ) -> ScopedStateProjection | None:
        self._validate_scoped_key(tenant_id, app_id, scope, subject_id)
        async with self._lock:
            current = self._scoped.get((tenant_id, app_id, scope, subject_id))
            return deepcopy(current)

    async def compare_and_set_scoped_state(
        self,
        projection: ScopedStateProjection,
        *,
        expected_version: int | None,
    ) -> WriteResult:
        self._validate_scoped_key(
            projection.tenant_id,
            projection.app_id,
            projection.scope,
            projection.subject_id,
        )
        if projection.app_revision < 1:
            raise ValueError("app_revision must be positive")
        if projection.version < 0:
            raise ValueError("ScopedState version must not be negative")
        if expected_version is not None and expected_version < 0:
            raise ValueError("expected_version must be non-negative or None")
        key = (
            projection.tenant_id,
            projection.app_id,
            projection.scope,
            projection.subject_id,
        )
        async with self._lock:
            current = self._scoped.get(key)
            if current is None:
                if expected_version is not None:
                    return WriteResult(WriteDisposition.CONFLICT, None)
                self._scoped[key] = deepcopy(projection)
                return WriteResult(WriteDisposition.APPLIED, projection.version)
            if expected_version != current.version:
                return WriteResult(WriteDisposition.CONFLICT, current.version)
            if projection.version < current.version:
                raise WatermarkRegressionError("ScopedState version cannot move backwards")
            if projection.version == current.version:
                if projection != current:
                    raise BackendConflictError("ScopedState version has conflicting content")
                return WriteResult(WriteDisposition.UNCHANGED, current.version)
            self._scoped[key] = deepcopy(projection)
            return WriteResult(WriteDisposition.APPLIED, projection.version)

    @staticmethod
    def _validate_scoped_key(
        tenant_id: str,
        app_id: str,
        scope: str,
        subject_id: str,
    ) -> None:
        validate_tenant_id(tenant_id)
        validate_nonempty(app_id, "app_id", max_length=64)
        validate_nonempty(subject_id, "subject_id")
        if scope not in {"app", "user"}:
            raise ValueError("ScopedState scope must be 'app' or 'user'")

    async def get_summary(
        self,
        tenant_id: str,
        session_id: str,
    ) -> SummaryProjection | None:
        validate_tenant_id(tenant_id)
        validate_nonempty(session_id, "session_id", max_length=128)
        async with self._lock:
            return deepcopy(self._summaries.get((tenant_id, session_id)))

    async def put_summary_if_newer(self, projection: SummaryProjection) -> WriteResult:
        validate_tenant_id(projection.tenant_id)
        validate_nonempty(projection.session_id, "session_id", max_length=128)
        validate_nonempty(projection.summarizer_version, "summarizer_version", max_length=64)
        if projection.through_seq < 0:
            raise ValueError("through_seq must not be negative")
        key = (projection.tenant_id, projection.session_id)
        async with self._lock:
            current = self._summaries.get(key)
            if current is None:
                self._summaries[key] = deepcopy(projection)
                return WriteResult(
                    WriteDisposition.APPLIED,
                    projection.through_seq,
                    projection.through_seq,
                )
            if projection.through_seq < current.through_seq:
                raise WatermarkRegressionError("Summary watermark cannot move backwards")
            if projection.through_seq == current.through_seq:
                if projection != current:
                    raise BackendConflictError("Summary watermark has conflicting content")
                return WriteResult(
                    WriteDisposition.UNCHANGED,
                    current.through_seq,
                    current.through_seq,
                )
            self._summaries[key] = deepcopy(projection)
            return WriteResult(
                WriteDisposition.APPLIED,
                projection.through_seq,
                projection.through_seq,
            )

    async def put_memory_once(self, projection: MemoryProjection) -> WriteResult:
        validate_tenant_id(projection.tenant_id)
        for value, field_name, maximum in (
            (projection.memory_id, "memory_id", 128),
            (projection.principal_id, "principal_id", 256),
            (projection.session_id, "session_id", 128),
            (projection.source_event_id, "source_event_id", 128),
            (projection.extractor_version, "extractor_version", 64),
        ):
            validate_nonempty(value, field_name, max_length=maximum)
        if projection.record_version < 0:
            raise ValueError("record_version must not be negative")
        key = (
            projection.tenant_id,
            projection.source_event_id,
            projection.extractor_version,
        )
        id_key = (projection.tenant_id, projection.memory_id)
        async with self._lock:
            key_for_id = self._memory_ids.get(id_key)
            if key_for_id is not None and key_for_id != key:
                raise BackendConflictError("memory_id was reused for another extraction")
            current = self._memories.get(key)
            if current is not None:
                if current != projection:
                    raise BackendConflictError("Memory extraction key has conflicting content")
                return WriteResult(WriteDisposition.UNCHANGED, current.record_version)
            self._memories[key] = deepcopy(projection)
            self._memory_ids[id_key] = key
            return WriteResult(WriteDisposition.APPLIED, projection.record_version)

    async def list_memories(
        self,
        tenant_id: str,
        principal_id: str,
        *,
        after_version: int = -1,
        limit: int = 100,
    ) -> tuple[MemoryProjection, ...]:
        validate_tenant_id(tenant_id)
        validate_nonempty(principal_id, "principal_id")
        if after_version < -1:
            raise ValueError("after_version must be at least -1")
        if not 1 <= limit <= 1_000:
            raise ValueError("limit must be between 1 and 1000")
        async with self._lock:
            selected = sorted(
                (
                    projection
                    for projection in self._memories.values()
                    if projection.tenant_id == tenant_id
                    and projection.principal_id == principal_id
                    and projection.record_version > after_version
                ),
                key=lambda item: (item.record_version, item.memory_id),
            )[:limit]
            return tuple(deepcopy(selected))

    async def get_latest_document(
        self,
        tenant_id: str,
        document_id: str,
    ) -> KnowledgeDocument | None:
        validate_tenant_id(tenant_id)
        validate_nonempty(document_id, "document_id", max_length=128)
        async with self._lock:
            return deepcopy(self._knowledge.get((tenant_id, document_id)))

    async def put_document_if_newer(self, document: KnowledgeDocument) -> WriteResult:
        validate_tenant_id(document.tenant_id)
        validate_nonempty(document.document_id, "document_id", max_length=128)
        if document.version < 0:
            raise ValueError("Knowledge version must not be negative")
        if not 0 <= document.indexed_version <= document.version:
            raise ValueError("indexed_version must be between zero and document version")
        expected_hash = hashlib.sha256(document.content.encode("utf-8")).hexdigest()
        if document.content_hash != expected_hash:
            raise ValueError("Knowledge content_hash does not match content")
        key = (document.tenant_id, document.document_id)
        async with self._lock:
            current = self._knowledge.get(key)
            if current is not None and document.version < current.version:
                raise WatermarkRegressionError("Knowledge version cannot move backwards")
            if current is not None and document.version == current.version:
                if current != document:
                    raise BackendConflictError("Knowledge version has conflicting content")
                return WriteResult(WriteDisposition.UNCHANGED, current.version)
            self._knowledge[key] = deepcopy(document)
            return WriteResult(WriteDisposition.APPLIED, document.version)

    async def get_latest_artifact(
        self,
        tenant_id: str,
        artifact_id: str,
    ) -> ArtifactObject | None:
        validate_tenant_id(tenant_id)
        validate_nonempty(artifact_id, "artifact_id", max_length=128)
        async with self._lock:
            return deepcopy(self._artifacts.get((tenant_id, artifact_id)))

    async def put_artifact_if_newer(self, artifact: ArtifactObject) -> WriteResult:
        validate_tenant_id(artifact.tenant_id)
        validate_nonempty(artifact.artifact_id, "artifact_id", max_length=128)
        validate_nonempty(artifact.media_type, "media_type", max_length=128)
        if artifact.version < 0:
            raise ValueError("Artifact version must not be negative")
        if artifact.content_hash != hashlib.sha256(artifact.content).hexdigest():
            raise ValueError("Artifact content_hash does not match content")
        key = (artifact.tenant_id, artifact.artifact_id)
        async with self._lock:
            current = self._artifacts.get(key)
            if current is not None and artifact.version < current.version:
                raise WatermarkRegressionError("Artifact version cannot move backwards")
            if current is not None and artifact.version == current.version:
                if current != artifact:
                    raise BackendConflictError("Artifact version has conflicting content")
                return WriteResult(WriteDisposition.UNCHANGED, current.version)
            self._artifacts[key] = deepcopy(artifact)
            return WriteResult(WriteDisposition.APPLIED, artifact.version)
