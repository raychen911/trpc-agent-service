"""Tenant-aware contracts for replaceable, non-canonical data backends.

Canonical Inbox, Run and Event durability remains in the SQL reliability plane.
These contracts describe projections and external data stores without pretending
that Redis, a vector index and object storage share one consistency model.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

_TENANT_ID = re.compile(r"\A[A-Za-z0-9][A-Za-z0-9._-]{0,63}\Z")


class BackendError(RuntimeError):
    """Base class for backend contract failures."""


class BackendConflictError(BackendError):
    """An idempotency key or version was reused with different content."""


class WatermarkRegressionError(BackendError):
    """A write attempted to move a monotonic watermark backwards."""


class BackendCorruptionError(BackendError):
    """Stored content did not satisfy its integrity envelope."""


class BackendRole(StrEnum):
    """Whether a backend is authoritative or rebuildable."""

    AUTHORITY = "authority"
    PROJECTION = "projection"
    EXTERNAL_SYSTEM = "external_system"


class ReadVisibility(StrEnum):
    """Documented read-after-write behavior."""

    LINEARIZABLE = "linearizable"
    READ_AFTER_WRITE = "read_after_write"
    EVENTUAL = "eventual"
    PROCESS_LOCAL = "process_local"


class DurabilityClass(StrEnum):
    """Failure domain survived by acknowledged writes."""

    PROCESS_LOCAL = "process_local"
    REMOTE_VOLATILE = "remote_volatile"
    DURABLE = "durable"
    EXTERNAL = "external"


@dataclass(frozen=True, slots=True)
class ConsistencyMetadata:
    """Machine-readable claims used by routing and operational diagnostics."""

    backend_id: str
    role: BackendRole
    visibility: ReadVisibility
    durability: DurabilityClass
    supports_cas: bool
    supports_monotonic_watermark: bool
    development_only: bool = False
    notes: str = ""


class WriteDisposition(StrEnum):
    """Result of an idempotent or CAS projection write."""

    APPLIED = "applied"
    UNCHANGED = "unchanged"
    CONFLICT = "conflict"


@dataclass(frozen=True, slots=True)
class WriteResult:
    """Current version information after a projection write attempt."""

    disposition: WriteDisposition
    current_version: int | None
    current_watermark: int | None = None


@dataclass(frozen=True, slots=True)
class SessionProjection:
    """Rebuildable session-scope state at a committed-event watermark."""

    tenant_id: str
    session_id: str
    version: int
    committed_through: int
    state: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ScopedStateProjection:
    """App- or user-scope state protected by an independent OCC version."""

    tenant_id: str
    app_id: str
    app_revision: int
    scope: str
    subject_id: str
    version: int
    state: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class SummaryProjection:
    """Summary derived through one committed SessionEvent sequence."""

    tenant_id: str
    session_id: str
    through_seq: int
    content: str
    summarizer_version: str


@dataclass(frozen=True, slots=True)
class MemoryProjection:
    """Idempotent long-term memory extracted from one source event."""

    tenant_id: str
    memory_id: str
    principal_id: str
    session_id: str
    source_event_id: str
    extractor_version: str
    record_version: int
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class KnowledgeDocument:
    """Versioned knowledge content; semantic indexing is backend-specific."""

    tenant_id: str
    document_id: str
    version: int
    content: str
    content_hash: str
    metadata: dict[str, Any] = field(default_factory=dict)
    indexed_version: int = 0
    tombstone: bool = False


@dataclass(frozen=True, slots=True)
class ArtifactObject:
    """Versioned binary object for development or an object-store adapter."""

    tenant_id: str
    artifact_id: str
    version: int
    media_type: str
    content: bytes = field(repr=False)
    content_hash: str
    metadata: dict[str, Any] = field(default_factory=dict)


def validate_tenant_id(tenant_id: str) -> None:
    """Reject blank, oversized or unsafe tenant identifiers at every boundary."""

    if _TENANT_ID.fullmatch(tenant_id) is None:
        raise ValueError("tenant_id is not a safe logical identifier")


def validate_nonempty(value: str, field_name: str, *, max_length: int = 256) -> None:
    """Validate a logical identifier without imposing a storage-specific alphabet."""

    if not value or len(value) > max_length or "\x00" in value:
        raise ValueError(f"{field_name} must be non-empty and at most {max_length} characters")


def canonical_json_hash(value: Any) -> str:
    """Return a deterministic SHA-256 digest for JSON-compatible content."""

    encoded = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@runtime_checkable
class SessionProjectionBackend(Protocol):
    """CAS store for a rebuildable tenant Session projection."""

    @property
    def consistency(self) -> ConsistencyMetadata: ...

    async def get_session(
        self,
        tenant_id: str,
        session_id: str,
    ) -> SessionProjection | None: ...

    async def compare_and_set_session(
        self,
        projection: SessionProjection,
        *,
        expected_version: int | None,
    ) -> WriteResult: ...


@runtime_checkable
class ScopedStateBackend(Protocol):
    """Independent CAS store for app/user state; never implicit Session state."""

    @property
    def consistency(self) -> ConsistencyMetadata: ...

    async def get_scoped_state(
        self,
        tenant_id: str,
        app_id: str,
        scope: str,
        subject_id: str,
    ) -> ScopedStateProjection | None: ...

    async def compare_and_set_scoped_state(
        self,
        projection: ScopedStateProjection,
        *,
        expected_version: int | None,
    ) -> WriteResult: ...


@runtime_checkable
class MemoryBackend(Protocol):
    """Idempotent memory extraction store."""

    @property
    def consistency(self) -> ConsistencyMetadata: ...

    async def put_memory_once(self, projection: MemoryProjection) -> WriteResult: ...

    async def list_memories(
        self,
        tenant_id: str,
        principal_id: str,
        *,
        after_version: int = -1,
        limit: int = 100,
    ) -> tuple[MemoryProjection, ...]: ...


@runtime_checkable
class SummaryBackend(Protocol):
    """Monotonic summary projection store."""

    @property
    def consistency(self) -> ConsistencyMetadata: ...

    async def get_summary(
        self,
        tenant_id: str,
        session_id: str,
    ) -> SummaryProjection | None: ...

    async def put_summary_if_newer(self, projection: SummaryProjection) -> WriteResult: ...


@runtime_checkable
class KnowledgeBackend(Protocol):
    """Versioned knowledge store; this contract does not claim vector search."""

    @property
    def consistency(self) -> ConsistencyMetadata: ...

    async def get_latest_document(
        self,
        tenant_id: str,
        document_id: str,
    ) -> KnowledgeDocument | None: ...

    async def put_document_if_newer(self, document: KnowledgeDocument) -> WriteResult: ...


@runtime_checkable
class ArtifactBackend(Protocol):
    """Versioned artifact object store."""

    @property
    def consistency(self) -> ConsistencyMetadata: ...

    async def get_latest_artifact(
        self,
        tenant_id: str,
        artifact_id: str,
    ) -> ArtifactObject | None: ...

    async def put_artifact_if_newer(self, artifact: ArtifactObject) -> WriteResult: ...
