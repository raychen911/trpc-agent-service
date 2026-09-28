"""Persistence infrastructure exports."""

from trpc_service.storage.database import build_engine, build_session_factory
from trpc_service.storage.embedding import EmbeddingProvider
from trpc_service.storage.errors import (
    ArtifactIntegrityError,
    EmbeddingDimensionError,
    ExecutionAlreadyRunning,
    IdempotencyConflict,
    SessionVersionConflict,
    StaleExecutionLease,
    StoredObjectNotFound,
)
from trpc_service.storage.ports import (
    ArtifactStore,
    AuditStore,
    KnowledgeStore,
    MemoryStore,
    OutboxStore,
    SessionStore,
    SummaryStore,
)
from trpc_service.storage.registry import (
    StorageBackend,
    StorageBackendAlreadyRegistered,
    StorageBackendNotFound,
    StorageBackendRegistry,
)
from trpc_service.storage.router import (
    BackendProfile,
    ResolvedStorage,
    StorageCapabilityMissing,
    StorageRouter,
)
from trpc_service.storage.session_cache import (
    RedisSessionSnapshotCache,
    SessionSnapshotCache,
)
from trpc_service.storage.types import (
    ArtifactMetadata,
    ArtifactRef,
    AuditRecord,
    ExecutionClaim,
    ExecutionCommit,
    InboxClaimRequest,
    KnowledgeDocument,
    KnowledgeHit,
    MemoryHit,
    MemoryRecord,
    OutboxMessage,
    SessionEvent,
    SessionSnapshot,
    SessionSummary,
)

__all__ = [
    "ArtifactMetadata",
    "ArtifactIntegrityError",
    "ArtifactRef",
    "ArtifactStore",
    "AuditRecord",
    "AuditStore",
    "BackendProfile",
    "ExecutionCommit",
    "ExecutionClaim",
    "ExecutionAlreadyRunning",
    "EmbeddingDimensionError",
    "EmbeddingProvider",
    "KnowledgeDocument",
    "KnowledgeHit",
    "KnowledgeStore",
    "MemoryHit",
    "MemoryRecord",
    "MemoryStore",
    "IdempotencyConflict",
    "InboxClaimRequest",
    "OutboxMessage",
    "OutboxStore",
    "ResolvedStorage",
    "RedisSessionSnapshotCache",
    "SessionEvent",
    "SessionSnapshot",
    "SessionSnapshotCache",
    "SessionSummary",
    "SessionStore",
    "SessionVersionConflict",
    "SummaryStore",
    "StorageBackend",
    "StorageBackendAlreadyRegistered",
    "StorageBackendNotFound",
    "StorageBackendRegistry",
    "StorageCapabilityMissing",
    "StorageRouter",
    "StaleExecutionLease",
    "StoredObjectNotFound",
    "build_engine",
    "build_session_factory",
]
