from __future__ import annotations

from collections.abc import AsyncIterator, Mapping, Sequence
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class SessionIdentity:
    tenant_id: str
    agent_app_id: str
    user_id: str
    session_id: str


@dataclass(frozen=True, slots=True)
class SessionSnapshot:
    identity: SessionIdentity
    state: Mapping[str, Any]
    version: int
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class EventInput:
    event_type: str
    role: str | None
    payload: Mapping[str, Any]
    trace_id: str
    channel: str | None = None
    external_message_id: str | None = None


@dataclass(frozen=True, slots=True)
class EventRecord:
    id: str
    tenant_id: str
    session_id: str
    sequence_no: int
    event_type: str
    role: str | None
    payload: Mapping[str, Any]
    trace_id: str
    channel: str | None
    external_message_id: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class SummaryRecord:
    id: str
    tenant_id: str
    session_id: str
    version: int
    through_sequence: int
    content: str
    created_at: datetime


@dataclass(frozen=True, slots=True)
class OutboxInput:
    topic: str
    payload: Mapping[str, Any]
    dedupe_key: str


@dataclass(frozen=True, slots=True)
class MemoryInput:
    memory_key: str
    text: str
    topics: Sequence[str] = field(default_factory=tuple)
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OutboxRecord:
    id: str
    tenant_id: str
    topic: str
    payload: Mapping[str, Any]
    dedupe_key: str
    attempts: int
    status: str
    available_at: datetime
    created_at: datetime


@dataclass(frozen=True, slots=True)
class TurnCommit:
    identity: SessionIdentity
    expected_version: int
    event: EventInput
    next_state: Mapping[str, Any]
    summary_content: str | None = None
    memories: Sequence[MemoryInput] = field(default_factory=tuple)
    outbox: Sequence[OutboxInput] = field(default_factory=tuple)
    execution_id: str | None = None


@dataclass(frozen=True, slots=True)
class TurnCommitResult:
    session: SessionSnapshot
    event: EventRecord
    summary: SummaryRecord | None
    outbox: Sequence[OutboxRecord]


@dataclass(frozen=True, slots=True)
class IdempotencyRecord:
    key: str
    status: str
    result: Mapping[str, Any] | None


@dataclass(frozen=True, slots=True)
class RateLimitDecision:
    allowed: bool
    remaining: int
    retry_after_seconds: float


@dataclass(frozen=True, slots=True)
class VectorDocument:
    id: str
    namespace: str
    text: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class VectorMatch:
    document: VectorDocument
    score: float


@dataclass(frozen=True, slots=True)
class ArtifactMetadata:
    tenant_id: str
    object_key: str
    mime_type: str
    size_bytes: int
    checksum: str
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ArtifactObject:
    metadata: ArtifactMetadata
    content: bytes


@dataclass(frozen=True, slots=True)
class AuditEntry:
    tenant_id: str
    agent_app_id: str
    agent_name: str
    decision: str
    trace_id: str
    request_id: str
    channel: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    tool_name: str | None = None
    latency_ms: int = 0
    error_type: str | None = None
    cost: float = 0
    details: Mapping[str, Any] = field(default_factory=dict)


class SessionStore(Protocol):
    async def get_session(self, identity: SessionIdentity) -> SessionSnapshot | None: ...

    async def create_session(self, identity: SessionIdentity) -> SessionSnapshot: ...

    async def compare_and_swap_state(
        self,
        identity: SessionIdentity,
        expected_version: int,
        next_state: Mapping[str, Any],
    ) -> SessionSnapshot: ...


class ConversationStore(SessionStore, Protocol):
    async def commit_turn(self, commit: TurnCommit) -> TurnCommitResult: ...

    async def list_events(
        self, identity: SessionIdentity, after_sequence: int = -1
    ) -> Sequence[EventRecord]: ...

    async def latest_summary(self, identity: SessionIdentity) -> SummaryRecord | None: ...


class SessionLockManager(Protocol):
    def acquire(
        self, key: str, *, ttl_seconds: float = 30, wait_timeout_seconds: float = 5
    ) -> AbstractAsyncContextManager[str]: ...


class IdempotencyStore(Protocol):
    async def claim(self, key: str, *, ttl_seconds: int = 86_400) -> bool: ...

    async def complete(
        self, key: str, result: Mapping[str, Any], *, ttl_seconds: int = 86_400
    ) -> None: ...

    async def get(self, key: str) -> IdempotencyRecord | None: ...

    async def abandon(self, key: str) -> None: ...


class RateLimiter(Protocol):
    async def check(self, key: str, *, limit: int, window_seconds: int) -> RateLimitDecision: ...


class EphemeralStateStore(Protocol):
    async def set_state(self, key: str, value: Mapping[str, Any], *, ttl_seconds: int) -> None: ...

    async def get_state(self, key: str) -> Mapping[str, Any] | None: ...

    async def delete_state(self, key: str) -> None: ...


class VectorStore(Protocol):
    async def upsert(self, documents: Sequence[VectorDocument]) -> None: ...

    async def delete(self, namespace: str, document_ids: Sequence[str]) -> None: ...

    async def search(self, namespace: str, query: str, limit: int = 5) -> Sequence[VectorMatch]: ...


class SemanticMemoryStore(Protocol):
    async def upsert_memory(
        self,
        tenant_id: str,
        agent_app_id: str,
        user_id: str,
        memory_id: str,
        text: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None: ...

    async def search_memories(
        self, tenant_id: str, agent_app_id: str, user_id: str, query: str, limit: int = 5
    ) -> Sequence[VectorMatch]: ...


class KnowledgeStore(Protocol):
    async def upsert_knowledge(
        self,
        tenant_id: str,
        agent_app_id: str,
        document_id: str,
        text: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None: ...

    async def search_knowledge(
        self, tenant_id: str, agent_app_id: str, query: str, limit: int = 5
    ) -> Sequence[VectorMatch]: ...


class ArtifactStore(Protocol):
    async def put(
        self,
        tenant_id: str,
        object_key: str,
        content: bytes,
        mime_type: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> ArtifactMetadata: ...

    async def get(self, tenant_id: str, object_key: str) -> ArtifactObject: ...

    async def delete(self, tenant_id: str, object_key: str) -> None: ...


class OutboxStore(Protocol):
    async def claim_batch(self, worker_id: str, limit: int = 100) -> Sequence[OutboxRecord]: ...

    async def mark_processed(self, record_id: str) -> None: ...

    async def mark_failed(self, record_id: str, error: str) -> None: ...


class AuditStore(Protocol):
    async def append_audit(self, entry: AuditEntry) -> str: ...


class CoordinationStore(
    SessionLockManager,
    IdempotencyStore,
    RateLimiter,
    EphemeralStateStore,
    Protocol,
):
    pass


class DataPlaneStore(ConversationStore, OutboxStore, AuditStore, Protocol):
    pass


class OutboxHandler(Protocol):
    async def __call__(self, record: OutboxRecord) -> None: ...


class OutboxStream(Protocol):
    def stream(self) -> AsyncIterator[OutboxRecord]: ...
