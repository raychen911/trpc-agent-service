"""Storage contracts shared by Gateway, Worker, Policy, and adapters."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import StrEnum
from typing import Any, Protocol


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


class IdempotencyStatus(StrEnum):
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass(slots=True)
class SessionEvent:
    tenant_id: str
    session_id: str
    event_id: str
    event_type: str
    payload: dict[str, Any]
    trace_id: str
    idempotency_key: str | None = None
    seq: int = 0
    created_at: datetime = field(default_factory=now_utc)


@dataclass(slots=True)
class SessionState:
    tenant_id: str
    session_id: str
    state: dict[str, Any] = field(default_factory=dict)
    state_version: int = 0
    latest_event_seq: int = 0


@dataclass(slots=True)
class MemoryItem:
    tenant_id: str
    memory_id: str
    scope_key: str
    content: str
    metadata: dict[str, Any] = field(default_factory=dict)
    version: int = 1
    created_at: datetime = field(default_factory=now_utc)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "MemoryItem":
        item = dict(value)
        if isinstance(item.get("created_at"), str):
            item["created_at"] = datetime.fromisoformat(item["created_at"])
        return cls(**item)


@dataclass(slots=True)
class Summary:
    tenant_id: str
    session_id: str
    content: str
    source_event_seq: int
    summary_version: int = 1
    created_at: datetime = field(default_factory=now_utc)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "Summary":
        item = dict(value)
        if isinstance(item.get("created_at"), str):
            item["created_at"] = datetime.fromisoformat(item["created_at"])
        return cls(**item)


@dataclass(slots=True)
class AuditRecord:
    audit_id: str
    tenant_id: str
    decision: str
    trace_id: str
    channel: str | None = None
    user_id: str | None = None
    session_id: str | None = None
    agent_name: str | None = None
    tool_name: str | None = None
    latency_ms: int | None = None
    error_type: str | None = None
    token_usage: int | None = None
    cost: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=now_utc)

    @classmethod
    def from_dict(cls, value: dict[str, Any]) -> "AuditRecord":
        item = dict(value)
        if isinstance(item.get("created_at"), str):
            item["created_at"] = datetime.fromisoformat(item["created_at"])
        return cls(**item)


@dataclass(slots=True)
class IdempotencyRecord:
    tenant_id: str
    key: str
    status: IdempotencyStatus
    response_ref: str | None = None
    result: dict[str, Any] | None = None
    trace_id: str | None = None
    attempt: int = 1
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


@dataclass(slots=True)
class CompensationTask:
    """Durable retry item for derived state written after the session event."""

    task_id: str
    tenant_id: str
    operation: str
    payload: dict[str, Any]
    status: str = "pending"
    attempt: int = 0
    available_at: datetime = field(default_factory=now_utc)
    last_error: str | None = None
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)


class SessionStore(Protocol):
    def append_event(self, event: SessionEvent, fencing_token: int | None = None) -> int: ...

    def load_events(self, tenant_id: str, session_id: str, after_seq: int = 0) -> list[SessionEvent]: ...

    def load_state(self, tenant_id: str, session_id: str) -> SessionState: ...

    def compare_and_set_state(
        self,
        tenant_id: str,
        session_id: str,
        expected_version: int,
        state: dict[str, Any],
        fencing_token: int | None = None,
    ) -> bool: ...

    def restore_state(
        self,
        tenant_id: str,
        session_id: str,
        state: dict[str, Any],
        state_version: int,
    ) -> None: ...


class MemoryStore(Protocol):
    def put(self, item: MemoryItem) -> None: ...

    def search(
        self,
        tenant_id: str,
        query: str,
        limit: int = 5,
        scope_keys: tuple[str, ...] | None = None,
    ) -> list[MemoryItem]: ...


class SummaryStore(Protocol):
    def put(self, summary: Summary) -> None: ...

    def latest(self, tenant_id: str, session_id: str) -> Summary | None: ...


class AuditLogStore(Protocol):
    def append(self, record: AuditRecord) -> None: ...

    def list_by_tenant(self, tenant_id: str, limit: int = 100) -> list[AuditRecord]: ...


class IdempotencyStore(Protocol):
    def start(
        self,
        tenant_id: str,
        key: str,
        trace_id: str,
        lease_seconds: int = 180,
    ) -> IdempotencyRecord: ...

    def complete(self, tenant_id: str, key: str, response_ref: str, result: dict[str, Any]) -> IdempotencyRecord: ...

    def fail(self, tenant_id: str, key: str, error_type: str) -> IdempotencyRecord: ...

    def get(self, tenant_id: str, key: str) -> IdempotencyRecord | None: ...

    def claim_delivery(self, tenant_id: str, key: str) -> bool: ...

    def release_delivery(self, tenant_id: str, key: str) -> None: ...


class CompensationStore(Protocol):
    def enqueue(
        self,
        tenant_id: str,
        operation: str,
        payload: dict[str, Any],
        task_id: str | None = None,
    ) -> CompensationTask: ...

    def claim(self, limit: int = 10, tenant_id: str | None = None) -> list[CompensationTask]: ...

    def complete(self, task_id: str, tenant_id: str | None = None) -> None: ...

    def fail(
        self,
        task_id: str,
        error: str,
        retry_after_seconds: int = 30,
        tenant_id: str | None = None,
    ) -> None: ...


class ArtifactStore(Protocol):
    def put(self, tenant_id: str, content: bytes, content_type: str = "application/octet-stream") -> Any: ...

    def put_with_id(
        self, tenant_id: str, object_id: str, content: bytes, content_type: str = "application/octet-stream"
    ) -> Any: ...

    def get(self, tenant_id: str, object_id: str) -> bytes: ...

    def list_by_tenant(self, tenant_id: str) -> list[Any]: ...


class KnowledgeStore(Protocol):
    def upsert(self, chunk: Any) -> None: ...

    def search(self, tenant_id: str, collection: str, query: str, limit: int = 5) -> list[Any]: ...

    def list_by_tenant(self, tenant_id: str) -> list[Any]: ...


class StorageAdapter(Protocol):
    session: SessionStore
    memory: MemoryStore
    summary: SummaryStore
    audit: AuditLogStore
    idempotency: IdempotencyStore
    compensation: CompensationStore
    artifacts: ArtifactStore
    knowledge: KnowledgeStore
    inbox_outbox: Any
