"""Serializable domain records used by the durable runtime.

The records deliberately contain no ORM objects.  This makes the worker usable
with the SQL implementation in production and with the in-memory implementation
in the deterministic demo/tests.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum, StrEnum
from hashlib import sha256
from typing import Any


def utcnow() -> datetime:
    return datetime.now(UTC)


def stable_id(prefix: str, *parts: object) -> str:
    """Return an opaque deterministic ID suitable for retry-safe ledger rows."""

    material = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}_{sha256(material).hexdigest()[:32]}"


def content_hash(value: Any) -> str:
    encoded = json.dumps(to_primitive(value), sort_keys=True, separators=(",", ":"))
    return sha256(encoded.encode("utf-8")).hexdigest()


def to_primitive(value: Any) -> Any:
    """Convert records to JSON-safe primitives for APIs, outbox payloads and tests."""

    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if is_dataclass(value):
        return {key: to_primitive(item) for key, item in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(key): to_primitive(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [to_primitive(item) for item in value]
    return value


class InboxStatus(StrEnum):
    RECEIVED = "received"
    QUEUED = "queued"
    CLAIMED = "claimed"
    COMMITTED = "committed"
    REPLY_PENDING = "reply_pending"
    DELIVERED = "delivered"
    RETRYABLE = "retryable"
    UNKNOWN = "unknown"
    FAILED = "failed"


class OutboxStatus(StrEnum):
    PENDING = "pending"
    PROCESSING = "processing"
    PUBLISHED = "published"
    DELIVERED = "delivered"
    UNKNOWN = "unknown"
    DEAD = "dead"


class ExecutionMode(StrEnum):
    NORMAL = "normal"
    DRAINING = "draining"
    SUSPENDED = "suspended"
    EMERGENCY_STOP = "emergency_stop"


class AttemptStatus(StrEnum):
    CLAIMED = "claimed"
    RUNNING = "running"
    COMMITTED = "committed"
    LOST_FENCE = "lost_fence"
    CANCELLED = "cancelled"
    FAILED = "failed"


class ReservationStatus(StrEnum):
    RESERVED = "reserved"
    SETTLED = "settled"
    RELEASED = "released"
    EXPIRED = "expired"
    UNKNOWN = "unknown"


class ToolCapability(StrEnum):
    IDEMPOTENT = "idempotent"
    QUERYABLE = "queryable"
    NON_RETRIABLE = "non_retriable"


class ToolStatus(StrEnum):
    PREPARED = "prepared"
    CONFIRMED = "confirmed"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    UNKNOWN = "unknown"
    RECONCILING = "reconciling"
    MANUAL_REVIEW = "manual_review"


class ToolRecoveryAction(StrEnum):
    REUSE_RESULT = "reuse_result"
    RETRY_WITH_IDEMPOTENCY_KEY = "retry_with_idempotency_key"
    RECONCILE = "reconcile"
    MANUAL_REVIEW = "manual_review"


class MigrationStatus(StrEnum):
    PREPARING = "preparing"
    BACKFILLING = "backfilling"
    CATCHING_UP = "catching_up"
    DRAINING = "draining"
    VERIFYING = "verifying"
    ACTIVE = "active"
    READONLY = "readonly"
    RETIRED = "retired"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class TenantContext:
    tenant_id: str
    actor_id: str = "system"
    request_id: str = ""
    trace_id: str = ""


@dataclass(slots=True)
class TenantRuntimeState:
    tenant_id: str
    routing_epoch: int = 1
    security_epoch: int = 1
    credential_revocation_epoch: int = 1
    execution_mode: ExecutionMode = ExecutionMode.NORMAL
    tool_denylist: set[str] = field(default_factory=set)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True, slots=True)
class InboundEnvelope:
    """A normalized, already-authenticated provider callback.

    `tenant_id` must have been resolved by the protected channel-binding locator;
    callers cannot use this type to select an arbitrary tenant because acceptance
    checks it against :class:`TenantContext`.
    """

    tenant_id: str
    channel_binding_id: str
    agent_id: str
    session_id: str
    idempotency_key: str
    external_message_id: str | None
    payload: Mapping[str, Any]
    config_version: int = 1
    subject_id: str | None = None
    request_id: str = ""
    trace_id: str = ""
    received_at: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class InboxRecord:
    tenant_id: str
    inbox_id: str
    channel_binding_id: str
    agent_id: str
    session_id: str
    config_version: int
    subject_id: str | None
    idempotency_key: str
    external_message_id: str | None
    status: InboxStatus
    execution_id: str | None
    execution_attempt: int
    claimed_lease_fence: int | None
    claimed_routing_epoch: int | None
    claimed_security_epoch: int | None
    request_id: str
    trace_id: str
    payload: dict[str, Any]
    payload_hash: str
    received_at: datetime
    updated_at: datetime


@dataclass(slots=True)
class OutboxRecord:
    tenant_id: str
    outbox_id: str
    aggregate_type: str
    aggregate_id: str
    inbox_id: str | None
    event_type: str
    payload: dict[str, Any]
    idempotency_key: str
    trace_id: str
    status: OutboxStatus = OutboxStatus.PENDING
    attempts: int = 0
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    available_at: datetime = field(default_factory=utcnow)
    created_at: datetime = field(default_factory=utcnow)
    published_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class Acceptance:
    inbox: InboxRecord
    outbox: OutboxRecord
    duplicate: bool


@dataclass(slots=True)
class SessionRecord:
    tenant_id: str
    session_id: str
    agent_id: str
    channel_binding_id: str
    config_version: int
    state: dict[str, Any] = field(default_factory=dict)
    version: int = 0
    last_event_seq: int = 0
    active_inbox_id: str | None = None
    lease_owner: str | None = None
    lease_expires_at: datetime | None = None
    lease_fence: int = 0
    status: str = "active"
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(frozen=True, slots=True)
class SessionEventDraft:
    event_type: str
    role: str | None
    payload: Mapping[str, Any]
    event_id: str | None = None
    subject_id: str | None = None
    external_message_id: str | None = None
    occurred_at: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class SessionEvent:
    tenant_id: str
    session_id: str
    seq: int
    event_id: str
    event_type: str
    role: str | None
    subject_id: str | None
    external_message_id: str | None
    payload: dict[str, Any]
    trace_id: str
    occurred_at: datetime


@dataclass(slots=True)
class SessionSummary:
    """A durable, bounded summary of a session's committed conversation.

    Summaries are derived inside the same transaction as their source events.
    They are therefore safe to use as model context after a worker restart and
    never claim to cover events that were not durably committed.
    """

    tenant_id: str
    session_id: str
    summary_id: str
    based_on_seq: int
    content: str
    content_hash: str
    model_ref: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class BudgetRequest:
    budget_name: str
    estimated_units: int
    unit: str = "tokens"


@dataclass(slots=True)
class BudgetAccount:
    tenant_id: str
    budget_name: str
    unit: str
    limit_units: int
    spent_units: int = 0
    reserved_units: int = 0
    version: int = 0
    period_start: datetime = field(default_factory=utcnow)
    period_end: datetime = field(default_factory=lambda: utcnow() + timedelta(days=30))


@dataclass(slots=True)
class BudgetReservation:
    tenant_id: str
    reservation_id: str
    budget_name: str
    execution_id: str
    estimated_units: int
    status: ReservationStatus
    expires_at: datetime
    actual_units: int | None = None
    created_at: datetime = field(default_factory=utcnow)
    settled_at: datetime | None = None


@dataclass(slots=True)
class ExecutionAttempt:
    tenant_id: str
    execution_id: str
    attempt_no: int
    inbox_id: str
    session_id: str
    worker_id: str
    lease_fence: int
    routing_epoch: int
    security_epoch: int
    status: AttemptStatus
    lease_expires_at: datetime
    claimed_at: datetime
    finished_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ExecutionClaim:
    tenant_id: str
    inbox_id: str
    execution_id: str
    session_id: str
    worker_id: str
    lease_fence: int
    routing_epoch: int
    security_epoch: int
    session_version: int
    lease_expires_at: datetime
    attempt_no: int
    config_version: int


@dataclass(frozen=True, slots=True)
class MemoryIntentDraft:
    memory_type: str
    content: str
    source_event_id: str | None = None
    subject_id: str | None = None
    acl: Mapping[str, Any] = field(default_factory=dict)
    expires_at: datetime | None = None
    memory_id: str | None = None


@dataclass(slots=True)
class MemoryIntent:
    tenant_id: str
    memory_id: str
    session_id: str
    subject_id: str | None
    memory_type: str
    content: str
    content_hash: str
    acl: dict[str, Any]
    source_event_id: str | None
    version: int
    expires_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ReplyDraft:
    blocks: Sequence[Mapping[str, Any]]
    channel_binding_id: str | None = None
    recipient_id: str | None = None
    delivery_key: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CommitInput:
    expected_session_version: int
    new_state: Mapping[str, Any]
    events: Sequence[SessionEventDraft] = ()
    memories: Sequence[MemoryIntentDraft] = ()
    reply: ReplyDraft | None = None
    actual_budget_units: Mapping[str, int] = field(default_factory=dict)
    audit_decision: str = "committed"
    # Structured audit facts contain identifiers and hashes only—never raw
    # provider credentials or conversation content.
    audit_metadata: Mapping[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class CommitResult:
    session: SessionRecord
    inbox: InboxRecord
    events: list[SessionEvent]
    summary: SessionSummary | None
    reply_outbox: OutboxRecord | None
    memory_outboxes: list[OutboxRecord]


@dataclass(slots=True)
class ToolExecution:
    tenant_id: str
    tool_call_id: str
    inbox_id: str
    execution_id: str
    session_id: str
    tool_step: int
    tool_name: str
    arguments_hash: str
    retry_capability: ToolCapability
    provider_idempotency_key: str | None
    lease_fence: int
    routing_epoch: int
    security_epoch: int
    status: ToolStatus
    trace_id: str
    result: dict[str, Any] | None = None
    last_error_code: str | None = None
    provider_operation_id: str | None = None
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class StorageRoute:
    tenant_id: str
    routing_epoch: int
    profile: dict[str, Any]
    status: MigrationStatus
    source_watermark: str | None = None
    target_watermark: str | None = None
    created_at: datetime = field(default_factory=utcnow)
    activated_at: datetime | None = None


@dataclass(slots=True)
class StorageMigration:
    tenant_id: str
    migration_id: str
    source_profile: dict[str, Any]
    target_profile: dict[str, Any]
    status: MigrationStatus
    source_routing_epoch: int
    target_routing_epoch: int | None = None
    source_watermark: str | None = None
    target_watermark: str | None = None
    error: str | None = None
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)


@dataclass(slots=True)
class AuditRecord:
    tenant_id: str
    audit_id: str
    decision: str
    trace_id: str
    request_id: str
    session_id: str | None = None
    reason_code: str | None = None
    channel: str | None = None
    subject_id: str | None = None
    agent_name: str | None = None
    tool_name: str | None = None
    policy_version: str | None = None
    latency_ms: int | None = None
    error_type: str | None = None
    input_hash: str | None = None
    output_hash: str | None = None
    token_in: int | None = None
    token_out: int | None = None
    cost_micros: int | None = None
    encrypted_detail_ref: str | None = None
    occurred_at: datetime = field(default_factory=utcnow)
