"""Typed contracts for the SQL reliability repository.

The repository returns explicit outcomes instead of ambiguous booleans so callers
cannot accidentally treat a stale fence or a payload conflict as a successful retry.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class ReliabilityError(RuntimeError):
    """Base class for reliability-plane contract failures."""


class IdempotencyConflictError(ReliabilityError):
    """A stable idempotency key was reused with different content."""


class StaleClaimError(ReliabilityError):
    """A worker attempted to mutate state with an expired or superseded fence."""


class StaleVersionError(ReliabilityError):
    """A session append used a stale optimistic-concurrency version."""


class InvalidStateTransitionError(ReliabilityError):
    """A persisted state machine transition was not legal."""


class ReliabilityInvariantError(ReliabilityError):
    """Persisted rows violate an invariant that should be atomic."""


class InboxDisposition(StrEnum):
    """Result of accepting an external delivery."""

    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"


class AppendDisposition(StrEnum):
    """Result of appending a canonical event."""

    APPENDED = "appended"
    ALREADY_APPENDED = "already_appended"


class FinalizeDisposition(StrEnum):
    """Result of publishing one logical Agent run."""

    FINALIZED = "finalized"
    ALREADY_FINALIZED = "already_finalized"


class ProjectionFinalizeDisposition(StrEnum):
    """Result of atomically publishing one projection job's outputs."""

    FINALIZED = "finalized"
    ALREADY_FINALIZED = "already_finalized"


class ToolReservationDisposition(StrEnum):
    """Result of reserving or recovering a tool effect."""

    EXECUTE = "execute"
    IN_PROGRESS = "in_progress"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED_FINAL = "failed_final"
    UNKNOWN = "unknown"


class OutboxDeliveryOutcome(StrEnum):
    """Terminal or delayed result of one fenced outbound delivery attempt."""

    SENT = "sent"
    RETRY_WAIT = "retry_wait"
    DEAD_LETTER = "dead_letter"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ReplyCredentialData:
    """Already-encrypted, short-lived channel credential accepted at T0.

    The reliability layer never accepts or returns a plaintext secret.
    """

    credential_kind: str
    ciphertext: str = field(repr=False)
    ciphertext_hash: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class ReplyCredentialRef:
    """Encrypted credential handed only to the authorized Outbox dispatcher."""

    credential_id: str
    credential_kind: str
    ciphertext: str = field(repr=False)
    ciphertext_hash: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class InboxEnvelope:
    """Trusted, normalized inbound message ready for durable acceptance."""

    tenant_id: str
    binding_id: str
    session_id: str
    app_id: str
    app_revision: int
    config_revision: int
    scope: str
    principal_id: str
    external_delivery_id: str
    payload: dict[str, Any]
    payload_hash: str
    request_id: str
    trace_id: str
    reply_credential: ReplyCredentialData | None = None


@dataclass(frozen=True, slots=True)
class InboxAcceptance:
    """Durable acceptance result returned after commit."""

    disposition: InboxDisposition
    inbox_id: str
    accepted_seq: int
    status: str
    credential_id: str | None = None


@dataclass(frozen=True, slots=True)
class SessionClaim:
    """Lease and fencing capability handed to exactly one current worker."""

    tenant_id: str
    session_id: str
    inbox_id: str
    run_id: str
    worker_id: str
    fencing_token: int
    attempt_no: int
    expected_version: int
    lease_expires_at: datetime
    request_id: str
    trace_id: str


@dataclass(frozen=True, slots=True)
class ClaimInput:
    """Tenant-safe immutable view consumed by a Worker after claiming an Inbox."""

    tenant_id: str
    session_id: str
    inbox_id: str
    run_id: str
    binding_id: str
    app_id: str
    app_revision: int
    config_revision: int
    scope: str
    principal_id: str
    accepted_seq: int
    external_delivery_id: str
    payload: dict[str, Any]
    request_id: str
    trace_id: str
    attempt_no: int
    fencing_token: int


@dataclass(frozen=True, slots=True)
class CommittedEvent:
    """One immutable event visible in a committed session replay."""

    seq: int
    event_id: str
    event_type: str
    role: str | None
    content_ref: str | None
    payload: dict[str, Any]
    state_delta: dict[str, Any]
    framework_event_id: str | None
    created_at: datetime


@dataclass(frozen=True, slots=True)
class CommittedSessionView:
    """Coherent session-scope state and ordered committed event history."""

    tenant_id: str
    session_id: str
    state: dict[str, Any]
    state_version: int
    log_version: int
    events: tuple[CommittedEvent, ...]


@dataclass(frozen=True, slots=True)
class EventData:
    """One non-partial canonical event emitted by the Runner wrapper."""

    event_id: str
    event_key: str
    event_type: str
    payload: dict[str, Any]
    payload_hash: str | None = None
    role: str | None = None
    content_ref: str | None = None
    state_delta: dict[str, Any] = field(default_factory=dict)
    framework_event_id: str | None = None
    visibility: str = "staged"


@dataclass(frozen=True, slots=True)
class EventAppend:
    """Canonical sequence allocated to an event append."""

    disposition: AppendDisposition
    event_id: str
    seq: int
    version: int


@dataclass(frozen=True, slots=True)
class ReplyPart:
    """One ordered outbound reply part created during finalization."""

    reply_id: str
    part_no: int
    payload: dict[str, Any]
    payload_hash: str | None = None


@dataclass(frozen=True, slots=True)
class AuditData:
    """Allow-listed audit attributes for the run finalization decision."""

    channel: str
    user_id: str
    agent_name: str
    decision: str
    action: str
    resource: str
    config_revision: int
    policy_revision: int
    reason: str | None = None
    latency_ms: int = 0
    error_type: str | None = None
    cost_micros: int = 0
    tool_name: str | None = None
    idempotency_key: str | None = None
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class FinalizeResult:
    """Published run and its durable reply records."""

    disposition: FinalizeDisposition
    run_id: str
    last_seq: int
    outbox_ids: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ProjectionClaim:
    """Lease and monotonically increasing fence for one durable projection job."""

    tenant_id: str
    job_id: str
    run_id: str
    session_id: str
    config_revision: int
    through_seq: int
    worker_id: str
    fencing_token: int
    attempt_no: int
    lease_expires_at: datetime


@dataclass(frozen=True, slots=True)
class ProjectionEvent:
    """Detached committed event exposed to a projection algorithm."""

    seq: int
    event_id: str
    event_type: str
    role: str | None
    content_ref: str | None
    payload: dict[str, Any]
    state_delta: dict[str, Any]
    created_at: datetime


@dataclass(frozen=True, slots=True)
class ProjectionInput:
    """Immutable canonical input bounded by the job's committed watermark."""

    tenant_id: str
    job_id: str
    run_id: str
    session_id: str
    principal_id: str
    config_revision: int
    through_seq: int
    state_version: int
    events: tuple[ProjectionEvent, ...]


@dataclass(frozen=True, slots=True)
class ProjectionFinalizeResult:
    """Outcome of atomically persisting projections and completing their job."""

    disposition: ProjectionFinalizeDisposition
    job_id: str
    summary_applied: bool
    memories_applied: int


@dataclass(frozen=True, slots=True)
class OutboxDeliveryClaim:
    """One ordered outbound part protected by an internal attempt fence."""

    tenant_id: str
    outbox_id: str
    run_id: str
    binding_id: str
    session_id: str
    delivery_id: str
    reply_id: str
    part_no: int
    payload: dict[str, Any]
    payload_hash: str
    dispatcher_id: str
    delivery_token: str
    attempt_no: int
    claim_expires_at: datetime
    reply_credential: ReplyCredentialRef | None = None


@dataclass(frozen=True, slots=True)
class ToolEffectRequest:
    """Stable description of an external tool operation."""

    idempotency_key: str
    tool_name: str
    tool_version: str
    effect_class: str
    args_hash: str
    downstream_key: str | None = None


@dataclass(frozen=True, slots=True)
class ToolReservation:
    """Reservation state used to decide whether an external call may run."""

    disposition: ToolReservationDisposition
    effect_id: str
    status: str
    execution_token: str
    attempt_count: int
    result_ref: str | None = None
    result_hash: str | None = None
