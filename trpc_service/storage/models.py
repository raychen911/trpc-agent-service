"""Persistence-neutral runtime records."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from trpc_service.channels.envelopes import InboundEnvelope, OutboundEnvelope
from trpc_service.tenant.models import ChannelBinding, TenantContext


class RecordModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class TurnStatus(StrEnum):
    PROCESSING = "processing"
    COMMITTED = "committed"
    FAILED = "failed"
    NEEDS_CONFIRMATION = "needs_confirmation"


class MailboxStatus(StrEnum):
    """Durable state of one session's ordered mailbox."""

    IDLE = "IDLE"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"


class MailboxClaimStatus(StrEnum):
    CLAIMED = "CLAIMED"
    STALE = "STALE"
    RUNNING = "RUNNING"
    EMPTY = "EMPTY"


class BindingRoute(RecordModel):
    binding: ChannelBinding
    tenant_active: bool = True
    active_config_version: int = Field(ge=1)
    candidate_config_version: int | None = Field(default=None, ge=1)
    candidate_percent: float = Field(default=0, ge=0, le=100)


class Acceptance(RecordModel):
    inbound_id: str
    context: TenantContext
    envelope: InboundEnvelope
    accepted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    duplicate: bool = False


class PreparedInbound(RecordModel):
    """Verified inbound data with routing and config frozen before persistence."""

    context: TenantContext
    envelope: InboundEnvelope
    trace_headers: dict[str, str] = Field(default_factory=dict)


class StoredEvent(RecordModel):
    event_id: str
    author: str
    timestamp: float
    event: dict[str, Any]
    state_delta: dict[str, Any] = Field(default_factory=dict)


class SequencedEvent(StoredEvent):
    sequence: int = Field(ge=1)


class SessionSnapshot(RecordModel):
    tenant_id: str
    app_id: str
    session_id: str
    principal_id: str
    version: int = Field(default=0, ge=0)
    next_sequence: int = Field(default=1, ge=1)
    state: dict[str, Any] = Field(default_factory=dict)
    events: tuple[SequencedEvent, ...] = ()


class SummarySnapshot(RecordModel):
    """The last summary known to be complete through a session sequence."""

    tenant_id: str
    session_id: str
    up_to_sequence: int = Field(ge=0)
    summary: dict[str, object] = Field(default_factory=dict)
    version: int = Field(default=1, ge=1)


class SessionLease(RecordModel):
    tenant_id: str
    session_id: str
    turn_id: str
    inbound_id: str
    worker_id: str
    fencing_token: int = Field(ge=1)
    expires_at: datetime
    attempt: int = Field(default=1, ge=1)
    snapshot: SessionSnapshot
    # PostgreSQL v2 claims intentionally return a metadata-only snapshot so
    # the mailbox claim transaction never reads the complete session history.
    # Existing callers constructing a fully materialized lease retain the
    # historical default.
    snapshot_hydrated: bool = True


class SessionMailbox(RecordModel):
    """Monotonic per-session mailbox counters and its fenced processing lease."""

    tenant_id: str
    session_id: str
    status: MailboxStatus = MailboxStatus.IDLE
    accepted_sequence: int = Field(default=0, ge=0)
    resolved_sequence: int = Field(default=0, ge=0)
    processing_sequence: int | None = Field(default=None, ge=1)
    processing_inbound_id: str | None = None
    queue_generation: int = Field(default=0, ge=0)
    lease_owner: str | None = None
    lease_epoch: int = Field(default=0, ge=0)
    lease_expires_at: datetime | None = None
    retry_count: int = Field(default=0, ge=0)
    attempt: int = Field(default=0, ge=0)
    priority: int = Field(default=0, ge=0)
    retry_at: datetime | None = None
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @property
    def state(self) -> MailboxStatus:
        return self.status

    @property
    def owner_id(self) -> str | None:
        return self.lease_owner


class MailboxItem(RecordModel):
    """The inbound message assigned a monotonic sequence in a mailbox."""

    tenant_id: str
    session_id: str
    inbound_id: str
    sequence: int = Field(ge=1)
    trace_id: str = Field(default="", max_length=512)
    priority: int = Field(default=0, ge=0)
    retry_count: int = Field(default=0, ge=0)
    attempt: int = Field(default=0, ge=0)
    retry_at: datetime | None = None
    accepted_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class MailboxLease(RecordModel):
    """Fenced claim for one mailbox sequence."""

    tenant_id: str
    session_id: str
    inbound_id: str
    sequence: int = Field(ge=1)
    owner_id: str
    epoch: int = Field(ge=1)
    expires_at: datetime
    attempt: int = Field(default=1, ge=1)
    retry_count: int = Field(default=0, ge=0)
    priority: int = Field(default=0, ge=0)

    @property
    def fencing_token(self) -> int:
        return self.epoch

    @property
    def lease_owner(self) -> str:
        return self.owner_id


class WeComBindingLeaseGrant(RecordModel):
    """Fenced ownership of one tenant-scoped WeCom connection."""

    tenant_id: str
    binding_id: str
    owner_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    epoch: int = Field(ge=1)
    acquired_at: datetime


class SessionClaim(RecordModel):
    """Result of a mailbox claim, including non-claim outcomes."""

    status: MailboxClaimStatus
    mailbox: SessionMailbox
    lease: MailboxLease | None = None
    acceptance: Acceptance | None = None
    # A mailbox claim is only a wake-up/ordering claim.  Runtime callers need
    # the independently materialized turn lease and snapshot to execute work.
    execution_lease: SessionLease | None = None

    @property
    def claimed(self) -> bool:
        return self.status == MailboxClaimStatus.CLAIMED


class TurnCommit(RecordModel):
    context: TenantContext
    lease: SessionLease
    state: dict[str, Any]
    events: tuple[StoredEvent, ...]
    outbound: OutboundEnvelope | None = None


class CommitResult(RecordModel):
    turn_id: str
    first_sequence: int | None = None
    last_sequence: int | None = None
    outbound_id: str | None = None


class OutboxRecord(RecordModel):
    outbox_id: str
    tenant_id: str
    event_type: str
    aggregate_id: str
    payload: dict[str, Any]
    trace_headers: dict[str, str] = Field(default_factory=dict)
    attempts: int = Field(default=0, ge=0)


class DeliveryAttempt(RecordModel):
    """Durable ownership token for one provider delivery attempt.

    The attempt number is allocated while the outbound row is locked.  A
    dispatcher must retain this value and present it when recording the
    provider result; a late result from an expired claim is therefore unable
    to overwrite a newer attempt.
    """

    tenant_id: str
    outbound_id: str
    attempt_number: int = Field(ge=1)
    owner_id: str


class AuditCursorPage(RecordModel):
    entries: tuple[dict[str, Any], ...]
    next_cursor: str | None = None


__all__ = [
    "Acceptance",
    "AuditCursorPage",
    "BindingRoute",
    "CommitResult",
    "DeliveryAttempt",
    "MailboxClaimStatus",
    "MailboxItem",
    "MailboxLease",
    "MailboxStatus",
    "OutboxRecord",
    "PreparedInbound",
    "SequencedEvent",
    "SessionClaim",
    "SessionLease",
    "SessionMailbox",
    "SessionSnapshot",
    "StoredEvent",
    "SummarySnapshot",
    "TurnCommit",
    "TurnStatus",
    "WeComBindingLeaseGrant",
]
