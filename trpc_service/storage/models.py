"""Immutable idempotency state machine and content identity models."""

from __future__ import annotations

from datetime import datetime
from enum import IntEnum, StrEnum
from hashlib import sha256
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from trpc_service.channels.contracts import Channel, DeliveryAction, InboundMessage
from trpc_service.channels.identity import ProviderReplyContext


class IdempotencyState(StrEnum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED_PRE_START = "failed_pre_start"
    FAILED_POST_START = "failed_post_start"
    OUTCOME_UNKNOWN = "outcome_unknown"


class ClaimDisposition(StrEnum):
    ACQUIRED = "acquired"
    PROCESSING = "processing"
    COMPLETED = "completed"
    CONFLICT = "conflict"
    OUTCOME_UNKNOWN = "outcome_unknown"


class ExecutionStatus(StrEnum):
    SUCCEEDED = "succeeded"
    FAILED_POST_START = "failed_post_start"
    OUTCOME_UNKNOWN = "outcome_unknown"


class ExecutionPhase(IntEnum):
    CLAIMED = 1
    PREPARED = 2
    EXECUTION_STARTED = 3
    FINALIZING = 4
    TERMINAL = 5


class RecoveryState(StrEnum):
    TERMINAL_PENDING = "terminal_pending"
    RECONCILED = "reconciled"
    CONFLICT_REVIEW = "conflict_review"


class _Frozen(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class NodeIdentity(_Frozen):
    node_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")


class MessageFence(_Frozen):
    key_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation: int = Field(gt=0)
    owner_node: NodeIdentity
    owner_token: str = Field(min_length=16, repr=False)
    owner_trace_id: UUID
    expires_at: datetime


class SessionFence(_Frozen):
    session_key_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    generation: int = Field(gt=0)
    owner_node: NodeIdentity
    owner_token: str = Field(min_length=16, repr=False)
    expires_at: datetime


class AdapterFence(_Frozen):
    identity_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    node_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
    generation: int = Field(gt=0)
    owner_token: str = Field(min_length=16, repr=False)
    expires_at: datetime


class AdapterOwnershipPhase(StrEnum):
    STANDBY = "standby"
    CONNECTING = "connecting"
    READY = "ready"
    DRAINING = "draining"


class AdapterOwnershipState(_Frozen):
    identity_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    owner_node_id: str | None = Field(default=None, max_length=64)
    generation: int = Field(ge=0)
    phase: AdapterOwnershipPhase = AdapterOwnershipPhase.STANDBY
    expires_in_ms: int = Field(default=0, ge=0)


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    SENDING = "sending"
    RETRY_WAIT = "retry_wait"
    DELIVERED = "delivered"
    DELIVERY_FAILED = "delivery_failed"
    DELIVERY_UNKNOWN = "delivery_unknown"

    @property
    def terminal(self) -> bool:
        return self in {
            DeliveryStatus.DELIVERED,
            DeliveryStatus.DELIVERY_FAILED,
            DeliveryStatus.DELIVERY_UNKNOWN,
        }


class DeliveryOutcome(StrEnum):
    SUCCEEDED = "succeeded"
    TRANSIENT = "transient"
    PERMANENT = "permanent"
    UNKNOWN = "unknown"
    FENCE_REJECTED = "fence_rejected"


class DeliveryRecord(_Frozen):
    delivery_id: UUID
    tenant_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,63}$")
    binding_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,95}$")
    channel: Channel
    idempotency_key_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    execution_trace_id: UUID
    reply_context: ProviderReplyContext
    result_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    status: DeliveryStatus
    adapter_generation: int = Field(gt=0)
    next_attempt_at: datetime | None = None
    created_at: datetime
    updated_at: datetime

    @model_validator(mode="after")
    def validate_state(self) -> Self:
        for name, value in (("created_at", self.created_at), ("updated_at", self.updated_at)):
            if value.tzinfo is None or value.utcoffset() is None or value.utcoffset().total_seconds() != 0:
                raise ValueError(f"{name} must be UTC-aware")
        if self.updated_at < self.created_at:
            raise ValueError("updated_at cannot precede created_at")
        if self.status == DeliveryStatus.RETRY_WAIT and self.next_attempt_at is None:
            raise ValueError("retry_wait requires next_attempt_at")
        if self.status != DeliveryStatus.RETRY_WAIT and self.next_attempt_at is not None:
            raise ValueError("next_attempt_at is only valid for retry_wait")
        return self

    def transition(
        self,
        target: DeliveryStatus,
        now: datetime,
        *,
        next_attempt_at: datetime | None = None,
        adapter_generation: int | None = None,
    ) -> "DeliveryRecord":
        if self.status.terminal:
            raise ValueError("delivery status is terminal")
        allowed = {
            DeliveryStatus.PENDING: {DeliveryStatus.SENDING},
            DeliveryStatus.SENDING: {
                DeliveryStatus.DELIVERED,
                DeliveryStatus.RETRY_WAIT,
                DeliveryStatus.DELIVERY_FAILED,
                DeliveryStatus.DELIVERY_UNKNOWN,
            },
            DeliveryStatus.RETRY_WAIT: {DeliveryStatus.SENDING},
        }
        if target not in allowed.get(self.status, set()):
            raise ValueError("invalid delivery transition")
        return self.model_copy(
            update={
                "status": target,
                "updated_at": now,
                "next_attempt_at": next_attempt_at if target == DeliveryStatus.RETRY_WAIT else None,
                "adapter_generation": adapter_generation or self.adapter_generation,
            }
        )


class DeliveryAttempt(_Frozen):
    attempt_id: UUID
    delivery_id: UUID
    attempt_no: int = Field(ge=1, le=4)
    trace_id: UUID
    adapter_node_id: str = Field(pattern=r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
    adapter_generation: int = Field(gt=0)
    started_at: datetime
    finished_at: datetime | None = None
    outcome: DeliveryOutcome | None = None
    safe_error_code: str | None = Field(
        default=None, pattern=r"^[a-z][a-z0-9_]*$"
    )
    retry_delay_seconds: int | None = None

    @model_validator(mode="after")
    def validate_attempt(self) -> Self:
        values = [self.started_at]
        if self.finished_at is not None:
            values.append(self.finished_at)
        if any(value.tzinfo is None or value.utcoffset() is None or value.utcoffset().total_seconds() != 0 for value in values):
            raise ValueError("delivery attempt timestamps must be UTC-aware")
        if self.finished_at is not None and self.finished_at < self.started_at:
            raise ValueError("finished_at cannot precede started_at")
        if self.outcome == DeliveryOutcome.TRANSIENT:
            if self.retry_delay_seconds not in {1, 2, 4} or self.safe_error_code is None:
                raise ValueError("transient outcome requires safe retry metadata")
        elif self.retry_delay_seconds is not None:
            raise ValueError("retry delay is only valid for transient outcome")
        if self.outcome in {DeliveryOutcome.PERMANENT, DeliveryOutcome.UNKNOWN} and self.safe_error_code is None:
            raise ValueError("failed outcome requires a safe error code")
        return self


class RecoveryMarker(_Frozen):
    recovery_id: UUID
    tenant_id: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{0,63}$")
    idempotency_key_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    message_generation: int = Field(gt=0)
    session_generation: int = Field(gt=0)
    execution_trace_id: UUID
    result_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: RecoveryState
    created_at: datetime
    updated_at: datetime

    def transition(
        self, target: RecoveryState, now: datetime
    ) -> "RecoveryMarker":
        if self.state != RecoveryState.TERMINAL_PENDING:
            raise ValueError("recovery marker is already terminal")
        if target not in {RecoveryState.RECONCILED, RecoveryState.CONFLICT_REVIEW}:
            raise ValueError("invalid recovery transition")
        return self.model_copy(update={"state": target, "updated_at": now})


class IdempotencyKey(_Frozen):
    tenant_id: str
    channel: Channel = Channel.LOCAL_HTTP
    binding_id: str
    external_message_id: str


class ExecutionResult(_Frozen):
    status: ExecutionStatus
    response_text: str | None = Field(default=None, min_length=1, max_length=4000)
    error_code: str | None = None
    error_message: str | None = None
    original_trace_id: UUID
    platform_session_id: str
    started_at: datetime
    finished_at: datetime
    agent_event_count: int = Field(ge=0)
    final_response_count: int = Field(ge=0)
    delivery_action: DeliveryAction

    @model_validator(mode="after")
    def validate_result(self) -> Self:
        if self.status == ExecutionStatus.SUCCEEDED:
            if self.response_text is None or self.final_response_count != 1:
                raise ValueError("successful execution requires exactly one final response")
        elif self.error_code is None or self.response_text is not None:
            raise ValueError("failed execution requires a safe error")
        return self


class IdempotencyRecord(_Frozen):
    key: IdempotencyKey
    content_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    state: IdempotencyState
    attempt: int = Field(ge=1)
    owner_token: str | None = Field(repr=False)
    generation: int = Field(default=1, gt=0)
    execution_phase: ExecutionPhase = ExecutionPhase.CLAIMED
    first_claim_trace_id: UUID
    owner_trace_id: UUID | None
    execution_trace_id: UUID | None = None
    result: ExecutionResult | None = None
    created_at: datetime
    updated_at: datetime
    pre_start_error: str | None = None

    @classmethod
    def pending(cls, *, key: IdempotencyKey, content_fingerprint: str, owner_token: str, trace_id: UUID, now: datetime) -> IdempotencyRecord:
        return cls(
            key=key,
            content_fingerprint=content_fingerprint,
            state=IdempotencyState.PENDING,
            attempt=1,
            owner_token=owner_token,
            first_claim_trace_id=trace_id,
            owner_trace_id=trace_id,
            created_at=now,
            updated_at=now,
        )

    def _require(self, state: IdempotencyState, owner_token: str) -> None:
        if self.state != state or self.owner_token != owner_token:
            raise ValueError("invalid idempotency transition")

    def mark_running(self, owner_token: str, execution_trace_id: UUID, now: datetime) -> IdempotencyRecord:
        self._require(IdempotencyState.PENDING, owner_token)
        return self.model_copy(update={"state": IdempotencyState.RUNNING, "execution_phase": ExecutionPhase.EXECUTION_STARTED, "execution_trace_id": execution_trace_id, "updated_at": now})

    def mark_pre_start_failed(self, owner_token: str, safe_error: str, now: datetime) -> IdempotencyRecord:
        self._require(IdempotencyState.PENDING, owner_token)
        return self.model_copy(update={"state": IdempotencyState.FAILED_PRE_START, "owner_token": None, "owner_trace_id": None, "pre_start_error": safe_error, "updated_at": now})

    def reclaim(self, owner_token: str, trace_id: UUID, now: datetime) -> IdempotencyRecord:
        if self.state != IdempotencyState.FAILED_PRE_START:
            raise ValueError("invalid idempotency transition")
        return self.model_copy(update={"state": IdempotencyState.PENDING, "attempt": self.attempt + 1, "generation": self.generation + 1, "execution_phase": ExecutionPhase.CLAIMED, "owner_token": owner_token, "owner_trace_id": trace_id, "execution_trace_id": None, "result": None, "updated_at": now})

    def complete(self, owner_token: str, result: ExecutionResult, now: datetime) -> IdempotencyRecord:
        self._require(IdempotencyState.RUNNING, owner_token)
        terminal = IdempotencyState(result.status.value)
        return self.model_copy(update={"state": terminal, "execution_phase": ExecutionPhase.TERMINAL, "owner_token": None, "owner_trace_id": None, "result": result, "updated_at": now})


class ClaimResult(_Frozen):
    disposition: ClaimDisposition
    owner_token: str | None = None
    attempt: int | None = None
    original_trace_id: UUID | None = None
    result: ExecutionResult | None = None


def _digest_parts(*parts: str) -> str:
    digest = sha256()
    for part in parts:
        encoded = part.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def content_fingerprint(message: InboundMessage) -> str:
    return _digest_parts(
        message.channel.value,
        message.external_user_id,
        message.conversation_type.value,
        message.external_conversation_id,
        message.text,
    )
