"""Immutable, tenant-scoped governance domain values and state machines."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from typing import Any, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class PolicyStatus(StrEnum):
    DRAFT = "draft"
    ACTIVE = "active"
    SUPERSEDED = "superseded"
    DISABLED = "disabled"


class Decision(StrEnum):
    ALLOW = "allow"
    DENY = "deny"
    CONFIRMATION_REQUIRED = "confirmation_required"


class ToolRiskLevel(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class SideEffectClass(StrEnum):
    NONE = "none"
    REVERSIBLE = "reversible"
    EXTERNAL = "external"


class UsageDimension(StrEnum):
    REQUEST = "request"
    TOOL_CALL = "tool_call"
    TOKEN = "token"
    COST = "cost"


class ConfirmationStatus(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    EXECUTING = "executing"
    COMPLETED = "completed"
    EXPIRED = "expired"
    CANCELLED = "cancelled"
    OUTCOME_UNKNOWN = "outcome_unknown"


class ReservationStatus(StrEnum):
    RESERVED = "reserved"
    SETTLED = "settled"
    RELEASED = "released"
    REVIEW_REQUIRED = "review_required"


class RecoveryStage(StrEnum):
    ADMITTED = "admitted"
    RESERVED = "reserved"
    EXECUTION_STARTED = "execution_started"
    RESULT_DURABLE = "result_durable"
    SETTLED = "settled"
    AUDITED = "audited"
    DELIVERY_PENDING = "delivery_pending"
    REVIEW_REQUIRED = "review_required"


class PolicyScope(StrEnum):
    TENANT = "tenant"
    AGENT = "agent"
    BINDING = "binding"


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError("timestamp must be UTC-aware")
    return value


class PolicyDocument(_Model):
    allowed_tools: frozenset[str] = frozenset()
    dangerous_tools: frozenset[str] = frozenset()
    confirmation_ttl_seconds: int = Field(default=300, ge=1, le=3600)
    budget_maximums: dict[UsageDimension, Decimal] = Field(default_factory=dict)
    redaction_rules: tuple[str, ...] = ()

    @field_validator("allowed_tools", "dangerous_tools", mode="before")
    @classmethod
    def normalize_tools(cls, value: Any) -> frozenset[str]:
        return frozenset(str(item) for item in (value or ()))

    @field_validator("budget_maximums", mode="before")
    @classmethod
    def normalize_budget(cls, value: Any) -> dict[UsageDimension, Decimal]:
        return {UsageDimension(key): Decimal(str(amount)) for key, amount in (value or {}).items()}

    @model_validator(mode="after")
    def validate_tools(self) -> PolicyDocument:
        if not self.dangerous_tools.issubset(self.allowed_tools):
            raise ValueError("dangerous tools must be allowed tools")
        return self


class GovernancePolicyVersion(_Model):
    policy_id: str
    tenant_id: str
    scope: PolicyScope = PolicyScope.TENANT
    version: int = Field(gt=0)
    status: PolicyStatus = PolicyStatus.DRAFT
    document: PolicyDocument
    created_by_digest: str = Field(min_length=16)
    created_at: datetime

    _validate_created_at = field_validator("created_at")(_utc)


class ActiveGovernancePolicy(_Model):
    tenant_id: str
    policy_id: str
    version: int = Field(gt=0)
    generation: int = Field(ge=0)
    document: PolicyDocument


class ChannelPrincipal(_Model):
    tenant_id: str = Field(min_length=1)
    channel: str = Field(pattern=r"^(feishu|wecom)$")
    binding_id: str = Field(min_length=1)
    provider_subject: str = Field(min_length=1)
    subject_digest: str = Field(pattern=r"^[0-9a-f]{64}$")

    @classmethod
    def issue(cls, *, tenant_id: str, channel: str, binding_id: str, provider_subject: str, key: bytes = b"governance-test-key") -> ChannelPrincipal:
        digest = sha256(key + b"\0" + tenant_id.encode() + b"\0" + channel.encode() + b"\0" + provider_subject.encode()).hexdigest()
        return cls(tenant_id=tenant_id, channel=channel, binding_id=binding_id, provider_subject=provider_subject, subject_digest=digest)


class PrincipalGrant(_Model):
    grant_id: str
    tenant_id: str
    channel: str
    binding_id: str
    provider_subject_digest: str
    agent_name: str | None = None
    permissions: frozenset[str] = frozenset()
    enabled: bool = True
    valid_from: datetime | None = None
    expires_at: datetime | None = None
    created_at: datetime
    revoked_at: datetime | None = None


class PrincipalGrantDecision(_Model):
    allowed: bool
    reason_code: str


class ToolDescriptor(_Model):
    tool_name: str = Field(min_length=1, max_length=128)
    side_effect_class: SideEffectClass
    risk_level: ToolRiskLevel
    confirmation_required: bool | None = None
    usage_dimensions: frozenset[UsageDimension] = frozenset()
    max_usage: "UsageVector" = Field(default_factory=lambda: UsageVector())

    @field_validator("usage_dimensions", mode="before")
    @classmethod
    def normalize_dimensions(cls, value: Any) -> frozenset[UsageDimension]:
        return frozenset(UsageDimension(item) for item in (value or ()))

    @model_validator(mode="after")
    def set_confirmation(self) -> ToolDescriptor:
        if self.confirmation_required is None:
            object.__setattr__(self, "confirmation_required", self.risk_level == ToolRiskLevel.HIGH)
        return self


class UsageVector(_Model):
    request: Decimal = Field(default=Decimal("0"), ge=0)
    tool_call: Decimal = Field(default=Decimal("0"), ge=0)
    token: Decimal = Field(default=Decimal("0"), ge=0)
    cost: Decimal = Field(default=Decimal("0"), ge=0)

    def for_dimension(self, dimension: UsageDimension) -> Decimal:
        return getattr(self, dimension.value)

    def exceeds(self, maximum: UsageVector) -> bool:
        return any(self.for_dimension(d) > maximum.for_dimension(d) for d in UsageDimension)


class PolicyDecision(_Model):
    decision: Decision
    policy_id: str | None = None
    policy_version: int | None = None
    reason_code: str
    tool_name: str | None = None
    latency_ms: int = Field(default=0, ge=0)


class BudgetReservation(_Model):
    tenant_id: str
    execution_id: str
    status: ReservationStatus
    maximum: UsageVector = Field(default_factory=UsageVector)
    actual: UsageVector = Field(default_factory=UsageVector)
    execution_started: bool = False
    reservation_id: str | None = None

    @classmethod
    def initial(cls, tenant_id: str, execution_id: str, maximum: UsageVector | None = None) -> BudgetReservation:
        return cls(tenant_id=tenant_id, execution_id=execution_id, status=ReservationStatus.RESERVED, maximum=maximum or UsageVector(), reservation_id=execution_id)

    def transition(self, target: ReservationStatus | str, *, actual: UsageVector | None = None) -> BudgetReservation:
        target = ReservationStatus(target)
        if self.status != ReservationStatus.RESERVED:
            raise ValueError("reservation terminal state is immutable")
        if target == ReservationStatus.SETTLED:
            candidate = actual or UsageVector()
            if candidate.exceeds(self.maximum):
                raise ValueError("actual usage exceeds reservation")
            return self.model_copy(update={"status": target, "actual": candidate})
        if target in {ReservationStatus.RELEASED, ReservationStatus.REVIEW_REQUIRED}:
            if target == ReservationStatus.REVIEW_REQUIRED and not self.execution_started:
                raise ValueError("review requires started execution")
            return self.model_copy(update={"status": target})
        raise ValueError("invalid reservation transition")


class PendingConfirmation(_Model):
    confirmation_id: str
    tenant_id: str
    reservation_id: str
    channel: str = ""
    binding_id: str = ""
    principal_digest: str = ""
    session_id: str = ""
    tool_name: str = ""
    arguments_digest: str = ""
    policy_version: int = 0
    token_digest: str = ""
    status: ConfirmationStatus = ConfirmationStatus.PENDING
    expires_at: datetime | None = None

    @classmethod
    def new(cls, *, tenant_id: str, confirmation_id: str, reservation_id: str, expires_at: datetime | None = None, **kwargs: Any) -> PendingConfirmation:
        return cls(tenant_id=tenant_id, confirmation_id=confirmation_id, reservation_id=reservation_id, expires_at=expires_at, **kwargs)


class GovernanceContext(_Model):
    tenant_id: str
    agent_name: str
    binding_id: str
    principal_digest: str
    session_id: str
    execution_id: str
    policy_version: int
    reservation_id: str | None = None
    fencing_generation: int | None = None
    trace_id: str | None = None


class GovernanceAdmission(_Model):
    decision: PolicyDecision
    context: GovernanceContext | None = None


class GovernanceRecoveryMarker(_Model):
    marker_id: str
    tenant_id: str
    execution_id: str
    stage: RecoveryStage
    generation: int = Field(ge=0)


class BudgetReservationSet(_Model):
    tenant_id: str
    execution_id: str
    reservation: BudgetReservation


class BudgetSettlement(_Model):
    tenant_id: str
    execution_id: str
    actual: UsageVector
    reservation: BudgetReservation


class ConfirmationIntent(_Model):
    tenant_id: str
    channel: str
    binding_id: str
    principal_digest: str
    session_id: str
    confirmation_id: str
    token_digest: str


class ConfirmationClaim(_Model):
    confirmation: PendingConfirmation
    claim_token: str
    cached_result_digest: str | None = None


ToolDescriptor.model_rebuild()
