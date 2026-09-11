"""Strict request/response schemas for the Admin API."""

from __future__ import annotations

from datetime import date, datetime
from uuid import UUID

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import StrictBool
from pydantic import StrictInt
from pydantic import field_validator

from trpc_service.audit.models import ExecutionAuditEvent
from trpc_service.audit.query import UnifiedAuditRecord
from trpc_service.config.tenant import AgentAppConfig
from trpc_service.governance.approval import OrphanedApproval
from trpc_service.config.tenant import TenantBackendProfile
from trpc_service.config.tenant import TenantAuditPolicy
from trpc_service.config.tenant import TenantConfig
from trpc_service.config.tenant import TenantConfigDraft
from trpc_service.config.tenant import TenantGovernanceConfig
from trpc_service.channels.binding import ChannelBinding
from trpc_service.config.rollout import TenantConfigRollout
from trpc_service.storage.message_repository import MessageAuditEvent
from trpc_service.tenant.context import InvalidTenantIdError
from trpc_service.tenant.context import validate_tenant_id as _validate_tenant_id


class TenantCreateRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    tenant_id: str
    enabled: StrictBool
    app: AgentAppConfig
    governance: TenantGovernanceConfig
    backend_profile: TenantBackendProfile
    audit_policy: TenantAuditPolicy

    @field_validator("tenant_id")
    @classmethod
    def _validate_tenant_id(cls, v: str) -> str:
        try:
            _validate_tenant_id(v)
        except InvalidTenantIdError:
            raise ValueError("invalid tenant ID format") from None
        return v

    def to_draft(self) -> TenantConfigDraft:
        return TenantConfigDraft(
            enabled=self.enabled,
            app=self.app,
            governance=self.governance,
            backend_profile=self.backend_profile,
            audit_policy=self.audit_policy,
        )


class TenantUpdateRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    expected_version: StrictInt = Field(ge=1)
    desired: TenantConfigDraft


class TenantRollbackRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    expected_version: StrictInt = Field(ge=1)
    target_version: StrictInt = Field(ge=1)


class TenantRolloutBeginRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    expected_active_version: StrictInt = Field(ge=1)
    desired: TenantConfigDraft
    candidate_percent: StrictInt = Field(ge=1, le=99)


class TenantRolloutMutationRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    expected_candidate_version: StrictInt = Field(ge=1)


class TenantRolloutResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    tenant_id: str
    active_version: int
    candidate_version: int
    candidate_percent: int
    started_at: datetime
    status: str
    counts: dict[str, int] = Field(default_factory=dict)

    @classmethod
    def from_rollout(cls, rollout: TenantConfigRollout, counts: dict[str, int] | None = None):
        return cls(tenant_id=rollout.tenant_id,
                   active_version=rollout.active_version,
                   candidate_version=rollout.candidate_version,
                   candidate_percent=rollout.candidate_percent,
                   started_at=rollout.started_at,
                   status=rollout.status,
                   counts=counts or {})


class TenantConfigResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    tenant_id: str
    enabled: StrictBool
    version: StrictInt
    app: AgentAppConfig
    governance: TenantGovernanceConfig
    backend_profile: TenantBackendProfile
    audit_policy: TenantAuditPolicy

    @classmethod
    def from_config(cls, config: TenantConfig) -> "TenantConfigResponse":
        return cls(
            tenant_id=config.tenant_id,
            enabled=config.enabled,
            version=config.version,
            app=config.app,
            governance=config.governance,
            backend_profile=config.backend_profile,
            audit_policy=config.audit_policy,
        )


class TenantVersionsResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    versions: list[TenantConfigResponse]


class ChannelBindingCreateRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    binding: ChannelBinding


class ChannelBindingUpdateRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    expected_version: StrictInt = Field(ge=1)
    desired: ChannelBinding


class ChannelBindingRollbackRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    expected_version: StrictInt = Field(ge=1)
    target_version: StrictInt = Field(ge=1)


class ChannelBindingListResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    bindings: list[ChannelBinding]


class MessageAuditEventResponse(BaseModel):
    """Immutable audit event response — no raw text or credentials."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    audit_id: UUID
    receipt_id: UUID
    tenant_id: str
    app_id: str
    channel: str
    user_id: str
    session_id: str
    message_id: str
    event_type: str
    request_id: UUID
    config_version: int
    error_code: str | None
    latency_ms: int | None
    message_digest: str
    response_digest: str | None
    occurred_at: datetime

    @classmethod
    def from_audit_event(cls, event: MessageAuditEvent) -> "MessageAuditEventResponse":
        return cls(
            audit_id=event.audit_id,
            receipt_id=event.receipt_id,
            tenant_id=event.tenant_id,
            app_id=event.app_id,
            channel=event.channel,
            user_id=event.user_id,
            session_id=event.session_id,
            message_id=event.message_id,
            event_type=event.event_type,
            request_id=event.request_id,
            config_version=event.config_version,
            error_code=event.error_code,
            latency_ms=event.latency_ms,
            message_digest=event.message_digest,
            response_digest=event.response_digest,
            occurred_at=event.occurred_at,
        )


class MessageAuditListResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    events: list[MessageAuditEventResponse]


class ExecutionAuditEventResponse(BaseModel):
    """One append-only governance fact — fixed fields, no free text ever.

    The field set mirrors ``ExecutionAuditEvent``: event/outcome/category
    enums, correlation ids, trace id, latency.  There is no column that
    could carry body, reply, tool args, or exception detail.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    audit_id: UUID
    tenant_id: str
    receipt_id: UUID | None
    request_id: UUID
    config_version: int
    trace_id: str | None
    event_type: str
    outcome: str
    category: str | None
    tool_name: str | None
    error_code: str | None
    latency_ms: int | None
    occurred_at: datetime

    @classmethod
    def from_event(cls, event: ExecutionAuditEvent) -> "ExecutionAuditEventResponse":
        return cls(
            audit_id=event.audit_id,
            tenant_id=event.tenant_id,
            receipt_id=event.receipt_id,
            request_id=event.request_id,
            config_version=event.config_version,
            trace_id=event.trace_id,
            event_type=event.event_type,
            outcome=event.outcome,
            category=event.category,
            tool_name=event.tool_name,
            error_code=event.error_code,
            latency_ms=event.latency_ms,
            occurred_at=event.occurred_at,
        )


class ExecutionAuditListResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    events: list[ExecutionAuditEventResponse]


class UnifiedAuditRecordResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    tenant_id: str
    channel: str | None
    user_id: str | None
    session_id: str | None
    agent_name: str | None
    tool_name: str | None
    decision: str
    latency_ms: int | None
    error_type: str | None
    error_code: str | None
    cost_microunits: int | None
    trace_id: str | None
    request_id: UUID
    config_version: int
    occurred_at: datetime

    @classmethod
    def from_record(cls, record: UnifiedAuditRecord) -> "UnifiedAuditRecordResponse":
        return cls(**record.model_dump())


class UnifiedAuditListResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    events: list[UnifiedAuditRecordResponse]


class UsageProfileResponse(BaseModel):
    """One (day, tenant, model_profile) usage row.

    None token/cost values mean UNKNOWN (never a fabricated zero);
    ``cost_state`` makes that explicit for clients.
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    model_profile: str
    requests: int
    input_tokens: int | None
    output_tokens: int | None
    cost_microunits: int | None
    cost_state: str


class TenantUsageResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    usage_date: date
    tenant_id: str
    profiles: list[UsageProfileResponse]


class OrphanedApprovalResponse(BaseModel):
    """One stale-executing candidate (Stage 6D) — opaque metadata only:
    args digest, never tool args, never response text."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    approval_id: UUID
    state: str
    decision: str
    tool_name: str
    args_digest: str
    function_call_id: str
    session_id: str
    decided_at: datetime
    age_seconds: int

    @classmethod
    def from_orphan(cls, orphan: OrphanedApproval) -> "OrphanedApprovalResponse":
        return cls(
            approval_id=orphan.approval_id,
            state=orphan.state,
            decision=orphan.decision,
            tool_name=orphan.tool_name,
            args_digest=orphan.args_digest,
            function_call_id=orphan.function_call_id,
            session_id=orphan.session_id,
            decided_at=orphan.decided_at,
            age_seconds=orphan.age_seconds,
        )


class OrphanedApprovalListResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    approvals: list[OrphanedApprovalResponse]


class ApprovalTerminationResponse(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    approval_id: UUID
    disposition: str
    state: str


__all__ = [
    "TenantConfigResponse",
    "TenantCreateRequest",
    "TenantRollbackRequest",
    "TenantRolloutBeginRequest",
    "TenantRolloutMutationRequest",
    "TenantRolloutResponse",
    "TenantUpdateRequest",
    "TenantVersionsResponse",
    "MessageAuditEventResponse",
    "MessageAuditListResponse",
    "ExecutionAuditEventResponse",
    "ExecutionAuditListResponse",
    "UnifiedAuditRecordResponse",
    "UnifiedAuditListResponse",
    "UsageProfileResponse",
    "TenantUsageResponse",
    "OrphanedApprovalResponse",
    "OrphanedApprovalListResponse",
    "ApprovalTerminationResponse",
]
