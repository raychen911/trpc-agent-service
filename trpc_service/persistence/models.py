"""SQLAlchemy schema for the phase-two control and reliability planes."""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from decimal import Decimal

from sqlalchemy import Boolean
from sqlalchemy import CheckConstraint
from sqlalchemy import DateTime
from sqlalchemy import Float
from sqlalchemy import Integer
from sqlalchemy import Numeric
from sqlalchemy import JSON
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import UniqueConstraint
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class PendingActionRow(Base):
    __tablename__ = "pending_actions"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    action_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    execution_id: Mapped[str] = mapped_column(String(64), nullable=False)
    tool_call_id: Mapped[str] = mapped_column(String(128), nullable=False)
    binding_id: Mapped[str] = mapped_column(String(64), nullable=False)
    session_id: Mapped[str] = mapped_column(String(128), nullable=False)
    requester_actor_id: Mapped[str] = mapped_column(String(128), nullable=False)
    requester_epoch: Mapped[int] = mapped_column(Integer, nullable=False)
    approvers_json: Mapped[list] = mapped_column(JSON, nullable=False)
    approver_actor_id: Mapped[str | None] = mapped_column(String(128))
    approver_epoch: Mapped[int | None] = mapped_column(Integer)
    tool_name: Mapped[str] = mapped_column(String(128), nullable=False)
    tool_revision: Mapped[str] = mapped_column(String(64), nullable=False)
    config_version: Mapped[int] = mapped_column(Integer, nullable=False)
    policy_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    arguments_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    encrypted_payload: Mapped[str] = mapped_column(Text, nullable=False)
    safe_summary: Mapped[dict] = mapped_column(JSON, nullable=False)
    conversation_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    nonce_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    revision: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, index=True)
    result_ref: Mapped[str | None] = mapped_column(String(255))
    __table_args__ = (UniqueConstraint("tenant_id", "execution_id"),)


class ActionJobRow(Base):
    __tablename__ = "action_jobs"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    action_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    owner_id: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_type: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class ActionNoticeRow(Base):
    __tablename__ = "action_notices"
    notice_id: Mapped[str] = mapped_column(String(32), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    action_id: Mapped[str] = mapped_column(String(32), nullable=False)
    binding_id: Mapped[str] = mapped_column(String(64), nullable=False)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    encrypted_payload: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    attempt: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    owner_generation: Mapped[int | None] = mapped_column(Integer)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    receipt_json: Mapped[dict | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    __table_args__ = (UniqueConstraint("tenant_id", "action_id", "kind"),)


class ServiceSchemaRow(Base):
    __tablename__ = "service_schema"
    component: Mapped[str] = mapped_column(String(64), primary_key=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)


class ModelPriceRow(Base):
    __tablename__ = "model_prices"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    price_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    model_id: Mapped[str] = mapped_column(String(128), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    input_per_million: Mapped[Decimal] = mapped_column(Numeric(24, 10), nullable=False)
    output_per_million: Mapped[Decimal] = mapped_column(Numeric(24, 10), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class BudgetPolicyRow(Base):
    __tablename__ = "budget_policies"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    currency: Mapped[str] = mapped_column(String(3), primary_key=True)
    timezone: Mapped[str] = mapped_column(String(64), nullable=False)
    daily_limit: Mapped[Decimal] = mapped_column(Numeric(24, 10), nullable=False)
    monthly_limit: Mapped[Decimal] = mapped_column(Numeric(24, 10), nullable=False)
    revision: Mapped[int] = mapped_column(Integer, nullable=False)
    blocked: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)


class BudgetAccountRow(Base):
    __tablename__ = "budget_accounts"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    period: Mapped[str] = mapped_column(String(128), primary_key=True)
    currency: Mapped[str] = mapped_column(String(3), primary_key=True)
    limit_amount: Mapped[Decimal] = mapped_column(Numeric(24, 10), nullable=False)
    reserved: Mapped[Decimal] = mapped_column(Numeric(24, 10), default=Decimal(0), nullable=False)
    settled: Mapped[Decimal] = mapped_column(Numeric(24, 10), default=Decimal(0), nullable=False)
    __table_args__ = (
        CheckConstraint("reserved >= 0 AND settled >= 0 AND limit_amount >= 0", name="budget_nonnegative_amounts"),
    )


class ModelAttemptRow(Base):
    __tablename__ = "model_attempts"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    attempt_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    execution_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    price_id: Mapped[str] = mapped_column(String(64), nullable=False)
    model_id: Mapped[str] = mapped_column(String(128), nullable=False)
    currency: Mapped[str] = mapped_column(String(3), nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    reservation_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    periods_json: Mapped[list] = mapped_column(JSON, nullable=False)
    reserved_amount: Mapped[Decimal] = mapped_column(Numeric(24, 10), nullable=False)
    actual_amount: Mapped[Decimal | None] = mapped_column(Numeric(24, 10))
    max_input_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    max_output_tokens: Mapped[int] = mapped_column(Integer, nullable=False)
    input_tokens: Mapped[int | None] = mapped_column(Integer)
    output_tokens: Mapped[int | None] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    evidence_hash: Mapped[str | None] = mapped_column(String(64))
    overrun: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    sent_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class IMAccountRow(Base):
    """Permanent account ownership survives disabled or replaced configuration snapshots."""

    __tablename__ = "im_accounts"
    channel: Mapped[str] = mapped_column(String(32), primary_key=True)
    external_account_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    binding_id: Mapped[str] = mapped_column(String(64), nullable=False)
    owner_id: Mapped[str | None] = mapped_column(String(128))
    generation: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    poll_offset: Mapped[int | None] = mapped_column(Integer)
    __table_args__ = (UniqueConstraint("tenant_id", "binding_id"), )


class IMMembershipRow(Base):
    __tablename__ = "im_memberships"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    binding_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    actor_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    active: Mapped[bool] = mapped_column(Boolean, nullable=False)
    authz_epoch: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    grants_json: Mapped[list] = mapped_column(JSON, default=list, nullable=False)


class IMEventReceiptRow(Base):
    __tablename__ = "im_event_receipts"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    binding_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    external_event_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    event_kind: Mapped[str] = mapped_column(String(32), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    disposition: Mapped[str] = mapped_column(String(32), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(64))
    actor_id: Mapped[str | None] = mapped_column(String(128))
    authz_epoch: Mapped[int | None] = mapped_column(Integer)
    inbound_id: Mapped[str | None] = mapped_column(String(64), index=True)
    owner_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    context_ciphertext: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class IMDeliveryAttemptRow(Base):
    __tablename__ = "im_delivery_attempts"
    attempt_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    outbox_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    part_no: Mapped[int] = mapped_column(Integer, nullable=False)
    owner_generation: Mapped[int] = mapped_column(Integer, nullable=False)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False)
    receipt_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    network_started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class SimulatedDeliveryRow(Base):
    """Receiver-side durable deduplication, separate from sender confirmation."""

    __tablename__ = "simulated_deliveries"
    delivery_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    inbound_message_id: Mapped[str] = mapped_column(String(64), nullable=False)
    part_no: Mapped[int] = mapped_column(Integer, nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, onupdate=utcnow, nullable=False)


class TenantRow(Base, TimestampMixin):
    __tablename__ = "tenants"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    active_config_version: Mapped[int] = mapped_column(Integer, nullable=False)


class TenantRolloutRow(Base):
    __tablename__ = "tenant_rollouts"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    base_version: Mapped[int] = mapped_column(Integer, nullable=False)
    target_version: Mapped[int] = mapped_column(Integer, nullable=False)
    percent: Mapped[int] = mapped_column(Integer, nullable=False)


class TenantConfigVersionRow(Base):
    __tablename__ = "tenant_config_versions"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    config_version: Mapped[int] = mapped_column(Integer, primary_key=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    config_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_by: Mapped[str] = mapped_column(String(255), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class BackendProfileRow(Base, TimestampMixin):
    __tablename__ = "backend_profiles"
    profile_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    profile_revision: Mapped[int] = mapped_column(Integer, primary_key=True)
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    endpoint: Mapped[str] = mapped_column(String(2048), nullable=False)
    secret_ref: Mapped[str] = mapped_column(String(1024), nullable=False)
    options_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    capabilities_json: Mapped[list] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)


class TenantBackendBindingRow(Base):
    __tablename__ = "tenant_backend_bindings"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    config_version: Mapped[int] = mapped_column(Integer, primary_key=True)
    resource_type: Mapped[str] = mapped_column(String(32), primary_key=True)
    profile_id: Mapped[str] = mapped_column(String(64), nullable=False)
    profile_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    storage_revision: Mapped[int] = mapped_column(Integer, nullable=False)


class AgentAppRow(Base):
    __tablename__ = "agent_apps"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    app_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    config_version: Mapped[int] = mapped_column(Integer, primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)
    config_json: Mapped[dict] = mapped_column(JSON, nullable=False)


class ChannelBindingRow(Base):
    __tablename__ = "channel_bindings"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    binding_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    webhook_public_id: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)
    external_account_id: Mapped[str] = mapped_column(String(255), nullable=False)
    agent_app_id: Mapped[str] = mapped_column(String(64), nullable=False)
    credential_ref: Mapped[str] = mapped_column(String(1024), nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)


class SessionRow(Base, TimestampMixin):
    __tablename__ = "sessions"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    app_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    next_event_seq: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    state_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    last_fencing_token: Mapped[int] = mapped_column(Integer, default=0, nullable=False)


class SessionEventRow(Base):
    __tablename__ = "session_events"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    app_id: Mapped[str] = mapped_column(String(64), nullable=False)
    user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    seq_no: Mapped[int] = mapped_column(Integer, nullable=False)
    execution_id: Mapped[str] = mapped_column(String(64), nullable=False)
    invocation_id: Mapped[str | None] = mapped_column(String(128))
    author: Mapped[str | None] = mapped_column(String(128))
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    content_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    state_delta_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    __table_args__ = (UniqueConstraint("tenant_id", "session_id", "seq_no"), )


class SessionSummaryRow(Base):
    __tablename__ = "session_summaries"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    summary_id: Mapped[str] = mapped_column(String(128), nullable=False)
    covered_event_seq: Mapped[int] = mapped_column(Integer, nullable=False)
    summary_text: Mapped[str] = mapped_column(Text, nullable=False)
    model_version: Mapped[str] = mapped_column(String(255), nullable=False)
    replaces_version: Mapped[int | None] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class MemoryRow(Base, TimestampMixin):
    __tablename__ = "memories"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    user_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    memory_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    source_event_id: Mapped[str] = mapped_column(String(128), nullable=False)
    memory_type: Mapped[str] = mapped_column(String(64), nullable=False)
    content_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    embedding_ref: Mapped[str | None] = mapped_column(String(512))
    provider_version: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    __table_args__ = (UniqueConstraint("tenant_id", "source_event_id", "memory_type"), )


class ArtifactSequenceRow(Base):
    __tablename__ = "artifact_sequences"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    artifact_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    next_version: Mapped[int] = mapped_column(Integer, nullable=False)


class ArtifactMetadataRow(Base, TimestampMixin):
    __tablename__ = "artifact_metadata"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    artifact_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    version: Mapped[int] = mapped_column(Integer, primary_key=True)
    session_id: Mapped[str | None] = mapped_column(String(128))
    user_id: Mapped[str] = mapped_column(String(128), nullable=False)
    filename: Mapped[str] = mapped_column(String(255), nullable=False)
    object_uri: Mapped[str] = mapped_column(String(2048), nullable=False)
    mime_type: Mapped[str | None] = mapped_column(String(255))
    size: Mapped[int] = mapped_column(Integer, nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)


class KnowledgeBaseRow(Base):
    __tablename__ = "knowledge_bases"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    knowledge_base_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    active_index_version: Mapped[int | None] = mapped_column(Integer)


class KnowledgeDocumentRow(Base):
    __tablename__ = "knowledge_documents"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    knowledge_base_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    document_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    object_uri: Mapped[str] = mapped_column(String(2048), nullable=False)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)


class KnowledgeIndexVersionRow(Base):
    __tablename__ = "knowledge_index_versions"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    knowledge_base_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    index_version: Mapped[int] = mapped_column(Integer, primary_key=True)
    embedding_model: Mapped[str] = mapped_column(String(255), nullable=False)
    dimensions: Mapped[int] = mapped_column(Integer, nullable=False)
    vector_profile_id: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)


class AuditLogRow(Base):
    __tablename__ = "audit_logs"
    audit_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    channel: Mapped[str | None] = mapped_column(String(32))
    user_id: Mapped[str | None] = mapped_column(String(128))
    session_id: Mapped[str | None] = mapped_column(String(128))
    agent_name: Mapped[str | None] = mapped_column(String(128))
    tool_name: Mapped[str | None] = mapped_column(String(128))
    decision: Mapped[str] = mapped_column(String(64), nullable=False)
    latency_ms: Mapped[float | None] = mapped_column(Float)
    error_type: Mapped[str | None] = mapped_column(String(128))
    cost: Mapped[float | None] = mapped_column(Float)
    trace_id: Mapped[str | None] = mapped_column(String(64))
    request_id: Mapped[str | None] = mapped_column(String(128))
    config_version: Mapped[int | None] = mapped_column(Integer)
    policy_version: Mapped[int | None] = mapped_column(Integer)
    redacted: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    metadata_json: Mapped[dict] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)


class InboundMessageRow(Base, TimestampMixin):
    __tablename__ = "inbound_messages"
    inbound_message_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    channel_binding_id: Mapped[str] = mapped_column(String(64), nullable=False)
    external_message_id: Mapped[str] = mapped_column(String(255), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    request_id: Mapped[str] = mapped_column(String(128), nullable=False)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False)
    execution_id: Mapped[str] = mapped_column(String(64), nullable=False)
    partition_key: Mapped[str] = mapped_column(String(255), nullable=False)
    config_version: Mapped[int] = mapped_column(Integer, nullable=False)
    storage_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_type: Mapped[str | None] = mapped_column(String(128))
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    __table_args__ = (UniqueConstraint("tenant_id", "channel_binding_id", "external_message_id"), )


class ExecutionAttemptRow(Base):
    __tablename__ = "execution_attempts"
    attempt_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    execution_id: Mapped[str] = mapped_column(String(64), index=True, nullable=False)
    worker_id: Mapped[str] = mapped_column(String(128), nullable=False)
    config_version: Mapped[int] = mapped_column(Integer, nullable=False)
    storage_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow, nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_type: Mapped[str | None] = mapped_column(String(128))


class OutboxMessageRow(Base, TimestampMixin):
    __tablename__ = "outbox_messages"
    outbox_message_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    inbound_message_id: Mapped[str] = mapped_column(String(64), nullable=False)
    part_no: Mapped[int] = mapped_column(Integer, nullable=False)
    payload_json: Mapped[dict] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivery_id: Mapped[str | None] = mapped_column(String(255))
    error_type: Mapped[str | None] = mapped_column(String(128))
    __table_args__ = (UniqueConstraint("tenant_id", "inbound_message_id", "part_no"), )


class ToolResultRow(Base):
    __tablename__ = "tool_results"
    execution_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tool_call_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    result_json: Mapped[dict] = mapped_column(JSON, nullable=False)


class ToolInvocationRow(Base, TimestampMixin):
    __tablename__ = "tool_invocations"
    execution_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tool_call_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    tool_name: Mapped[str] = mapped_column(String(128), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    request_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    result_hash: Mapped[str | None] = mapped_column(String(64))
    external_operation_id: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    error_type: Mapped[str | None] = mapped_column(String(128))


class PostTurnTaskRow(Base, TimestampMixin):
    __tablename__ = "post_turn_tasks"
    task_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    session_id: Mapped[str] = mapped_column(String(128), nullable=False)
    task_type: Mapped[str] = mapped_column(String(32), nullable=False)
    source_event_seq: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_retry_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_type: Mapped[str | None] = mapped_column(String(128))
    __table_args__ = (UniqueConstraint("tenant_id", "session_id", "task_type", "source_event_seq"), )


class MigrationJobRow(Base, TimestampMixin):
    __tablename__ = "migration_jobs"
    migration_job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    resource_type: Mapped[str] = mapped_column(String(32), nullable=False)
    source_profile_id: Mapped[str] = mapped_column(String(64), nullable=False)
    target_profile_id: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    source_watermark: Mapped[int | None] = mapped_column(Integer)
    cutover_watermark: Mapped[int | None] = mapped_column(Integer)
    config_version: Mapped[int] = mapped_column(Integer, nullable=False)
    storage_revision: Mapped[int] = mapped_column(Integer, nullable=False)
    rollback_deadline: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_type: Mapped[str | None] = mapped_column(String(128))


class TenantMaintenanceRow(Base, TimestampMixin):
    __tablename__ = "tenant_maintenance"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False)


class OfflineMigrationRow(Base, TimestampMixin):
    __tablename__ = "offline_migrations"
    job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    resource_type: Mapped[str] = mapped_column(String(32), default="session", nullable=False)
    source_version: Mapped[int] = mapped_column(Integer, nullable=False)
    target_version: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    items_json: Mapped[list] = mapped_column(JSON, nullable=False)
    copied: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_type: Mapped[str | None] = mapped_column(String(128))


class MigrationCheckpointRow(Base, TimestampMixin):
    __tablename__ = "migration_checkpoints"
    migration_job_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    partition_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    last_source_key: Mapped[str | None] = mapped_column(String(512))
    last_source_revision: Mapped[int | None] = mapped_column(Integer)
    copied_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    verified_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_type: Mapped[str | None] = mapped_column(String(128))


class SessionLeaseRow(Base):
    __tablename__ = "session_leases"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    owner_id: Mapped[str] = mapped_column(String(128), nullable=False)
    fencing_token: Mapped[int] = mapped_column(Integer, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
