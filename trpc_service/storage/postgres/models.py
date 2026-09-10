"""SQLAlchemy schema for authoritative configuration, audit and recovery."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SchemaMigrationRow(Base):
    __tablename__ = "schema_migrations"
    __table_args__ = (CheckConstraint("version > 0"),)

    version: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120))
    checksum: Mapped[str] = mapped_column(String(64))
    applied_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TenantRow(Base):
    __tablename__ = "tenants"
    __table_args__ = (
        CheckConstraint("config_version > 0"),
        CheckConstraint("status IN ('active', 'disabled')"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(120))
    status: Mapped[str] = mapped_column(String(16))
    config_version: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class AgentApplicationRow(Base):
    __tablename__ = "agent_applications"
    __table_args__ = (
        CheckConstraint("config_version > 0"),
        CheckConstraint("status IN ('active', 'disabled')"),
    )

    tenant_id: Mapped[str] = mapped_column(
        String(64), ForeignKey("tenants.tenant_id"), primary_key=True
    )
    agent_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    agent_name: Mapped[str] = mapped_column(String(120))
    status: Mapped[str] = mapped_column(String(16))
    model_profile: Mapped[str] = mapped_column(String(64))
    instruction: Mapped[str] = mapped_column(Text)
    config_version: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ChannelBindingRow(Base):
    __tablename__ = "channel_bindings"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_id"],
            ["agent_applications.tenant_id", "agent_applications.agent_id"],
        ),
        CheckConstraint("config_version > 0"),
        CheckConstraint("status IN ('active', 'disabled')"),
    )

    binding_id: Mapped[str] = mapped_column(String(96), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64))
    agent_id: Mapped[str] = mapped_column(String(64))
    channel: Mapped[str] = mapped_column(String(32))
    status: Mapped[str] = mapped_column(String(16))
    secret_ref: Mapped[str] = mapped_column(String(128))
    signature_version: Mapped[str] = mapped_column(String(16))
    config_version: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    provider_tenant_key: Mapped[str | None] = mapped_column(String(128))
    provider_app_or_bot_id: Mapped[str | None] = mapped_column(String(128))
    channel_identity_digest: Mapped[str | None] = mapped_column(String(64))


class PersistentAuditRecordRow(Base):
    __tablename__ = "persistent_audit_records"
    __table_args__ = (
        CheckConstraint("latency_ms >= 0"),
        CheckConstraint("cost >= 0"),
        Index("ix_audit_tenant_trace", "tenant_id", "trace_id", "created_at"),
        Index("ix_audit_tenant_session", "tenant_id", "platform_session_id", "created_at"),
        Index("ix_audit_tenant_agent", "tenant_id", "agent_id", "created_at"),
        Index("ix_audit_tenant_message", "tenant_id", "external_message_digest", "created_at"),
        UniqueConstraint(
            "tenant_id",
            "external_message_digest",
            "message_generation",
            "decision",
            "audit_kind",
            name="uq_business_terminal_generation",
        ),
    )

    audit_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    audit_kind: Mapped[str] = mapped_column(String(16))
    decision: Mapped[str] = mapped_column(String(40))
    trace_id: Mapped[str] = mapped_column(String(36))
    first_claim_trace_id: Mapped[str | None] = mapped_column(String(36))
    owner_trace_id: Mapped[str | None] = mapped_column(String(36))
    execution_trace_id: Mapped[str | None] = mapped_column(String(36))
    tenant_id: Mapped[str | None] = mapped_column(String(64))
    agent_id: Mapped[str | None] = mapped_column(String(64))
    channel: Mapped[str] = mapped_column(String(32), default="local_http")
    node_id: Mapped[str] = mapped_column(String(64))
    process_instance_id: Mapped[str] = mapped_column(String(36))
    binding_id_digest: Mapped[str] = mapped_column(String(71))
    external_message_digest: Mapped[str | None] = mapped_column(String(71))
    platform_session_id: Mapped[str | None] = mapped_column(String(69))
    message_generation: Mapped[int | None] = mapped_column(BigInteger)
    session_generation: Mapped[int | None] = mapped_column(BigInteger)
    rejected_generation: Mapped[int | None] = mapped_column(BigInteger)
    current_generation: Mapped[int | None] = mapped_column(BigInteger)
    error_type: Mapped[str | None] = mapped_column(String(64))
    result_digest: Mapped[str | None] = mapped_column(String(64))
    latency_ms: Mapped[Decimal] = mapped_column(Numeric)
    cost: Mapped[Decimal] = mapped_column(Numeric)
    recovery_status: Mapped[str | None] = mapped_column(String(32))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    adapter_node_id: Mapped[str | None] = mapped_column(String(64))
    adapter_generation: Mapped[int | None] = mapped_column(BigInteger)
    channel_identity_digest: Mapped[str | None] = mapped_column(String(64))
    provider_message_digest: Mapped[str | None] = mapped_column(String(71))
    delivery_id: Mapped[str | None] = mapped_column(String(36))
    delivery_attempt_no: Mapped[int | None] = mapped_column(BigInteger)
    delivery_status: Mapped[str | None] = mapped_column(String(32))


class RecoveryMarkerRow(Base):
    __tablename__ = "recovery_markers"
    __table_args__ = (
        CheckConstraint("message_generation > 0"),
        CheckConstraint("session_generation > 0"),
        CheckConstraint("replay_allowed = false"),
        UniqueConstraint(
            "tenant_id",
            "binding_id_digest",
            "external_message_digest",
            "execution_trace_id",
            name="uq_recovery_identity",
        ),
    )

    recovery_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64))
    binding_id_digest: Mapped[str] = mapped_column(String(71))
    external_message_digest: Mapped[str] = mapped_column(String(71))
    platform_session_id: Mapped[str] = mapped_column(String(69))
    idempotency_key_digest: Mapped[str] = mapped_column(String(64))
    message_generation: Mapped[int] = mapped_column(BigInteger)
    session_generation: Mapped[int] = mapped_column(BigInteger)
    execution_trace_id: Mapped[str] = mapped_column(String(36))
    state: Mapped[str] = mapped_column(String(32))
    result_status: Mapped[str] = mapped_column(String(32))
    result_payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    result_digest: Mapped[str] = mapped_column(String(64))
    replay_allowed: Mapped[bool] = mapped_column(Boolean, default=False)
    failure_stage: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    reconciled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class GovernancePolicyVersionRow(Base):
    __tablename__ = "governance_policy_versions"
    __table_args__ = (
        CheckConstraint("version > 0"),
        UniqueConstraint("tenant_id", "scope_type", "scope_id", "version"),
    )

    policy_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    scope_type: Mapped[str] = mapped_column(String(16))
    scope_id: Mapped[str] = mapped_column(String(128))
    version: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(16))
    policy_document: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_by_digest: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    disabled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class GovernancePolicyActiveRow(Base):
    __tablename__ = "governance_policy_active"
    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    scope_type: Mapped[str] = mapped_column(String(16), primary_key=True)
    scope_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    policy_id: Mapped[str] = mapped_column(String(36))
    version: Mapped[int] = mapped_column(BigInteger)
    activation_generation: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class PrincipalGrantRow(Base):
    __tablename__ = "principal_grants"
    __table_args__ = (Index("ix_principal_grants_lookup", "tenant_id", "channel", "binding_id", "provider_subject_digest"),)

    grant_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64))
    channel: Mapped[str] = mapped_column(String(32))
    binding_id: Mapped[str] = mapped_column(String(96))
    provider_subject_digest: Mapped[str] = mapped_column(String(64))
    agent_name: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(16))
    permissions: Mapped[dict[str, Any]] = mapped_column(JSON)
    valid_from: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class BudgetAccountRow(Base):
    __tablename__ = "budget_accounts"
    __table_args__ = (
        CheckConstraint("hard_limit >= 0"),
        CheckConstraint("reserved_amount >= 0"),
        CheckConstraint("settled_amount >= 0"),
        CheckConstraint("reserved_amount + settled_amount <= hard_limit"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    policy_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    period_key: Mapped[str] = mapped_column(String(64), primary_key=True)
    dimension: Mapped[str] = mapped_column(String(32), primary_key=True)
    hard_limit: Mapped[Decimal] = mapped_column(Numeric)
    reserved_amount: Mapped[Decimal] = mapped_column(Numeric, default=Decimal("0"))
    settled_amount: Mapped[Decimal] = mapped_column(Numeric, default=Decimal("0"))


class BudgetReservationRow(Base):
    __tablename__ = "budget_reservations"
    __table_args__ = (
        CheckConstraint("owner_generation >= 0"),
        UniqueConstraint("tenant_id", "execution_id"),
    )

    reservation_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64))
    execution_id: Mapped[str] = mapped_column(String(128))
    policy_id: Mapped[str] = mapped_column(String(36))
    state: Mapped[str] = mapped_column(String(32))
    maximum_usage: Mapped[dict[str, Any]] = mapped_column(JSON)
    actual_usage: Mapped[dict[str, Any]] = mapped_column(JSON)
    execution_started: Mapped[bool] = mapped_column(Boolean, default=False)
    owner_generation: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class GovernanceRecoveryMarkerRow(Base):
    __tablename__ = "governance_recovery_markers"
    marker_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64))
    execution_id: Mapped[str] = mapped_column(String(128))
    stage: Mapped[str] = mapped_column(String(32))
    owner_node_id: Mapped[str | None] = mapped_column(String(128))
    owner_generation: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class DeliveryRecordRow(Base):
    __tablename__ = "delivery_records"
    __table_args__ = (
        CheckConstraint(
            "status IN ('pending', 'sending', 'retry_wait', 'delivered', "
            "'delivery_failed', 'delivery_unknown')"
        ),
        CheckConstraint("adapter_generation > 0"),
        UniqueConstraint(
            "tenant_id",
            "idempotency_key_digest",
            "channel",
            "binding_id",
            name="uq_delivery_execution_scope",
        ),
        Index("ix_delivery_due", "tenant_id", "status", "next_attempt_at"),
        Index("ix_delivery_execution_trace", "tenant_id", "execution_trace_id"),
    )

    delivery_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), ForeignKey("tenants.tenant_id"))
    binding_id: Mapped[str] = mapped_column(
        String(96), ForeignKey("channel_bindings.binding_id")
    )
    channel: Mapped[str] = mapped_column(String(32))
    idempotency_key_digest: Mapped[str] = mapped_column(String(64))
    execution_trace_id: Mapped[str] = mapped_column(String(36))
    reply_context: Mapped[dict[str, Any]] = mapped_column(JSON)
    result_digest: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32))
    adapter_generation: Mapped[int] = mapped_column(BigInteger)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class DeliveryAttemptRow(Base):
    __tablename__ = "delivery_attempts"
    __table_args__ = (
        CheckConstraint("attempt_no BETWEEN 1 AND 4"),
        CheckConstraint("adapter_generation > 0"),
        CheckConstraint("retry_delay_seconds IS NULL OR retry_delay_seconds IN (1, 2, 4)"),
        UniqueConstraint(
            "delivery_id", "attempt_no", name="uq_delivery_attempt_number"
        ),
    )

    attempt_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    delivery_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("delivery_records.delivery_id")
    )
    attempt_no: Mapped[int] = mapped_column(BigInteger)
    trace_id: Mapped[str] = mapped_column(String(36))
    adapter_node_id: Mapped[str] = mapped_column(String(64))
    adapter_generation: Mapped[int] = mapped_column(BigInteger)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str | None] = mapped_column(String(32))
    safe_error_code: Mapped[str | None] = mapped_column(String(64))
    retry_delay_seconds: Mapped[int | None] = mapped_column(BigInteger)


# Phase-seven durable data model.  These tables are deliberately independent
# of vendor-specific Redis/vector/object-store schemas; each business key is
# tenant-scoped and every mutable projection carries a version/watermark.
class SessionStreamRow(Base):
    __tablename__ = "session_streams"
    __table_args__ = (
        CheckConstraint("watermark >= 0"),
        CheckConstraint("generation >= 0"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_key: Mapped[str] = mapped_column(String(256), primary_key=True)
    watermark: Mapped[int] = mapped_column(BigInteger, default=0)
    authority: Mapped[str] = mapped_column(String(32), default="POSTGRES")
    rollback_eligible: Mapped[bool] = mapped_column(Boolean, default=True)
    generation: Mapped[int] = mapped_column(BigInteger, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SessionEventRow(Base):
    __tablename__ = "session_events"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id", "session_key"], ["session_streams.tenant_id", "session_streams.session_key"]),
        UniqueConstraint("tenant_id", "session_key", "event_id", name="uq_session_event_id"),
        UniqueConstraint("tenant_id", "session_key", "sequence", name="uq_session_event_sequence"),
        Index("ix_session_events_scope_sequence", "tenant_id", "session_key", "sequence"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_key: Mapped[str] = mapped_column(String(256), primary_key=True)
    event_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    sequence: Mapped[int] = mapped_column(BigInteger)
    event_type: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    content_digest: Mapped[str] = mapped_column(String(64))
    trace_id: Mapped[str | None] = mapped_column(String(36))
    owner_trace_id: Mapped[str | None] = mapped_column(String(36))
    execution_trace_id: Mapped[str | None] = mapped_column(String(36))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MemoryRecordRow(Base):
    __tablename__ = "memory_records"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.tenant_id"]),
        CheckConstraint("version > 0"),
        CheckConstraint("source_event_watermark IS NULL OR source_event_watermark >= 0"),
        UniqueConstraint("tenant_id", "namespace", "memory_key", name="uq_memory_scope_key"),
        Index("ix_memory_tenant_namespace", "tenant_id", "namespace"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    namespace: Mapped[str] = mapped_column(String(128), primary_key=True)
    memory_key: Mapped[str] = mapped_column(String(256), primary_key=True)
    content: Mapped[dict[str, Any]] = mapped_column(JSON)
    content_digest: Mapped[str] = mapped_column(String(64))
    byte_size: Mapped[int] = mapped_column(BigInteger)
    version: Mapped[int] = mapped_column(BigInteger)
    source_event_watermark: Mapped[int | None] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SummaryRecordRow(Base):
    __tablename__ = "summary_records"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id", "session_key"], ["session_streams.tenant_id", "session_streams.session_key"]),
        CheckConstraint("version > 0"),
        CheckConstraint("event_watermark >= 0"),
        UniqueConstraint("tenant_id", "session_key", name="uq_summary_scope_session"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_key: Mapped[str] = mapped_column(String(256), primary_key=True)
    content: Mapped[dict[str, Any]] = mapped_column(JSON)
    content_digest: Mapped[str] = mapped_column(String(64))
    event_watermark: Mapped[int] = mapped_column(BigInteger)
    version: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ArtifactMetadataRow(Base):
    __tablename__ = "artifact_metadata"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.tenant_id"]),
        CheckConstraint("byte_size >= 0"),
        CheckConstraint("version > 0"),
        UniqueConstraint("tenant_id", "artifact_id", name="uq_artifact_scope_id"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    artifact_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    storage_ref: Mapped[str] = mapped_column(String(512))
    content_digest: Mapped[str] = mapped_column(String(64))
    byte_size: Mapped[int] = mapped_column(BigInteger)
    media_type: Mapped[str] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32))
    version: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ArtifactUploadRow(Base):
    __tablename__ = "artifact_uploads"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id", "artifact_id"], ["artifact_metadata.tenant_id", "artifact_metadata.artifact_id"]),
        CheckConstraint("generation >= 0"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    upload_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    artifact_id: Mapped[str] = mapped_column(String(128))
    temp_ref_digest: Mapped[str] = mapped_column(String(64))
    expected_digest: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32))
    expected_metadata_version: Mapped[int | None] = mapped_column(BigInteger)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    generation: Mapped[int] = mapped_column(BigInteger, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class KnowledgeDocumentRow(Base):
    __tablename__ = "knowledge_documents"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.tenant_id"]),
        CheckConstraint("version > 0"),
        UniqueConstraint("tenant_id", "document_id", name="uq_knowledge_scope_id"),
        Index("ix_knowledge_tenant_status", "tenant_id", "index_status"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    document_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON)
    content_digest: Mapped[str] = mapped_column(String(64))
    embedding_ref: Mapped[str | None] = mapped_column(String(512))
    index_status: Mapped[str] = mapped_column(String(32))
    version: Mapped[int] = mapped_column(BigInteger)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MigrationStateRow(Base):
    __tablename__ = "migration_states"
    __table_args__ = (
        CheckConstraint("copied_watermark >= 0"),
        CheckConstraint("generation >= 0"),
        UniqueConstraint("tenant_id", "stream", name="uq_migration_scope_stream"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    stream: Mapped[str] = mapped_column(String(256), primary_key=True)
    state: Mapped[str] = mapped_column(String(48))
    authority: Mapped[str] = mapped_column(String(32))
    source_watermark: Mapped[int | None] = mapped_column(BigInteger)
    copied_watermark: Mapped[int] = mapped_column(BigInteger, default=0)
    source_digest: Mapped[str | None] = mapped_column(String(64))
    target_digest: Mapped[str | None] = mapped_column(String(64))
    rollback_eligible: Mapped[bool] = mapped_column(Boolean, default=True)
    generation: Mapped[int] = mapped_column(BigInteger, default=1)
    lease_owner_digest: Mapped[str | None] = mapped_column(String(64))
    failure_code: Mapped[str | None] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class DataRecoveryMarkerRow(Base):
    __tablename__ = "data_recovery_markers"
    __table_args__ = (
        CheckConstraint("generation >= 0"),
        UniqueConstraint("tenant_id", "operation", "stage", "result_digest", name="uq_data_recovery_marker"),
    )

    marker_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    operation: Mapped[str] = mapped_column(String(64))
    stage: Mapped[str] = mapped_column(String(64))
    result_digest: Mapped[str | None] = mapped_column(String(64))
    generation: Mapped[int] = mapped_column(BigInteger, default=1)
    review_reason: Mapped[str | None] = mapped_column(String(128))
    confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


# Phase-eight observability and operations schema.  Immutable relations
# (snapshots, pins, signals, transition events) expose no update business
# path; mutable projections (releases, routes) carry revision/fence CAS keys.
class ConfigurationSnapshotRow(Base):
    __tablename__ = "configuration_snapshots"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.tenant_id"]),
        CheckConstraint("sequence > 0"),
        UniqueConstraint("tenant_id", "sequence", name="uq_config_snapshot_sequence"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    snapshot_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    sequence: Mapped[int] = mapped_column(BigInteger)
    contract_version: Mapped[str] = mapped_column(String(32))
    min_runtime_contract: Mapped[str] = mapped_column(String(32))
    agent_config_ref: Mapped[str] = mapped_column(String(128))
    governance_policy_ref: Mapped[str] = mapped_column(String(128))
    data_backend_profile_ref: Mapped[str] = mapped_column(String(128))
    payload_digest: Mapped[str] = mapped_column(String(64))
    change_summary: Mapped[str] = mapped_column(String(512))
    created_by_digest: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ConfigurationReleaseRow(Base):
    __tablename__ = "configuration_releases"
    __table_args__ = (
        CheckConstraint("state IN ('draft', 'validated', 'canary', 'completed', 'paused_quality', 'paused_insufficient_sample', 'rolling_back', 'rolled_back', 'failed', 'failed_requires_repair')"),
        CheckConstraint("revision > 0"),
        CheckConstraint("observation_window_seconds > 0"),
        CheckConstraint("minimum_sample > 0"),
    )

    release_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    candidate_snapshot_id: Mapped[str] = mapped_column(String(36))
    rollback_snapshot_id: Mapped[str] = mapped_column(String(36))
    cohorts: Mapped[list[Any]] = mapped_column(JSON)
    observation_window_seconds: Mapped[int] = mapped_column(BigInteger)
    minimum_sample: Mapped[int] = mapped_column(BigInteger)
    quality_gates: Mapped[list[Any]] = mapped_column(JSON)
    hard_gate_types: Mapped[list[Any]] = mapped_column(JSON)
    state: Mapped[str] = mapped_column(String(40))
    revision: Mapped[int] = mapped_column(BigInteger)
    owner_fence_generation: Mapped[int] = mapped_column(BigInteger, default=0)
    created_by_digest: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ReleaseTargetRow(Base):
    __tablename__ = "release_targets"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.tenant_id"]),
        CheckConstraint("target_role IN ('stable', 'candidate')"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    release_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    target_role: Mapped[str] = mapped_column(String(16))
    added_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class TenantConfigRouteRow(Base):
    __tablename__ = "tenant_config_routes"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.tenant_id"]),
        CheckConstraint("route_generation > 0"),
        CheckConstraint("owner_fence_generation >= 0"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    stable_snapshot_id: Mapped[str] = mapped_column(String(36))
    candidate_snapshot_id: Mapped[str | None] = mapped_column(String(36))
    release_id: Mapped[str | None] = mapped_column(String(36))
    route_generation: Mapped[int] = mapped_column(BigInteger)
    owner_fence_generation: Mapped[int] = mapped_column(BigInteger, default=0)
    hard_gate_latched: Mapped[bool] = mapped_column(Boolean, default=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ExecutionConfigPinRow(Base):
    __tablename__ = "execution_config_pins"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenants.tenant_id"]),
        CheckConstraint("route_generation > 0"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    idempotency_key_digest: Mapped[str] = mapped_column(String(64), primary_key=True)
    content_fingerprint: Mapped[str] = mapped_column(String(64))
    snapshot_id: Mapped[str] = mapped_column(String(36))
    route_generation: Mapped[int] = mapped_column(BigInteger)
    release_id: Mapped[str | None] = mapped_column(String(36))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ReleaseGateSignalRow(Base):
    __tablename__ = "release_gate_signals"
    __table_args__ = (
        CheckConstraint("severity IN ('hard', 'quality')"),
        CheckConstraint("observation_window_seconds > 0"),
        CheckConstraint("sample_count >= 0"),
        UniqueConstraint("tenant_id", "signal_digest", name="uq_release_gate_signal_digest"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    signal_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    release_id: Mapped[str] = mapped_column(String(36))
    signal_digest: Mapped[str] = mapped_column(String(64))
    gate_type: Mapped[str] = mapped_column(String(64))
    severity: Mapped[str] = mapped_column(String(16))
    observation_window_seconds: Mapped[int] = mapped_column(BigInteger)
    sample_count: Mapped[int] = mapped_column(BigInteger, default=0)
    observed_value: Mapped[float | None] = mapped_column(Numeric)
    evidence_digest: Mapped[str | None] = mapped_column(String(64))
    observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class ReleaseTransitionEventRow(Base):
    __tablename__ = "release_transition_events"
    __table_args__ = (
        CheckConstraint("from_revision > 0"),
        CheckConstraint("to_revision > 0"),
        UniqueConstraint("release_id", "command_id", "to_revision", name="uq_release_transition_command"),
    )

    event_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    release_id: Mapped[str] = mapped_column(String(36))
    command_id: Mapped[str] = mapped_column(String(128))
    from_state: Mapped[str] = mapped_column(String(40))
    to_state: Mapped[str] = mapped_column(String(40))
    from_revision: Mapped[int] = mapped_column(BigInteger)
    to_revision: Mapped[int] = mapped_column(BigInteger)
    actor_digest: Mapped[str] = mapped_column(String(64))
    reason_code: Mapped[str] = mapped_column(String(64))
    evidence_digest: Mapped[str | None] = mapped_column(String(64))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class AlertIncidentRow(Base):
    __tablename__ = "alert_incidents"
    __table_args__ = (
        CheckConstraint("state IN ('pending', 'firing', 'recovering', 'resolved')"),
        CheckConstraint("state_version > 0"),
        CheckConstraint("occurrence_count >= 0"),
        UniqueConstraint("fingerprint", name="uq_alert_incident_fingerprint"),
        Index("ix_alert_incidents_scope_state", "scope_digest", "state"),
    )

    incident_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    fingerprint: Mapped[str] = mapped_column(String(71))
    rule_id: Mapped[str] = mapped_column(String(64))
    severity: Mapped[str] = mapped_column(String(16))
    scope_digest: Mapped[str] = mapped_column(String(64))
    state: Mapped[str] = mapped_column(String(16))
    first_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_observed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    state_version: Mapped[int] = mapped_column(BigInteger)
    occurrence_count: Mapped[int] = mapped_column(BigInteger, default=1)
    evidence_digest: Mapped[str | None] = mapped_column(String(64))
    last_notification_id: Mapped[str | None] = mapped_column(String(96))
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RollbackDecisionRow(Base):
    __tablename__ = "rollback_decisions"
    __table_args__ = (
        CheckConstraint("affected_tenant_count >= 0"),
        CheckConstraint("from_revision > 0"),
        CheckConstraint("to_revision > 0"),
        UniqueConstraint("release_id", "command_id", name="uq_rollback_decision_command"),
    )

    decision_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    release_id: Mapped[str] = mapped_column(String(36))
    command_id: Mapped[str] = mapped_column(String(128))
    actor_digest: Mapped[str] = mapped_column(String(64))
    reason_code: Mapped[str] = mapped_column(String(64))
    target_snapshot_id: Mapped[str] = mapped_column(String(36))
    affected_tenant_count: Mapped[int] = mapped_column(BigInteger)
    from_revision: Mapped[int] = mapped_column(BigInteger)
    to_revision: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
