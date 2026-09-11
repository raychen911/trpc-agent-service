"""SQLAlchemy 2 mappings for the PostgreSQL reference schema.

Alembic executes revision-owned immutable SQL snapshots because they carry a
few PostgreSQL-only details (partitioning and deferrable cyclic constraints).
These mappings are deliberately complete enough for repositories and local
SQLite adapter tests; PostgreSQL remains the authoritative production schema.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

JsonValue = JSON().with_variant(JSONB, "postgresql")


class Base(DeclarativeBase):
    """Declarative base for the platform SQL schema."""


class Tenant(Base):
    __tablename__ = "tenant"

    tenant_id: Mapped[str] = mapped_column(Text, primary_key=True)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    audit_policy: Mapped[dict[str, Any]] = mapped_column(JsonValue, default=dict)
    budget_policy: Mapped[dict[str, Any]] = mapped_column(JsonValue, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("status IN ('active', 'suspended', 'deleted')", name="tenant_status_ck"),
    )


class TenantLocator(Base):
    """Scheduler-only tenant index; it is intentionally not an RLS business table."""

    __tablename__ = "tenant_locator"

    tenant_id: Mapped[str] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="CASCADE"), primary_key=True
    )
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

class TenantRuntimeState(Base):
    __tablename__ = "tenant_runtime_state"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    routing_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)
    security_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)
    credential_revocation_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)
    execution_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="normal")
    tool_denylist: Mapped[list[str]] = mapped_column(JsonValue, nullable=False, default=list)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("routing_epoch > 0", name="runtime_routing_epoch_ck"),
        CheckConstraint("security_epoch > 0", name="runtime_security_epoch_ck"),
        CheckConstraint(
            "execution_mode IN ('normal', 'draining', 'suspended', 'emergency_stop')",
            name="runtime_execution_mode_ck",
        ),
    )


class StorageRoute(Base):
    __tablename__ = "storage_route"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    routing_epoch: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    profile: Mapped[dict[str, Any]] = mapped_column(JsonValue, nullable=False, default=dict)
    route_status: Mapped[str] = mapped_column(String(32), nullable=False)
    source_watermark: Mapped[str | None] = mapped_column(Text)
    target_watermark: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    activated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        CheckConstraint(
            "route_status IN ('preparing', 'backfilling', 'catching_up', 'draining', "
            "'verifying', 'active', 'readonly', 'retired', 'failed')",
            name="storage_route_status_ck",
        ),
        Index(
            "storage_route_one_active_idx",
            "tenant_id",
            unique=True,
            postgresql_where=text("route_status = 'active'"),
            sqlite_where=text("route_status = 'active'"),
        ),
    )


class StorageMigration(Base):
    __tablename__ = "storage_migration"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    migration_id: Mapped[str] = mapped_column(Text, primary_key=True)
    source_profile: Mapped[dict[str, Any]] = mapped_column(JsonValue, nullable=False)
    target_profile: Mapped[dict[str, Any]] = mapped_column(JsonValue, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    source_routing_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    target_routing_epoch: Mapped[int | None] = mapped_column(BigInteger)
    source_watermark: Mapped[str | None] = mapped_column(Text)
    target_watermark: Mapped[str | None] = mapped_column(Text)
    error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint(
            "status IN ('preparing', 'backfilling', 'catching_up', 'draining', "
            "'verifying', 'active', 'readonly', 'retired', 'failed')",
            name="storage_migration_status_ck",
        ),
        Index(
            "storage_migration_active_idx",
            "tenant_id",
            "updated_at",
            postgresql_where=text(
                "status IN ('preparing', 'backfilling', 'catching_up', 'draining', "
                "'verifying', 'readonly')"
            ),
        ),
    )


class AgentApp(Base):
    __tablename__ = "agent_app"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    agent_id: Mapped[str] = mapped_column(Text, primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="draft")
    active_config_version: Mapped[int | None] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("status IN ('draft', 'active', 'disabled')", name="agent_app_status_ck"),
    )


class AgentRelease(Base):
    __tablename__ = "agent_release"

    tenant_id: Mapped[str] = mapped_column(primary_key=True)
    agent_id: Mapped[str] = mapped_column(primary_key=True)
    config_version: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    release_status: Mapped[str] = mapped_column(String(16), nullable=False)
    app_config: Mapped[dict[str, Any]] = mapped_column(JsonValue, default=dict)
    model_config: Mapped[dict[str, Any]] = mapped_column(JsonValue, default=dict)
    tool_policy: Mapped[dict[str, Any]] = mapped_column(JsonValue, default=dict)
    knowledge_config: Mapped[dict[str, Any]] = mapped_column(JsonValue, default=dict)
    created_by: Mapped[str] = mapped_column(Text, nullable=False)
    change_reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_id"], ["agent_app.tenant_id", "agent_app.agent_id"]
        ),
        CheckConstraint(
            "release_status IN ('staged', 'active', 'retired')", name="release_status_ck"
        ),
    )


class ChannelBinding(Base):
    __tablename__ = "channel_binding"

    tenant_id: Mapped[str] = mapped_column(primary_key=True)
    binding_id: Mapped[str] = mapped_column(Text, primary_key=True)
    agent_id: Mapped[str] = mapped_column(Text, nullable=False)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    external_account_id: Mapped[str] = mapped_column(Text, nullable=False)
    webhook_key_hash: Mapped[str] = mapped_column(Text, nullable=False)
    secret_ref: Mapped[str] = mapped_column(Text, nullable=False)
    capabilities: Mapped[dict[str, Any]] = mapped_column(JsonValue, default=dict)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_id"], ["agent_app.tenant_id", "agent_app.agent_id"]
        ),
        UniqueConstraint("provider", "external_account_id"),
        UniqueConstraint("webhook_key_hash"),
        UniqueConstraint("tenant_id", "binding_id", "webhook_key_hash"),
        CheckConstraint("status IN ('active', 'rotating', 'disabled')", name="binding_status_ck"),
    )


class ChannelBindingLocator(Base):
    """Bootstrap-only table.  Application roles only call the security function."""

    __tablename__ = "channel_binding_locator"

    webhook_key_hash: Mapped[str] = mapped_column(Text, primary_key=True)
    resolved_tenant_id: Mapped[str] = mapped_column(Text, nullable=False)
    binding_id: Mapped[str] = mapped_column(Text, nullable=False)
    provider: Mapped[str] = mapped_column(Text, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)

    __table_args__ = (
        ForeignKeyConstraint(
            ["resolved_tenant_id", "binding_id", "webhook_key_hash"],
            [
                "channel_binding.tenant_id",
                "channel_binding.binding_id",
                "channel_binding.webhook_key_hash",
            ],
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
    )


class IdentityMapping(Base):
    __tablename__ = "identity_mapping"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    provider: Mapped[str] = mapped_column(Text, primary_key=True)
    external_account_id: Mapped[str] = mapped_column(Text, primary_key=True)
    external_user_id: Mapped[str] = mapped_column(Text, primary_key=True)
    subject_id: Mapped[str] = mapped_column(Text, nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JsonValue, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "subject_id", "provider", "external_account_id", "external_user_id"
        ),
    )


class Session(Base):
    __tablename__ = "session"

    tenant_id: Mapped[str] = mapped_column(primary_key=True)
    session_id: Mapped[str] = mapped_column(Text, primary_key=True)
    agent_id: Mapped[str] = mapped_column(Text, nullable=False)
    channel_binding_id: Mapped[str] = mapped_column(Text, nullable=False)
    conversation_type: Mapped[str] = mapped_column(String(16), nullable=False)
    conversation_key_hash: Mapped[str] = mapped_column(Text, nullable=False)
    id_rule_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    config_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    state: Mapped[dict[str, Any]] = mapped_column(JsonValue, nullable=False, default=dict)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    last_event_seq: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    active_inbox_id: Mapped[str | None] = mapped_column(Text)
    lease_owner: Mapped[str | None] = mapped_column(Text)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_fence: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    status: Mapped[str] = mapped_column(Text, nullable=False, default="active")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_id"], ["agent_app.tenant_id", "agent_app.agent_id"]
        ),
        ForeignKeyConstraint(
            ["tenant_id", "agent_id", "config_version"],
            ["agent_release.tenant_id", "agent_release.agent_id", "agent_release.config_version"],
        ),
        ForeignKeyConstraint(
            ["tenant_id", "channel_binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
        ),
        CheckConstraint(
            "conversation_type IN ('direct', 'group')", name="session_conversation_type_ck"
        ),
        CheckConstraint("lease_fence >= 0", name="session_lease_fence_ck"),
    )


class SessionEvent(Base):
    __tablename__ = "session_event"

    tenant_id: Mapped[str] = mapped_column(primary_key=True)
    session_id: Mapped[str] = mapped_column(Text, primary_key=True)
    seq: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_id: Mapped[str] = mapped_column(Text, nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    role: Mapped[str | None] = mapped_column(Text)
    subject_id: Mapped[str | None] = mapped_column(Text)
    external_message_id: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JsonValue, nullable=False)
    trace_id: Mapped[str] = mapped_column(Text, nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "session_id"], ["session.tenant_id", "session.session_id"]
        ),
        UniqueConstraint("tenant_id", "event_id"),
        Index("session_event_trace_idx", "tenant_id", "trace_id"),
    )


class SessionSummary(Base):
    __tablename__ = "session_summary"

    tenant_id: Mapped[str] = mapped_column(primary_key=True)
    session_id: Mapped[str] = mapped_column(Text, primary_key=True)
    summary_id: Mapped[str] = mapped_column(Text, primary_key=True)
    based_on_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    model_ref: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "session_id"], ["session.tenant_id", "session.session_id"]
        ),
        UniqueConstraint("tenant_id", "session_id", "based_on_seq"),
    )


class Memory(Base):
    __tablename__ = "memory"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    memory_id: Mapped[str] = mapped_column(Text, primary_key=True)
    session_id: Mapped[str | None] = mapped_column(Text)
    subject_id: Mapped[str | None] = mapped_column(Text)
    memory_type: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str | None] = mapped_column(Text)
    encrypted_content_ref: Mapped[str | None] = mapped_column(Text)
    content_hash: Mapped[str] = mapped_column(Text, nullable=False)
    acl: Mapped[dict[str, Any]] = mapped_column(JsonValue, nullable=False, default=dict)
    source_event_id: Mapped[str | None] = mapped_column(Text)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "session_id"], ["session.tenant_id", "session.session_id"]
        ),
        CheckConstraint(
            "(content IS NOT NULL) <> (encrypted_content_ref IS NOT NULL)", name="memory_content_ck"
        ),
        Index("memory_subject_idx", "tenant_id", "subject_id", "updated_at"),
    )


class MemoryProjection(Base):
    __tablename__ = "memory_projection"

    tenant_id: Mapped[str] = mapped_column(primary_key=True)
    memory_id: Mapped[str] = mapped_column(Text, primary_key=True)
    target_id: Mapped[str] = mapped_column(Text, primary_key=True)
    requested_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    projected_version: Mapped[int | None] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    last_error_code: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(["tenant_id", "memory_id"], ["memory.tenant_id", "memory.memory_id"]),
        CheckConstraint(
            "status IN ('pending', 'processing', 'ready', 'failed')",
            name="memory_projection_status_ck",
        ),
    )


class KnowledgeDocument(Base):
    __tablename__ = "knowledge_document"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    document_id: Mapped[str] = mapped_column(Text, primary_key=True)
    knowledge_base_id: Mapped[str] = mapped_column(Text, nullable=False)
    object_uri: Mapped[str] = mapped_column(Text, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False, default="")
    checksum: Mapped[str] = mapped_column(Text, nullable=False)
    acl: Mapped[dict[str, Any]] = mapped_column(JsonValue, nullable=False, default=dict)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)
    index_status: Mapped[str] = mapped_column(Text, nullable=False, default="pending")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (UniqueConstraint("tenant_id", "knowledge_base_id", "checksum"),)


class Artifact(Base):
    __tablename__ = "artifact"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    artifact_id: Mapped[str] = mapped_column(Text, primary_key=True)
    session_id: Mapped[str | None] = mapped_column(Text)
    object_uri: Mapped[str] = mapped_column(Text, nullable=False)
    media_type: Mapped[str] = mapped_column(Text, nullable=False)
    byte_size: Mapped[int] = mapped_column(BigInteger, nullable=False)
    checksum: Mapped[str] = mapped_column(Text, nullable=False)
    encryption_key_ref: Mapped[str | None] = mapped_column(Text)
    scan_status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    retention_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "session_id"], ["session.tenant_id", "session.session_id"]
        ),
        UniqueConstraint("tenant_id", "object_uri"),
        CheckConstraint("byte_size >= 0", name="artifact_byte_size_ck"),
        CheckConstraint(
            "scan_status IN ('pending', 'clean', 'quarantined', 'failed')",
            name="artifact_scan_status_ck",
        ),
    )


class Inbox(Base):
    __tablename__ = "inbox"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    inbox_id: Mapped[str] = mapped_column(Text, primary_key=True)
    channel_binding_id: Mapped[str] = mapped_column(Text, nullable=False)
    agent_id: Mapped[str] = mapped_column(Text, nullable=False)
    config_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    subject_id: Mapped[str | None] = mapped_column(Text)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    external_message_id: Mapped[str | None] = mapped_column(Text)
    session_id: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    execution_id: Mapped[str | None] = mapped_column(Text)
    execution_attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    claimed_lease_fence: Mapped[int | None] = mapped_column(BigInteger)
    claimed_routing_epoch: Mapped[int | None] = mapped_column(BigInteger)
    claimed_security_epoch: Mapped[int | None] = mapped_column(BigInteger)
    request_id: Mapped[str] = mapped_column(Text, nullable=False)
    trace_id: Mapped[str] = mapped_column(Text, nullable=False)
    payload_hash: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JsonValue, nullable=False)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "channel_binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
        ),
        ForeignKeyConstraint(
            ["tenant_id", "agent_id"], ["agent_app.tenant_id", "agent_app.agent_id"]
        ),
        ForeignKeyConstraint(
            ["tenant_id", "agent_id", "config_version"],
            ["agent_release.tenant_id", "agent_release.agent_id", "agent_release.config_version"],
        ),
        UniqueConstraint("tenant_id", "idempotency_key"),
        UniqueConstraint("tenant_id", "execution_id"),
        UniqueConstraint("tenant_id", "inbox_id", "execution_id"),
        CheckConstraint(
            "status IN ('received', 'queued', 'claimed', 'committed', 'reply_pending', 'delivered', 'retryable', 'unknown', 'failed')",
            name="inbox_status_ck",
        ),
        Index("inbox_status_idx", "tenant_id", "status", "updated_at"),
    )


class Outbox(Base):
    __tablename__ = "outbox"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    outbox_id: Mapped[str] = mapped_column(Text, primary_key=True)
    aggregate_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_id: Mapped[str] = mapped_column(Text, nullable=False)
    inbox_id: Mapped[str | None] = mapped_column(Text)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JsonValue, nullable=False)
    idempotency_key: Mapped[str] = mapped_column(Text, nullable=False)
    trace_id: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, default="pending")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_owner: Mapped[str | None] = mapped_column(Text)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    available_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        ForeignKeyConstraint(["tenant_id", "inbox_id"], ["inbox.tenant_id", "inbox.inbox_id"]),
        UniqueConstraint("tenant_id", "idempotency_key"),
        UniqueConstraint("tenant_id", "aggregate_type", "aggregate_id", "event_type"),
        CheckConstraint(
            "status IN ('pending', 'processing', 'published', 'delivered', 'unknown', 'dead')",
            name="outbox_status_ck",
        ),
    )


class ExecutionAttempt(Base):
    __tablename__ = "execution_attempt"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    execution_id: Mapped[str] = mapped_column(Text, primary_key=True)
    attempt_no: Mapped[int] = mapped_column(Integer, primary_key=True)
    inbox_id: Mapped[str] = mapped_column(Text, nullable=False)
    session_id: Mapped[str] = mapped_column(Text, nullable=False)
    worker_id: Mapped[str] = mapped_column(Text, nullable=False)
    lease_fence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    routing_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    security_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    lease_expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    claimed_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "inbox_id", "execution_id"],
            ["inbox.tenant_id", "inbox.inbox_id", "inbox.execution_id"],
        ),
        ForeignKeyConstraint(
            ["tenant_id", "session_id"], ["session.tenant_id", "session.session_id"]
        ),
        UniqueConstraint("tenant_id", "inbox_id", "attempt_no"),
        CheckConstraint("attempt_no > 0", name="execution_attempt_no_ck"),
        CheckConstraint(
            "status IN ('claimed', 'running', 'committed', 'lost_fence', 'cancelled', 'failed')",
            name="execution_attempt_status_ck",
        ),
    )


class BudgetAccount(Base):
    __tablename__ = "budget_account"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    budget_name: Mapped[str] = mapped_column(Text, primary_key=True)
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    unit: Mapped[str] = mapped_column(String(16), nullable=False)
    period_end: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    limit_units: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reserved_units: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    spent_units: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint("unit IN ('cost_micros', 'tokens', 'tool_units')", name="budget_unit_ck"),
        CheckConstraint("limit_units >= 0", name="budget_limit_ck"),
        CheckConstraint("reserved_units >= 0", name="budget_reserved_ck"),
        CheckConstraint("spent_units >= 0", name="budget_spent_ck"),
        CheckConstraint("period_end > period_start", name="budget_period_ck"),
        CheckConstraint("reserved_units + spent_units <= limit_units", name="budget_total_ck"),
    )


class BudgetReservation(Base):
    __tablename__ = "budget_reservation"

    tenant_id: Mapped[str] = mapped_column(primary_key=True)
    reservation_id: Mapped[str] = mapped_column(Text, primary_key=True)
    budget_name: Mapped[str] = mapped_column(Text, nullable=False)
    period_start: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    execution_id: Mapped[str] = mapped_column(Text, nullable=False)
    estimated_units: Mapped[int] = mapped_column(BigInteger, nullable=False)
    actual_units: Mapped[int | None] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    settled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "execution_id"], ["inbox.tenant_id", "inbox.execution_id"]
        ),
        ForeignKeyConstraint(
            ["tenant_id", "budget_name", "period_start"],
            [
                "budget_account.tenant_id",
                "budget_account.budget_name",
                "budget_account.period_start",
            ],
        ),
        UniqueConstraint("tenant_id", "execution_id", "budget_name", "period_start"),
        CheckConstraint("estimated_units >= 0", name="reservation_estimated_ck"),
        CheckConstraint("actual_units IS NULL OR actual_units >= 0", name="reservation_actual_ck"),
        CheckConstraint(
            "status IN ('reserved', 'settled', 'released', 'expired', 'unknown')",
            name="reservation_status_ck",
        ),
    )


class ToolExecution(Base):
    __tablename__ = "tool_execution"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    tool_call_id: Mapped[str] = mapped_column(Text, primary_key=True)
    inbox_id: Mapped[str] = mapped_column(Text, nullable=False)
    execution_id: Mapped[str] = mapped_column(Text, nullable=False)
    session_id: Mapped[str] = mapped_column(Text, nullable=False)
    tool_step: Mapped[int] = mapped_column(Integer, nullable=False)
    tool_name: Mapped[str] = mapped_column(Text, nullable=False)
    arguments_hash: Mapped[str] = mapped_column(Text, nullable=False)
    retry_capability: Mapped[str] = mapped_column(String(16), nullable=False)
    provider_idempotency_key: Mapped[str | None] = mapped_column(Text)
    provider_operation_id: Mapped[str | None] = mapped_column(Text)
    lease_fence: Mapped[int] = mapped_column(BigInteger, nullable=False)
    routing_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    security_epoch: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    result_ref: Mapped[str | None] = mapped_column(Text)
    last_error_code: Mapped[str | None] = mapped_column(Text)
    resolved_by: Mapped[str | None] = mapped_column(Text)
    trace_id: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "inbox_id", "execution_id"],
            ["inbox.tenant_id", "inbox.inbox_id", "inbox.execution_id"],
        ),
        ForeignKeyConstraint(
            ["tenant_id", "session_id"], ["session.tenant_id", "session.session_id"]
        ),
        UniqueConstraint("tenant_id", "execution_id", "tool_step"),
        CheckConstraint("tool_step >= 0", name="tool_step_ck"),
        CheckConstraint(
            "retry_capability IN ('idempotent', 'queryable', 'non_retriable')",
            name="tool_retry_capability_ck",
        ),
        CheckConstraint(
            "status IN ('prepared', 'confirmed', 'running', 'succeeded', 'failed', 'unknown', 'reconciling', 'manual_review', 'compensating', 'compensated')",
            name="tool_status_ck",
        ),
    )


class DeliveryAttempt(Base):
    __tablename__ = "delivery_attempt"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    delivery_id: Mapped[str] = mapped_column(Text, primary_key=True)
    attempt_no: Mapped[int] = mapped_column(Integer, primary_key=True)
    outbox_id: Mapped[str] = mapped_column(Text, nullable=False)
    session_id: Mapped[str] = mapped_column(Text, nullable=False)
    channel_binding_id: Mapped[str] = mapped_column(Text, nullable=False)
    retry_capability: Mapped[str] = mapped_column(String(16), nullable=False)
    provider_idempotency_key: Mapped[str | None] = mapped_column(Text)
    provider_message_id: Mapped[str | None] = mapped_column(Text)
    request_hash: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False)
    last_error_code: Mapped[str | None] = mapped_column(Text)
    trace_id: Mapped[str] = mapped_column(Text, nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_owner: Mapped[str | None] = mapped_column(Text)
    lease_fence: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    __table_args__ = (
        ForeignKeyConstraint(["tenant_id", "outbox_id"], ["outbox.tenant_id", "outbox.outbox_id"]),
        ForeignKeyConstraint(
            ["tenant_id", "session_id"], ["session.tenant_id", "session.session_id"]
        ),
        ForeignKeyConstraint(
            ["tenant_id", "channel_binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
        ),
        CheckConstraint("attempt_no > 0", name="delivery_attempt_no_ck"),
        CheckConstraint(
            "retry_capability IN ('idempotent', 'queryable', 'non_retriable')",
            name="delivery_retry_capability_ck",
        ),
        CheckConstraint(
            "status IN ('prepared', 'sending', 'accepted', 'failed', 'unknown', 'reconciling', 'manual_review')",
            name="delivery_status_ck",
        ),
        Index(
            "delivery_attempt_lease_idx",
            "tenant_id",
            "status",
            "lease_expires_at",
            postgresql_where=text("status IN ('sending', 'reconciling')"),
        ),
    )


class AuditLog(Base):
    __tablename__ = "audit_log"

    tenant_id: Mapped[str] = mapped_column(ForeignKey("tenant.tenant_id"), primary_key=True)
    audit_id: Mapped[str] = mapped_column(Text, primary_key=True)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    channel: Mapped[str | None] = mapped_column(Text)
    subject_id: Mapped[str | None] = mapped_column(Text)
    session_id: Mapped[str | None] = mapped_column(Text)
    agent_name: Mapped[str | None] = mapped_column(Text)
    tool_name: Mapped[str | None] = mapped_column(Text)
    decision: Mapped[str] = mapped_column(Text, nullable=False)
    reason_code: Mapped[str | None] = mapped_column(Text)
    policy_version: Mapped[str | None] = mapped_column(Text)
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    error_type: Mapped[str | None] = mapped_column(Text)
    input_hash: Mapped[str | None] = mapped_column(Text)
    output_hash: Mapped[str | None] = mapped_column(Text)
    token_in: Mapped[int | None] = mapped_column(Integer)
    token_out: Mapped[int | None] = mapped_column(Integer)
    cost_micros: Mapped[int | None] = mapped_column(BigInteger)
    trace_id: Mapped[str] = mapped_column(Text, nullable=False)
    request_id: Mapped[str] = mapped_column(Text, nullable=False)
    encrypted_detail_ref: Mapped[str | None] = mapped_column(Text)

    __table_args__ = (
        Index("audit_trace_idx", "tenant_id", "trace_id", "occurred_at"),
        {"postgresql_partition_by": "RANGE (occurred_at)"},
    )


ALL_MODELS = (
    Tenant,
    TenantLocator,
    TenantRuntimeState,
    StorageRoute,
    StorageMigration,
    AgentApp,
    AgentRelease,
    ChannelBinding,
    ChannelBindingLocator,
    IdentityMapping,
    Session,
    SessionEvent,
    SessionSummary,
    Memory,
    MemoryProjection,
    KnowledgeDocument,
    Artifact,
    Inbox,
    Outbox,
    ExecutionAttempt,
    BudgetAccount,
    BudgetReservation,
    ToolExecution,
    DeliveryAttempt,
    AuditLog,
)
