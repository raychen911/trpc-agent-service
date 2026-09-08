import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy import Enum as SqlEnum
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from trpc_service.domain import (
    AppStatus,
    BackendKind,
    ChannelType,
    ConfigStatus,
    PermissionEffect,
    TenantStatus,
)


def new_id() -> str:
    return str(uuid.uuid4())


def enum_column(enum_type: type[Any], name: str) -> SqlEnum:
    return SqlEnum(
        enum_type,
        name=name,
        native_enum=False,
        create_constraint=True,
        validate_strings=True,
        values_callable=lambda values: [item.value for item in values],
    )


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp(),
        nullable=False,
    )


class Tenant(TimestampMixin, Base):
    __tablename__ = "tenants"
    __table_args__ = (CheckConstraint("version >= 1", name="ck_tenants_version_positive"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    slug: Mapped[str] = mapped_column(String(63), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[TenantStatus] = mapped_column(
        enum_column(TenantStatus, "tenant_status"), default=TenantStatus.ACTIVE, nullable=False
    )
    audit_policy: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    key_namespace: Mapped[str] = mapped_column(String(255), nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)


class AgentApp(TimestampMixin, Base):
    __tablename__ = "agent_apps"
    __table_args__ = (
        UniqueConstraint("tenant_id", "slug", name="uq_agent_apps_tenant_slug"),
        UniqueConstraint("tenant_id", "id", name="uq_agent_apps_tenant_id_id"),
        CheckConstraint("draft_version >= 1", name="ck_agent_apps_draft_version_positive"),
        CheckConstraint("lock_version >= 1", name="ck_agent_apps_lock_version_positive"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    slug: Mapped[str] = mapped_column(String(63), nullable=False)
    name: Mapped[str] = mapped_column(String(128), nullable=False)
    status: Mapped[AppStatus] = mapped_column(
        enum_column(AppStatus, "app_status"), default=AppStatus.DRAFT, nullable=False
    )
    active_version: Mapped[int | None] = mapped_column(Integer)
    draft_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    lock_version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)


class AgentAppRevision(TimestampMixin, Base):
    __tablename__ = "agent_app_revisions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_apps.tenant_id", "agent_apps.id"],
            ondelete="CASCADE",
            name="fk_app_revisions_tenant_app",
        ),
        UniqueConstraint("agent_app_id", "version", name="uq_app_revisions_app_version"),
        UniqueConstraint(
            "tenant_id", "agent_app_id", "version", name="uq_app_revisions_tenant_app_version"
        ),
        CheckConstraint("version >= 1", name="ck_app_revisions_version_positive"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    agent_app_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    description: Mapped[str] = mapped_column(String(512), default="", nullable=False)
    instruction: Mapped[str] = mapped_column(Text, default="", nullable=False)
    application_config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    status: Mapped[ConfigStatus] = mapped_column(
        enum_column(ConfigStatus, "config_status"), default=ConfigStatus.DRAFT, nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class ModelConfig(TimestampMixin, Base):
    __tablename__ = "model_configs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id", "config_version"],
            [
                "agent_app_revisions.tenant_id",
                "agent_app_revisions.agent_app_id",
                "agent_app_revisions.version",
            ],
            ondelete="CASCADE",
            name="fk_model_configs_revision",
        ),
        UniqueConstraint("agent_app_id", "config_version", name="uq_model_configs_app_version"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    agent_app_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    config_version: Mapped[int] = mapped_column(Integer, nullable=False)
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    model_name: Mapped[str] = mapped_column(String(128), nullable=False)
    base_url: Mapped[str | None] = mapped_column(String(512))
    api_key_secret_ref: Mapped[str | None] = mapped_column(String(512))
    parameters: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class ToolPermission(TimestampMixin, Base):
    __tablename__ = "tool_permissions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id", "config_version"],
            [
                "agent_app_revisions.tenant_id",
                "agent_app_revisions.agent_app_id",
                "agent_app_revisions.version",
            ],
            ondelete="CASCADE",
            name="fk_tool_permissions_revision",
        ),
        UniqueConstraint(
            "agent_app_id",
            "config_version",
            "tool_name",
            name="uq_tool_permissions_app_version_tool",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    agent_app_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    config_version: Mapped[int] = mapped_column(Integer, nullable=False)
    tool_name: Mapped[str] = mapped_column(String(128), nullable=False)
    effect: Mapped[PermissionEffect] = mapped_column(
        enum_column(PermissionEffect, "permission_effect"), nullable=False
    )
    requires_confirmation: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    constraints: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class ChannelBinding(TimestampMixin, Base):
    __tablename__ = "channel_bindings"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id", "config_version"],
            [
                "agent_app_revisions.tenant_id",
                "agent_app_revisions.agent_app_id",
                "agent_app_revisions.version",
            ],
            ondelete="CASCADE",
            name="fk_channel_bindings_revision",
        ),
        UniqueConstraint(
            "agent_app_id",
            "config_version",
            "channel_type",
            "account_id",
            name="uq_channel_bindings_app_version_account",
        ),
        Index("ix_channel_bindings_lookup", "channel_type", "account_id", "enabled"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    agent_app_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    config_version: Mapped[int] = mapped_column(Integer, nullable=False)
    channel_type: Mapped[ChannelType] = mapped_column(
        enum_column(ChannelType, "channel_type"), nullable=False
    )
    account_id: Mapped[str] = mapped_column(String(255), nullable=False)
    webhook_path: Mapped[str] = mapped_column(String(255), nullable=False)
    token_secret_ref: Mapped[str | None] = mapped_column(String(512))
    secret_ref: Mapped[str | None] = mapped_column(String(512))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)
    options: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class ImUserIdentity(TimestampMixin, Base):
    """Map one IM account user to a stable tenant-local platform user."""

    __tablename__ = "im_user_identities"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id"],
            ["tenants.id"],
            ondelete="CASCADE",
            name="fk_im_user_identities_tenant",
        ),
        UniqueConstraint(
            "tenant_id",
            "channel_type",
            "account_id",
            "external_user_id",
            name="uq_im_user_identities_external",
        ),
        CheckConstraint(
            "status IN ('active', 'disabled')",
            name="ck_im_user_identities_status",
        ),
        Index(
            "ix_im_user_identities_internal",
            "tenant_id",
            "internal_user_id",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    channel_type: Mapped[ChannelType] = mapped_column(
        enum_column(ChannelType, "im_identity_channel_type"), nullable=False
    )
    account_id: Mapped[str] = mapped_column(String(255), nullable=False)
    external_user_id: Mapped[str] = mapped_column(String(255), nullable=False)
    internal_user_id: Mapped[str] = mapped_column(String(255), nullable=False)
    display_name: Mapped[str | None] = mapped_column(String(255))
    status: Mapped[str] = mapped_column(String(16), default="active", nullable=False)
    attributes: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class BackendConfig(TimestampMixin, Base):
    __tablename__ = "backend_configs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id", "config_version"],
            [
                "agent_app_revisions.tenant_id",
                "agent_app_revisions.agent_app_id",
                "agent_app_revisions.version",
            ],
            ondelete="CASCADE",
            name="fk_backend_configs_revision",
        ),
        UniqueConstraint(
            "agent_app_id",
            "config_version",
            "backend_kind",
            name="uq_backend_configs_app_version_kind",
        ),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    agent_app_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    config_version: Mapped[int] = mapped_column(Integer, nullable=False)
    backend_kind: Mapped[BackendKind] = mapped_column(
        enum_column(BackendKind, "backend_kind"), nullable=False
    )
    backend_type: Mapped[str] = mapped_column(String(64), nullable=False)
    secret_ref: Mapped[str | None] = mapped_column(String(512))
    options: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class AgentSession(TimestampMixin, Base):
    __tablename__ = "sessions"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_apps.tenant_id", "agent_apps.id"],
            ondelete="CASCADE",
            name="fk_sessions_tenant_app",
        ),
        UniqueConstraint(
            "tenant_id", "agent_app_id", "session_key", name="uq_sessions_tenant_app_key"
        ),
        UniqueConstraint("tenant_id", "id", name="uq_sessions_tenant_id_id"),
        CheckConstraint("version >= 0", name="ck_sessions_version_nonnegative"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    agent_app_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    session_key: Mapped[str] = mapped_column(String(512), nullable=False)
    state: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=0, nullable=False)


class SessionEvent(Base):
    __tablename__ = "session_events"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["sessions.tenant_id", "sessions.id"],
            ondelete="CASCADE",
            name="fk_session_events_tenant_session",
        ),
        UniqueConstraint("session_id", "sequence_no", name="uq_session_events_sequence"),
        UniqueConstraint(
            "tenant_id",
            "channel_type",
            "external_message_id",
            name="uq_session_events_external_message",
        ),
        CheckConstraint("sequence_no >= 0", name="ck_session_events_sequence_nonnegative"),
        Index("ix_session_events_trace_id", "trace_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    session_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    sequence_no: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(64), nullable=False)
    role: Mapped[str | None] = mapped_column(String(32))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    channel_type: Mapped[str | None] = mapped_column(String(32))
    external_message_id: Mapped[str | None] = mapped_column(String(255))
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )


class Memory(TimestampMixin, Base):
    __tablename__ = "memories"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_apps.tenant_id", "agent_apps.id"],
            ondelete="CASCADE",
            name="fk_memories_tenant_app",
        ),
        UniqueConstraint(
            "tenant_id", "agent_app_id", "user_id", "memory_key", name="uq_memories_user_key"
        ),
        CheckConstraint("version >= 1", name="ck_memories_version_positive"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    agent_app_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    user_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    memory_key: Mapped[str] = mapped_column(String(255), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    topics: Mapped[list[str]] = mapped_column(JSON, default=list, nullable=False)
    version: Mapped[int] = mapped_column(Integer, default=1, nullable=False)


class Summary(Base):
    __tablename__ = "summaries"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["sessions.tenant_id", "sessions.id"],
            ondelete="CASCADE",
            name="fk_summaries_tenant_session",
        ),
        UniqueConstraint("session_id", "version", name="uq_summaries_session_version"),
        CheckConstraint("version >= 1", name="ck_summaries_version_positive"),
        CheckConstraint("through_sequence >= 0", name="ck_summaries_sequence_nonnegative"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    session_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    through_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )


class Artifact(TimestampMixin, Base):
    __tablename__ = "artifacts"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_apps.tenant_id", "agent_apps.id"],
            ondelete="CASCADE",
            name="fk_artifacts_tenant_app",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["sessions.tenant_id", "sessions.id"],
            name="fk_artifacts_tenant_session",
        ),
        UniqueConstraint("tenant_id", "object_key", name="uq_artifacts_tenant_object_key"),
        CheckConstraint("size_bytes >= 0", name="ck_artifacts_size_nonnegative"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    agent_app_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    session_id: Mapped[str | None] = mapped_column(String(36))
    object_key: Mapped[str] = mapped_column(String(1024), nullable=False)
    mime_type: Mapped[str] = mapped_column(String(255), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    checksum: Mapped[str] = mapped_column(String(128), nullable=False)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)


class AuditLog(Base):
    __tablename__ = "audit_logs"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_apps.tenant_id", "agent_apps.id"],
            ondelete="RESTRICT",
            name="fk_audit_logs_tenant_app",
        ),
        CheckConstraint("latency_ms >= 0", name="ck_audit_logs_latency_nonnegative"),
        CheckConstraint("cost >= 0", name="ck_audit_logs_cost_nonnegative"),
        Index("ix_audit_logs_tenant_created", "tenant_id", "created_at"),
        Index("ix_audit_logs_trace_id", "trace_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(36), nullable=False)
    agent_app_id: Mapped[str] = mapped_column(String(36), nullable=False)
    channel: Mapped[str | None] = mapped_column(String(32))
    user_id: Mapped[str | None] = mapped_column(String(255))
    # Audit records store the external, channel-derived session key rather than
    # only the internal UUID. IM account and user identifiers can make this
    # value substantially longer than 36 characters.
    session_id: Mapped[str | None] = mapped_column(String(512))
    agent_name: Mapped[str] = mapped_column(String(128), nullable=False)
    tool_name: Mapped[str | None] = mapped_column(String(128))
    decision: Mapped[str] = mapped_column(String(64), nullable=False)
    latency_ms: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_type: Mapped[str | None] = mapped_column(String(128))
    cost: Mapped[Decimal] = mapped_column(Numeric(18, 8), default=0, nullable=False)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False)
    request_id: Mapped[str] = mapped_column(String(64), nullable=False)
    details: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )


class OutboxMessage(Base):
    __tablename__ = "outbox_messages"
    __table_args__ = (
        UniqueConstraint("dedupe_key", name="uq_outbox_messages_dedupe_key"),
        CheckConstraint("attempts >= 0", name="ck_outbox_messages_attempts_nonnegative"),
        CheckConstraint(
            "status IN ('pending', 'processing', 'processed', 'failed')",
            name="ck_outbox_messages_status",
        ),
        Index("ix_outbox_messages_delivery", "status", "available_at", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict, nullable=False)
    dedupe_key: Mapped[str] = mapped_column(String(512), nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )
    locked_by: Mapped[str | None] = mapped_column(String(128))
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )


class OutboxDeadLetter(Base):
    __tablename__ = "outbox_dead_letters"
    __table_args__ = (Index("ix_outbox_dead_letters_tenant", "tenant_id", "created_at"),)

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    original_outbox_id: Mapped[str] = mapped_column(String(36), unique=True, nullable=False)
    tenant_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False
    )
    topic: Mapped[str] = mapped_column(String(128), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False)
    last_error: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )


class TenantBudgetUsage(TimestampMixin, Base):
    __tablename__ = "tenant_budget_usage"
    __table_args__ = (
        UniqueConstraint("tenant_id", "period", name="uq_tenant_budget_period"),
        CheckConstraint("request_count >= 0", name="ck_budget_requests_nonnegative"),
        CheckConstraint("token_count >= 0", name="ck_budget_tokens_nonnegative"),
        CheckConstraint("cost >= 0", name="ck_budget_cost_nonnegative"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    period: Mapped[str] = mapped_column(String(10), nullable=False)
    request_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    token_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    cost: Mapped[Decimal] = mapped_column(Numeric(18, 8), default=0, nullable=False)


class InboundMessage(TimestampMixin, Base):
    __tablename__ = "inbound_messages"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id", "channel", "external_message_id", name="uq_inbound_idempotency"
        ),
        CheckConstraint(
            "status IN ('pending', 'processing', 'completed', 'failed', 'uncertain')",
            name="ck_inbound_status",
        ),
        Index("ix_inbound_delivery", "status", "available_at", "created_at"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    agent_app_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    account_id: Mapped[str] = mapped_column(String(255), nullable=False)
    external_message_id: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(16), default="pending", nullable=False)
    attempts: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.current_timestamp(), nullable=False
    )
    locked_by: Mapped[str | None] = mapped_column(String(128))
    locked_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    last_error: Mapped[str | None] = mapped_column(Text)


class AgentExecution(TimestampMixin, Base):
    __tablename__ = "agent_executions"
    __table_args__ = (
        UniqueConstraint("inbound_message_id", name="uq_execution_inbound"),
        CheckConstraint(
            "status IN ('pending', 'runner_started', 'runner_completed', "
            "'platform_committed', 'delivery_enqueued', 'uncertain', 'failed')",
            name="ck_execution_status",
        ),
        Index("ix_agent_executions_trace", "trace_id"),
    )

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=new_id)
    inbound_message_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("inbound_messages.id", ondelete="CASCADE"), unique=True
    )
    tenant_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("tenants.id", ondelete="CASCADE"), nullable=False, index=True
    )
    agent_app_id: Mapped[str] = mapped_column(String(36), nullable=False, index=True)
    session_key: Mapped[str] = mapped_column(String(512), nullable=False)
    idempotency_key: Mapped[str] = mapped_column(String(768), unique=True, nullable=False)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(24), default="pending", nullable=False)
    runner_reply: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    platform_event_id: Mapped[str | None] = mapped_column(String(36))
    last_error: Mapped[str | None] = mapped_column(Text)
