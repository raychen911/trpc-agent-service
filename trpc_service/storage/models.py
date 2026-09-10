"""Tenant-scoped SQLAlchemy models for the small-scale runtime."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now() -> datetime:
    return datetime.now(UTC)


class Base(DeclarativeBase):
    pass


class TenantModel(Base):
    __tablename__ = "tenant"

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="active")
    audit_policy: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    storage_config: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class AgentAppModel(Base):
    __tablename__ = "agent_app"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenant.tenant_id"], ondelete="CASCADE"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    app_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    system_prompt: Mapped[str] = mapped_column(Text, nullable=False)
    model_config_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    tool_policy: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    active_config_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class ChannelBindingModel(Base):
    __tablename__ = "channel_binding"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenant.tenant_id"], ondelete="CASCADE"),
        ForeignKeyConstraint(
            ["tenant_id", "app_id"],
            ["agent_app.tenant_id", "agent_app.app_id"],
            ondelete="CASCADE",
        ),
        UniqueConstraint("channel_type", "account_id", name="uq_channel_account"),
        Index("ix_channel_binding_lookup", "channel_type", "account_id", "is_active"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    binding_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    app_id: Mapped[str] = mapped_column(String(64), nullable=False, default="assistant")
    channel_type: Mapped[str] = mapped_column(String(32), nullable=False)
    connection_mode: Mapped[str] = mapped_column(String(32), nullable=False, default="webhook")
    account_id: Mapped[str] = mapped_column(String(200), nullable=False)
    token_ref: Mapped[str | None] = mapped_column(String(500))
    secret_ref: Mapped[str | None] = mapped_column(String(500))
    aes_key_ref: Mapped[str | None] = mapped_column(String(500))
    webhook_path: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class InboundMessageModel(Base):
    __tablename__ = "inbound_message"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "tenant_id",
            "binding_id",
            "external_message_id",
            name="uq_inbound_provider_message",
        ),
        Index("ix_inbound_session", "tenant_id", "session_id", "created_at"),
    )

    inbound_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    binding_id: Mapped[str] = mapped_column(String(64), nullable=False)
    external_message_id: Mapped[str] = mapped_column(String(200), nullable=False)
    session_id: Mapped[str] = mapped_column(String(128), nullable=False)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="accepted")
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class SessionModel(Base):
    __tablename__ = "session"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "app_id"],
            ["agent_app.tenant_id", "agent_app.app_id"],
            ondelete="CASCADE",
        ),
        Index("ix_session_principal", "tenant_id", "principal_id", "updated_at"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    app_id: Mapped[str] = mapped_column(String(64), nullable=False)
    principal_id: Mapped[str] = mapped_column(String(200), nullable=False)
    channel_type: Mapped[str] = mapped_column(String(32), nullable=False)
    state: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_event_sequence: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class SessionEventModel(Base):
    __tablename__ = "session_event"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["session.tenant_id", "session.session_id"],
            ondelete="CASCADE",
        ),
        UniqueConstraint("tenant_id", "session_id", "sequence", name="uq_session_event_sequence"),
        Index("ix_session_event_history", "tenant_id", "session_id", "sequence"),
    )

    event_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    session_id: Mapped[str] = mapped_column(String(128), nullable=False)
    sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    event_type: Mapped[str] = mapped_column(String(32), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class MemoryModel(Base):
    __tablename__ = "memory"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenant.tenant_id"], ondelete="CASCADE"),
        UniqueConstraint("tenant_id", "source_event_id", name="uq_memory_source_event"),
        Index("ix_memory_principal", "tenant_id", "principal_id", "created_at"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    memory_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    principal_id: Mapped[str] = mapped_column(String(200), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    source_event_id: Mapped[str | None] = mapped_column(String(64))
    metadata_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class SummaryModel(Base):
    __tablename__ = "summary"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["session.tenant_id", "session.session_id"],
            ondelete="CASCADE",
        ),
        UniqueConstraint(
            "tenant_id", "session_id", "source_end_sequence", name="uq_summary_coverage"
        ),
        Index("ix_summary_latest", "tenant_id", "session_id", "source_end_sequence"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    summary_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(128), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    source_end_sequence: Mapped[int] = mapped_column(Integer, nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class KnowledgeModel(Base):
    __tablename__ = "knowledge"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenant.tenant_id"], ondelete="CASCADE"),
        Index("ix_knowledge_tenant_app", "tenant_id", "app_id", "updated_at"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    knowledge_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    app_id: Mapped[str | None] = mapped_column(String(64))
    title: Mapped[str] = mapped_column(String(500), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class ArtifactModel(Base):
    __tablename__ = "artifact"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenant.tenant_id"], ondelete="CASCADE"),
        Index("ix_artifact_tenant_session", "tenant_id", "session_id", "created_at"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    artifact_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_id: Mapped[str | None] = mapped_column(String(128))
    filename: Mapped[str] = mapped_column(String(500), nullable=False)
    media_type: Mapped[str] = mapped_column(String(200), nullable=False)
    size_bytes: Mapped[int] = mapped_column(Integer, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    storage_uri: Mapped[str] = mapped_column(Text, nullable=False)
    metadata_data: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False, default=dict)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class ExecutionOutboxModel(Base):
    __tablename__ = "execution_outbox"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenant.tenant_id"], ondelete="CASCADE"),
        UniqueConstraint("trace_id", name="uq_execution_outbox_trace"),
        Index("ix_execution_outbox_status", "status", "updated_at"),
    )

    outbox_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, nullable=False)
    status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_type: Mapped[str | None] = mapped_column(String(200))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utc_now, onupdate=utc_now
    )


class AuditLogModel(Base):
    __tablename__ = "audit_log"
    __table_args__ = (
        ForeignKeyConstraint(["tenant_id"], ["tenant.tenant_id"], ondelete="CASCADE"),
        Index("ix_audit_tenant_created", "tenant_id", "created_at"),
    )

    log_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    trace_id: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    tenant_id: Mapped[str] = mapped_column(String(64), nullable=False)
    channel: Mapped[str] = mapped_column(String(32), nullable=False)
    user_id: Mapped[str] = mapped_column(String(200), nullable=False)
    session_id: Mapped[str] = mapped_column(String(128), nullable=False)
    agent_name: Mapped[str] = mapped_column(String(200), nullable=False)
    tool_name: Mapped[str | None] = mapped_column(String(200))
    decision: Mapped[str] = mapped_column(String(32), nullable=False)
    latency_ms: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    error_type: Mapped[str | None] = mapped_column(String(200))
    cost: Mapped[float] = mapped_column(Float, nullable=False, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


__all__ = [
    "AgentAppModel",
    "ArtifactModel",
    "AuditLogModel",
    "Base",
    "ChannelBindingModel",
    "ExecutionOutboxModel",
    "InboundMessageModel",
    "KnowledgeModel",
    "MemoryModel",
    "SessionEventModel",
    "SessionModel",
    "SummaryModel",
    "TenantModel",
]
