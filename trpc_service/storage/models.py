"""Canonical reliability-plane schema.

The canonical event log and message ledgers stay in SQL even when a tenant chooses
Redis or a vector database for replaceable projections.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

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
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(UTC)


def new_id() -> str:
    """Return a sortable-enough opaque identifier for platform records."""

    return uuid4().hex


class Base(DeclarativeBase):
    """SQLAlchemy declarative base."""


class TimestampMixin:
    """Creation/update timestamps shared by mutable records."""

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=utc_now,
        onupdate=utc_now,
    )


class Tenant(Base, TimestampMixin):
    """Tenant security and cost boundary."""

    __tablename__ = "tenant"

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    display_name: Mapped[str] = mapped_column(String(200))
    __table_args__ = (
        CheckConstraint("status IN ('active', 'suspended', 'disabled')", name="ck_tenant_status"),
    )

    status: Mapped[str] = mapped_column(String(32), default="active", index=True)
    active_config_revision: Mapped[int | None] = mapped_column(Integer)
    audit_policy: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    budget_policy: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class TenantConfigRevision(Base):
    """Immutable published tenant configuration revision."""

    __tablename__ = "tenant_config_revision"
    __table_args__ = (
        CheckConstraint(
            "status IN ('draft', 'published', 'superseded')",
            name="ck_tenant_config_revision_status",
        ),
    )

    tenant_id: Mapped[str] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        primary_key=True,
        index=True,
    )
    revision: Mapped[int] = mapped_column(Integer, primary_key=True)
    schema_version: Mapped[int] = mapped_column(Integer, default=1)
    status: Mapped[str] = mapped_column(String(32), default="draft")
    spec: Mapped[dict[str, Any]] = mapped_column(JSON)
    content_hash: Mapped[str] = mapped_column(String(64))
    created_by: Mapped[str] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AgentApp(Base, TimestampMixin):
    """Versioned logical Agent application."""

    __tablename__ = "agent_app"
    __table_args__ = (
        CheckConstraint(
            "status IN ('draft', 'published', 'disabled', 'superseded')",
            name="ck_agent_app_status",
        ),
    )

    tenant_id: Mapped[str] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        primary_key=True,
        index=True,
    )
    app_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    revision: Mapped[int] = mapped_column(Integer, primary_key=True)
    status: Mapped[str] = mapped_column(String(32), default="draft")
    agent_name: Mapped[str] = mapped_column(String(128))
    prompt: Mapped[str] = mapped_column(Text)
    model_config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    tool_policy: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    storage_config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class ChannelBinding(Base, TimestampMixin):
    """Trusted mapping from an external account/callback to tenant and app."""

    __tablename__ = "channel_binding"
    __table_args__ = (
        UniqueConstraint("tenant_id", "binding_id"),
        UniqueConstraint("channel_type", "external_account_id"),
        UniqueConstraint("callback_path"),
        UniqueConstraint("public_callback_id"),
        ForeignKeyConstraint(
            ["tenant_id", "app_id", "app_revision"],
            ["agent_app.tenant_id", "agent_app.app_id", "agent_app.revision"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "status IN ('active', 'disabled', 'revoked')",
            name="ck_channel_binding_status",
        ),
    )

    binding_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        index=True,
    )
    app_id: Mapped[str] = mapped_column(String(64))
    app_revision: Mapped[int] = mapped_column(Integer)
    config_revision: Mapped[int] = mapped_column(Integer)
    channel_type: Mapped[str] = mapped_column(String(32), index=True)
    external_account_id: Mapped[str] = mapped_column(String(256))
    callback_path: Mapped[str] = mapped_column(String(256))
    public_callback_id: Mapped[str] = mapped_column(String(128))
    route_rule: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    secret_refs: Mapped[dict[str, str]] = mapped_column(JSON, default=dict)
    identity_policy: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    status: Mapped[str] = mapped_column(String(32), default="active", index=True)


class ChannelIngressRoute(Base, TimestampMixin):
    """Minimal public callback route used before a tenant RLS scope is known.

    This table intentionally contains no external identities, secrets, or payloads.
    """

    __tablename__ = "channel_ingress_route"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "status IN ('active', 'disabled', 'revoked')",
            name="ck_channel_ingress_route_status",
        ),
    )

    public_callback_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    binding_id: Mapped[str] = mapped_column(String(64), unique=True)
    channel_type: Mapped[str] = mapped_column(String(32), index=True)
    config_revision: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32), default="active", index=True)


class Session(Base, TimestampMixin):
    """Canonical session head and lease state."""

    __tablename__ = "session"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "app_id", "app_revision"],
            ["agent_app.tenant_id", "agent_app.app_id", "agent_app.revision"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
            ondelete="RESTRICT",
        ),
        Index("ix_session_lease", "lease_expires_at", "lease_owner"),
        CheckConstraint("state_version <= log_version", name="ck_session_state_watermark"),
        CheckConstraint(
            "scope IN ('private', 'group', 'group_member')",
            name="ck_session_scope",
        ),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    app_id: Mapped[str] = mapped_column(String(64), index=True)
    app_revision: Mapped[int] = mapped_column(Integer)
    binding_id: Mapped[str] = mapped_column(String(64), index=True)
    scope: Mapped[str] = mapped_column(String(32))
    principal_id: Mapped[str] = mapped_column(String(256), index=True)
    next_inbox_seq: Mapped[int] = mapped_column(BigInteger, default=1)
    log_version: Mapped[int] = mapped_column(BigInteger, default=0)
    state_version: Mapped[int] = mapped_column(BigInteger, default=0)
    fencing_token: Mapped[int] = mapped_column(BigInteger, default=0)
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    state: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class InboxMessage(Base):
    """Durable, deduplicated external delivery."""

    __tablename__ = "inbox_message"
    __table_args__ = (
        UniqueConstraint("tenant_id", "binding_id", "external_delivery_id"),
        UniqueConstraint("tenant_id", "session_id", "accepted_seq"),
        UniqueConstraint("tenant_id", "inbox_id"),
        ForeignKeyConstraint(
            ["tenant_id", "config_revision"],
            ["tenant_config_revision.tenant_id", "tenant_config_revision.revision"],
            name="fk_inbox_config_revision",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["session.tenant_id", "session.session_id"],
            ondelete="RESTRICT",
        ),
        Index("ix_inbox_dispatch", "status", "received_at"),
        CheckConstraint(
            "status IN ('received', 'running', 'succeeded', 'retry_wait', "
            "'dead_letter', 'reconcile')",
            name="ck_inbox_status",
        ),
    )

    inbox_id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    binding_id: Mapped[str] = mapped_column(String(64), index=True)
    session_id: Mapped[str] = mapped_column(String(128), index=True)
    config_revision: Mapped[int] = mapped_column(Integer)
    accepted_seq: Mapped[int] = mapped_column(BigInteger)
    external_delivery_id: Mapped[str] = mapped_column(String(256))
    payload_hash: Mapped[str] = mapped_column(String(64))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    status: Mapped[str] = mapped_column(String(32), default="received", index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    claim_fencing_token: Mapped[int | None] = mapped_column(BigInteger)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    request_id: Mapped[str] = mapped_column(String(64), index=True)
    trace_id: Mapped[str] = mapped_column(String(64), index=True)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_type: Mapped[str | None] = mapped_column(String(128))


class AgentRun(Base):
    """One logical Agent turn."""

    __tablename__ = "agent_run"
    __table_args__ = (
        UniqueConstraint("tenant_id", "request_id"),
        UniqueConstraint("tenant_id", "inbox_id"),
        UniqueConstraint("tenant_id", "run_id"),
        ForeignKeyConstraint(
            ["tenant_id", "inbox_id"],
            ["inbox_message.tenant_id", "inbox_message.inbox_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["session.tenant_id", "session.session_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "app_id", "app_revision"],
            ["agent_app.tenant_id", "agent_app.app_id", "agent_app.revision"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "status IN ('pending', 'running', 'succeeded', 'retry_wait', "
            "'failed_final', 'reconcile')",
            name="ck_agent_run_status",
        ),
    )

    run_id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    inbox_id: Mapped[str] = mapped_column(String(32), index=True)
    session_id: Mapped[str] = mapped_column(String(128), index=True)
    request_id: Mapped[str] = mapped_column(String(64))
    invocation_id: Mapped[str | None] = mapped_column(String(128))
    trace_id: Mapped[str] = mapped_column(String(64), index=True)
    app_id: Mapped[str] = mapped_column(String(64))
    app_revision: Mapped[int] = mapped_column(Integer)
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    attempt_no: Mapped[int] = mapped_column(Integer, default=0)
    claim_fencing_token: Mapped[int | None] = mapped_column(BigInteger)
    start_version: Mapped[int] = mapped_column(BigInteger, default=0)
    last_seq: Mapped[int] = mapped_column(BigInteger, default=0)
    final_event_id: Mapped[str | None] = mapped_column(String(128))
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_type: Mapped[str | None] = mapped_column(String(128))
    error_code: Mapped[str | None] = mapped_column(String(128))


class SessionEvent(Base):
    """Immutable canonical event."""

    __tablename__ = "session_event"
    __table_args__ = (
        UniqueConstraint("tenant_id", "session_id", "seq"),
        UniqueConstraint("tenant_id", "event_id"),
        UniqueConstraint("tenant_id", "run_id", "event_key"),
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["session.tenant_id", "session.session_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "run_id"],
            ["agent_run.tenant_id", "agent_run.run_id"],
            ondelete="RESTRICT",
        ),
        Index("ix_session_event_replay", "tenant_id", "session_id", "seq"),
        CheckConstraint(
            "visibility IN ('staged', 'committed', 'aborted')",
            name="ck_event_visibility",
        ),
    )

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(64))
    session_id: Mapped[str] = mapped_column(String(128))
    seq: Mapped[int] = mapped_column(BigInteger)
    event_id: Mapped[str] = mapped_column(String(128))
    run_id: Mapped[str] = mapped_column(String(32), index=True)
    attempt_no: Mapped[int] = mapped_column(Integer)
    event_key: Mapped[str] = mapped_column(String(256))
    event_type: Mapped[str] = mapped_column(String(64))
    visibility: Mapped[str] = mapped_column(String(16), default="staged", index=True)
    role: Mapped[str | None] = mapped_column(String(32))
    content_ref: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    payload_hash: Mapped[str] = mapped_column(String(64))
    state_delta: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    framework_event_id: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class EventObject(Base):
    """Immutable tenant-scoped ciphertext required for canonical event replay."""

    __tablename__ = "event_object"
    __table_args__ = (
        Index("ix_event_object_tenant_created", "tenant_id", "created_at"),
        CheckConstraint("size_bytes > 0", name="ck_event_object_size"),
        CheckConstraint(
            "length(ciphertext_sha256) = 64",
            name="ck_event_object_digest_length",
        ),
    )

    tenant_id: Mapped[str] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        primary_key=True,
    )
    object_key: Mapped[str] = mapped_column(String(160), primary_key=True)
    ciphertext: Mapped[str] = mapped_column(Text)
    ciphertext_sha256: Mapped[str] = mapped_column(String(64))
    size_bytes: Mapped[int] = mapped_column(BigInteger)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class ProjectionJob(Base):
    """Durable, fenced Summary/Memory projection work for one committed run."""

    __tablename__ = "projection_job"
    __table_args__ = (
        UniqueConstraint("tenant_id", "run_id"),
        ForeignKeyConstraint(
            ["tenant_id", "run_id"],
            ["agent_run.tenant_id", "agent_run.run_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["session.tenant_id", "session.session_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "config_revision"],
            ["tenant_config_revision.tenant_id", "tenant_config_revision.revision"],
            ondelete="RESTRICT",
        ),
        Index("ix_projection_job_claim", "status", "next_attempt_at", "created_at"),
        CheckConstraint(
            "status IN ('pending', 'processing', 'retry_wait', 'succeeded', 'dead_letter')",
            name="ck_projection_job_status",
        ),
        CheckConstraint("through_seq >= 0", name="ck_projection_job_through_seq"),
        CheckConstraint("attempt_count >= 0", name="ck_projection_job_attempt_count"),
        CheckConstraint("fencing_token >= 0", name="ck_projection_job_fencing_token"),
    )

    job_id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    run_id: Mapped[str] = mapped_column(String(32), index=True)
    session_id: Mapped[str] = mapped_column(String(128), index=True)
    config_revision: Mapped[int] = mapped_column(Integer)
    through_seq: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    fencing_token: Mapped[int] = mapped_column(BigInteger, default=0)
    claimed_by: Mapped[str | None] = mapped_column(String(128))
    claim_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    result_hash: Mapped[str | None] = mapped_column(String(64))
    last_error_type: Mapped[str | None] = mapped_column(String(128))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class SessionSummary(Base, TimestampMixin):
    """Monotonic summary projection."""

    __tablename__ = "session_summary"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["session.tenant_id", "session.session_id"],
            ondelete="RESTRICT",
        ),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    session_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    through_seq: Mapped[int] = mapped_column(BigInteger)
    content: Mapped[str] = mapped_column(Text)
    summarizer_version: Mapped[str] = mapped_column(String(64))


class MemoryRecord(Base, TimestampMixin):
    """Idempotent long-term memory projection."""

    __tablename__ = "memory_record"
    __table_args__ = (
        UniqueConstraint("tenant_id", "source_event_id", "extractor_version"),
        Index("ix_memory_query", "tenant_id", "principal_id", "record_version"),
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["session.tenant_id", "session.session_id"],
            ondelete="RESTRICT",
        ),
    )

    memory_id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        index=True,
    )
    principal_id: Mapped[str] = mapped_column(String(256))
    session_id: Mapped[str] = mapped_column(String(128))
    source_event_id: Mapped[str] = mapped_column(String(128))
    extractor_version: Mapped[str] = mapped_column(String(64))
    record_version: Mapped[int] = mapped_column(BigInteger)
    content: Mapped[str] = mapped_column(Text)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class ToolEffect(Base, TimestampMixin):
    """Idempotency ledger for side-effecting tools."""

    __tablename__ = "tool_effect"
    __table_args__ = (
        UniqueConstraint("tenant_id", "idempotency_key"),
        ForeignKeyConstraint(
            ["tenant_id", "run_id"],
            ["agent_run.tenant_id", "agent_run.run_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["session.tenant_id", "session.session_id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "status IN ('reserved', 'executing', 'succeeded', 'retry_wait', "
            "'failed_final', 'unknown')",
            name="ck_tool_effect_status",
        ),
    )

    effect_id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        index=True,
    )
    run_id: Mapped[str] = mapped_column(String(32), index=True)
    session_id: Mapped[str] = mapped_column(String(128), index=True)
    idempotency_key: Mapped[str] = mapped_column(String(256))
    tool_name: Mapped[str] = mapped_column(String(128))
    tool_version: Mapped[str] = mapped_column(String(64), default="1")
    effect_class: Mapped[str] = mapped_column(String(32), default="read")
    args_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), default="reserved", index=True)
    execution_token: Mapped[str] = mapped_column(String(64), default=new_id)
    execution_owner: Mapped[str | None] = mapped_column(String(128))
    execution_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    downstream_key: Mapped[str | None] = mapped_column(String(256))
    downstream_id: Mapped[str | None] = mapped_column(String(256))
    result_hash: Mapped[str | None] = mapped_column(String(64))
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    result_ref: Mapped[str | None] = mapped_column(Text)
    error_type: Mapped[str | None] = mapped_column(String(128))


class ReplyOutbox(Base):
    """Durable outbound delivery intent."""

    __tablename__ = "reply_outbox"
    __table_args__ = (
        UniqueConstraint("tenant_id", "reply_id", "part_no"),
        ForeignKeyConstraint(
            ["tenant_id", "run_id"],
            ["agent_run.tenant_id", "agent_run.run_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["session.tenant_id", "session.session_id"],
            ondelete="RESTRICT",
        ),
        Index("ix_outbox_claim", "status", "next_retry_at"),
        CheckConstraint(
            "status IN ('pending', 'sending', 'sent', 'retry_wait', 'dead_letter', 'unknown')",
            name="ck_outbox_status",
        ),
    )

    outbox_id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        index=True,
    )
    run_id: Mapped[str] = mapped_column(String(32), index=True)
    binding_id: Mapped[str] = mapped_column(String(64), index=True)
    session_id: Mapped[str] = mapped_column(String(128), index=True)
    reply_id: Mapped[str] = mapped_column(String(128))
    part_no: Mapped[int] = mapped_column(Integer, default=0)
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    payload_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), default="pending", index=True)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_retry_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
    claimed_by: Mapped[str | None] = mapped_column(String(128))
    claim_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivery_token: Mapped[str | None] = mapped_column(String(64))
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_type: Mapped[str | None] = mapped_column(String(128))
    external_message_id: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class KnowledgeDocument(Base, TimestampMixin):
    """Canonical metadata for versioned knowledge content."""

    __tablename__ = "knowledge_document"
    __table_args__ = (UniqueConstraint("tenant_id", "document_id", "version"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        index=True,
    )
    document_id: Mapped[str] = mapped_column(String(128))
    version: Mapped[int] = mapped_column(BigInteger)
    content_hash: Mapped[str] = mapped_column(String(64))
    object_ref: Mapped[str] = mapped_column(Text)
    indexed_version: Mapped[int] = mapped_column(BigInteger, default=0)
    tombstone: Mapped[bool] = mapped_column(Boolean, default=False)


class Artifact(Base, TimestampMixin):
    """Tenant-scoped artifact metadata."""

    __tablename__ = "artifact"
    __table_args__ = (UniqueConstraint("tenant_id", "artifact_id", "version"),)

    id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(
        ForeignKey("tenant.tenant_id", ondelete="RESTRICT"),
        index=True,
    )
    artifact_id: Mapped[str] = mapped_column(String(128))
    version: Mapped[int] = mapped_column(BigInteger)
    session_id: Mapped[str | None] = mapped_column(String(128))
    object_ref: Mapped[str] = mapped_column(Text)
    content_hash: Mapped[str] = mapped_column(String(64))
    media_type: Mapped[str] = mapped_column(String(128))
    size_bytes: Mapped[int] = mapped_column(BigInteger)


class AuditLog(Base):
    """Append-only security and cost decision record."""

    __tablename__ = "audit_log"
    __table_args__ = (
        Index("ix_audit_tenant_time", "tenant_id", "created_at"),
        Index("ix_audit_trace", "trace_id"),
    )

    audit_id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    channel: Mapped[str] = mapped_column(String(32))
    user_id: Mapped[str] = mapped_column(String(256))
    session_id: Mapped[str] = mapped_column(String(128))
    agent_name: Mapped[str] = mapped_column(String(128))
    tool_name: Mapped[str | None] = mapped_column(String(128))
    decision: Mapped[str] = mapped_column(String(64))
    reason: Mapped[str | None] = mapped_column(String(256))
    latency_ms: Mapped[int] = mapped_column(Integer, default=0)
    error_type: Mapped[str | None] = mapped_column(String(128))
    cost_micros: Mapped[int] = mapped_column(BigInteger, default=0)
    trace_id: Mapped[str] = mapped_column(String(64))
    request_id: Mapped[str] = mapped_column(String(64))
    invocation_id: Mapped[str | None] = mapped_column(String(128))
    action: Mapped[str] = mapped_column(String(128))
    resource: Mapped[str] = mapped_column(String(256))
    config_revision: Mapped[int] = mapped_column(Integer)
    policy_revision: Mapped[int] = mapped_column(Integer)
    idempotency_key: Mapped[str | None] = mapped_column(String(256))
    detail: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    record_hash: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)


class ScopedState(Base, TimestampMixin):
    """Versioned app- and user-scope state used by the tRPC session wrapper."""

    __tablename__ = "scoped_state"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "app_id", "app_revision"],
            ["agent_app.tenant_id", "agent_app.app_id", "agent_app.revision"],
            ondelete="RESTRICT",
        ),
        CheckConstraint("scope IN ('app', 'user')", name="ck_scoped_state_scope"),
    )

    tenant_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    app_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    app_revision: Mapped[int] = mapped_column(Integer)
    scope: Mapped[str] = mapped_column(String(16), primary_key=True)
    subject_id: Mapped[str] = mapped_column(String(256), primary_key=True)
    version: Mapped[int] = mapped_column(BigInteger, default=0)
    state: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)


class ChannelReplyCredential(Base):
    """Encrypted, single-use credential for asynchronous channel replies."""

    __tablename__ = "channel_reply_credential"
    __table_args__ = (
        UniqueConstraint("tenant_id", "inbox_id", "credential_kind"),
        ForeignKeyConstraint(
            ["tenant_id", "inbox_id"],
            ["inbox_message.tenant_id", "inbox_message.inbox_id"],
            ondelete="RESTRICT",
        ),
        CheckConstraint(
            "status IN ('active', 'consumed', 'expired', 'unknown')",
            name="ck_channel_reply_credential_status",
        ),
    )

    credential_id: Mapped[str] = mapped_column(String(32), primary_key=True, default=new_id)
    tenant_id: Mapped[str] = mapped_column(String(64), index=True)
    inbox_id: Mapped[str] = mapped_column(String(32), index=True)
    credential_kind: Mapped[str] = mapped_column(String(32))
    ciphertext: Mapped[str] = mapped_column(Text)
    ciphertext_hash: Mapped[str] = mapped_column(String(64))
    status: Mapped[str] = mapped_column(String(32), default="active", index=True)
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utc_now)
