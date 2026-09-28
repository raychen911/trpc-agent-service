"""SQL tables for durable Agent execution and reliable delivery facts."""

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKeyConstraint,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

# Import referenced control-plane models so standalone metadata creation in
# tests contains every target of the tenant-aware foreign keys below.
from trpc_service.agent.models import AgentApp  # noqa: F401
from trpc_service.channels.models import ChannelBinding  # noqa: F401
from trpc_service.storage.orm import Base, TimestampMixin, utc_now

JSON_VALUE = JSON().with_variant(JSONB(), "postgresql")


class AgentSession(Base, TimestampMixin):
    """Current materialized state and concurrency watermarks of one Session."""

    __tablename__ = "agent_session"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_agent_session_tenant_agent",
            ondelete="RESTRICT",
        ),
        CheckConstraint("version >= 0", name="agent_session_version_nonnegative"),
        CheckConstraint("last_event_seq >= 0", name="agent_session_event_seq_nonnegative"),
        CheckConstraint(
            "status IN ('ACTIVE', 'CLOSED', 'EXPIRED', 'DELETED')",
            name="agent_session_status",
        ),
        Index(
            "ix_agent_session_scope_activity",
            "tenant_id",
            "agent_app_id",
            "status",
            "last_activity_at",
        ),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    agent_app_id: Mapped[UUID] = mapped_column(primary_key=True)
    session_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="ACTIVE")
    version: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    last_event_seq: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    last_fencing_token: Mapped[int | None] = mapped_column(BigInteger)
    state: Mapped[dict[str, object]] = mapped_column(JSON_VALUE, nullable=False, default=dict)
    last_activity_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        server_default=func.now(),
    )
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class SessionExecutionFence(Base, TimestampMixin):
    """Database authority for the currently valid Session execution lease."""

    __tablename__ = "session_execution_fence"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_session_execution_fence_tenant_agent",
            ondelete="RESTRICT",
        ),
        CheckConstraint("issued_token >= 0", name="fence_token_nonnegative"),
        Index("ix_session_execution_fence_lease", "lease_until"),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    agent_app_id: Mapped[UUID] = mapped_column(primary_key=True)
    session_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    issued_token: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
        server_default="0",
    )
    lease_owner: Mapped[str | None] = mapped_column(String(255))
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class SessionEventRow(Base):
    """Immutable Event appended at one Session sequence and commit version."""

    __tablename__ = "session_event"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id", "session_id"],
            ["agent_session.tenant_id", "agent_session.agent_app_id", "agent_session.session_id"],
            name="fk_session_event_session",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "event_id",
            name="uq_session_event_scope_event",
        ),
        CheckConstraint("seq_no > 0", name="session_event_seq_positive"),
        CheckConstraint("committed_version > 0", name="session_event_version_positive"),
        Index("ix_session_event_scope_request", "tenant_id", "agent_app_id", "request_id"),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    agent_app_id: Mapped[UUID] = mapped_column(primary_key=True)
    session_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    seq_no: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    event_id: Mapped[str] = mapped_column(String(255), nullable=False)
    event_type: Mapped[str] = mapped_column(String(100), nullable=False)
    actor_type: Mapped[str | None] = mapped_column(String(40))
    actor_principal_id: Mapped[str | None] = mapped_column(String(255))
    request_id: Mapped[str | None] = mapped_column(String(128))
    trace_id: Mapped[str | None] = mapped_column(String(128))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        server_default=func.now(),
    )
    committed_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(JSON_VALUE, nullable=False, default=dict)


class InboxMessageRow(Base, TimestampMixin):
    """Inbound deduplication fact and its recoverable processing lifecycle."""

    __tablename__ = "inbox_message"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_inbox_message_tenant_agent",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
            name="fk_inbox_message_tenant_binding",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "binding_id",
            "external_message_id",
            name="uq_inbox_message_provider_identity",
        ),
        UniqueConstraint("tenant_id", "request_id", name="uq_inbox_message_request"),
        UniqueConstraint("tenant_id", "inbox_id", name="uq_inbox_message_tenant_id"),
        CheckConstraint("attempt_count >= 1", name="inbox_message_attempt_positive"),
        CheckConstraint(
            "status IN ('RECEIVED', 'QUEUED', 'RUNNING', 'SUCCEEDED', "
            "'RETRYABLE_FAILED', 'PERMANENT_FAILED', 'REPLIED')",
            name="inbox_message_status",
        ),
        CheckConstraint(
            "id_source IN ('PROVIDER', 'DERIVED', 'LEGACY')",
            name="inbox_message_id_source",
        ),
        Index(
            "ix_inbox_message_retry",
            "status",
            "next_attempt_at",
            postgresql_where=text("status IN ('RECEIVED', 'QUEUED', 'RETRYABLE_FAILED')"),
        ),
        Index("ix_inbox_message_scope_session", "tenant_id", "agent_app_id", "session_id"),
    )

    inbox_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    binding_id: Mapped[UUID] = mapped_column(nullable=False)
    agent_app_id: Mapped[UUID] = mapped_column(nullable=False)
    external_message_id: Mapped[str] = mapped_column(String(255), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    id_source: Mapped[str] = mapped_column(String(20), nullable=False, default="PROVIDER")
    request_id: Mapped[str] = mapped_column(String(128), nullable=False)
    trace_id: Mapped[str] = mapped_column(String(128), nullable=False)
    session_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="RECEIVED")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_owner: Mapped[str | None] = mapped_column(String(255))
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    reply_outbox_id: Mapped[str | None] = mapped_column(String(255))
    committed_session_version: Mapped[int | None] = mapped_column(BigInteger)
    result_state: Mapped[dict[str, object] | None] = mapped_column(JSON_VALUE)
    last_error_code: Mapped[str | None] = mapped_column(String(100))
    last_error_summary: Mapped[str | None] = mapped_column(Text)
    received_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class RunnerRequestRow(Base, TimestampMixin):
    """Recoverable logical Runner request linked one-to-one with an Inbox."""

    __tablename__ = "runner_request"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_runner_request_tenant_agent",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "inbox_id"],
            ["inbox_message.tenant_id", "inbox_message.inbox_id"],
            name="fk_runner_request_tenant_inbox",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("tenant_id", "inbox_id", name="uq_runner_request_inbox"),
        CheckConstraint("attempt_count >= 1", name="runner_request_attempt_positive"),
        CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'RETRYABLE_FAILED', "
            "'PERMANENT_FAILED', 'CANCELLED', 'UNKNOWN')",
            name="runner_request_status",
        ),
        Index("ix_runner_request_scope_session", "tenant_id", "agent_app_id", "session_id"),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    request_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    agent_app_id: Mapped[UUID] = mapped_column(nullable=False)
    inbox_id: Mapped[UUID | None] = mapped_column()
    session_id: Mapped[str] = mapped_column(String(255), nullable=False)
    config_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="RUNNING")
    next_step_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_tool_call_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    pending_tool_call_id: Mapped[str | None] = mapped_column(String(255))
    state_ref: Mapped[str | None] = mapped_column(String(500))
    fencing_token: Mapped[int | None] = mapped_column(BigInteger)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)


class ToolCallLedgerRow(Base, TimestampMixin):
    """Idempotent intent and terminal outcome of one governed Tool call."""

    __tablename__ = "tool_call_ledger"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_tool_call_ledger_tenant_agent",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "request_id",
            "logical_call_index",
            name="uq_tool_call_ledger_position",
        ),
        CheckConstraint(
            "status IN ('PREPARED', 'SUCCEEDED', 'FAILED', 'UNKNOWN')",
            name="tool_call_ledger_status",
        ),
        CheckConstraint(
            "logical_call_index >= 0",
            name="tool_call_ledger_index_nonnegative",
        ),
        Index("ix_tool_call_ledger_request", "tenant_id", "request_id"),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    agent_app_id: Mapped[UUID] = mapped_column(primary_key=True)
    call_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    request_id: Mapped[str] = mapped_column(String(128), nullable=False)
    logical_call_index: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    kind: Mapped[str] = mapped_column(String(30), nullable=False)
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    resource: Mapped[str | None] = mapped_column(String(1000))
    intent_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PREPARED")
    result_payload: Mapped[dict[str, object] | None] = mapped_column(JSON_VALUE)
    error_summary: Mapped[str | None] = mapped_column(Text)
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class AgentTaskRow(Base, TimestampMixin):
    """Durable Gateway-to-Worker dispatch item shared across process nodes."""

    __tablename__ = "agent_task"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_agent_task_tenant_agent",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
            name="fk_agent_task_tenant_binding",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "binding_id",
            "external_message_id",
            name="uq_agent_task_provider_identity",
        ),
        CheckConstraint("attempt_count >= 0", name="agent_task_attempt_nonnegative"),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', "
            "'retryable_failed', 'permanent_failed')",
            name="agent_task_status",
        ),
        Index(
            "ix_agent_task_claim",
            "status",
            "next_attempt_at",
            "created_at",
            postgresql_where=text("status IN ('queued', 'running', 'retryable_failed')"),
        ),
        Index("ix_agent_task_routing", "routing_key", "created_at"),
    )

    task_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    agent_app_id: Mapped[UUID] = mapped_column(nullable=False)
    binding_id: Mapped[UUID] = mapped_column(nullable=False)
    external_message_id: Mapped[str] = mapped_column(String(255), nullable=False)
    request_id: Mapped[str] = mapped_column(String(128), nullable=False)
    trace_id: Mapped[str] = mapped_column(String(128), nullable=False)
    session_id: Mapped[str] = mapped_column(String(255), nullable=False)
    config_version: Mapped[int] = mapped_column(BigInteger, nullable=False)
    routing_key: Mapped[str] = mapped_column(String(64), nullable=False)
    payload_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    request_payload: Mapped[dict[str, object]] = mapped_column(
        JSON_VALUE,
        nullable=False,
    )
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="queued")
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    lease_token: Mapped[int] = mapped_column(BigInteger, nullable=False, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_owner: Mapped[str | None] = mapped_column(String(255))
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error_code: Mapped[str | None] = mapped_column(String(100))
    last_error_summary: Mapped[str | None] = mapped_column(Text)


class RunnerAttemptRow(Base):
    """One Worker attempt fenced by the durable Agent task lease generation."""

    __tablename__ = "runner_attempt"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_runner_attempt_tenant_agent",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["task_id"],
            ["agent_task.task_id"],
            name="fk_runner_attempt_task",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "task_id",
            "attempt_no",
            name="uq_runner_attempt_number",
        ),
        CheckConstraint("attempt_no > 0", name="runner_attempt_number_positive"),
        CheckConstraint("fencing_token > 0", name="runner_attempt_fence_positive"),
        CheckConstraint(
            "status IN ('RUNNING', 'SUCCEEDED', 'RETRYABLE_FAILED', "
            "'PERMANENT_FAILED', 'UNKNOWN')",
            name="runner_attempt_status",
        ),
        Index("ix_runner_attempt_task_fence", "tenant_id", "task_id", "fencing_token"),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    attempt_id: Mapped[str] = mapped_column(String(320), primary_key=True)
    agent_app_id: Mapped[UUID] = mapped_column(nullable=False)
    task_id: Mapped[UUID] = mapped_column(nullable=False)
    request_id: Mapped[str] = mapped_column(String(128), nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    fencing_token: Mapped[int] = mapped_column(BigInteger, nullable=False)
    node_id: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True),
                                                 nullable=False,
                                                 default=utc_now,
                                                 server_default=func.now())
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    error_summary: Mapped[str | None] = mapped_column(Text)


class RunnerCheckpointRow(Base):
    """Ordered recovery position owned by one fenced Runner attempt."""

    __tablename__ = "runner_checkpoint"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "attempt_id"],
            ["runner_attempt.tenant_id", "runner_attempt.attempt_id"],
            name="fk_runner_checkpoint_attempt",
            ondelete="RESTRICT",
        ),
        CheckConstraint("sequence_no > 0", name="runner_checkpoint_sequence_positive"),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    attempt_id: Mapped[str] = mapped_column(String(320), primary_key=True)
    sequence_no: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    stage: Mapped[str] = mapped_column(String(100), nullable=False)
    state_ref: Mapped[str | None] = mapped_column(String(1000))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)


class RuntimeNodeRow(Base):
    """Heartbeat record used to observe independently deployed service nodes."""

    __tablename__ = "runtime_node"
    __table_args__ = (
        CheckConstraint(
            "role IN ('api', 'worker', 'api_worker', 'channel', 'supervisor')",
            name="runtime_node_role",
        ),
        CheckConstraint(
            "status IN ('active', 'draining', 'stopped')",
            name="runtime_node_status",
        ),
        CheckConstraint("worker_concurrency >= 0", name="runtime_node_concurrency_nonnegative"),
        Index("ix_runtime_node_role_heartbeat", "role", "heartbeat_at"),
    )

    node_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    role: Mapped[str] = mapped_column(String(20), nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="active")
    worker_concurrency: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    heartbeat_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    stopped_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class WorkerPoolControlRow(Base, TimestampMixin):
    """Singleton desired capacity reconciled by the runtime supervisor."""

    __tablename__ = "worker_pool_control"
    __table_args__ = (
        CheckConstraint("desired_nodes BETWEEN 1 AND 64", name="worker_pool_desired_range"),
        CheckConstraint("generation >= 1", name="worker_pool_generation_positive"),
    )

    pool_name: Mapped[str] = mapped_column(String(100), primary_key=True)
    desired_nodes: Mapped[int] = mapped_column(Integer, nullable=False)
    generation: Mapped[int] = mapped_column(BigInteger, nullable=False, default=1)
    updated_by: Mapped[str] = mapped_column(String(255), nullable=False)


class OutboxMessageRow(Base, TimestampMixin):
    """Reliable asynchronous task committed with its source business facts."""

    __tablename__ = "outbox_message"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_outbox_message_tenant_agent",
            ondelete="RESTRICT",
        ),
        ForeignKeyConstraint(
            ["tenant_id", "binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
            name="fk_outbox_message_tenant_binding",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "category",
            "idempotency_key",
            name="uq_outbox_message_idempotency",
        ),
        UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "category",
            "request_id",
            "sequence_no",
            name="uq_outbox_message_request_sequence",
        ),
        CheckConstraint("attempt_count >= 0", name="outbox_message_attempt_nonnegative"),
        CheckConstraint("retry_count >= 0", name="outbox_message_retry_nonnegative"),
        CheckConstraint("sequence_no >= 0", name="outbox_message_sequence_nonnegative"),
        CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'DELIVERED', 'RETRYABLE_FAILED', "
            "'UNKNOWN', 'DEAD_LETTER', 'CANCELLED')",
            name="outbox_message_status",
        ),
        Index(
            "ix_outbox_message_claim",
            "status",
            "next_attempt_at",
            "priority",
            postgresql_where=text("status IN ('PENDING', 'RETRYABLE_FAILED')"),
        ),
        Index("ix_outbox_message_scope_request", "tenant_id", "agent_app_id", "request_id"),
        Index("ix_outbox_message_stream_order", "tenant_id", "agent_app_id", "binding_id",
              "session_id", "status", "created_at", "sequence_no"),
    )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    agent_app_id: Mapped[UUID] = mapped_column(primary_key=True)
    outbox_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    request_id: Mapped[str | None] = mapped_column(String(128))
    session_id: Mapped[str | None] = mapped_column(String(255))
    category: Mapped[str] = mapped_column(String(100), nullable=False)
    destination: Mapped[str] = mapped_column(String(100), nullable=False)
    binding_id: Mapped[UUID | None] = mapped_column()
    sequence_no: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    idempotency_key: Mapped[str] = mapped_column(String(255), nullable=False)
    payload: Mapped[dict[str, object]] = mapped_column(JSON_VALUE, nullable=False, default=dict)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="PENDING")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    retry_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_attempt_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_owner: Mapped[str | None] = mapped_column(String(255))
    lease_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    external_receipt_id: Mapped[str | None] = mapped_column(String(255))
    last_error_code: Mapped[str | None] = mapped_column(String(100))
    last_error_summary: Mapped[str | None] = mapped_column(Text)
    delivered_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxAttemptRow(Base):
    """One immutable provider attempt for an Outbox task."""

    __tablename__ = "outbox_attempt"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id", "outbox_id"],
            [
                "outbox_message.tenant_id",
                "outbox_message.agent_app_id",
                "outbox_message.outbox_id",
            ],
            name="fk_outbox_attempt_message",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "outbox_id",
            "attempt_no",
            name="uq_outbox_attempt_number",
        ),
    )

    attempt_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    agent_app_id: Mapped[UUID] = mapped_column(nullable=False)
    outbox_id: Mapped[str] = mapped_column(String(255), nullable=False)
    attempt_no: Mapped[int] = mapped_column(Integer, nullable=False)
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    result: Mapped[str] = mapped_column(String(30), nullable=False, default="PROCESSING")
    provider_status_code: Mapped[str | None] = mapped_column(String(50))
    external_receipt_id: Mapped[str | None] = mapped_column(String(255))
    error_summary: Mapped[str | None] = mapped_column(Text)


class MemoryRecordRow(Base, TimestampMixin):
    """Tenant-scoped long-term memory for one principal."""

    __tablename__ = "memory_record"
    __table_args__ = (Index("ix_memory_record_scope_principal", "tenant_id", "agent_app_id",
                            "principal_id"), )

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    agent_app_id: Mapped[UUID] = mapped_column(primary_key=True)
    memory_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    principal_id: Mapped[str] = mapped_column(String(255), nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    attributes: Mapped[dict[str, object]] = mapped_column(JSON_VALUE, nullable=False, default=dict)


class SessionSummaryRow(Base, TimestampMixin):
    """Newest rebuildable summary for one Session."""

    __tablename__ = "session_summary"

    tenant_id: Mapped[UUID] = mapped_column(primary_key=True)
    agent_app_id: Mapped[UUID] = mapped_column(primary_key=True)
    session_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    source_event_seq: Mapped[int] = mapped_column(BigInteger, nullable=False)
    content: Mapped[str] = mapped_column(Text, nullable=False)
    attributes: Mapped[dict[str, object]] = mapped_column(JSON_VALUE, nullable=False, default=dict)


class AuditLogRow(Base):
    """Append-only governance and execution decision with queryable dimensions."""

    __tablename__ = "audit_log"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_audit_log_tenant_agent",
            ondelete="RESTRICT",
        ),
        Index("ix_audit_log_scope_occurred", "tenant_id", "agent_app_id", "occurred_at"),
        Index("ix_audit_log_scope_trace", "tenant_id", "trace_id"),
        Index("ix_audit_log_scope_session", "tenant_id", "session_id", "occurred_at"),
    )

    audit_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    agent_app_id: Mapped[UUID] = mapped_column(nullable=False)
    binding_id: Mapped[UUID | None] = mapped_column()
    principal_id: Mapped[str | None] = mapped_column(String(255))
    session_id: Mapped[str | None] = mapped_column(String(255))
    request_id: Mapped[str | None] = mapped_column(String(128))
    trace_id: Mapped[str | None] = mapped_column(String(128))
    action: Mapped[str] = mapped_column(String(100), nullable=False)
    decision: Mapped[str] = mapped_column(String(100), nullable=False)
    policy_version: Mapped[str | None] = mapped_column(String(100))
    tool_name: Mapped[str | None] = mapped_column(String(120))
    latency_ms: Mapped[int | None] = mapped_column(BigInteger)
    error_type: Mapped[str | None] = mapped_column(String(120))
    cost_amount: Mapped[str | None] = mapped_column(String(64))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
    )
    details_redacted: Mapped[dict[str, Any]] = mapped_column(
        JSON_VALUE,
        nullable=False,
        default=dict,
    )
    immutable: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)


class ApprovalRequestRow(Base, TimestampMixin):
    """Durable human decision bound to one exact capability invocation."""

    __tablename__ = "approval_request"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_approval_request_tenant_agent",
            ondelete="RESTRICT",
        ),
        UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "tool_call_id",
            name="uq_approval_request_logical_call",
        ),
        UniqueConstraint("short_code", name="uq_approval_request_short_code"),
        CheckConstraint("risk_level IN (2, 3)", name="approval_request_risk_level"),
        CheckConstraint(
            "status IN ('PENDING', 'APPROVED', 'REJECTED', 'EXPIRED', "
            "'EXECUTING', 'EXECUTED', 'UNKNOWN')",
            name="approval_request_status",
        ),
        Index(
            "ix_approval_request_scope_status",
            "tenant_id",
            "agent_app_id",
            "status",
            "expires_at",
        ),
    )

    approval_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    short_code: Mapped[str] = mapped_column(String(8), nullable=False)
    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    agent_app_id: Mapped[UUID] = mapped_column(nullable=False)
    binding_id: Mapped[UUID] = mapped_column(nullable=False)
    principal_id: Mapped[str] = mapped_column(String(255), nullable=False)
    session_id: Mapped[str] = mapped_column(String(255), nullable=False)
    tool_call_id: Mapped[str] = mapped_column(String(255), nullable=False)
    capability_kind: Mapped[str] = mapped_column(String(30), nullable=False)
    capability_name: Mapped[str] = mapped_column(String(160), nullable=False)
    action: Mapped[str] = mapped_column(String(80), nullable=False)
    resource: Mapped[str | None] = mapped_column(String(500))
    arguments_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    # Trusted attachment identifiers are part of the immutable approval scope;
    # attachments supplied by a later confirmation message are ignored.
    artifact_refs: Mapped[list[str]] = mapped_column(JSON_VALUE, default=list, nullable=False)
    # Kept for databases that already applied revision 0019. New approval flows
    # deliberately leave this historical RAG-recovery field empty.
    operation_arguments: Mapped[dict[str, Any]] = mapped_column(JSON_VALUE,
                                                                default=dict,
                                                                nullable=False)
    config_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    logical_call_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    risk_level: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="PENDING")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    decided_by: Mapped[str | None] = mapped_column(String(255))
    execution_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    executed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class UsageLedgerRow(Base):
    """Append-only business usage fact; Prometheus is not a billing authority."""

    __tablename__ = "usage_ledger"
    __table_args__ = (
        ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_usage_ledger_tenant_agent",
            ondelete="RESTRICT",
        ),
        UniqueConstraint("tenant_id", "request_id", name="uq_usage_ledger_tenant_request"),
        CheckConstraint(
            "input_tokens >= 0 AND output_tokens >= 0 AND total_tokens >= 0",
            name="usage_ledger_tokens_nonnegative",
        ),
        CheckConstraint(
            "reserved_tokens >= 0",
            name="usage_ledger_reserved_tokens_nonnegative",
        ),
        CheckConstraint(
            "status IN ('reserved', 'completed', 'cancelled')",
            name="usage_ledger_status_allowed",
        ),
        Index("ix_usage_ledger_scope_occurred", "tenant_id", "occurred_at"),
    )

    usage_id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    tenant_id: Mapped[UUID] = mapped_column(nullable=False)
    agent_app_id: Mapped[UUID] = mapped_column(nullable=False)
    request_id: Mapped[str] = mapped_column(String(128), nullable=False)
    trace_id: Mapped[str] = mapped_column(String(128), nullable=False)
    model_provider: Mapped[str] = mapped_column(String(80), nullable=False)
    model_name: Mapped[str] = mapped_column(String(120), nullable=False)
    input_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False)
    output_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False)
    total_tokens: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reserved_tokens: Mapped[int] = mapped_column(
        BigInteger,
        nullable=False,
        default=0,
        server_default="0",
    )
    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="completed",
        server_default="completed",
    )
    estimated_cost: Mapped[Decimal] = mapped_column(Numeric(20, 8), nullable=False)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=utc_now,
        server_default=func.now(),
    )
