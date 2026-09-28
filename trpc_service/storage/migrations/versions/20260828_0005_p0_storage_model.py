"""Promote the runtime schema to the production P0 storage model.

Revision ID: 20260828_0005
Revises: 20260827_0004
Create Date: 2026-08-28
"""

from collections.abc import Sequence

from alembic import op
from sqlalchemy.dialects import postgresql
import sqlalchemy as sa

revision: str = "20260828_0005"
down_revision: str | None = "20260827_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _timestamps() -> tuple[sa.Column[object], sa.Column[object]]:
    """Return timestamp columns shared by mutable target tables."""

    return (
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )


def _create_target_tables() -> None:
    """Create the clean P0 schema before copying legacy runtime data."""

    op.create_unique_constraint(
        "uq_channel_binding_tenant_id",
        "channel_binding",
        ["tenant_id", "binding_id"],
    )
    op.create_table(
        "agent_session",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.Column("last_event_seq", sa.BigInteger(), nullable=False),
        sa.Column("last_fencing_token", sa.BigInteger(), nullable=True),
        sa.Column("state", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column(
            "last_activity_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        *_timestamps(),
        sa.CheckConstraint("version >= 0", name="agent_session_version_nonnegative"),
        sa.CheckConstraint(
            "last_event_seq >= 0",
            name="agent_session_event_seq_nonnegative",
        ),
        sa.CheckConstraint(
            "status IN ('ACTIVE', 'CLOSED', 'EXPIRED', 'DELETED')",
            name="agent_session_status",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "agent_app_id",
            "session_id",
            name="pk_agent_session",
        ),
    )
    op.create_index(
        "ix_agent_session_scope_activity",
        "agent_session",
        ["tenant_id", "agent_app_id", "status", "last_activity_at"],
    )
    op.create_table(
        "session_event",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("seq_no", sa.BigInteger(), nullable=False),
        sa.Column("event_id", sa.String(length=255), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("actor_type", sa.String(length=40), nullable=True),
        sa.Column("actor_principal_id", sa.String(length=255), nullable=True),
        sa.Column("request_id", sa.String(length=128), nullable=True),
        sa.Column("trace_id", sa.String(length=128), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("committed_version", sa.BigInteger(), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.CheckConstraint("seq_no > 0", name="session_event_seq_positive"),
        sa.CheckConstraint(
            "committed_version > 0",
            name="session_event_version_positive",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "agent_app_id", "session_id"],
            ["agent_session.tenant_id", "agent_session.agent_app_id", "agent_session.session_id"],
            name="fk_session_event_session",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "agent_app_id",
            "session_id",
            "seq_no",
            name="pk_session_event",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "event_id",
            name="uq_session_event_scope_event",
        ),
    )
    op.create_index(
        "ix_session_event_scope_request",
        "session_event",
        ["tenant_id", "agent_app_id", "request_id"],
    )
    op.create_table(
        "inbox_message",
        sa.Column("inbox_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("binding_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("external_message_id", sa.String(length=255), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column("id_source", sa.String(length=20), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("trace_id", sa.String(length=128), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_owner", sa.String(length=255), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reply_outbox_id", sa.String(length=255), nullable=True),
        sa.Column("committed_session_version", sa.BigInteger(), nullable=True),
        sa.Column("result_state", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("last_error_code", sa.String(length=100), nullable=True),
        sa.Column("last_error_summary", sa.Text(), nullable=True),
        sa.Column("received_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        *_timestamps(),
        sa.CheckConstraint(
            "attempt_count >= 1",
            name="inbox_message_attempt_positive",
        ),
        sa.CheckConstraint(
            "status IN ('RECEIVED', 'QUEUED', 'RUNNING', 'SUCCEEDED', "
            "'RETRYABLE_FAILED', 'PERMANENT_FAILED', 'REPLIED')",
            name="inbox_message_status",
        ),
        sa.CheckConstraint(
            "id_source IN ('PROVIDER', 'DERIVED', 'LEGACY')",
            name="inbox_message_id_source",
        ),
        sa.PrimaryKeyConstraint("inbox_id", name="pk_inbox_message"),
        sa.UniqueConstraint(
            "tenant_id",
            "binding_id",
            "external_message_id",
            name="uq_inbox_message_provider_identity",
        ),
        sa.UniqueConstraint("tenant_id", "request_id", name="uq_inbox_message_request"),
        sa.UniqueConstraint("tenant_id", "inbox_id", name="uq_inbox_message_tenant_id"),
    )
    op.create_index(
        "ix_inbox_message_retry",
        "inbox_message",
        ["status", "next_attempt_at"],
        postgresql_where=sa.text("status IN ('RECEIVED', 'QUEUED', 'RETRYABLE_FAILED')"),
    )
    op.create_index(
        "ix_inbox_message_scope_session",
        "inbox_message",
        ["tenant_id", "agent_app_id", "session_id"],
    )
    op.create_table(
        "runner_request",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("inbox_id", sa.Uuid(), nullable=True),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("config_version", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("next_step_index", sa.Integer(), nullable=False),
        sa.Column("next_tool_call_index", sa.Integer(), nullable=False),
        sa.Column("pending_tool_call_id", sa.String(length=255), nullable=True),
        sa.Column("state_ref", sa.String(length=500), nullable=True),
        sa.Column("fencing_token", sa.BigInteger(), nullable=True),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.Text(), nullable=True),
        *_timestamps(),
        sa.CheckConstraint(
            "attempt_count >= 1",
            name="runner_request_attempt_positive",
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'RUNNING', 'COMPLETED', 'RETRYABLE_FAILED', "
            "'PERMANENT_FAILED', 'CANCELLED', 'UNKNOWN')",
            name="runner_request_status",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "inbox_id"],
            ["inbox_message.tenant_id", "inbox_message.inbox_id"],
            name="fk_runner_request_tenant_inbox",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("tenant_id", "request_id", name="pk_runner_request"),
        sa.UniqueConstraint("tenant_id", "inbox_id", name="uq_runner_request_inbox"),
    )
    op.create_index(
        "ix_runner_request_scope_session",
        "runner_request",
        ["tenant_id", "agent_app_id", "session_id"],
    )
    op.create_table(
        "outbox_message",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("outbox_id", sa.String(length=255), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=True),
        sa.Column("session_id", sa.String(length=255), nullable=True),
        sa.Column("category", sa.String(length=100), nullable=False),
        sa.Column("destination", sa.String(length=100), nullable=False),
        sa.Column("binding_id", sa.Uuid(), nullable=True),
        sa.Column("sequence_no", sa.Integer(), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False),
        sa.Column("priority", sa.Integer(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_owner", sa.String(length=255), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("external_receipt_id", sa.String(length=255), nullable=True),
        sa.Column("last_error_code", sa.String(length=100), nullable=True),
        sa.Column("last_error_summary", sa.Text(), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        *_timestamps(),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name="outbox_message_attempt_nonnegative",
        ),
        sa.CheckConstraint(
            "sequence_no >= 0",
            name="outbox_message_sequence_nonnegative",
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'DELIVERED', 'RETRYABLE_FAILED', "
            "'UNKNOWN', 'DEAD_LETTER', 'CANCELLED')",
            name="outbox_message_status",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "agent_app_id",
            "outbox_id",
            name="pk_outbox_message",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "category",
            "idempotency_key",
            name="uq_outbox_message_idempotency",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "category",
            "request_id",
            "sequence_no",
            name="uq_outbox_message_request_sequence",
        ),
    )
    op.create_index(
        "ix_outbox_message_claim",
        "outbox_message",
        ["status", "next_attempt_at", "priority"],
        postgresql_where=sa.text("status IN ('PENDING', 'RETRYABLE_FAILED')"),
    )
    op.create_index(
        "ix_outbox_message_scope_request",
        "outbox_message",
        ["tenant_id", "agent_app_id", "request_id"],
    )
    op.create_table(
        "outbox_attempt",
        sa.Column("attempt_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("outbox_id", sa.String(length=255), nullable=False),
        sa.Column("attempt_no", sa.Integer(), nullable=False),
        sa.Column("worker_id", sa.String(length=255), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("result", sa.String(length=30), nullable=False),
        sa.Column("provider_status_code", sa.String(length=50), nullable=True),
        sa.Column("external_receipt_id", sa.String(length=255), nullable=True),
        sa.Column("error_summary", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["tenant_id", "agent_app_id", "outbox_id"],
            ["outbox_message.tenant_id", "outbox_message.agent_app_id", "outbox_message.outbox_id"],
            name="fk_outbox_attempt_message",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("attempt_id", name="pk_outbox_attempt"),
        sa.UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "outbox_id",
            "attempt_no",
            name="uq_outbox_attempt_number",
        ),
    )
    op.create_table(
        "memory_record",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("memory_id", sa.String(length=255), nullable=False),
        sa.Column("principal_id", sa.String(length=255), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_timestamps(),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "agent_app_id",
            "memory_id",
            name="pk_memory_record",
        ),
    )
    op.create_index(
        "ix_memory_record_scope_principal",
        "memory_record",
        ["tenant_id", "agent_app_id", "principal_id"],
    )
    op.create_table(
        "session_summary",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("source_event_seq", sa.BigInteger(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("attributes", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        *_timestamps(),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "agent_app_id",
            "session_id",
            name="pk_session_summary",
        ),
    )
    op.create_table(
        "audit_log",
        sa.Column("audit_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("binding_id", sa.Uuid(), nullable=True),
        sa.Column("principal_id", sa.String(length=255), nullable=True),
        sa.Column("session_id", sa.String(length=255), nullable=True),
        sa.Column("request_id", sa.String(length=128), nullable=True),
        sa.Column("trace_id", sa.String(length=128), nullable=True),
        sa.Column("action", sa.String(length=100), nullable=False),
        sa.Column("decision", sa.String(length=100), nullable=False),
        sa.Column("policy_version", sa.String(length=100), nullable=True),
        sa.Column("tool_name", sa.String(length=120), nullable=True),
        sa.Column("latency_ms", sa.BigInteger(), nullable=True),
        sa.Column("error_type", sa.String(length=120), nullable=True),
        sa.Column("cost_amount", sa.String(length=64), nullable=True),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("details_redacted", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("immutable", sa.Boolean(), nullable=False),
        sa.PrimaryKeyConstraint("audit_id", name="pk_audit_log"),
    )
    op.create_index(
        "ix_audit_log_scope_occurred",
        "audit_log",
        ["tenant_id", "agent_app_id", "occurred_at"],
    )
    op.create_index("ix_audit_log_scope_trace", "audit_log", ["tenant_id", "trace_id"])
    op.create_index(
        "ix_audit_log_scope_session",
        "audit_log",
        ["tenant_id", "session_id", "occurred_at"],
    )


def _copy_legacy_data() -> None:
    """Copy every legacy row, quarantining unknown binding identity with a sentinel."""

    uuid_pattern = "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    op.execute(
        sa.text("""
        INSERT INTO agent_session (
            tenant_id, agent_app_id, session_id, status, version, last_event_seq,
            last_fencing_token, state, last_activity_at, expires_at, created_at, updated_at
        )
        SELECT s.tenant_id, s.agent_app_id, s.session_id, 'ACTIVE', s.version,
               COALESCE((SELECT MAX(e.ordinal) FROM runtime_session_event e
                         WHERE e.tenant_id = s.tenant_id
                           AND e.agent_app_id = s.agent_app_id
                           AND e.session_id = s.session_id), 0),
               s.last_fencing_token, s.state::jsonb, s.updated_at, NULL,
               s.created_at, s.updated_at
        FROM runtime_session s
    """))
    op.execute(
        sa.text("""
        INSERT INTO session_event (
            tenant_id, agent_app_id, session_id, seq_no, event_id, event_type,
            actor_type, actor_principal_id, request_id, trace_id, occurred_at,
            created_at, committed_version, payload
        )
        SELECT tenant_id, agent_app_id, session_id, ordinal, event_id, event_type,
               NULL, NULL, NULL, NULL, occurred_at, occurred_at,
               committed_version, payload::jsonb
        FROM runtime_session_event
    """))
    op.execute(
        sa.text(f"""
        INSERT INTO inbox_message (
            inbox_id, tenant_id, binding_id, agent_app_id, external_message_id,
            payload_hash, id_source, request_id, trace_id, session_id, status,
            attempt_count, next_attempt_at, lease_owner, lease_until,
            reply_outbox_id, committed_session_version, result_state,
            last_error_code, last_error_summary, received_at, started_at,
            completed_at, expires_at, created_at, updated_at
        )
        SELECT md5(tenant_id::text || agent_app_id::text || message_id)::uuid,
               tenant_id,
               CASE WHEN split_part(message_id, ':', 1) ~* '{uuid_pattern}'
                    THEN split_part(message_id, ':', 1)::uuid
                    ELSE '00000000-0000-0000-0000-000000000000'::uuid END,
               agent_app_id,
               CASE WHEN position(':' in message_id) > 0
                    THEN substring(message_id from position(':' in message_id) + 1)
                    ELSE message_id END,
               md5(message_id) || md5('legacy:' || message_id),
               'LEGACY',
               'legacy-' || md5(tenant_id::text || agent_app_id::text || message_id),
               'legacy-' || md5('trace:' || tenant_id::text || agent_app_id::text || message_id),
               session_id, 'SUCCEEDED', 1, NULL, NULL, NULL, NULL,
               session_version, session_state::jsonb, NULL, NULL,
               created_at, created_at, updated_at, NULL, created_at, updated_at
        FROM runtime_inbox
    """))
    op.execute(
        sa.text("""
        INSERT INTO runner_request (
            tenant_id, request_id, agent_app_id, inbox_id, session_id,
            config_version, status, next_step_index, next_tool_call_index,
            pending_tool_call_id, state_ref, fencing_token, attempt_count,
            started_at, completed_at, last_error, created_at, updated_at
        )
        SELECT tenant_id, left(checkpoint_id, 128), agent_app_id, NULL, session_id,
               1, 'COMPLETED', 0, 0, NULL, NULL, NULL, 1,
               created_at, updated_at, NULL, created_at, updated_at
        FROM runtime_runner_checkpoint
    """))
    op.execute(
        sa.text(f"""
        INSERT INTO outbox_message (
            tenant_id, agent_app_id, outbox_id, request_id, session_id,
            category, destination, binding_id, sequence_no, idempotency_key,
            payload, status, priority, attempt_count, next_attempt_at,
            lease_owner, lease_until, external_receipt_id, last_error_code,
            last_error_summary, delivered_at, expires_at, created_at, updated_at
        )
        SELECT tenant_id, agent_app_id, outbox_id,
               'legacy-' || md5(tenant_id::text || agent_app_id::text || outbox_id),
               NULL,
               CASE WHEN category = 'channel.reply' THEN 'IM_REPLY' ELSE upper(category) END,
               COALESCE(payload::jsonb ->> 'channel_type', category),
               CASE WHEN split_part(idempotency_key, ':', 1) ~* '{uuid_pattern}'
                    THEN split_part(idempotency_key, ':', 1)::uuid ELSE NULL END,
               0, idempotency_key, payload::jsonb,
               CASE WHEN published_at IS NULL THEN 'UNKNOWN' ELSE 'DELIVERED' END,
               100, 0, NULL, NULL, NULL, NULL, NULL, NULL,
               published_at, NULL, created_at, updated_at
        FROM runtime_outbox
    """))
    op.execute(
        sa.text("""
        INSERT INTO memory_record (
            tenant_id, agent_app_id, memory_id, principal_id, content,
            attributes, created_at, updated_at
        )
        SELECT tenant_id, agent_app_id, memory_id, principal_id, content,
               attributes::jsonb, created_at, updated_at
        FROM runtime_memory
    """))
    op.execute(
        sa.text("""
        INSERT INTO session_summary (
            tenant_id, agent_app_id, session_id, source_event_seq, content,
            attributes, created_at, updated_at
        )
        SELECT tenant_id, agent_app_id, session_id, source_event_seq, content,
               attributes::jsonb, created_at, updated_at
        FROM runtime_summary
    """))
    op.execute(
        sa.text("""
        INSERT INTO audit_log (
            audit_id, tenant_id, agent_app_id, binding_id, principal_id,
            session_id, request_id, trace_id, action, decision, policy_version,
            tool_name, latency_ms, error_type, cost_amount, occurred_at,
            created_at, details_redacted, immutable
        )
        SELECT audit_id, tenant_id, agent_app_id, NULL, NULL, NULL, NULL, NULL,
               action, decision, NULL, NULL, NULL, NULL, NULL, occurred_at,
               occurred_at, attributes::jsonb, true
        FROM runtime_audit
    """))


def _add_forward_only_foreign_keys() -> None:
    """Protect new writes while retaining pre-existing orphan test records."""

    statements = (
        "ALTER TABLE agent_session ADD CONSTRAINT fk_agent_session_tenant_agent "
        "FOREIGN KEY (tenant_id, agent_app_id) "
        "REFERENCES agent_app (tenant_id, agent_app_id) ON DELETE RESTRICT NOT VALID",
        "ALTER TABLE inbox_message ADD CONSTRAINT fk_inbox_message_tenant_agent "
        "FOREIGN KEY (tenant_id, agent_app_id) "
        "REFERENCES agent_app (tenant_id, agent_app_id) ON DELETE RESTRICT NOT VALID",
        "ALTER TABLE inbox_message ADD CONSTRAINT fk_inbox_message_tenant_binding "
        "FOREIGN KEY (tenant_id, binding_id) "
        "REFERENCES channel_binding (tenant_id, binding_id) ON DELETE RESTRICT NOT VALID",
        "ALTER TABLE runner_request ADD CONSTRAINT fk_runner_request_tenant_agent "
        "FOREIGN KEY (tenant_id, agent_app_id) "
        "REFERENCES agent_app (tenant_id, agent_app_id) ON DELETE RESTRICT NOT VALID",
        "ALTER TABLE outbox_message ADD CONSTRAINT fk_outbox_message_tenant_agent "
        "FOREIGN KEY (tenant_id, agent_app_id) "
        "REFERENCES agent_app (tenant_id, agent_app_id) ON DELETE RESTRICT NOT VALID",
        "ALTER TABLE outbox_message ADD CONSTRAINT fk_outbox_message_tenant_binding "
        "FOREIGN KEY (tenant_id, binding_id) "
        "REFERENCES channel_binding (tenant_id, binding_id) ON DELETE RESTRICT NOT VALID",
        "ALTER TABLE audit_log ADD CONSTRAINT fk_audit_log_tenant_agent "
        "FOREIGN KEY (tenant_id, agent_app_id) "
        "REFERENCES agent_app (tenant_id, agent_app_id) ON DELETE RESTRICT NOT VALID",
    )
    for statement in statements:
        op.execute(statement)


def _drop_legacy_tables() -> None:
    """Remove old physical tables only after their rows have been copied."""

    op.drop_index("ix_runtime_audit_scope_occurred", table_name="runtime_audit")
    op.drop_table("runtime_audit")
    op.drop_table("runtime_summary")
    op.drop_index("ix_runtime_memory_scope_principal", table_name="runtime_memory")
    op.drop_table("runtime_memory")
    op.drop_table("runtime_outbox")
    op.drop_table("runtime_runner_checkpoint")
    op.drop_table("runtime_inbox")
    op.drop_table("runtime_session_event")
    op.drop_table("runtime_session")


def upgrade() -> None:
    """Create, copy, constrain and switch to the P0 storage schema."""

    _create_target_tables()
    _copy_legacy_data()
    _add_forward_only_foreign_keys()
    _drop_legacy_tables()


def downgrade() -> None:
    """Restore the legacy shape; new delivery-attempt detail is not representable."""

    op.create_table(
        "runtime_session",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
        sa.Column("last_fencing_token", sa.Integer(), nullable=True),
        sa.Column("state", sa.JSON(), nullable=False),
        *_timestamps(),
        sa.PrimaryKeyConstraint("tenant_id",
                                "agent_app_id",
                                "session_id",
                                name="pk_runtime_session"),
    )
    op.create_table(
        "runtime_session_event",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("ordinal", sa.Integer(), nullable=False),
        sa.Column("event_id", sa.String(length=255), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("committed_version", sa.Integer(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "agent_app_id",
            "session_id",
            "ordinal",
            name="pk_runtime_session_event",
        ),
        sa.UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "event_id",
            name="uq_runtime_session_event_scope_event",
        ),
    )
    op.create_table(
        "runtime_inbox",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("message_id", sa.String(length=255), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("session_version", sa.Integer(), nullable=False),
        sa.Column("session_state", sa.JSON(), nullable=False),
        *_timestamps(),
        sa.PrimaryKeyConstraint("tenant_id", "agent_app_id", "message_id", name="pk_runtime_inbox"),
    )
    op.create_table(
        "runtime_runner_checkpoint",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("checkpoint_id", sa.String(length=255), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("session_version", sa.Integer(), nullable=False),
        *_timestamps(),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "agent_app_id",
            "checkpoint_id",
            name="pk_runtime_runner_checkpoint",
        ),
    )
    op.create_table(
        "runtime_outbox",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("outbox_id", sa.String(length=255), nullable=False),
        sa.Column("category", sa.String(length=100), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        *_timestamps(),
        sa.PrimaryKeyConstraint("tenant_id", "agent_app_id", "outbox_id", name="pk_runtime_outbox"),
        sa.UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "idempotency_key",
            name="uq_runtime_outbox_scope_idempotency",
        ),
    )
    op.create_table(
        "runtime_memory",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("memory_id", sa.String(length=255), nullable=False),
        sa.Column("principal_id", sa.String(length=255), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("attributes", sa.JSON(), nullable=False),
        *_timestamps(),
        sa.PrimaryKeyConstraint("tenant_id", "agent_app_id", "memory_id", name="pk_runtime_memory"),
    )
    op.create_index(
        "ix_runtime_memory_scope_principal",
        "runtime_memory",
        ["tenant_id", "agent_app_id", "principal_id"],
    )
    op.create_table(
        "runtime_summary",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("source_event_seq", sa.Integer(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("attributes", sa.JSON(), nullable=False),
        *_timestamps(),
        sa.PrimaryKeyConstraint("tenant_id",
                                "agent_app_id",
                                "session_id",
                                name="pk_runtime_summary"),
    )
    op.create_table(
        "runtime_audit",
        sa.Column("audit_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("action", sa.String(length=100), nullable=False),
        sa.Column("decision", sa.String(length=100), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("attributes", sa.JSON(), nullable=False),
        sa.PrimaryKeyConstraint("audit_id", name="pk_runtime_audit"),
    )
    op.create_index(
        "ix_runtime_audit_scope_occurred",
        "runtime_audit",
        ["tenant_id", "agent_app_id", "occurred_at"],
    )

    op.execute(
        sa.text("""
        INSERT INTO runtime_session
        SELECT tenant_id, agent_app_id, session_id, version::integer,
               last_fencing_token::integer, state::json, created_at, updated_at
        FROM agent_session
    """))
    op.execute(
        sa.text("""
        INSERT INTO runtime_session_event
        SELECT tenant_id, agent_app_id, session_id, seq_no::integer, event_id,
               event_type, occurred_at, committed_version::integer, payload::json
        FROM session_event
    """))
    op.execute(
        sa.text("""
        INSERT INTO runtime_inbox
        SELECT tenant_id, agent_app_id,
               CASE WHEN id_source = 'LEGACY' THEN external_message_id
                    ELSE binding_id::text || ':' || external_message_id END,
               session_id, committed_session_version::integer, result_state::json,
               created_at, updated_at
        FROM inbox_message
        WHERE status IN ('SUCCEEDED', 'REPLIED')
          AND committed_session_version IS NOT NULL
          AND result_state IS NOT NULL
    """))
    op.execute(
        sa.text("""
        INSERT INTO runtime_runner_checkpoint
        SELECT tenant_id, agent_app_id, request_id, session_id,
               COALESCE((SELECT version FROM agent_session s
                         WHERE s.tenant_id = r.tenant_id
                           AND s.agent_app_id = r.agent_app_id
                           AND s.session_id = r.session_id), 0)::integer,
               created_at, updated_at
        FROM runner_request r
        WHERE status = 'COMPLETED'
    """))
    op.execute(
        sa.text("""
        INSERT INTO runtime_outbox
        SELECT tenant_id, agent_app_id, outbox_id, category, idempotency_key,
               payload::json, delivered_at, created_at, updated_at
        FROM outbox_message
    """))
    op.execute(
        sa.text("""
        INSERT INTO runtime_memory
        SELECT tenant_id, agent_app_id, memory_id, principal_id, content,
               attributes::json, created_at, updated_at
        FROM memory_record
    """))
    op.execute(
        sa.text("""
        INSERT INTO runtime_summary
        SELECT tenant_id, agent_app_id, session_id, source_event_seq::integer,
               content, attributes::json, created_at, updated_at
        FROM session_summary
    """))
    op.execute(
        sa.text("""
        INSERT INTO runtime_audit
        SELECT audit_id, tenant_id, agent_app_id, action, decision, occurred_at,
               details_redacted::json
        FROM audit_log
    """))

    op.drop_index("ix_audit_log_scope_session", table_name="audit_log")
    op.drop_index("ix_audit_log_scope_trace", table_name="audit_log")
    op.drop_index("ix_audit_log_scope_occurred", table_name="audit_log")
    op.drop_table("audit_log")
    op.drop_table("session_summary")
    op.drop_index("ix_memory_record_scope_principal", table_name="memory_record")
    op.drop_table("memory_record")
    op.drop_table("outbox_attempt")
    op.drop_index("ix_outbox_message_scope_request", table_name="outbox_message")
    op.drop_index("ix_outbox_message_claim", table_name="outbox_message")
    op.drop_table("outbox_message")
    op.drop_index("ix_runner_request_scope_session", table_name="runner_request")
    op.drop_table("runner_request")
    op.drop_index("ix_inbox_message_scope_session", table_name="inbox_message")
    op.drop_index("ix_inbox_message_retry", table_name="inbox_message")
    op.drop_table("inbox_message")
    op.drop_index("ix_session_event_scope_request", table_name="session_event")
    op.drop_table("session_event")
    op.drop_index("ix_agent_session_scope_activity", table_name="agent_session")
    op.drop_table("agent_session")
    op.drop_constraint("uq_channel_binding_tenant_id", "channel_binding", type_="unique")
