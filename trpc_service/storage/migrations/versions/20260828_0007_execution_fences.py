"""Add authoritative execution fences and scoped delivery constraints.

Revision ID: 20260828_0007
Revises: 20260828_0006
Create Date: 2026-08-28
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260828_0007"
down_revision: str | None = "20260828_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TIMESTAMP_TABLES = (
    "tenant",
    "agent_app",
    "channel_binding",
    "agent_session",
    "inbox_message",
    "runner_request",
    "outbox_message",
    "memory_record",
    "session_summary",
)


def upgrade() -> None:
    """Install Session fencing, partial worker indexes and database timestamps."""

    op.create_table(
        "session_execution_fence",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("issued_token", sa.BigInteger(), nullable=False, server_default="0"),
        sa.Column("lease_owner", sa.String(length=255), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
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
        sa.CheckConstraint(
            "issued_token >= 0",
            name="fence_token_nonnegative",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "agent_app_id",
            "session_id",
            name="pk_session_execution_fence",
        ),
    )
    op.create_index(
        "ix_session_execution_fence_lease",
        "session_execution_fence",
        ["lease_until"],
    )
    op.execute("""
        INSERT INTO session_execution_fence (
            tenant_id, agent_app_id, session_id, issued_token,
            lease_owner, lease_until, created_at, updated_at
        )
        SELECT tenant_id, agent_app_id, session_id,
               COALESCE(last_fencing_token, 0), NULL, NULL, created_at, updated_at
        FROM agent_session
    """)
    # Backfill first: NOT VALID preserves historical orphan rows, but like all
    # PostgreSQL foreign keys it still validates writes made after creation.
    op.execute("ALTER TABLE session_execution_fence "
               "ADD CONSTRAINT fk_session_execution_fence_tenant_agent "
               "FOREIGN KEY (tenant_id, agent_app_id) "
               "REFERENCES agent_app (tenant_id, agent_app_id) "
               "ON DELETE RESTRICT NOT VALID")

    op.drop_index("ix_inbox_message_retry", table_name="inbox_message")
    op.create_index(
        "ix_inbox_message_retry",
        "inbox_message",
        ["status", "next_attempt_at"],
        postgresql_where=sa.text("status IN ('RECEIVED', 'QUEUED', 'RETRYABLE_FAILED')"),
    )
    op.drop_index("ix_outbox_message_claim", table_name="outbox_message")
    op.create_index(
        "ix_outbox_message_claim",
        "outbox_message",
        ["status", "next_attempt_at", "priority"],
        postgresql_where=sa.text("status IN ('PENDING', 'RETRYABLE_FAILED')"),
    )

    # Idempotency belongs to an Agent application. Different Agents in one
    # tenant may legitimately receive the same provider identity.
    op.drop_constraint(
        "uq_outbox_message_idempotency",
        "outbox_message",
        type_="unique",
    )
    op.drop_constraint(
        "uq_outbox_message_request_sequence",
        "outbox_message",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_outbox_message_idempotency",
        "outbox_message",
        ["tenant_id", "agent_app_id", "category", "idempotency_key"],
    )
    op.create_unique_constraint(
        "uq_outbox_message_request_sequence",
        "outbox_message",
        ["tenant_id", "agent_app_id", "category", "request_id", "sequence_no"],
    )

    # The legacy schema never retained normalized request content. Mark every
    # migrated identity explicitly so replay does not compare a synthetic hash.
    op.execute("UPDATE inbox_message SET id_source = 'LEGACY' "
               "WHERE request_id LIKE 'legacy-%'")

    for table_name in _TIMESTAMP_TABLES:
        op.alter_column(
            table_name,
            "created_at",
            existing_type=sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        )
        op.alter_column(
            table_name,
            "updated_at",
            existing_type=sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        )
    for table_name in ("session_event", "audit_log"):
        op.alter_column(
            table_name,
            "created_at",
            existing_type=sa.DateTime(timezone=True),
            server_default=sa.func.now(),
        )
    op.alter_column(
        "agent_session",
        "last_activity_at",
        existing_type=sa.DateTime(timezone=True),
        server_default=sa.func.now(),
    )


def downgrade() -> None:
    """Remove fencing authority and restore the revision-0006 index shape."""

    op.alter_column(
        "agent_session",
        "last_activity_at",
        existing_type=sa.DateTime(timezone=True),
        server_default=None,
    )
    for table_name in ("session_event", "audit_log"):
        op.alter_column(
            table_name,
            "created_at",
            existing_type=sa.DateTime(timezone=True),
            server_default=None,
        )
    for table_name in reversed(_TIMESTAMP_TABLES):
        op.alter_column(
            table_name,
            "updated_at",
            existing_type=sa.DateTime(timezone=True),
            server_default=None,
        )
        op.alter_column(
            table_name,
            "created_at",
            existing_type=sa.DateTime(timezone=True),
            server_default=None,
        )

    op.drop_constraint(
        "uq_outbox_message_request_sequence",
        "outbox_message",
        type_="unique",
    )
    op.drop_constraint(
        "uq_outbox_message_idempotency",
        "outbox_message",
        type_="unique",
    )
    op.create_unique_constraint(
        "uq_outbox_message_idempotency",
        "outbox_message",
        ["tenant_id", "category", "idempotency_key"],
    )
    op.create_unique_constraint(
        "uq_outbox_message_request_sequence",
        "outbox_message",
        ["tenant_id", "category", "request_id", "sequence_no"],
    )

    op.drop_index("ix_outbox_message_claim", table_name="outbox_message")
    op.create_index(
        "ix_outbox_message_claim",
        "outbox_message",
        ["status", "next_attempt_at", "priority"],
    )
    op.drop_index("ix_inbox_message_retry", table_name="inbox_message")
    op.create_index(
        "ix_inbox_message_retry",
        "inbox_message",
        ["status", "next_attempt_at"],
    )
    op.drop_index(
        "ix_session_execution_fence_lease",
        table_name="session_execution_fence",
    )
    op.drop_table("session_execution_fence")
