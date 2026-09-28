"""Add durable Agent runtime storage tables.

Revision ID: 20260827_0003
Revises: 20260825_0002
Create Date: 2026-08-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260827_0003"
down_revision: str | None = "20260825_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _timestamps() -> tuple[sa.Column[object], sa.Column[object]]:
    """Return the timestamp columns shared by mutable runtime tables."""

    return (
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )


def upgrade() -> None:
    """Create scoped Session, Inbox, Outbox, Memory, Summary and Audit tables."""

    op.create_table(
        "runtime_session",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("version", sa.Integer(), nullable=False),
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
        unique=False,
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
        unique=False,
    )


def downgrade() -> None:
    """Drop runtime storage tables in reverse dependency order."""

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
