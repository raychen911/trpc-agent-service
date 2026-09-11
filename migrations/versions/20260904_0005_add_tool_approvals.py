"""add tool approval request/audit tables (Stage 6A2)

Revision ID: 0005_add_tool_approvals
Revises: 0004_add_tenant_governance
Create Date: 2026-09-04

Creates the approval single source of truth (tool_approval_requests) and the
append-only audit trail (tool_approval_audit_events).  Tool arguments live
only in the restricted JSONB column of the request table; audit rows carry
the SHA-256 digest plus decision metadata only.  State/terminal-field CHECKs
make illegal transitions impossible at the database level.  Downgrade drops
both tables symmetrically (audit first, FK RESTRICT order).
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0005_add_tool_approvals"
down_revision: Union[str, None] = "0004_add_tenant_governance"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tool_approval_requests",
        sa.Column("approval_id", sa.UUID, primary_key=True),
        sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
        sa.Column("app_id", sa.TEXT, nullable=False),
        sa.Column("config_version", sa.BIGINT, nullable=False),
        sa.Column("channel", sa.TEXT, nullable=False),
        sa.Column("user_id", sa.TEXT, nullable=False),
        sa.Column("session_id", sa.TEXT, nullable=False),
        sa.Column("receipt_id", sa.UUID, nullable=False),
        sa.Column("function_call_id", sa.TEXT, nullable=False),
        sa.Column("tool_name", sa.TEXT, nullable=False),
        sa.Column("tool_args", JSONB, nullable=False),
        sa.Column("args_digest", sa.CHAR(64), nullable=False),
        sa.Column("state", sa.TEXT, nullable=False, server_default=sa.text("'pending'")),
        sa.Column("decision", sa.TEXT, nullable=True),
        sa.Column("decision_message_id", sa.TEXT, nullable=True),
        sa.Column("response_text", sa.TEXT, nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("decided_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["receipt_id"],
            ["message_receipts.receipt_id"],
            ondelete="RESTRICT",
            name="tool_approval_requests_receipt_fk",
        ),
        sa.UniqueConstraint("receipt_id", "function_call_id", name="tool_approval_requests_receipt_call_uk"),
        sa.CheckConstraint(
            "tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name="tool_approval_requests_tenant_id_format",
        ),
        sa.CheckConstraint("config_version >= 1", name="tool_approval_requests_version_positive"),
        sa.CheckConstraint("btrim(app_id) <> ''", name="tool_approval_requests_app_not_blank"),
        sa.CheckConstraint("btrim(channel) <> ''", name="tool_approval_requests_channel_not_blank"),
        sa.CheckConstraint("btrim(user_id) <> ''", name="tool_approval_requests_user_not_blank"),
        sa.CheckConstraint("btrim(session_id) <> ''", name="tool_approval_requests_session_not_blank"),
        sa.CheckConstraint(
            "btrim(function_call_id) <> ''",
            name="tool_approval_requests_call_id_not_blank",
        ),
        sa.CheckConstraint("btrim(tool_name) <> ''", name="tool_approval_requests_tool_not_blank"),
        sa.CheckConstraint(
            "jsonb_typeof(tool_args) = 'object'",
            name="tool_approval_requests_args_is_object",
        ),
        sa.CheckConstraint(
            "args_digest ~ '^[0-9a-f]{64}$'",
            name="tool_approval_requests_digest_format",
        ),
        sa.CheckConstraint(
            "state IN ('pending', 'executing', 'completed', 'rejected', 'failed')",
            name="tool_approval_requests_state_valid",
        ),
        sa.CheckConstraint(
            "decision IS NULL OR decision IN ('approve', 'reject')",
            name="tool_approval_requests_decision_valid",
        ),
        sa.CheckConstraint(
            "(state = 'pending' AND decision IS NULL AND decided_at IS NULL"
            " AND finished_at IS NULL AND response_text IS NULL)"
            " OR state != 'pending'",
            name="tool_approval_requests_pending_no_decision",
        ),
        sa.CheckConstraint(
            "state != 'executing' OR decision IS NOT NULL",
            name="tool_approval_requests_executing_has_decision",
        ),
        sa.CheckConstraint(
            "(state IN ('completed', 'rejected', 'failed') AND finished_at IS NOT NULL"
            " AND decided_at IS NOT NULL) OR state NOT IN ('completed', 'rejected', 'failed')",
            name="tool_approval_requests_terminal_has_timestamps",
        ),
        sa.CheckConstraint(
            "(state IN ('completed', 'rejected') AND response_text IS NOT NULL)"
            " OR state NOT IN ('completed', 'rejected')",
            name="tool_approval_requests_terminal_has_response",
        ),
    )
    op.create_table(
        "tool_approval_audit_events",
        sa.Column("audit_id", sa.UUID, primary_key=True),
        sa.Column("approval_id", sa.UUID, nullable=False),
        sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
        sa.Column("event_type", sa.TEXT, nullable=False),
        sa.Column("decision", sa.TEXT, nullable=True),
        sa.Column("message_id", sa.TEXT, nullable=True),
        sa.Column("args_digest", sa.CHAR(64), nullable=False),
        sa.Column("occurred_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["approval_id"],
            ["tool_approval_requests.approval_id"],
            ondelete="RESTRICT",
            name="tool_approval_audit_events_approval_fk",
        ),
        sa.CheckConstraint(
            "event_type IN ('created', 'decided', 'completed', 'rejected', 'failed')",
            name="tool_approval_audit_events_type_valid",
        ),
        sa.CheckConstraint(
            "decision IS NULL OR decision IN ('approve', 'reject')",
            name="tool_approval_audit_events_decision_valid",
        ),
        sa.CheckConstraint(
            "args_digest ~ '^[0-9a-f]{64}$'",
            name="tool_approval_audit_events_digest_format",
        ),
    )


def downgrade() -> None:
    op.drop_table("tool_approval_audit_events")
    op.drop_table("tool_approval_requests")
