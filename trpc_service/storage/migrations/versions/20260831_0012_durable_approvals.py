"""Add durable human approval state for high-risk capabilities.

Revision ID: 20260831_0012
Revises: 20260831_0011
Create Date: 2026-08-31
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260831_0012"
down_revision: str | None = "20260831_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the cross-node approval authority without storing Tool arguments."""

    op.create_table(
        "approval_request",
        sa.Column("approval_id", sa.Uuid(), nullable=False),
        sa.Column("short_code", sa.String(length=8), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("binding_id", sa.Uuid(), nullable=False),
        sa.Column("principal_id", sa.String(length=255), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("tool_call_id", sa.String(length=255), nullable=False),
        sa.Column("capability_kind", sa.String(length=30), nullable=False),
        sa.Column("capability_name", sa.String(length=160), nullable=False),
        sa.Column("action", sa.String(length=80), nullable=False),
        sa.Column("resource", sa.String(length=500), nullable=True),
        sa.Column("arguments_hash", sa.String(length=64), nullable=False),
        sa.Column("risk_level", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("decided_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("decided_by", sa.String(length=255), nullable=True),
        sa.Column("execution_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("executed_at", sa.DateTime(timezone=True), nullable=True),
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
        sa.CheckConstraint("risk_level IN (2, 3)", name="approval_request_risk_level"),
        sa.CheckConstraint(
            "status IN ('PENDING', 'APPROVED', 'REJECTED', 'EXPIRED', "
            "'EXECUTING', 'EXECUTED', 'UNKNOWN')",
            name="approval_request_status",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_approval_request_tenant_agent",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("approval_id", name=op.f("pk_approval_request")),
        sa.UniqueConstraint(
            "tenant_id",
            "agent_app_id",
            "tool_call_id",
            name="uq_approval_request_logical_call",
        ),
        sa.UniqueConstraint("short_code", name="uq_approval_request_short_code"),
    )
    op.create_index(
        "ix_approval_request_scope_status",
        "approval_request",
        ["tenant_id", "agent_app_id", "status", "expires_at"],
    )


def downgrade() -> None:
    """Remove approval state after pending operations have been reconciled."""

    op.drop_index("ix_approval_request_scope_status", table_name="approval_request")
    op.drop_table("approval_request")
