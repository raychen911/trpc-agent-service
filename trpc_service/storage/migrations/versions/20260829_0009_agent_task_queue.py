"""Add durable multi-node Agent task dispatch.

Revision ID: 20260829_0009
Revises: 20260828_0008
Create Date: 2026-08-29
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260829_0009"
down_revision: str | None = "20260828_0008"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the SQL fact that Gateways enqueue and Workers lease."""

    op.create_table(
        "agent_task",
        sa.Column("task_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("binding_id", sa.Uuid(), nullable=False),
        sa.Column("external_message_id", sa.String(length=255), nullable=False),
        sa.Column("request_id", sa.String(length=128), nullable=False),
        sa.Column("trace_id", sa.String(length=128), nullable=False),
        sa.Column("session_id", sa.String(length=255), nullable=False),
        sa.Column("config_version", sa.BigInteger(), nullable=False),
        sa.Column("routing_key", sa.String(length=64), nullable=False),
        sa.Column("payload_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "request_payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("status", sa.String(length=30), nullable=False, server_default="queued"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_owner", sa.String(length=255), nullable=True),
        sa.Column("lease_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.String(length=100), nullable=True),
        sa.Column("last_error_summary", sa.Text(), nullable=True),
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
        sa.CheckConstraint("attempt_count >= 0", name="agent_task_attempt_nonnegative"),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', "
            "'retryable_failed', 'permanent_failed')",
            name="agent_task_status",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_agent_task_tenant_agent",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "binding_id"],
            ["channel_binding.tenant_id", "channel_binding.binding_id"],
            name="fk_agent_task_tenant_binding",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("task_id", name=op.f("pk_agent_task")),
        sa.UniqueConstraint(
            "tenant_id",
            "binding_id",
            "external_message_id",
            name="uq_agent_task_provider_identity",
        ),
    )
    op.create_index(
        "ix_agent_task_claim",
        "agent_task",
        ["status", "next_attempt_at", "created_at"],
        postgresql_where=sa.text("status IN ('queued', 'running', 'retryable_failed')"),
    )
    op.create_index(
        "ix_agent_task_routing",
        "agent_task",
        ["routing_key", "created_at"],
    )
    op.create_table(
        "runtime_node",
        sa.Column("node_id", sa.String(length=255), nullable=False),
        sa.Column("role", sa.String(length=20), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False, server_default="active"),
        sa.Column("worker_concurrency", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("heartbeat_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stopped_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint(
            "role IN ('api', 'worker', 'api_worker')",
            name="runtime_node_role",
        ),
        sa.CheckConstraint(
            "status IN ('active', 'stopped')",
            name="runtime_node_status",
        ),
        sa.CheckConstraint(
            "worker_concurrency >= 0",
            name="runtime_node_concurrency_nonnegative",
        ),
        sa.PrimaryKeyConstraint("node_id", name=op.f("pk_runtime_node")),
    )
    op.create_index(
        "ix_runtime_node_role_heartbeat",
        "runtime_node",
        ["role", "heartbeat_at"],
    )


def downgrade() -> None:
    """Remove the dispatch queue after operators drain all pending work."""

    op.drop_index("ix_runtime_node_role_heartbeat", table_name="runtime_node")
    op.drop_table("runtime_node")
    op.drop_index("ix_agent_task_routing", table_name="agent_task")
    op.drop_index("ix_agent_task_claim", table_name="agent_task")
    op.drop_table("agent_task")
