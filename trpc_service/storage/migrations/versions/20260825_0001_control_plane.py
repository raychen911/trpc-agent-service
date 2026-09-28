"""Create control-plane tenant, agent, and channel binding tables.

Revision ID: 20260825_0001
Revises:
Create Date: 2026-08-25
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260825_0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the pgvector extension and initial control-plane tables."""

    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.create_table(
        "tenant",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("isolation_mode", sa.String(length=20), nullable=False),
        sa.Column("audit_policy", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", name=op.f("pk_tenant")),
        sa.UniqueConstraint("name", name=op.f("uq_tenant_name")),
    )
    op.create_table(
        "agent_app",
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("application_config", sa.JSON(), nullable=False),
        sa.Column("model_config", sa.JSON(), nullable=False),
        sa.Column("tool_permissions", sa.JSON(), nullable=False),
        sa.Column("knowledge_config", sa.JSON(), nullable=False),
        sa.Column("backend_config", sa.JSON(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenant.tenant_id"],
            name=op.f("fk_agent_app_tenant_id_tenant"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("agent_app_id", name=op.f("pk_agent_app")),
        sa.UniqueConstraint("tenant_id", "name", name="uq_agent_app_tenant_name"),
    )
    op.create_index(op.f("ix_agent_app_tenant_id"), "agent_app", ["tenant_id"])
    op.create_table(
        "channel_binding",
        sa.Column("binding_id", sa.Uuid(), nullable=False),
        sa.Column("binding_public_id", sa.String(length=64), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("channel_type", sa.String(length=40), nullable=False),
        sa.Column("external_account_hash", sa.String(length=128), nullable=False),
        sa.Column("account_config", sa.JSON(), nullable=False),
        sa.Column("secret_ref_map", sa.JSON(), nullable=False),
        sa.Column("capabilities", sa.JSON(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["agent_app_id"],
            ["agent_app.agent_app_id"],
            name=op.f("fk_channel_binding_agent_app_id_agent_app"),
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenant.tenant_id"],
            name=op.f("fk_channel_binding_tenant_id_tenant"),
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("binding_id", name=op.f("pk_channel_binding")),
        sa.UniqueConstraint("binding_public_id", name=op.f("uq_channel_binding_binding_public_id")),
        sa.UniqueConstraint(
            "channel_type",
            "external_account_hash",
            name="uq_channel_binding_external_account",
        ),
    )
    op.create_index(
        op.f("ix_channel_binding_agent_app_id"),
        "channel_binding",
        ["agent_app_id"],
    )
    op.create_index(
        op.f("ix_channel_binding_tenant_id"),
        "channel_binding",
        ["tenant_id"],
    )


def downgrade() -> None:
    """Remove control-plane tables in reverse dependency order."""

    op.drop_index(op.f("ix_channel_binding_tenant_id"), table_name="channel_binding")
    op.drop_index(op.f("ix_channel_binding_agent_app_id"), table_name="channel_binding")
    op.drop_table("channel_binding")
    op.drop_index(op.f("ix_agent_app_tenant_id"), table_name="agent_app")
    op.drop_table("agent_app")
    op.drop_table("tenant")
