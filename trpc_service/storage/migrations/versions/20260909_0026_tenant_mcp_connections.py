"""Add tenant-owned remote MCP connections.

Revision ID: 20260909_0026
Revises: 20260908_0025
Create Date: 2026-09-09
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260909_0026"
down_revision: str | None = "20260908_0025"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the tenant MCP control-plane table."""

    op.create_table(
        "mcp_connection",
        sa.Column("connection_id", sa.Uuid(), nullable=False),
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("name", sa.String(length=120), nullable=False),
        sa.Column("endpoint_url", sa.String(length=1000), nullable=False),
        sa.Column("auth_type", sa.String(length=20), nullable=False),
        sa.Column("secret_ref", sa.String(length=500), nullable=True),
        sa.Column("timeout_seconds", sa.Integer(), nullable=False),
        sa.Column("tool_catalog", sa.JSON(), nullable=False),
        sa.Column("catalog_refreshed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_code", sa.String(length=80), nullable=True),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint("auth_type IN ('none', 'bearer')", name="mcp_connection_auth_type"),
        sa.CheckConstraint("status IN ('active', 'disabled')", name="mcp_connection_status"),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenant.tenant_id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("connection_id"),
        sa.UniqueConstraint("tenant_id", "connection_id", name="uq_mcp_connection_tenant_id"),
        sa.UniqueConstraint("tenant_id", "name", name="uq_mcp_connection_tenant_name"),
    )
    op.create_index("ix_mcp_connection_tenant_id", "mcp_connection", ["tenant_id"])


def downgrade() -> None:
    op.drop_index("ix_mcp_connection_tenant_id", table_name="mcp_connection")
    op.drop_table("mcp_connection")
