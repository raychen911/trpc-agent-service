"""Enforce the tenant boundary on channel-to-agent references.

Revision ID: 20260825_0002
Revises: 20260825_0001
Create Date: 2026-08-25
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260825_0002"
down_revision: str | None = "20260825_0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Replace the Agent-only foreign key with a tenant-aware composite key."""

    op.create_unique_constraint(
        "uq_agent_app_tenant_id",
        "agent_app",
        ["tenant_id", "agent_app_id"],
    )
    op.drop_constraint(
        "fk_channel_binding_agent_app_id_agent_app",
        "channel_binding",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_channel_binding_tenant_agent",
        "channel_binding",
        "agent_app",
        ["tenant_id", "agent_app_id"],
        ["tenant_id", "agent_app_id"],
        ondelete="RESTRICT",
    )


def downgrade() -> None:
    """Restore the original Agent-only foreign key."""

    op.drop_constraint(
        "fk_channel_binding_tenant_agent",
        "channel_binding",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_channel_binding_agent_app_id_agent_app",
        "channel_binding",
        "agent_app",
        ["agent_app_id"],
        ["agent_app_id"],
        ondelete="RESTRICT",
    )
    op.drop_constraint("uq_agent_app_tenant_id", "agent_app", type_="unique")
