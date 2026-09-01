"""Pin the immutable tenant configuration revision on accepted Inbox rows.

Revision ID: 0004
Revises: 0003
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0004"
down_revision: str | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Backfill and require the configuration revision captured at ingress."""

    with op.batch_alter_table("inbox_message", schema=None) as batch_op:
        batch_op.add_column(sa.Column("config_revision", sa.Integer(), nullable=True))

    op.execute(
        """
        UPDATE inbox_message
        SET config_revision = (
            SELECT channel_binding.config_revision
            FROM channel_binding
            WHERE channel_binding.tenant_id = inbox_message.tenant_id
              AND channel_binding.binding_id = inbox_message.binding_id
        )
        WHERE config_revision IS NULL
        """
    )
    with op.batch_alter_table("inbox_message", schema=None) as batch_op:
        batch_op.alter_column(
            "config_revision",
            existing_type=sa.Integer(),
            nullable=False,
        )
        batch_op.create_foreign_key(
            "fk_inbox_config_revision",
            "tenant_config_revision",
            ["tenant_id", "config_revision"],
            ["tenant_id", "revision"],
            ondelete="RESTRICT",
        )


def downgrade() -> None:
    """Remove Inbox configuration revision pinning."""

    with op.batch_alter_table("inbox_message", schema=None) as batch_op:
        batch_op.drop_constraint("fk_inbox_config_revision", type_="foreignkey")
        batch_op.drop_column("config_revision")
