"""Extend approval identity with configuration and call metadata.

Revision ID: 20260905_0019
Revises: 20260905_0018
Create Date: 2026-09-05
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260905_0019"
down_revision: str | None = "20260905_0018"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Bind confirmation to the original config and logical call position."""

    op.add_column(
        "approval_request",
        sa.Column("config_version", sa.Integer(), nullable=False, server_default="1"),
    )
    op.add_column(
        "approval_request",
        sa.Column("logical_call_index", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "approval_request",
        sa.Column(
            "operation_arguments",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )


def downgrade() -> None:
    """Remove configuration, call-position and legacy operation fields."""

    op.drop_column("approval_request", "operation_arguments")
    op.drop_column("approval_request", "logical_call_index")
    op.drop_column("approval_request", "config_version")
