"""add atomic tenant usage reservations

Revision ID: 8b6d1e4f2a90
Revises: 6e2f6d0a09c1
Create Date: 2026-08-30
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "8b6d1e4f2a90"
down_revision: str | Sequence[str] | None = "6e2f6d0a09c1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "usage_reservations",
        sa.Column("tenant_id", sa.String(length=63), nullable=False),
        sa.Column("reservation_id", sa.String(length=80), nullable=False),
        sa.Column("period", sa.String(length=7), nullable=False),
        sa.Column("reserved_tokens", sa.BigInteger(), nullable=False),
        sa.Column("reserved_cost_usd", sa.Float(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("tenant_id", "reservation_id"),
    )
    op.create_index(
        "ix_usage_reservations_period_expiry",
        "usage_reservations",
        ["tenant_id", "period", "expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_usage_reservations_period_expiry",
        table_name="usage_reservations",
    )
    op.drop_table("usage_reservations")
