"""Expand audit session identifiers for channel-derived session keys.

Revision ID: 0004
Revises: 0003
"""

import sqlalchemy as sa
from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.alter_column(
        "audit_logs",
        "session_id",
        existing_type=sa.String(length=36),
        type_=sa.String(length=512),
        existing_nullable=True,
    )


def downgrade() -> None:
    op.alter_column(
        "audit_logs",
        "session_id",
        existing_type=sa.String(length=512),
        type_=sa.String(length=36),
        existing_nullable=True,
    )
