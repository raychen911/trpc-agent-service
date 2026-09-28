"""Persist the latest Session fencing token.

Revision ID: 20260827_0004
Revises: 20260827_0003
Create Date: 2026-08-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "20260827_0004"
down_revision: str | None = "20260827_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add an optional fence while deployments transition to leased Workers."""

    op.add_column(
        "runtime_session",
        sa.Column("last_fencing_token", sa.Integer(), nullable=True),
    )


def downgrade() -> None:
    """Remove persisted Session fencing state."""

    op.drop_column("runtime_session", "last_fencing_token")
