"""Bind capability approvals to their tenant-scoped attachments.

Revision ID: 20260905_0018
Revises: 20260903_0017
Create Date: 2026-09-05
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260905_0018"
down_revision: str | None = "20260903_0017"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Persist only trusted Artifact identifiers needed after confirmation."""

    op.add_column(
        "approval_request",
        sa.Column(
            "artifact_refs",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'[]'::jsonb"),
        ),
    )


def downgrade() -> None:
    """Remove attachment recovery context from approvals."""

    op.drop_column("approval_request", "artifact_refs")
