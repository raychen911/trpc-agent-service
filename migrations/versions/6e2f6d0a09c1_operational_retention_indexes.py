"""add indexes for bounded receipt and outbox retention

Revision ID: 6e2f6d0a09c1
Revises: 1dc9c6ba78b8
Create Date: 2026-08-29
"""

from collections.abc import Sequence

from alembic import op

revision: str = "6e2f6d0a09c1"
down_revision: str | Sequence[str] | None = "1dc9c6ba78b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_index(
        "ix_receipts_status_updated",
        "inbound_receipts",
        ["status", "updated_at"],
        unique=False,
    )
    op.create_index(
        "ix_outbox_status_updated",
        "outbox",
        ["status", "updated_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_outbox_status_updated", table_name="outbox")
    op.drop_index("ix_receipts_status_updated", table_name="inbound_receipts")
