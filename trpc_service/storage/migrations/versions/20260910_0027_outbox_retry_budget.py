"""Separate replay retry budget from immutable delivery attempt numbering.

Revision ID: 20260910_0027
Revises: 20260909_0026
Create Date: 2026-09-10
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260910_0027"
down_revision: str | None = "20260909_0026"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Give each delivery cycle an independent, resettable retry counter."""

    op.add_column(
        "outbox_message",
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
    )
    # Existing in-flight rows retain their consumed retry budget.
    op.execute("UPDATE outbox_message SET retry_count = attempt_count")
    op.create_check_constraint(
        "outbox_message_retry_nonnegative",
        "outbox_message",
        "retry_count >= 0",
    )


def downgrade() -> None:
    """Remove the independent retry budget."""

    op.drop_constraint(
        "outbox_message_retry_nonnegative",
        "outbox_message",
        type_="check",
    )
    op.drop_column("outbox_message", "retry_count")
