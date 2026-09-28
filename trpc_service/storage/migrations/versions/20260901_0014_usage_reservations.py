"""Make multi-node usage budgets atomic.

Revision ID: 20260901_0014
Revises: 20260831_0013
Create Date: 2026-09-01
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260901_0014"
down_revision: str | None = "20260831_0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Track outstanding token reservations before provider invocation."""

    op.add_column(
        "usage_ledger",
        sa.Column(
            "reserved_tokens",
            sa.BigInteger(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.add_column(
        "usage_ledger",
        sa.Column(
            "status",
            sa.String(length=20),
            nullable=False,
            server_default=sa.text("'completed'"),
        ),
    )
    op.create_check_constraint(
        "usage_ledger_reserved_tokens_nonnegative",
        "usage_ledger",
        "reserved_tokens >= 0",
    )
    op.create_check_constraint(
        "usage_ledger_status_allowed",
        "usage_ledger",
        "status IN ('reserved', 'completed', 'cancelled')",
    )


def downgrade() -> None:
    """Return to completed-only usage facts."""

    op.drop_constraint(
        "usage_ledger_status_allowed",
        "usage_ledger",
        type_="check",
    )
    op.drop_constraint(
        "usage_ledger_reserved_tokens_nonnegative",
        "usage_ledger",
        type_="check",
    )
    op.drop_column("usage_ledger", "status")
    op.drop_column("usage_ledger", "reserved_tokens")
