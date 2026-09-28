"""Index unresolved predecessors used by ordered reply delivery.

Revision ID: 20260924_0028
Revises: 20260910_0027
Create Date: 2026-09-24
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260924_0028"
down_revision: str | None = "20260910_0027"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Keep predecessor lookup scoped as delivery history grows."""

    op.create_index("ix_outbox_message_stream_order", "outbox_message", [
        "tenant_id",
        "agent_app_id",
        "binding_id",
        "session_id",
        "status",
        "created_at",
        "sequence_no",
    ])


def downgrade() -> None:
    """Remove the delivery predecessor index."""

    op.drop_index("ix_outbox_message_stream_order", table_name="outbox_message")
