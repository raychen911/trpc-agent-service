"""Fence durable delivery attempts independently from Redis consumer ownership."""

from __future__ import annotations

from alembic import op

revision = "0014_delivery_attempt_leases"
down_revision = "0013_knowledge_projection"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE delivery_attempt ADD COLUMN IF NOT EXISTS lease_owner text")
    op.execute(
        "ALTER TABLE delivery_attempt ADD COLUMN IF NOT EXISTS lease_fence bigint NOT NULL DEFAULT 0"
    )
    op.execute(
        "ALTER TABLE delivery_attempt ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz"
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS delivery_attempt_lease_idx
        ON delivery_attempt (tenant_id, status, lease_expires_at)
        WHERE status IN ('sending', 'reconciling')
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS delivery_attempt_lease_idx")
    op.execute("ALTER TABLE delivery_attempt DROP COLUMN IF EXISTS lease_expires_at")
    op.execute("ALTER TABLE delivery_attempt DROP COLUMN IF EXISTS lease_fence")
    op.execute("ALTER TABLE delivery_attempt DROP COLUMN IF EXISTS lease_owner")
