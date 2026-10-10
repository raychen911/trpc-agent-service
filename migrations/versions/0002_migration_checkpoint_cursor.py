"""Persist migration cursors for crash-safe batch resume.

Revision ID: 0002_migration_checkpoint_cursor
Revises: 0001_production_runtime
"""

from __future__ import annotations

from alembic import op

revision = "0002_migration_checkpoint_cursor"
down_revision = "0001_production_runtime"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE migration_checkpoints ADD COLUMN cursor text")


def downgrade() -> None:
    op.execute("ALTER TABLE migration_checkpoints DROP COLUMN cursor")
