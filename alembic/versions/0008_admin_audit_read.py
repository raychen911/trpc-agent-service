"""Allow tenant-scoped admin audit reads without granting workers the same access."""

from __future__ import annotations

from alembic import op

revision = "0008_admin_audit_read"
down_revision = "0007_storage_migration_worker"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("GRANT SELECT ON audit_log TO agent_admin")


def downgrade() -> None:
    op.execute("REVOKE SELECT ON audit_log FROM agent_admin")
