"""Permit the restricted worker to execute storage migration jobs."""

from __future__ import annotations

from alembic import op

revision = "0007_storage_migration_worker"
down_revision = "0006_runtime_role_grants"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("GRANT SELECT ON tenant_locator TO agent_worker")
    op.execute(
        "GRANT UPDATE (status, source_watermark, target_watermark, error, updated_at) "
        "ON storage_migration TO agent_worker"
    )
    op.execute(
        "GRANT UPDATE (execution_mode, security_epoch, updated_at) "
        "ON tenant_runtime_state TO agent_worker"
    )


def downgrade() -> None:
    op.execute("REVOKE SELECT ON tenant_locator FROM agent_worker")
    op.execute(
        "REVOKE UPDATE (status, source_watermark, target_watermark, error, updated_at) "
        "ON storage_migration FROM agent_worker"
    )
    op.execute(
        "REVOKE UPDATE (execution_mode, security_epoch, updated_at) "
        "ON tenant_runtime_state FROM agent_worker"
    )
