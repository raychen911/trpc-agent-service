"""Permit conflict-key lookup without exposing audit_log to workload roles."""

from __future__ import annotations

from alembic import op

revision = "0010_audit_dedup_read_grant"
down_revision = "0009_audit_dedup"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("GRANT SELECT ON audit_dedup TO agent_worker, agent_dispatcher, agent_admin")


def downgrade() -> None:
    op.execute("REVOKE SELECT ON audit_dedup FROM agent_worker, agent_dispatcher, agent_admin")
