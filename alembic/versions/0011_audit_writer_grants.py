"""Make audit append privileges explicit for existing split-role deployments."""

from __future__ import annotations

from alembic import op

revision = "0011_audit_writer_grants"
down_revision = "0010_audit_dedup_read_grant"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("GRANT INSERT ON audit_log TO agent_worker, agent_dispatcher, agent_admin")
    op.execute("GRANT SELECT, INSERT ON audit_dedup TO agent_worker, agent_dispatcher, agent_admin")


def downgrade() -> None:
    op.execute("REVOKE INSERT ON audit_log FROM agent_worker, agent_dispatcher, agent_admin")
