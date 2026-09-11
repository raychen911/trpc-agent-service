"""Grant audit writers access to the active default audit partition."""

from __future__ import annotations

from alembic import op

revision = "0012_audit_default_grants"
down_revision = "0011_audit_writer_grants"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("GRANT INSERT ON audit_log_default TO agent_worker, agent_dispatcher, agent_admin")


def downgrade() -> None:
    op.execute("REVOKE INSERT ON audit_log_default FROM agent_worker, agent_dispatcher, agent_admin")
