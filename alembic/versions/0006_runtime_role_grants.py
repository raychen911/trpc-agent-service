"""Grant dispatcher reaping and admin reconciliation access under RLS."""

from __future__ import annotations

from alembic import op

revision = "0006_runtime_role_grants"
down_revision = "0005_repeatable_outbox_events"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("GRANT SELECT, UPDATE ON budget_reservation TO agent_dispatcher")
    op.execute(
        "GRANT SELECT, UPDATE (reserved_units, version, updated_at) "
        "ON budget_account TO agent_dispatcher"
    )
    op.execute("GRANT SELECT, INSERT, UPDATE ON tool_execution, delivery_attempt TO agent_admin")


def downgrade() -> None:
    op.execute("REVOKE SELECT, UPDATE ON budget_reservation FROM agent_dispatcher")
    op.execute("REVOKE UPDATE (reserved_units, version, updated_at) ON budget_account FROM agent_dispatcher")
    op.execute("REVOKE SELECT, INSERT, UPDATE ON tool_execution, delivery_attempt FROM agent_admin")
