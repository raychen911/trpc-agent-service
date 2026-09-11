"""Permit repeatable replies for a durable session.

The Outbox idempotency key is deterministic for one execution.  A uniqueness
constraint on the session/event kind incorrectly turned every later reply in a
conversation into a transaction rollback.
"""

from __future__ import annotations

from alembic import op

revision = "0005_repeatable_outbox_events"
down_revision = "0004_audit_partition_rls"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "ALTER TABLE outbox DROP CONSTRAINT IF EXISTS "
        "outbox_tenant_id_aggregate_type_aggregate_id_event_type_key"
    )


def downgrade() -> None:
    op.execute(
        "ALTER TABLE outbox ADD CONSTRAINT "
        "outbox_tenant_id_aggregate_type_aggregate_id_event_type_key "
        "UNIQUE (tenant_id, aggregate_type, aggregate_id, event_type)"
    )
