"""PostgreSQL tenant isolation and append-only audit hardening.

Revision ID: 0002
Revises: 0001
"""

from __future__ import annotations

from collections.abc import Sequence

from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TENANT_TABLES = (
    "tenant",
    "tenant_config_revision",
    "agent_app",
    "channel_binding",
    "session",
    "inbox_message",
    "agent_run",
    "session_event",
    "session_summary",
    "memory_record",
    "tool_effect",
    "reply_outbox",
    "knowledge_document",
    "artifact",
    "audit_log",
    "scoped_state",
    "channel_reply_credential",
)


def upgrade() -> None:
    """Apply PostgreSQL-only defense-in-depth controls."""

    if op.get_bind().dialect.name != "postgresql":
        return
    for table in _TENANT_TABLES:
        op.execute(f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY')
        op.execute(f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY')
        op.execute(
            f"""
            CREATE POLICY tenant_isolation ON "{table}"
            USING (
                tenant_id = NULLIF(current_setting('app.tenant_id', true), '')
            )
            WITH CHECK (
                tenant_id = NULLIF(current_setting('app.tenant_id', true), '')
            )
            """
        )

    op.execute(
        """
        CREATE FUNCTION reject_audit_mutation() RETURNS trigger
        LANGUAGE plpgsql AS $$
        BEGIN
            RAISE EXCEPTION 'audit_log is append-only';
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER audit_log_append_only
        BEFORE UPDATE OR DELETE ON audit_log
        FOR EACH ROW EXECUTE FUNCTION reject_audit_mutation()
        """
    )
    op.execute("REVOKE UPDATE, DELETE ON audit_log FROM PUBLIC")
    op.execute(
        """
        CREATE INDEX ix_inbox_ready_postgresql
        ON inbox_message (next_attempt_at, received_at, tenant_id, session_id, accepted_seq)
        WHERE status IN ('received', 'retry_wait', 'running')
        """
    )
    op.execute(
        """
        CREATE INDEX ix_outbox_ready_postgresql
        ON reply_outbox (next_retry_at, created_at, tenant_id, part_no)
        WHERE status IN ('pending', 'retry_wait', 'sending')
        """
    )


def downgrade() -> None:
    """Remove PostgreSQL-only hardening objects."""

    if op.get_bind().dialect.name != "postgresql":
        return
    op.execute("DROP INDEX IF EXISTS ix_outbox_ready_postgresql")
    op.execute("DROP INDEX IF EXISTS ix_inbox_ready_postgresql")
    op.execute("DROP TRIGGER IF EXISTS audit_log_append_only ON audit_log")
    op.execute("DROP FUNCTION IF EXISTS reject_audit_mutation()")
    for table in reversed(_TENANT_TABLES):
        op.execute(f'DROP POLICY IF EXISTS tenant_isolation ON "{table}"')
        op.execute(f'ALTER TABLE "{table}" NO FORCE ROW LEVEL SECURITY')
        op.execute(f'ALTER TABLE "{table}" DISABLE ROW LEVEL SECURITY')
