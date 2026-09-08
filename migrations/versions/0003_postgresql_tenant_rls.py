"""Enable PostgreSQL row-level tenant isolation.

Revision ID: 0003
Revises: 0002
"""

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None

TENANT_TABLES = (
    "agent_apps",
    "agent_app_revisions",
    "model_configs",
    "tool_permissions",
    "channel_bindings",
    "im_user_identities",
    "backend_configs",
    "sessions",
    "session_events",
    "memories",
    "summaries",
    "artifacts",
    "audit_logs",
    "outbox_messages",
    "outbox_dead_letters",
    "tenant_budget_usage",
    "inbound_messages",
    "agent_executions",
)


def _policy_expression(column: str) -> str:
    return (
        "current_setting('trpc.rls_bypass', true) = 'on' OR "
        f"{column}::text = current_setting('trpc.tenant_id', true)"
    )


def upgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return
    tables = (("tenants", "id"), *((table, "tenant_id") for table in TENANT_TABLES))
    for table, column in tables:
        expression = _policy_expression(column)
        op.execute(f'ALTER TABLE "{table}" ENABLE ROW LEVEL SECURITY')
        op.execute(f'ALTER TABLE "{table}" FORCE ROW LEVEL SECURITY')
        op.execute(
            f'CREATE POLICY trpc_tenant_isolation ON "{table}" '
            f"USING ({expression}) WITH CHECK ({expression})"
        )


def downgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return
    tables = ("tenants", *TENANT_TABLES)
    for table in tables:
        op.execute(f'DROP POLICY IF EXISTS trpc_tenant_isolation ON "{table}"')
        op.execute(f'ALTER TABLE "{table}" DISABLE ROW LEVEL SECURITY')
