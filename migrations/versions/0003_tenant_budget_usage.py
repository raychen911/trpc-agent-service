"""Add an atomic monthly tenant budget ledger.

Revision ID: 0003_tenant_budget_usage
Revises: 0002_migration_checkpoint_cursor
"""

from __future__ import annotations

from alembic import op

revision = "0003_tenant_budget_usage"
down_revision = "0002_migration_checkpoint_cursor"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE tenant_budget_usage (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            usage_month date NOT NULL,
            token_units bigint NOT NULL DEFAULT 0 CHECK (token_units >= 0),
            cost_units bigint NOT NULL DEFAULT 0 CHECK (cost_units >= 0),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, usage_month)
        );
        ALTER TABLE tenant_budget_usage ENABLE ROW LEVEL SECURITY;
        CREATE POLICY tenant_isolation_tenant_budget_usage ON tenant_budget_usage
        USING (tenant_id = nullif(current_setting('app.tenant_id', true), ''))
        WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id', true), ''));
        DO $block$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_runtime') THEN
                GRANT SELECT,INSERT,UPDATE,DELETE ON tenant_budget_usage TO trpc_runtime;
            END IF;
        END
        $block$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS tenant_budget_usage")
