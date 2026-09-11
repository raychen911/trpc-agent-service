"""Add a scheduler-only tenant locator without granting RLS bypass to workers."""

from __future__ import annotations

from alembic import op

revision = "0003_tenant_locator"
down_revision = "0002_storage_migration"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS tenant_locator (
            tenant_id text PRIMARY KEY REFERENCES tenant(tenant_id) ON DELETE CASCADE,
            enabled boolean NOT NULL DEFAULT true,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        )
        """
    )
    op.execute(
        """
        INSERT INTO tenant_locator (tenant_id, enabled, created_at, updated_at)
        SELECT tenant_id, status = 'active', created_at, updated_at FROM tenant
        ON CONFLICT (tenant_id) DO UPDATE SET enabled = EXCLUDED.enabled, updated_at = EXCLUDED.updated_at
        """
    )
    op.execute("ALTER TABLE tenant_locator OWNER TO platform_schema_owner")
    op.execute("REVOKE ALL ON tenant_locator FROM PUBLIC")
    op.execute("GRANT SELECT ON tenant_locator TO agent_dispatcher")
    op.execute("GRANT INSERT, UPDATE ON tenant_locator TO agent_admin")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS tenant_locator")
