"""Add the durable storage-migration ledger for existing deployments."""

from __future__ import annotations

from alembic import op

revision = "0002_storage_migration"
down_revision = "0001_platform_schema"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS storage_migration (
            tenant_id text NOT NULL REFERENCES tenant(tenant_id),
            migration_id text NOT NULL,
            source_profile jsonb NOT NULL,
            target_profile jsonb NOT NULL,
            status text NOT NULL CHECK (status IN (
                'preparing', 'backfilling', 'catching_up', 'draining', 'verifying',
                'active', 'readonly', 'retired', 'failed'
            )),
            source_routing_epoch bigint NOT NULL,
            target_routing_epoch bigint,
            source_watermark text,
            target_watermark text,
            error text,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, migration_id)
        )
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS storage_migration_active_idx
        ON storage_migration (tenant_id, updated_at DESC)
        WHERE status IN ('preparing', 'backfilling', 'catching_up', 'draining', 'verifying', 'readonly')
        """
    )
    op.execute("ALTER TABLE storage_migration ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE storage_migration FORCE ROW LEVEL SECURITY")
    op.execute("DROP POLICY IF EXISTS tenant_isolation ON storage_migration")
    op.execute(
        """
        CREATE POLICY tenant_isolation ON storage_migration
        USING (tenant_id = app_security.current_tenant_id())
        WITH CHECK (tenant_id = app_security.current_tenant_id())
        """
    )
    op.execute("GRANT SELECT ON storage_migration TO agent_worker, agent_dispatcher")
    op.execute("GRANT SELECT, INSERT ON storage_migration TO agent_admin")
    op.execute(
        """
        GRANT UPDATE (status, target_routing_epoch, source_watermark, target_watermark, error, updated_at)
        ON storage_migration TO agent_admin
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS storage_migration CASCADE")
