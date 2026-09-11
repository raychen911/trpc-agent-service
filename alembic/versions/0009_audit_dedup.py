"""Deduplicate partitioned audit facts without widening worker read grants."""

from __future__ import annotations

from alembic import op

revision = "0009_audit_dedup"
down_revision = "0008_admin_audit_read"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_dedup (
            tenant_id text NOT NULL REFERENCES tenant(tenant_id),
            audit_id text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, audit_id)
        )
        """
    )
    # The immutable initial-schema snapshot used by fresh deployments already
    # contains this table. These statements keep older upgrades correct while
    # remaining safe for a clean database.
    op.execute("ALTER TABLE audit_dedup OWNER TO platform_schema_owner")
    op.execute("ALTER TABLE audit_dedup ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE audit_dedup FORCE ROW LEVEL SECURITY")
    op.execute(
        """
        DROP POLICY IF EXISTS tenant_isolation ON audit_dedup;
        CREATE POLICY tenant_isolation ON audit_dedup
        USING (tenant_id = app_security.current_tenant_id())
        WITH CHECK (tenant_id = app_security.current_tenant_id())
        """
    )
    op.execute("REVOKE ALL ON audit_dedup FROM PUBLIC")
    op.execute("GRANT SELECT, INSERT ON audit_dedup TO agent_worker, agent_dispatcher, agent_admin")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS audit_dedup")
