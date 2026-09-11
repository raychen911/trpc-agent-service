"""Harden the default audit partition with the parent tenant RLS policy."""

from __future__ import annotations

from alembic import op

revision = "0004_audit_partition_rls"
down_revision = "0003_tenant_locator"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE audit_log_default OWNER TO platform_schema_owner")
    op.execute("ALTER TABLE audit_log_default ENABLE ROW LEVEL SECURITY")
    op.execute("ALTER TABLE audit_log_default FORCE ROW LEVEL SECURITY")
    op.execute("DROP POLICY IF EXISTS tenant_isolation ON audit_log_default")
    op.execute(
        """
        CREATE POLICY tenant_isolation ON audit_log_default
        USING (tenant_id = app_security.current_tenant_id())
        WITH CHECK (tenant_id = app_security.current_tenant_id())
        """
    )
    op.execute("REVOKE ALL ON audit_log_default FROM PUBLIC")


def downgrade() -> None:
    op.execute("DROP POLICY IF EXISTS tenant_isolation ON audit_log_default")
    op.execute("ALTER TABLE audit_log_default DISABLE ROW LEVEL SECURITY")
