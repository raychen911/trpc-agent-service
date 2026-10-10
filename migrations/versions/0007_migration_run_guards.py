"""Add immutable migration manifests and tenant-scoped migration leases.

Revision ID: 0007_migration_run_guards
Revises: 0006_fault_stage_controls
"""

from __future__ import annotations

from alembic import op

revision = "0007_migration_run_guards"
down_revision = "0006_fault_stage_controls"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE migration_scope_manifests (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            migration_id text NOT NULL
                CHECK (char_length(migration_id) BETWEEN 1 AND 256),
            source_kind text NOT NULL CHECK (
                source_kind IN ('redis','local_vector','external_vector','external_memory')
            ),
            kinds text[] NOT NULL CHECK (
                cardinality(kinds) BETWEEN 1 AND 5
                AND kinds <@ ARRAY['session','memory','summary','artifact','knowledge']::text[]
            ),
            source_snapshot_id text NOT NULL
                CHECK (char_length(source_snapshot_id) BETWEEN 1 AND 256),
            source_count bigint NOT NULL CHECK (source_count >= 0),
            source_checksum text NOT NULL CHECK (source_checksum ~ '^[0-9a-f]{64}$'),
            app_id text NOT NULL CHECK (char_length(app_id) BETWEEN 1 AND 256),
            app_revision bigint NOT NULL DEFAULT 1 CHECK (app_revision >= 1),
            config_version bigint NOT NULL CHECK (config_version >= 1),
            binding_id text NOT NULL CHECK (char_length(binding_id) BETWEEN 1 AND 256),
            binding_revision bigint NOT NULL CHECK (binding_revision >= 1),
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, migration_id)
        );
        CREATE INDEX ix_migration_scope_manifests_tenant_source
            ON migration_scope_manifests (tenant_id, source_kind, created_at DESC);

        CREATE TABLE migration_leases (
            tenant_id text NOT NULL,
            migration_id text NOT NULL,
            owner_id text NOT NULL CHECK (char_length(owner_id) BETWEEN 1 AND 256),
            lease_epoch bigint NOT NULL CHECK (lease_epoch >= 1),
            expires_at timestamptz NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, migration_id),
            FOREIGN KEY (tenant_id, migration_id)
                REFERENCES migration_scope_manifests(tenant_id, migration_id)
        );
        CREATE INDEX ix_migration_leases_tenant_expiry
            ON migration_leases (tenant_id, expires_at);
        CREATE INDEX ix_migration_leases_tenant_owner
            ON migration_leases (tenant_id, owner_id, lease_epoch);

        ALTER TABLE migration_scope_manifests ENABLE ROW LEVEL SECURITY;
        ALTER TABLE migration_leases ENABLE ROW LEVEL SECURITY;
        CREATE POLICY tenant_isolation_migration_scope_manifests
            ON migration_scope_manifests
            USING (tenant_id = nullif(current_setting('app.tenant_id', true), ''))
            WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id', true), ''));
        CREATE POLICY tenant_isolation_migration_leases
            ON migration_leases
            USING (tenant_id = nullif(current_setting('app.tenant_id', true), ''))
            WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id', true), ''));
        REVOKE ALL ON TABLE migration_scope_manifests FROM PUBLIC;
        REVOKE ALL ON TABLE migration_leases FROM PUBLIC;
        DO $block$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_runtime') THEN
                GRANT SELECT, INSERT, UPDATE, DELETE
                    ON TABLE migration_scope_manifests, migration_leases TO trpc_runtime;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_migration') THEN
                ALTER TABLE migration_scope_manifests OWNER TO trpc_migration;
                ALTER TABLE migration_leases OWNER TO trpc_migration;
            END IF;
        END
        $block$;
        """
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS migration_leases CASCADE")
    op.execute("DROP TABLE IF EXISTS migration_scope_manifests CASCADE")
