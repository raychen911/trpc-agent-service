"""Persist and enforce the tenant migration write barrier.

Revision ID: 0013_migration_write_barrier
Revises: 0012_migration_lease_owner_instance

The barrier is intentionally database-enforced.  A normal runtime transaction
does not carry the migration fence settings and is rejected while a migration
is active; the migration target sets the settings and validates the matching
lease in the same transaction as its data write.
"""

from __future__ import annotations

from alembic import op

revision = "0013_migration_write_barrier"
down_revision = "0012_migration_lease_owner_instance"
branch_labels = None
depends_on = None


_BARRIER_TABLES = (
    "inbound_messages",
    "outbound_messages",
    "delivery_attempts",
    "sessions",
    "session_turns",
    "turn_intents",
    "session_events",
    "session_summaries",
    "memories",
    "artifacts",
    "knowledge_items",
    "knowledge_embeddings",
    "outbox_events",
    "dead_letters",
    "tool_executions",
    "confirmation_challenges",
    "audit_logs",
    "session_mailboxes",
    "session_mailbox_items",
)


def upgrade() -> None:
    table_literal = ",".join(f"'{name}'" for name in _BARRIER_TABLES)
    op.execute(
        """
        CREATE TABLE migration_write_barriers (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            migration_id text NOT NULL,
            owner_instance text NOT NULL
                CHECK (char_length(owner_instance) BETWEEN 1 AND 256),
            lease_epoch bigint NOT NULL CHECK (lease_epoch >= 1),
            mode text NOT NULL CHECK (mode IN ('active','released')),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id),
            FOREIGN KEY (tenant_id, migration_id)
                REFERENCES migration_scope_manifests(tenant_id, migration_id)
        );
        CREATE INDEX ix_migration_write_barriers_tenant_fence
            ON migration_write_barriers (tenant_id, migration_id, owner_instance, lease_epoch);
        ALTER TABLE migration_write_barriers ENABLE ROW LEVEL SECURITY;
        CREATE POLICY tenant_isolation_migration_write_barriers
            ON migration_write_barriers
            USING (tenant_id = nullif(current_setting('app.tenant_id', true), ''))
            WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id', true), ''));
        REVOKE ALL ON TABLE migration_write_barriers FROM PUBLIC;
        DO $grant$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_runtime') THEN
                GRANT SELECT, INSERT, UPDATE, DELETE
                    ON TABLE migration_write_barriers TO trpc_runtime;
            END IF;
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_migration') THEN
                ALTER TABLE migration_write_barriers OWNER TO trpc_migration;
            END IF;
        END
        $grant$;

        ALTER TABLE migration_scope_manifests
            DROP CONSTRAINT IF EXISTS migration_scope_manifests_source_count_max;
        ALTER TABLE migration_scope_manifests
            ADD CONSTRAINT migration_scope_manifests_source_count_max
            CHECK (source_count <= 1000000);
        ALTER TABLE migration_checkpoints
            DROP CONSTRAINT IF EXISTS migration_checkpoints_source_count_max;
        ALTER TABLE migration_checkpoints
            ADD CONSTRAINT migration_checkpoints_source_count_max
            CHECK (source_count BETWEEN 0 AND 1000000);
        ALTER TABLE migration_checkpoints
            DROP CONSTRAINT IF EXISTS migration_checkpoints_target_count_max;
        ALTER TABLE migration_checkpoints
            ADD CONSTRAINT migration_checkpoints_target_count_max
            CHECK (target_count BETWEEN 0 AND 1000000);

        CREATE OR REPLACE FUNCTION migration_write_barrier_guard()
        RETURNS trigger
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = public
        AS $function$
        DECLARE
            effective_tenant text;
            barrier migration_write_barriers%ROWTYPE;
            active_migration text;
            active_owner_instance text;
            active_epoch text;
        BEGIN
            effective_tenant := COALESCE(NEW.tenant_id, OLD.tenant_id);
            SELECT * INTO barrier
              FROM public.migration_write_barriers
             WHERE tenant_id=effective_tenant AND mode='active';
            IF FOUND THEN
                active_migration := current_setting('app.migration_id', true);
                active_owner_instance := current_setting('app.migration_owner_instance', true);
                active_epoch := current_setting('app.migration_lease_epoch', true);
                IF active_migration IS DISTINCT FROM barrier.migration_id
                   OR active_owner_instance IS DISTINCT FROM barrier.owner_instance
                   OR active_epoch IS DISTINCT FROM barrier.lease_epoch::text THEN
                    RAISE EXCEPTION
                        'tenant % is protected by an active migration write barrier',
                        effective_tenant
                        USING ERRCODE='55000';
                END IF;
            END IF;
            RETURN COALESCE(NEW, OLD);
        END;
        $function$;
        REVOKE ALL ON FUNCTION migration_write_barrier_guard() FROM PUBLIC;
        """
    )
    op.execute(
        f"""
        DO $triggers$
        DECLARE
            table_name text;
        BEGIN
            FOREACH table_name IN ARRAY ARRAY[{table_literal}] LOOP
                IF to_regclass('public.' || table_name) IS NOT NULL THEN
                    EXECUTE format(
                        'DROP TRIGGER IF EXISTS migration_write_barrier_%I ON public.%I',
                        table_name, table_name
                    );
                    EXECUTE format(
                        'CREATE TRIGGER migration_write_barrier_%I '
                        'BEFORE INSERT OR UPDATE OR DELETE ON public.%I '
                        'FOR EACH ROW EXECUTE FUNCTION public.migration_write_barrier_guard()',
                        table_name, table_name
                    );
                END IF;
            END LOOP;
        END;
        $triggers$;
        """
    )


def downgrade() -> None:
    table_literal = ",".join(f"'{name}'" for name in _BARRIER_TABLES)
    op.execute(
        f"""
        DO $triggers$
        DECLARE
            table_name text;
        BEGIN
            FOREACH table_name IN ARRAY ARRAY[{table_literal}] LOOP
                IF to_regclass('public.' || table_name) IS NOT NULL THEN
                    EXECUTE format(
                        'DROP TRIGGER IF EXISTS migration_write_barrier_%I ON public.%I',
                        table_name, table_name
                    );
                END IF;
            END LOOP;
        END;
        $triggers$;
        DROP FUNCTION IF EXISTS migration_write_barrier_guard();
        DROP TABLE IF EXISTS migration_write_barriers CASCADE;
        ALTER TABLE migration_checkpoints
            DROP CONSTRAINT IF EXISTS migration_checkpoints_source_count_max;
        ALTER TABLE migration_checkpoints
            DROP CONSTRAINT IF EXISTS migration_checkpoints_target_count_max;
        ALTER TABLE migration_scope_manifests
            DROP CONSTRAINT IF EXISTS migration_scope_manifests_source_count_max;
        """
    )
