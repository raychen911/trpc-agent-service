"""Expose bounded target counts for worker-owned migration tables.

Revision ID: 0019_migration_protected_target_counts
Revises: 0018_performance_fixture_cleanup

The runtime migration guard must prove that every guarded tenant table is
empty, but the IM connection and acceptance-evidence tables are deliberately
worker-only.  This function exposes only the two row counts for the current
tenant context without granting the runtime role direct table access.
"""

from __future__ import annotations

from alembic import op

revision = "0019_migration_protected_target_counts"
down_revision = "0018_performance_fixture_cleanup"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        r"""
        CREATE OR REPLACE FUNCTION public.migration_protected_target_counts(
            p_tenant_id text
        )
        RETURNS TABLE(table_name text, row_count bigint)
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
        BEGIN
            IF p_tenant_id IS NULL
               OR pg_catalog.length(p_tenant_id) NOT BETWEEN 1 AND 256
               OR p_tenant_id IS DISTINCT FROM
                    pg_catalog.current_setting('app.tenant_id', true) THEN
                RAISE EXCEPTION 'migration target tenant context mismatch'
                    USING ERRCODE = '42501';
            END IF;

            RETURN QUERY
            SELECT 'wecom_connection_state'::text, pg_catalog.count(*)::bigint
              FROM public.wecom_connection_state
             WHERE tenant_id = p_tenant_id
            UNION ALL
            SELECT 'im_acceptance_evidence_events'::text,
                   pg_catalog.count(*)::bigint
              FROM public.im_acceptance_evidence_events
             WHERE tenant_id = p_tenant_id;
        END;
        $function$;

        ALTER FUNCTION public.migration_protected_target_counts(text)
            OWNER TO trpc_migration;
        REVOKE ALL ON FUNCTION public.migration_protected_target_counts(text)
            FROM PUBLIC, trpc_runtime, trpc_worker, trpc_metrics;
        GRANT EXECUTE ON FUNCTION public.migration_protected_target_counts(text)
            TO trpc_runtime;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        REVOKE ALL ON FUNCTION public.migration_protected_target_counts(text)
            FROM PUBLIC, trpc_runtime, trpc_worker, trpc_metrics;
        DROP FUNCTION IF EXISTS public.migration_protected_target_counts(text);
        """
    )
