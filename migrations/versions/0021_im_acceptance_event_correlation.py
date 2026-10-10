"""Correlate provider IM events with bounded acceptance evidence.

Deployment order: Migrations 0021 and 0022 must finish before any application
Pod that writes or reads the columns and table introduced here is started.

Revision ID: 0021_im_acceptance_event_correlation
Revises: 0020_fault_stage_fixture_cleanup
"""

from __future__ import annotations

from alembic import op

revision = "0021_im_acceptance_event_correlation"
down_revision = "0020_fault_stage_fixture_cleanup"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE public.inbound_messages
            ADD COLUMN provider_event_hash text
                CHECK (
                    provider_event_hash IS NULL
                    OR provider_event_hash ~ '^[0-9a-f]{64}$'
                ),
            ADD COLUMN delivery_count integer NOT NULL DEFAULT 1
                CHECK (delivery_count >= 1);

        ALTER TABLE public.delivery_attempts
            ADD COLUMN retry_after_seconds double precision
                CHECK (
                    retry_after_seconds IS NULL
                    OR (
                        retry_after_seconds >= 0
                        AND retry_after_seconds <= 3600
                    )
                );

        CREATE TABLE public.im_acceptance_runs (
            tenant_id text NOT NULL,
            binding_id text NOT NULL,
            channel text NOT NULL CHECK (channel IN ('feishu','wecom_ai_bot')),
            run_id_sha256 text NOT NULL CHECK (run_id_sha256 ~ '^[0-9a-f]{64}$'),
            run_nonce_sha256 text NOT NULL
                CHECK (run_nonce_sha256 ~ '^[0-9a-f]{64}$'),
            run_binding_sha256 text NOT NULL
                CHECK (run_binding_sha256 ~ '^[0-9a-f]{64}$'),
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            expires_at timestamptz NOT NULL,
            provider_event_hash text
                CHECK (
                    provider_event_hash IS NULL
                    OR provider_event_hash ~ '^[0-9a-f]{64}$'
                ),
            bound_at timestamptz,
            PRIMARY KEY (tenant_id, binding_id, channel, run_id_sha256),
            CONSTRAINT uq_im_acceptance_run_provider_event
                UNIQUE (tenant_id, binding_id, channel, provider_event_hash),
            FOREIGN KEY (tenant_id, binding_id)
                REFERENCES public.channel_bindings(tenant_id, binding_id)
                ON DELETE CASCADE,
            CHECK (expires_at > created_at),
            CHECK (
                (provider_event_hash IS NULL AND bound_at IS NULL)
                OR (provider_event_hash IS NOT NULL AND bound_at IS NOT NULL)
            )
        );

        ALTER TABLE public.im_acceptance_runs ENABLE ROW LEVEL SECURITY;
        CREATE POLICY tenant_isolation_im_acceptance_runs
            ON public.im_acceptance_runs
            USING (
                tenant_id = nullif(current_setting('app.tenant_id', true), '')
            )
            WITH CHECK (
                tenant_id = nullif(current_setting('app.tenant_id', true), '')
            );

        REVOKE ALL ON TABLE public.im_acceptance_runs
            FROM PUBLIC, trpc_runtime, trpc_worker;
        GRANT SELECT ON TABLE
            public.wecom_connection_state,
            public.im_acceptance_evidence_events
            TO trpc_runtime;
        GRANT SELECT, INSERT, UPDATE, DELETE
            ON TABLE public.im_acceptance_runs TO trpc_runtime;

        CREATE TRIGGER migration_write_barrier_im_acceptance_runs
            BEFORE INSERT OR UPDATE OR DELETE ON public.im_acceptance_runs
            FOR EACH ROW EXECUTE FUNCTION public.migration_write_barrier_guard();

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
            SELECT 'wecom_connection_state'::text,
                   pg_catalog.count(*)::bigint
              FROM public.wecom_connection_state
             WHERE tenant_id = p_tenant_id
            UNION ALL
            SELECT 'im_acceptance_evidence_events'::text,
                   pg_catalog.count(*)::bigint
              FROM public.im_acceptance_evidence_events
             WHERE tenant_id = p_tenant_id
            UNION ALL
            SELECT 'im_acceptance_runs'::text,
                   pg_catalog.count(*)::bigint
              FROM public.im_acceptance_runs
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
            SELECT 'wecom_connection_state'::text,
                   pg_catalog.count(*)::bigint
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

        REVOKE SELECT ON TABLE
            public.wecom_connection_state,
            public.im_acceptance_evidence_events
            FROM trpc_runtime;
        REVOKE ALL ON TABLE public.im_acceptance_runs
            FROM PUBLIC, trpc_runtime, trpc_worker;
        DROP TABLE IF EXISTS public.im_acceptance_runs;
        ALTER TABLE public.delivery_attempts
            DROP COLUMN IF EXISTS retry_after_seconds;
        ALTER TABLE public.inbound_messages
            DROP COLUMN IF EXISTS delivery_count,
            DROP COLUMN IF EXISTS provider_event_hash;
        """
    )
