"""Expose the authoritative SessionReady backlog to a metrics-only role.

Redis ``SessionReady`` is a reconstructable wake-up transport.  The number
of Redis stream entries or pending deliveries is therefore not the amount of
work waiting for a worker.  PostgreSQL ``session_mailboxes`` is authoritative
and this migration exposes only the bounded count needed by the HPA.

The ``trpc_metrics`` login is provisioned out-of-band.  It receives no table
privileges and can execute only the read-only ``SECURITY DEFINER`` function
created here.
"""

from __future__ import annotations

from alembic import op

revision = "0016_session_ready_backlog_metric"
down_revision = "0015_worker_database_role_contract"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $roles$
        DECLARE
            metrics_can_login boolean;
            metrics_is_superuser boolean;
            metrics_can_create_database boolean;
            metrics_can_create_role boolean;
            metrics_inherits boolean;
            metrics_bypasses_rls boolean;
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_metrics') THEN
                RAISE EXCEPTION
                    'trpc_metrics role must be provisioned before migration 0016';
            END IF;
            SELECT rolcanlogin, rolsuper, rolcreatedb, rolcreaterole,
                   rolinherit, rolbypassrls
              INTO metrics_can_login, metrics_is_superuser,
                   metrics_can_create_database, metrics_can_create_role,
                   metrics_inherits, metrics_bypasses_rls
              FROM pg_roles
             WHERE rolname = 'trpc_metrics';
            IF metrics_can_login IS DISTINCT FROM TRUE
               OR metrics_is_superuser IS DISTINCT FROM FALSE
               OR metrics_can_create_database IS DISTINCT FROM FALSE
               OR metrics_can_create_role IS DISTINCT FROM FALSE
               OR metrics_inherits IS DISTINCT FROM FALSE
               OR metrics_bypasses_rls IS DISTINCT FROM FALSE THEN
                RAISE EXCEPTION
                    'trpc_metrics must be LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT NOBYPASSRLS before migration 0016';
            END IF;
        END
        $roles$;

        CREATE INDEX IF NOT EXISTS ix_session_mailboxes_ready_backlog
            ON public.session_mailboxes (retry_at)
            WHERE status='QUEUED'
              AND accepted_sequence > resolved_sequence;

        CREATE OR REPLACE FUNCTION public.count_session_ready_backlog()
        RETURNS bigint
        LANGUAGE sql
        VOLATILE
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
            SELECT count(*)::bigint
              FROM public.session_mailboxes
             WHERE status='QUEUED'
               AND accepted_sequence > resolved_sequence
               AND (retry_at IS NULL OR retry_at <= clock_timestamp())
        $function$;

        REVOKE ALL ON FUNCTION public.count_session_ready_backlog() FROM PUBLIC;
        REVOKE ALL ON FUNCTION public.count_session_ready_backlog()
            FROM trpc_runtime, trpc_worker;
        REVOKE ALL ON TABLE public.session_mailboxes FROM trpc_metrics;
        GRANT USAGE ON SCHEMA public TO trpc_metrics;
        GRANT EXECUTE ON FUNCTION public.count_session_ready_backlog() TO trpc_metrics;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        REVOKE ALL ON FUNCTION public.count_session_ready_backlog() FROM PUBLIC;
        REVOKE ALL ON FUNCTION public.count_session_ready_backlog()
            FROM trpc_metrics, trpc_runtime, trpc_worker;
        DROP FUNCTION IF EXISTS public.count_session_ready_backlog();
        DROP INDEX IF EXISTS public.ix_session_mailboxes_ready_backlog;
        """
    )
