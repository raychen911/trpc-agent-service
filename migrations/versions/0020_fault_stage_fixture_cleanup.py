"""Add a bounded fault-stage fixture cleanup entry point.

Revision ID: 0020_fault_stage_fixture_cleanup
Revises: 0019_migration_protected_target_counts

The runtime role deliberately cannot delete the worker-owned IM acceptance
tables.  Cleanup therefore runs through one database-owned function whose
scope is limited to an ownership-proven synthetic fault-stage tenant.
"""

from __future__ import annotations

from alembic import op

revision = "0020_fault_stage_fixture_cleanup"
down_revision = "0019_migration_protected_target_counts"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        r"""
        CREATE OR REPLACE FUNCTION public.cleanup_fault_stage_fixture(
            p_tenant_id text,
            p_run_id text,
            p_case_id text
        )
        RETURNS jsonb
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
        DECLARE
            expected_tenant_id text;
            ownership_count bigint;
            deleted_count bigint;
            deleted_counts jsonb := '{}'::jsonb;
        BEGIN
            IF p_run_id IS NULL
               OR pg_catalog.length(p_run_id) NOT BETWEEN 1 AND 128
               OR p_run_id IS DISTINCT FROM pg_catalog.btrim(p_run_id) THEN
                RAISE EXCEPTION 'fault-stage run identity is invalid'
                    USING ERRCODE = '22023';
            END IF;
            IF p_case_id IS NULL
               OR p_case_id !~ '^case-[0-9a-f]{32}$' THEN
                RAISE EXCEPTION 'fault-stage case identity is invalid'
                    USING ERRCODE = '22023';
            END IF;

            expected_tenant_id :=
                'fault-' || pg_catalog.substr(p_run_id, 1, 40) || '-' ||
                pg_catalog.substr(
                    pg_catalog.encode(
                        public.digest(
                            pg_catalog.convert_to(p_run_id, 'UTF8'),
                            'sha256'
                        ),
                        'hex'
                    ),
                    1,
                    12
                ) || '-' || pg_catalog.substr(p_case_id, 6);
            IF p_tenant_id IS NULL
               OR p_tenant_id IS DISTINCT FROM expected_tenant_id
               OR p_tenant_id IS DISTINCT FROM
                    pg_catalog.current_setting('app.tenant_id', true) THEN
                RAISE EXCEPTION 'fault-stage tenant context mismatch'
                    USING ERRCODE = '42501';
            END IF;

            PERFORM pg_catalog.pg_advisory_xact_lock(
                pg_catalog.hashtextextended(p_tenant_id, 0)
            );

            IF EXISTS (
                SELECT 1
                  FROM public.migration_write_barriers AS barrier
                 WHERE barrier.tenant_id = p_tenant_id
                   AND barrier.mode = 'active'
            ) THEN
                RAISE EXCEPTION
                    'fault-stage fixture tenant has an active migration write barrier'
                    USING ERRCODE = '55000';
            END IF;

            SELECT pg_catalog.count(*)
              INTO ownership_count
              FROM public.tenants AS tenant
              JOIN public.audit_logs AS audit
                ON audit.tenant_id = tenant.tenant_id
              JOIN public.admin_idempotency AS idempotency
                ON idempotency.tenant_id = tenant.tenant_id
             WHERE tenant.tenant_id = p_tenant_id
               AND tenant.display_name = 'fault-stage-acceptance'
               AND audit.user_id = 'fault-stage-acceptance'
               AND audit.decision = 'tenant_created'
               AND audit.idempotency_key = p_case_id || '\:tenant'
               AND audit.trace_id = 'admin:' || p_case_id || '\:tenant'
               AND idempotency.idempotency_key = p_case_id || '\:tenant'
               AND idempotency.operation = 'create_tenant'
               AND idempotency.request_hash = pg_catalog.encode(
                    public.digest(
                        pg_catalog.convert_to(p_case_id, 'UTF8'),
                        'sha256'
                    ),
                    'hex'
               )
               AND idempotency.response_status = 201
               AND idempotency.response_json ->> 'tenant_id' = p_tenant_id
               AND idempotency.response_json ->> 'display_name' =
                    'fault-stage-acceptance';
            IF ownership_count <> 1 THEN
                RAISE EXCEPTION 'fault-stage fixture ownership proof is missing'
                    USING ERRCODE = '22023';
            END IF;

            DELETE FROM public.session_mailbox_items
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'session_mailbox_items', deleted_count
            );

            DELETE FROM public.delivery_attempts
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'delivery_attempts', deleted_count
            );

            DELETE FROM public.outbound_messages
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'outbound_messages', deleted_count
            );

            DELETE FROM public.turn_intents
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'turn_intents', deleted_count
            );

            DELETE FROM public.session_events
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'session_events', deleted_count
            );

            DELETE FROM public.session_summaries
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'session_summaries', deleted_count
            );

            DELETE FROM public.tool_executions
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'tool_executions', deleted_count
            );

            DELETE FROM public.session_turns
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'session_turns', deleted_count
            );

            DELETE FROM public.inbound_messages
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'inbound_messages', deleted_count
            );

            DELETE FROM public.channel_identities
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'channel_identities', deleted_count
            );

            DELETE FROM public.im_acceptance_evidence_events
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'im_acceptance_evidence_events', deleted_count
            );

            DELETE FROM public.wecom_connection_state
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'wecom_connection_state', deleted_count
            );

            DELETE FROM public.channel_bindings
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'channel_bindings', deleted_count
            );

            DELETE FROM public.memories
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'memories', deleted_count
            );

            DELETE FROM public.artifacts
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'artifacts', deleted_count
            );

            DELETE FROM public.knowledge_embeddings
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'knowledge_embeddings', deleted_count
            );

            DELETE FROM public.knowledge_items
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'knowledge_items', deleted_count
            );

            DELETE FROM public.outbox_events
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'outbox_events', deleted_count
            );

            DELETE FROM public.dead_letters
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'dead_letters', deleted_count
            );

            DELETE FROM public.confirmation_challenges
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'confirmation_challenges', deleted_count
            );

            DELETE FROM public.tenant_budget_usage
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'tenant_budget_usage', deleted_count
            );

            DELETE FROM public.audit_logs
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'audit_logs', deleted_count
            );

            DELETE FROM public.migration_checkpoints
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'migration_checkpoints', deleted_count
            );

            DELETE FROM public.migration_leases
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'migration_leases', deleted_count
            );

            DELETE FROM public.migration_write_barriers
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'migration_write_barriers', deleted_count
            );

            DELETE FROM public.migration_scope_manifests
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'migration_scope_manifests', deleted_count
            );

            DELETE FROM public.fault_stage_controls
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'fault_stage_controls', deleted_count
            );

            DELETE FROM public.admin_idempotency
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'admin_idempotency', deleted_count
            );

            DELETE FROM public.config_revisions
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'config_revisions', deleted_count
            );

            DELETE FROM public.storage_profiles
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'storage_profiles', deleted_count
            );

            DELETE FROM public.tenant_policies
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'tenant_policies', deleted_count
            );

            DELETE FROM public.session_mailboxes
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'session_mailboxes', deleted_count
            );

            DELETE FROM public.sessions
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'sessions', deleted_count
            );

            DELETE FROM public.agent_apps
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'agent_apps', deleted_count
            );

            DELETE FROM public.tenants
             WHERE tenant_id = p_tenant_id;
            GET DIAGNOSTICS deleted_count = ROW_COUNT;
            IF deleted_count <> 1 THEN
                RAISE EXCEPTION 'fault-stage fixture tenant cleanup was incomplete'
                    USING ERRCODE = 'P0001';
            END IF;
            deleted_counts := deleted_counts || pg_catalog.jsonb_build_object(
                'tenants', deleted_count
            );

            RETURN deleted_counts;
        END;
        $function$;

        ALTER FUNCTION public.cleanup_fault_stage_fixture(text, text, text)
            OWNER TO trpc_migration;
        REVOKE ALL ON FUNCTION public.cleanup_fault_stage_fixture(text, text, text)
            FROM PUBLIC, trpc_worker, trpc_metrics;
        GRANT EXECUTE ON FUNCTION public.cleanup_fault_stage_fixture(text, text, text)
            TO trpc_runtime;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        REVOKE ALL ON FUNCTION public.cleanup_fault_stage_fixture(text, text, text)
            FROM PUBLIC, trpc_runtime, trpc_worker, trpc_metrics;
        DROP FUNCTION IF EXISTS public.cleanup_fault_stage_fixture(text, text, text);
        """
    )
