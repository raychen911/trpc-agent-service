"""Separate cross-tenant Worker SQL from the ordinary tenant runtime role.

Revision ID: 0014_worker_database_role
Revises: 0013_migration_write_barrier

The database bootstrap/provisioning path creates ``trpc_worker`` as a
non-superuser with ``BYPASSRLS``.  This migration is deliberately limited to
object privileges: an existing database must run the administrator bootstrap
before Alembic so the role exists, and the migration then moves the global
queue/recovery entry points to that explicit login.

``resolve_channel_binding`` remains available to ``trpc_runtime`` because the
callback gateway uses its unique binding identifier to discover the tenant
before a tenant context exists.  The dedicated Worker login also receives
this narrow routing lookup because the WeCom connector and worker startup
probe use it.  All queue/recovery functions that enumerate or mutate rows
across tenants are Worker-only.
"""

from __future__ import annotations

from alembic import op

revision = "0014_worker_database_role"
down_revision = "0013_migration_write_barrier"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        DO $roles$
        DECLARE
            runtime_is_superuser boolean;
            runtime_bypasses_rls boolean;
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_worker') THEN
                RAISE EXCEPTION
                    'trpc_worker role must be provisioned before migration 0014';
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'trpc_runtime') THEN
                RAISE EXCEPTION
                    'trpc_runtime role must be provisioned before migration 0014';
            END IF;
            SELECT rolsuper, rolbypassrls
              INTO runtime_is_superuser, runtime_bypasses_rls
              FROM pg_roles
             WHERE rolname = 'trpc_runtime';
            IF runtime_is_superuser OR runtime_bypasses_rls THEN
                RAISE EXCEPTION
                    'trpc_runtime must be NOSUPERUSER NOBYPASSRLS before migration 0014';
            END IF;
        END
        $roles$;

        REVOKE EXECUTE ON FUNCTION public.list_channel_bindings(text)
            FROM PUBLIC, trpc_runtime;
        REVOKE EXECUTE ON FUNCTION public.claim_outbox_events(text,text,integer,integer)
            FROM PUBLIC, trpc_runtime;
        REVOKE EXECUTE ON FUNCTION public.sweep_expired_session_leases(integer)
            FROM PUBLIC, trpc_runtime;
        REVOKE EXECUTE ON FUNCTION public.schedule_session_mailbox_retries(integer)
            FROM PUBLIC, trpc_runtime;
        REVOKE EXECUTE ON FUNCTION public.reconcile_session_mailboxes(integer)
            FROM PUBLIC, trpc_runtime;
        REVOKE EXECUTE ON FUNCTION public.reconcile_session_mailboxes_v2(integer,integer)
            FROM PUBLIC, trpc_runtime;
        GRANT EXECUTE ON FUNCTION public.resolve_channel_binding(text) TO trpc_worker;
        GRANT EXECUTE ON FUNCTION public.list_channel_bindings(text) TO trpc_worker;
        GRANT EXECUTE ON FUNCTION public.claim_outbox_events(text,text,integer,integer)
            TO trpc_worker;
        GRANT EXECUTE ON FUNCTION public.sweep_expired_session_leases(integer) TO trpc_worker;
        GRANT EXECUTE ON FUNCTION public.schedule_session_mailbox_retries(integer)
            TO trpc_worker;
        GRANT EXECUTE ON FUNCTION public.reconcile_session_mailboxes(integer) TO trpc_worker;
        GRANT EXECUTE ON FUNCTION public.reconcile_session_mailboxes_v2(integer,integer)
            TO trpc_worker;

        GRANT USAGE ON SCHEMA public TO trpc_worker;
        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE
            tenants,
            agent_apps,
            config_revisions,
            storage_profiles,
            tenant_policies,
            admin_idempotency,
            channel_bindings,
            channel_identities,
            inbound_messages,
            outbound_messages,
            delivery_attempts,
            sessions,
            session_turns,
            turn_intents,
            session_events,
            session_summaries,
            memories,
            artifacts,
            knowledge_items,
            knowledge_embeddings,
            outbox_events,
            dead_letters,
            tool_executions,
            confirmation_challenges,
            audit_logs,
            tenant_budget_usage,
            session_mailboxes,
            session_mailbox_items
            TO trpc_worker;
        GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE fault_stage_controls TO trpc_worker;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        REVOKE EXECUTE ON FUNCTION public.list_channel_bindings(text) FROM trpc_worker;
        REVOKE EXECUTE ON FUNCTION public.claim_outbox_events(text,text,integer,integer)
            FROM trpc_worker;
        REVOKE EXECUTE ON FUNCTION public.sweep_expired_session_leases(integer)
            FROM trpc_worker;
        REVOKE EXECUTE ON FUNCTION public.schedule_session_mailbox_retries(integer)
            FROM trpc_worker;
        REVOKE EXECUTE ON FUNCTION public.reconcile_session_mailboxes(integer)
            FROM trpc_worker;
        REVOKE EXECUTE ON FUNCTION public.reconcile_session_mailboxes_v2(integer,integer)
            FROM trpc_worker;
        REVOKE EXECUTE ON FUNCTION public.resolve_channel_binding(text) FROM trpc_worker;
        GRANT EXECUTE ON FUNCTION public.list_channel_bindings(text) TO trpc_runtime;
        GRANT EXECUTE ON FUNCTION public.claim_outbox_events(text,text,integer,integer)
            TO trpc_runtime;
        GRANT EXECUTE ON FUNCTION public.sweep_expired_session_leases(integer) TO trpc_runtime;
        GRANT EXECUTE ON FUNCTION public.schedule_session_mailbox_retries(integer)
            TO trpc_runtime;
        GRANT EXECUTE ON FUNCTION public.reconcile_session_mailboxes(integer) TO trpc_runtime;
        GRANT EXECUTE ON FUNCTION public.reconcile_session_mailboxes_v2(integer,integer)
            TO trpc_runtime;
        REVOKE USAGE ON SCHEMA public FROM trpc_worker;
        REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLE
            tenants,
            agent_apps,
            config_revisions,
            storage_profiles,
            tenant_policies,
            admin_idempotency,
            channel_bindings,
            channel_identities,
            inbound_messages,
            outbound_messages,
            delivery_attempts,
            sessions,
            session_turns,
            turn_intents,
            session_events,
            session_summaries,
            memories,
            artifacts,
            knowledge_items,
            knowledge_embeddings,
            outbox_events,
            dead_letters,
            tool_executions,
            confirmation_challenges,
            audit_logs,
            tenant_budget_usage,
            session_mailboxes,
            session_mailbox_items
            FROM trpc_worker;
        REVOKE SELECT, INSERT, UPDATE, DELETE ON TABLE fault_stage_controls FROM trpc_worker;
        """
    )
