"""Finalize fencing columns and immutable application revisions.

The runtime already fences session/outbox work.  This revision gives tool
execution rows the same owner/epoch state and gives migration manifests an
exact application revision to validate instead of selecting an arbitrary
latest configuration.
"""

from __future__ import annotations

from alembic import op

revision = "0010_consistency_guards"
down_revision = "0009_session_ready_replay_guard"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE agent_apps
            ADD COLUMN IF NOT EXISTS control_version bigint NOT NULL DEFAULT 1;
        DO $constraint$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                 WHERE conname='agent_apps_control_version_positive'
            ) THEN
                ALTER TABLE agent_apps
                    ADD CONSTRAINT agent_apps_control_version_positive
                    CHECK (control_version > 0);
            END IF;
        END
        $constraint$;

        ALTER TABLE tool_executions
            ADD COLUMN IF NOT EXISTS lease_owner text;
        ALTER TABLE tool_executions
            ADD COLUMN IF NOT EXISTS lease_epoch bigint NOT NULL DEFAULT 0;
        ALTER TABLE memories
            ADD COLUMN IF NOT EXISTS source_record_id text;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_memories_tenant_source_record
            ON memories (tenant_id, source_record_id)
            WHERE source_record_id IS NOT NULL;
        CREATE UNIQUE INDEX IF NOT EXISTS uq_outbox_active_outbound
            ON outbox_events (tenant_id, aggregate_id, event_type)
            WHERE aggregate_type='outbound' AND published_at IS NULL;
        UPDATE tool_executions
           SET lease_owner=execution_key
         WHERE lease_owner IS NULL;
        ALTER TABLE tool_executions
            ALTER COLUMN lease_owner SET NOT NULL;
        DO $constraint$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                 WHERE conname='tool_executions_lease_epoch_nonnegative'
            ) THEN
                ALTER TABLE tool_executions
                    ADD CONSTRAINT tool_executions_lease_epoch_nonnegative
                    CHECK (lease_epoch >= 0);
            END IF;
        END
        $constraint$;
        CREATE INDEX IF NOT EXISTS ix_tool_executions_tenant_owner
            ON tool_executions (tenant_id, lease_owner, lease_epoch);

        -- Keep recovery fail-closed when a v1/v2 scheduler cutover left the
        -- mailbox and authoritative session lease out of sync.  A mailbox is
        -- re-queued only when there is no active session lease belonging to a
        -- different worker; this prevents reconciliation from evicting a
        -- live replacement worker.
        ALTER FUNCTION reconcile_session_mailboxes_v2(integer,integer)
            RENAME TO reconcile_session_mailboxes_v2_legacy;
        CREATE FUNCTION reconcile_session_mailboxes_v2(
            p_limit integer,
            p_replay_cooldown_seconds integer
        )
        RETURNS integer
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
        BEGIN
            IF p_limit < 1 OR p_limit > 1000 THEN
                RAISE EXCEPTION 'recovery limit must be between 1 and 1000';
            END IF;
            IF p_replay_cooldown_seconds < 5
               OR p_replay_cooldown_seconds > 86400 THEN
                RAISE EXCEPTION
                    'ready replay cooldown must be between 5 and 86400 seconds';
            END IF;
            UPDATE public.session_mailboxes AS m
               SET status='QUEUED',
                   processing_sequence=NULL,
                   processing_inbound_id=NULL,
                   lease_owner=NULL,
                   lease_expires_at=NULL,
                   retry_at=NULL,
                   queue_generation=queue_generation+1,
                   updated_at=clock_timestamp()
             WHERE m.status='RUNNING'
               AND m.lease_expires_at <= clock_timestamp()
               AND NOT EXISTS (
                    SELECT 1 FROM public.sessions AS same
                     WHERE same.tenant_id=m.tenant_id
                       AND same.session_id=m.session_id
                       AND same.lease_owner=m.lease_owner
                       AND same.lease_epoch=m.lease_epoch
                       AND same.lease_expires_at IS NOT DISTINCT FROM m.lease_expires_at
                       AND same.lease_expires_at > clock_timestamp()
               )
               AND NOT EXISTS (
                    SELECT 1 FROM public.sessions AS active
                     WHERE active.tenant_id=m.tenant_id
                       AND active.session_id=m.session_id
                       AND active.lease_owner IS NOT NULL
                       AND active.lease_expires_at > clock_timestamp()
               );
            RETURN reconcile_session_mailboxes_v2_legacy(
                p_limit, p_replay_cooldown_seconds
            );
        END
        $function$;
        REVOKE ALL ON FUNCTION reconcile_session_mailboxes_v2(integer,integer) FROM PUBLIC;
        DO $grant$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_runtime') THEN
                GRANT EXECUTE ON FUNCTION reconcile_session_mailboxes_v2(integer,integer)
                    TO trpc_runtime;
            END IF;
        END
        $grant$;
        """
    )


def downgrade() -> None:
    op.execute(
        """
        DROP INDEX IF EXISTS ix_tool_executions_tenant_owner;
        DROP FUNCTION IF EXISTS reconcile_session_mailboxes_v2(integer,integer);
        ALTER FUNCTION reconcile_session_mailboxes_v2_legacy(integer,integer)
            RENAME TO reconcile_session_mailboxes_v2;
        DROP INDEX IF EXISTS uq_memories_tenant_source_record;
        DROP INDEX IF EXISTS uq_outbox_active_outbound;
        ALTER TABLE memories DROP COLUMN IF EXISTS source_record_id;
        ALTER TABLE tool_executions
            DROP CONSTRAINT IF EXISTS tool_executions_lease_epoch_nonnegative;
        ALTER TABLE tool_executions DROP COLUMN IF EXISTS lease_epoch;
        ALTER TABLE tool_executions DROP COLUMN IF EXISTS lease_owner;
        ALTER TABLE agent_apps
            DROP CONSTRAINT IF EXISTS agent_apps_control_version_positive;
        ALTER TABLE agent_apps DROP COLUMN IF EXISTS control_version;
        """
    )
