"""Bound and repeatable SessionReady replay after durable publication.

Revision ID: 0009_session_ready_replay_guard
Revises: 0008_session_mailboxes

The 0008 reconciler correctly kept a queued generation stable, but it could
still reset a published outbox row on every recovery tick.  This revision
keeps the old one-argument function name as a compatibility wrapper while
moving the implementation to a parameterized function used by new workers.
The replay marker stores the last replay time, so a generation can recover
again after a later publish-then-loss while remaining bounded by cooldown.
"""

from __future__ import annotations

from alembic import op

revision = "0009_session_ready_replay_guard"
down_revision = "0008_session_mailboxes"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE outbox_events
            ADD COLUMN ready_replayed_at timestamptz;

        ALTER FUNCTION reconcile_session_mailboxes(integer)
            RENAME TO reconcile_session_mailboxes_legacy;

        CREATE OR REPLACE FUNCTION reconcile_session_mailboxes_v2(
            p_limit integer,
            p_replay_cooldown_seconds integer
        )
        RETURNS integer
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
        DECLARE
            rec record;
            next_status text;
            next_generation bigint;
            next_sequence bigint;
            next_retry_at timestamptz;
            next_priority integer;
            next_trace_id text;
            next_inbound_id uuid;
            processed integer := 0;
        BEGIN
            IF p_limit < 1 OR p_limit > 1000 THEN
                RAISE EXCEPTION 'recovery limit must be between 1 and 1000';
            END IF;
            IF p_replay_cooldown_seconds < 5
               OR p_replay_cooldown_seconds > 86400 THEN
                RAISE EXCEPTION
                    'ready replay cooldown must be between 5 and 86400 seconds';
            END IF;
            FOR rec IN
                SELECT m.tenant_id, m.session_id, m.status AS old_status,
                       m.accepted_sequence, m.resolved_sequence,
                       m.queue_generation,
                       m.processing_sequence, m.processing_inbound_id,
                       m.lease_owner, m.lease_epoch, m.lease_expires_at,
                       m.updated_at,
                       s.lease_owner AS session_lease_owner,
                       s.lease_epoch AS session_lease_epoch,
                       s.lease_expires_at AS session_lease_expires_at,
                       i.retry_at AS item_retry_at, i.priority AS item_priority,
                       i.trace_id, i.inbound_id, i.resolved_at,
                       inbound.status AS inbound_status
                  FROM public.session_mailboxes AS m
                  LEFT JOIN public.session_mailbox_items AS i
                    ON i.tenant_id=m.tenant_id
                   AND i.session_id=m.session_id
                   AND i.sequence=coalesce(m.processing_sequence,
                                           m.resolved_sequence+1)
                  LEFT JOIN public.inbound_messages AS inbound
                    ON inbound.tenant_id=i.tenant_id
                   AND inbound.inbound_id=i.inbound_id
                  LEFT JOIN public.sessions AS s
                    ON s.tenant_id=m.tenant_id
                   AND s.session_id=m.session_id
                 WHERE (
                       (m.status <> 'RUNNING'
                        AND (m.processing_sequence IS NOT NULL
                             OR m.processing_inbound_id IS NOT NULL
                             OR m.lease_owner IS NOT NULL
                             OR m.lease_expires_at IS NOT NULL))
                    OR (m.accepted_sequence <= m.resolved_sequence
                        AND m.status <> 'IDLE')
                    OR (m.accepted_sequence > m.resolved_sequence
                        AND m.status='IDLE')
                    OR (m.status='RETRY_WAIT' AND m.retry_at IS NULL)
                    OR (m.status='QUEUED' AND i.inbound_id IS NULL)
                    OR (m.status='QUEUED'
                        AND m.updated_at <= clock_timestamp() - interval '5 seconds')
                    OR (inbound.status='committed' AND i.resolved_at IS NULL)
                 )
                 ORDER BY m.updated_at, m.tenant_id, m.session_id
                 LIMIT p_limit
                 FOR UPDATE OF m SKIP LOCKED
            LOOP
                IF rec.old_status='RUNNING'
                   AND rec.processing_sequence IS NOT NULL
                   AND rec.processing_inbound_id IS NOT NULL
                   AND rec.lease_owner IS NOT NULL
                   AND rec.lease_expires_at > clock_timestamp()
                   AND rec.session_lease_owner=rec.lease_owner
                   AND rec.session_lease_epoch=rec.lease_epoch
                   AND rec.session_lease_expires_at IS NOT DISTINCT FROM rec.lease_expires_at
                   AND NOT (rec.inbound_status='committed'
                            AND rec.resolved_at IS NULL) THEN
                    processed := processed + 1;
                    CONTINUE;
                END IF;
                IF rec.inbound_status='committed' AND rec.resolved_at IS NULL THEN
                    next_sequence := coalesce(rec.processing_sequence,
                                              rec.resolved_sequence+1);
                    UPDATE public.session_mailbox_items
                       SET resolved_at=coalesce(resolved_at,clock_timestamp())
                     WHERE tenant_id=rec.tenant_id
                       AND session_id=rec.session_id
                       AND sequence=next_sequence;
                    SELECT i.retry_at, i.priority, i.trace_id, i.inbound_id
                      INTO next_retry_at, next_priority, next_trace_id,
                           next_inbound_id
                      FROM public.session_mailbox_items AS i
                     WHERE i.tenant_id=rec.tenant_id
                       AND i.session_id=rec.session_id
                       AND i.sequence=next_sequence+1;
                    IF next_inbound_id IS NULL
                       OR next_sequence >= rec.accepted_sequence THEN
                        next_status := 'IDLE';
                    ELSIF next_retry_at IS NOT NULL
                          AND next_retry_at > clock_timestamp() THEN
                        next_status := 'RETRY_WAIT';
                    ELSE
                        next_status := 'QUEUED';
                    END IF;
                    UPDATE public.session_mailboxes
                       SET resolved_sequence=next_sequence,
                           status=next_status,
                           processing_sequence=NULL,
                           processing_inbound_id=NULL,
                           lease_owner=NULL,
                           lease_expires_at=NULL,
                           retry_at=CASE WHEN next_status='RETRY_WAIT'
                                         THEN next_retry_at ELSE NULL END,
                           queue_generation=queue_generation
                               + CASE WHEN next_status='QUEUED' THEN 1 ELSE 0 END,
                           updated_at=clock_timestamp()
                     WHERE tenant_id=rec.tenant_id AND session_id=rec.session_id
                    RETURNING queue_generation INTO next_generation;
                    UPDATE public.sessions
                       SET lease_owner=NULL, lease_expires_at=NULL,
                           updated_at=clock_timestamp()
                     WHERE tenant_id=rec.tenant_id AND session_id=rec.session_id
                       AND lease_owner=rec.lease_owner
                       AND lease_epoch=rec.lease_epoch
                       AND lease_expires_at IS NOT DISTINCT FROM rec.lease_expires_at;
                    IF next_status='QUEUED' THEN
                        INSERT INTO public.outbox_events (
                            tenant_id, aggregate_type, aggregate_id, event_type,
                            payload_json
                        ) VALUES (
                            rec.tenant_id, 'session', rec.session_id,
                            'session.ready.v2',
                            jsonb_build_object(
                                'generation', next_generation,
                                'priority', coalesce(next_priority, 0),
                                'trace_id', coalesce(next_trace_id,
                                    next_inbound_id::text, rec.session_id),
                                'created_at', to_char(clock_timestamp()
                                    AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                            )
                        ) ON CONFLICT DO NOTHING;
                    END IF;
                    processed := processed + 1;
                    CONTINUE;
                END IF;
                IF rec.accepted_sequence <= rec.resolved_sequence
                   OR rec.inbound_id IS NULL THEN
                    next_status := 'IDLE';
                ELSIF rec.item_retry_at IS NOT NULL
                      AND rec.item_retry_at > clock_timestamp() THEN
                    next_status := 'RETRY_WAIT';
                ELSE
                    next_status := 'QUEUED';
                END IF;
                UPDATE public.session_mailboxes
                   SET status=next_status,
                       processing_sequence=NULL,
                       processing_inbound_id=NULL,
                       lease_owner=NULL,
                       lease_expires_at=NULL,
                       retry_at=CASE WHEN next_status='RETRY_WAIT'
                                     THEN rec.item_retry_at ELSE NULL END,
                       queue_generation=queue_generation
                           + CASE WHEN next_status='QUEUED'
                                  AND (rec.old_status <> 'QUEUED'
                                       OR rec.queue_generation < 1)
                                  THEN 1 ELSE 0 END,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=rec.tenant_id AND session_id=rec.session_id
                RETURNING queue_generation INTO next_generation;
                UPDATE public.sessions
                   SET lease_owner=NULL, lease_expires_at=NULL,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=rec.tenant_id AND session_id=rec.session_id
                   AND lease_owner=rec.lease_owner
                   AND lease_epoch=rec.lease_epoch
                   AND lease_expires_at IS NOT DISTINCT FROM rec.lease_expires_at;
                IF next_status='QUEUED'
                   AND (rec.old_status <> 'QUEUED'
                        OR rec.queue_generation < 1) THEN
                    INSERT INTO public.outbox_events (
                        tenant_id, aggregate_type, aggregate_id, event_type,
                        payload_json
                    ) VALUES (
                        rec.tenant_id, 'session', rec.session_id, 'session.ready.v2',
                        jsonb_build_object(
                            'generation', next_generation,
                            'priority', coalesce(rec.item_priority, 0),
                            'trace_id', coalesce(rec.trace_id, rec.inbound_id::text,
                                rec.session_id),
                            'created_at', to_char(clock_timestamp() AT TIME ZONE 'UTC',
                                'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                        )
                        ) ON CONFLICT DO NOTHING;
                ELSIF next_status='QUEUED' THEN
                    -- A published event is replay evidence only after the
                    -- bounded observation window.  The durable marker stores
                    -- the last replay time, so this generation can recover
                    -- again after a later publish-then-loss without being
                    -- reset on every 5s pass.
                    UPDATE public.outbox_events
                       SET published_at=NULL,
                           claimed_by=NULL,
                           claim_expires_at=NULL,
                           available_at=clock_timestamp(),
                           last_error_type=NULL,
                           ready_replayed_at=clock_timestamp()
                     WHERE tenant_id=rec.tenant_id
                       AND aggregate_type='session'
                       AND aggregate_id=rec.session_id
                       AND event_type='session.ready.v2'
                       AND (payload_json->>'generation')::bigint=next_generation
                       AND published_at IS NOT NULL
                       AND greatest(published_at,coalesce(ready_replayed_at,published_at))
                          <= clock_timestamp()
                               - make_interval(secs => p_replay_cooldown_seconds)
                       AND (claim_expires_at IS NULL
                            OR claim_expires_at <= clock_timestamp());
                    IF NOT FOUND THEN
                        INSERT INTO public.outbox_events (
                            tenant_id, aggregate_type, aggregate_id, event_type,
                            payload_json
                        ) VALUES (
                            rec.tenant_id, 'session', rec.session_id,
                            'session.ready.v2',
                            jsonb_build_object(
                                'generation', next_generation,
                                'priority', coalesce(rec.item_priority, 0),
                                'trace_id', coalesce(rec.trace_id, rec.inbound_id::text,
                                    rec.session_id),
                                'created_at', to_char(clock_timestamp()
                                    AT TIME ZONE 'UTC',
                                    'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                            )
                        ) ON CONFLICT DO NOTHING;
                    END IF;
                END IF;
                processed := processed + 1;
            END LOOP;
            RETURN processed;
        END
        $function$;
        REVOKE ALL ON FUNCTION reconcile_session_mailboxes_v2(integer,integer)
            FROM PUBLIC;

        CREATE FUNCTION reconcile_session_mailboxes(p_limit integer)
        RETURNS integer
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $wrapper$
        BEGIN
            RETURN reconcile_session_mailboxes_v2(p_limit, 30);
        END
        $wrapper$;
        REVOKE ALL ON FUNCTION reconcile_session_mailboxes(integer) FROM PUBLIC;

        DO $grant$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_runtime') THEN
                GRANT EXECUTE ON FUNCTION reconcile_session_mailboxes(integer)
                    TO trpc_runtime;
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
        DROP FUNCTION IF EXISTS reconcile_session_mailboxes(integer);
        DROP FUNCTION IF EXISTS reconcile_session_mailboxes_v2(integer,integer);
        ALTER FUNCTION reconcile_session_mailboxes_legacy(integer)
            RENAME TO reconcile_session_mailboxes;
        ALTER TABLE outbox_events
            DROP COLUMN IF EXISTS ready_replayed_at;
        """
    )
