"""Make mailbox recovery bounded and authoritative-session fenced.

The earlier mailbox migrations are already applied in deployed databases and
are intentionally kept immutable.  This revision replaces their recovery
functions in-place for both fresh upgrades and databases upgrading from
``0010_consistency_guards``.

Recovery always locks ``session_mailboxes`` before ``sessions``.  The latter
is the authoritative execution lease; an active lease therefore prevents a
mailbox reset even when the derived mailbox lease is expired or belongs to an
older owner/epoch.  Both functions use a bounded ``FOR UPDATE SKIP LOCKED``
selection and create/re-open the durable ``session.ready.v2`` outbox row in
the same transaction as the mailbox transition.
"""

from __future__ import annotations

from alembic import op

revision = "0011_mailbox_recovery_fencing"
down_revision = "0010_consistency_guards"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE OR REPLACE FUNCTION sweep_expired_session_leases(p_limit integer)
        RETURNS integer
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
        DECLARE
            rec record;
            session_owner text;
            session_epoch bigint;
            session_expires_at timestamptz;
            next_status text;
            next_generation bigint;
            processed integer := 0;
        BEGIN
            IF p_limit < 1 OR p_limit > 1000 THEN
                RAISE EXCEPTION 'recovery limit must be between 1 and 1000';
            END IF;
            FOR rec IN
                SELECT m.tenant_id, m.session_id, m.accepted_sequence,
                       m.resolved_sequence, m.processing_inbound_id,
                       m.lease_owner, m.lease_epoch, m.lease_expires_at,
                       i.retry_at AS item_retry_at, i.priority AS item_priority,
                       i.trace_id
                  FROM public.session_mailboxes AS m
                  LEFT JOIN public.session_mailbox_items AS i
                    ON i.tenant_id=m.tenant_id
                   AND i.session_id=m.session_id
                   AND i.sequence=m.processing_sequence
                 WHERE m.status='RUNNING'
                   AND m.lease_expires_at <= clock_timestamp()
                   -- Read-only pre-filter prevents a full batch of already
                   -- active sessions from starving later recoverable rows.
                   -- The locked session check below remains mandatory for
                   -- the race between this snapshot and the loop body.
                   AND NOT EXISTS (
                        SELECT 1
                          FROM public.sessions AS active
                         WHERE active.tenant_id=m.tenant_id
                           AND active.session_id=m.session_id
                           AND active.lease_owner IS NOT NULL
                           AND active.lease_expires_at > clock_timestamp()
                   )
                 ORDER BY m.lease_expires_at, m.tenant_id, m.session_id
                 LIMIT p_limit
                 FOR UPDATE OF m SKIP LOCKED
            LOOP
                -- The mailbox is locked by the SELECT above.  Lock the
                -- authoritative session only after it, preserving one global
                -- lock order for workers and recovery roles.
                session_owner := NULL;
                session_epoch := NULL;
                session_expires_at := NULL;
                SELECT s.lease_owner, s.lease_epoch, s.lease_expires_at
                  INTO session_owner, session_epoch, session_expires_at
                  FROM public.sessions AS s
                 WHERE s.tenant_id=rec.tenant_id
                   AND s.session_id=rec.session_id
                 FOR UPDATE;
                IF session_owner IS NOT NULL
                   AND session_expires_at IS NOT NULL
                   AND session_expires_at > clock_timestamp() THEN
                    CONTINUE;
                END IF;

                IF rec.accepted_sequence <= rec.resolved_sequence THEN
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
                           + CASE WHEN next_status='QUEUED' THEN 1 ELSE 0 END,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=rec.tenant_id AND session_id=rec.session_id
                RETURNING queue_generation INTO next_generation;

                -- Clear only the lease represented by the expired mailbox.
                -- A newer owner/epoch, even if itself expired, is not erased.
                UPDATE public.sessions
                   SET lease_owner=NULL, lease_expires_at=NULL,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=rec.tenant_id AND session_id=rec.session_id
                   AND lease_owner IS NOT DISTINCT FROM rec.lease_owner
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
                            'priority', coalesce(rec.item_priority, 0),
                            'trace_id', coalesce(rec.trace_id,
                                rec.processing_inbound_id::text, rec.session_id),
                            'created_at', to_char(clock_timestamp() AT TIME ZONE 'UTC',
                                'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                        )
                    ) ON CONFLICT DO NOTHING;
                END IF;
                processed := processed + 1;
            END LOOP;
            RETURN processed;
        END
        $function$;
        REVOKE ALL ON FUNCTION sweep_expired_session_leases(integer) FROM PUBLIC;

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
            session_owner text;
            session_epoch bigint;
            session_expires_at timestamptz;
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
                 WHERE (
                       (m.status <> 'RUNNING'
                        AND (m.processing_sequence IS NOT NULL
                             OR m.processing_inbound_id IS NOT NULL
                             OR m.lease_owner IS NOT NULL
                             OR m.lease_expires_at IS NOT NULL))
                    OR (m.status='RUNNING'
                        AND m.lease_expires_at <= clock_timestamp())
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
                   AND NOT EXISTS (
                        SELECT 1
                          FROM public.sessions AS active
                         WHERE active.tenant_id=m.tenant_id
                           AND active.session_id=m.session_id
                           AND active.lease_owner IS NOT NULL
                           AND active.lease_expires_at > clock_timestamp()
                   )
                 ORDER BY m.updated_at, m.tenant_id, m.session_id
                 LIMIT p_limit
                 FOR UPDATE OF m SKIP LOCKED
            LOOP
                -- This is intentionally a second query: the mailbox row is
                -- already locked, and the session row is locked after it.
                -- Never reset a mailbox while any authoritative session lease
                -- (owner + epoch + unexpired timestamp) remains valid.
                session_owner := NULL;
                session_epoch := NULL;
                session_expires_at := NULL;
                SELECT s.lease_owner, s.lease_epoch, s.lease_expires_at
                  INTO session_owner, session_epoch, session_expires_at
                  FROM public.sessions AS s
                 WHERE s.tenant_id=rec.tenant_id
                   AND s.session_id=rec.session_id
                 FOR UPDATE;
                IF session_owner IS NOT NULL
                   AND session_expires_at IS NOT NULL
                   AND session_expires_at > clock_timestamp() THEN
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
                       AND lease_owner IS NOT DISTINCT FROM rec.lease_owner
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
                   AND lease_owner IS NOT DISTINCT FROM rec.lease_owner
                   AND lease_epoch=rec.lease_epoch
                   AND lease_expires_at IS NOT DISTINCT FROM rec.lease_expires_at;

                IF next_status='QUEUED'
                   AND (rec.old_status <> 'QUEUED'
                        OR rec.queue_generation < 1) THEN
                    INSERT INTO public.outbox_events (
                        tenant_id, aggregate_type, aggregate_id, event_type,
                        payload_json
                    ) VALUES (
                        rec.tenant_id, 'session', rec.session_id,
                        'session.ready.v2',
                        jsonb_build_object(
                            'generation', next_generation,
                            'priority', coalesce(rec.item_priority, 0),
                            'trace_id', coalesce(rec.trace_id,
                                rec.inbound_id::text, rec.session_id),
                            'created_at', to_char(clock_timestamp()
                                AT TIME ZONE 'UTC',
                                'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                        )
                    ) ON CONFLICT DO NOTHING;
                ELSIF next_status='QUEUED' THEN
                    -- Re-open the existing generation in the same transaction
                    -- when its prior publication/claim is no longer active.
                    -- This is bounded by the selected mailbox batch; no
                    -- grace window is used to delay recovery of an expired
                    -- authoritative lease.
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
                                'trace_id', coalesce(rec.trace_id,
                                    rec.inbound_id::text, rec.session_id),
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

        CREATE OR REPLACE FUNCTION reconcile_session_mailboxes(p_limit integer)
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
                GRANT EXECUTE ON FUNCTION sweep_expired_session_leases(integer)
                    TO trpc_runtime;
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
    # Restore the exact recovery definitions that were active after 0010.
    # This migration replaces functions in-place, so merely changing the
    # Alembic revision would otherwise leave the database running the 0011
    # implementation after a rollback.
    op.execute(
        """
        CREATE OR REPLACE FUNCTION sweep_expired_session_leases(p_limit integer)
        RETURNS integer
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
        DECLARE
            rec record;
            next_status text;
            next_generation bigint;
            processed integer := 0;
        BEGIN
            IF p_limit < 1 OR p_limit > 1000 THEN
                RAISE EXCEPTION 'recovery limit must be between 1 and 1000';
            END IF;
            FOR rec IN
                SELECT m.tenant_id, m.session_id, m.accepted_sequence,
                       m.resolved_sequence, m.processing_inbound_id,
                       m.lease_owner, m.lease_epoch,
                       i.retry_at AS item_retry_at, i.priority AS item_priority,
                       i.trace_id
                  FROM public.session_mailboxes AS m
                  LEFT JOIN public.session_mailbox_items AS i
                    ON i.tenant_id=m.tenant_id
                   AND i.session_id=m.session_id
                   AND i.sequence=m.processing_sequence
                 WHERE m.status='RUNNING'
                   AND m.lease_expires_at <= clock_timestamp()
                 ORDER BY m.lease_expires_at, m.tenant_id, m.session_id
                 LIMIT p_limit
                 FOR UPDATE OF m SKIP LOCKED
            LOOP
                IF rec.accepted_sequence <= rec.resolved_sequence THEN
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
                           + CASE WHEN next_status='QUEUED' THEN 1 ELSE 0 END,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=rec.tenant_id AND session_id=rec.session_id
                RETURNING queue_generation INTO next_generation;
                UPDATE public.sessions
                   SET lease_owner=NULL, lease_expires_at=NULL,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=rec.tenant_id AND session_id=rec.session_id
                   AND lease_owner=rec.lease_owner
                   AND lease_epoch=rec.lease_epoch;
                IF next_status='QUEUED' THEN
                    INSERT INTO public.outbox_events (
                        tenant_id, aggregate_type, aggregate_id, event_type,
                        payload_json
                    ) VALUES (
                        rec.tenant_id, 'session', rec.session_id,
                        'session.ready.v2',
                        jsonb_build_object(
                            'generation', next_generation,
                            'priority', coalesce(rec.item_priority, 0),
                            'trace_id', coalesce(rec.trace_id,
                                rec.processing_inbound_id::text, rec.session_id),
                            'created_at', to_char(clock_timestamp() AT TIME ZONE 'UTC',
                                'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                        )
                    ) ON CONFLICT DO NOTHING;
                END IF;
                processed := processed + 1;
            END LOOP;
            RETURN processed;
        END
        $function$;
        REVOKE ALL ON FUNCTION sweep_expired_session_leases(integer) FROM PUBLIC;

        CREATE OR REPLACE FUNCTION reconcile_session_mailboxes_v2(
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

        CREATE OR REPLACE FUNCTION reconcile_session_mailboxes(p_limit integer)
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
                GRANT EXECUTE ON FUNCTION sweep_expired_session_leases(integer)
                    TO trpc_runtime;
                GRANT EXECUTE ON FUNCTION reconcile_session_mailboxes(integer)
                    TO trpc_runtime;
                GRANT EXECUTE ON FUNCTION reconcile_session_mailboxes_v2(integer,integer)
                    TO trpc_runtime;
            END IF;
        END
        $grant$;
        """
    )
