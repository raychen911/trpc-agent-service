"""Add durable per-session mailbox ordering and fenced claims.

Revision ID: 0008_session_mailboxes
Revises: 0007_migration_run_guards
"""

from __future__ import annotations

from alembic import op

revision = "0008_session_mailboxes"
down_revision = "0007_migration_run_guards"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE session_mailboxes (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 256),
            status text NOT NULL DEFAULT 'IDLE' CHECK (
                status IN ('IDLE','QUEUED','RUNNING','RETRY_WAIT')
            ),
            accepted_sequence bigint NOT NULL DEFAULT 0 CHECK (accepted_sequence >= 0),
            resolved_sequence bigint NOT NULL DEFAULT 0 CHECK (
                resolved_sequence >= 0 AND resolved_sequence <= accepted_sequence
            ),
            processing_sequence bigint CHECK (
                processing_sequence IS NULL OR (
                    processing_sequence > resolved_sequence
                    AND processing_sequence <= accepted_sequence
                )
            ),
            processing_inbound_id uuid,
            queue_generation bigint NOT NULL DEFAULT 0 CHECK (queue_generation >= 0),
            lease_owner text CHECK (lease_owner IS NULL OR char_length(lease_owner) BETWEEN 1 AND 256),
            lease_epoch bigint NOT NULL DEFAULT 0 CHECK (lease_epoch >= 0),
            lease_expires_at timestamptz,
            retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
            attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
            priority integer NOT NULL DEFAULT 0 CHECK (priority >= 0),
            retry_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            PRIMARY KEY (tenant_id, session_id),
            CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
            CHECK (
                (status = 'RUNNING'
                 AND processing_sequence IS NOT NULL
                 AND processing_inbound_id IS NOT NULL
                 AND lease_owner IS NOT NULL
                 AND lease_expires_at IS NOT NULL)
                OR
                (status <> 'RUNNING'
                 AND processing_sequence IS NULL
                 AND processing_inbound_id IS NULL
                 AND lease_owner IS NULL
                 AND lease_expires_at IS NULL)
            )
        );
        CREATE TABLE session_mailbox_items (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            session_id text NOT NULL CHECK (char_length(session_id) BETWEEN 1 AND 256),
            sequence bigint NOT NULL CHECK (sequence >= 1),
            inbound_id uuid NOT NULL,
            trace_id text NOT NULL CHECK (char_length(trace_id) BETWEEN 1 AND 512),
            priority integer NOT NULL DEFAULT 0 CHECK (priority >= 0),
            retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
            attempt integer NOT NULL DEFAULT 0 CHECK (attempt >= 0),
            retry_at timestamptz,
            accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
            resolved_at timestamptz,
            PRIMARY KEY (tenant_id, session_id, sequence),
            UNIQUE (tenant_id, session_id, inbound_id),
            FOREIGN KEY (tenant_id, inbound_id)
                REFERENCES inbound_messages(tenant_id, inbound_id),
            FOREIGN KEY (tenant_id, session_id)
                REFERENCES session_mailboxes(tenant_id, session_id)
        );
        CREATE INDEX ix_session_mailboxes_tenant_state
            ON session_mailboxes (tenant_id, status, retry_at, priority DESC, session_id);
        CREATE INDEX ix_session_mailboxes_tenant_generation
            ON session_mailboxes (tenant_id, queue_generation, updated_at);
        CREATE INDEX ix_session_mailbox_items_tenant_session_order
            ON session_mailbox_items (tenant_id, session_id, sequence);
        CREATE INDEX ix_session_mailbox_items_tenant_ready
            ON session_mailbox_items (tenant_id, retry_at, priority DESC, accepted_at);

        ALTER TABLE outbox_events
            ADD CONSTRAINT outbox_session_ready_v2_payload_check CHECK (
                event_type <> 'session.ready.v2'
                OR (
                    aggregate_type = 'session'
                    AND char_length(aggregate_id) BETWEEN 1 AND 256
                    AND jsonb_typeof(payload_json) = 'object'
                    AND payload_json ? 'generation'
                    AND jsonb_typeof(payload_json->'generation') = 'number'
                    AND (payload_json->>'generation') ~ '^[1-9][0-9]*$'
                    AND payload_json ? 'priority'
                    AND jsonb_typeof(payload_json->'priority') = 'number'
                    AND (payload_json->>'priority') ~ '^(0|[1-9][0-9]*)$'
                    AND payload_json ? 'trace_id'
                    AND jsonb_typeof(payload_json->'trace_id') = 'string'
                    AND char_length(payload_json->>'trace_id') BETWEEN 1 AND 512
                    AND payload_json ? 'created_at'
                    AND jsonb_typeof(payload_json->'created_at') = 'string'
                    AND char_length(payload_json->>'created_at') > 0
                )
            );
        CREATE UNIQUE INDEX uq_outbox_session_ready_v2_generation
            ON outbox_events (
                event_type, tenant_id, aggregate_id,
                (CASE WHEN (payload_json->>'generation') ~ '^[1-9][0-9]*$'
                      THEN (payload_json->>'generation')::bigint END)
            )
            WHERE event_type = 'session.ready.v2';

        ALTER TABLE session_mailboxes ENABLE ROW LEVEL SECURITY;
        ALTER TABLE session_mailbox_items ENABLE ROW LEVEL SECURITY;
        CREATE POLICY tenant_isolation_session_mailboxes
            ON session_mailboxes
            USING (tenant_id = nullif(current_setting('app.tenant_id', true), ''))
            WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id', true), ''));
        CREATE POLICY tenant_isolation_session_mailbox_items
            ON session_mailbox_items
            USING (tenant_id = nullif(current_setting('app.tenant_id', true), ''))
            WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id', true), ''));
        REVOKE ALL ON TABLE session_mailboxes, session_mailbox_items FROM PUBLIC;
        DO $block$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_runtime') THEN
                GRANT SELECT, INSERT, UPDATE, DELETE
                    ON TABLE session_mailboxes, session_mailbox_items TO trpc_runtime;
            END IF;
        END
        $block$;

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
                        rec.tenant_id, 'session', rec.session_id, 'session.ready.v2',
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

        CREATE OR REPLACE FUNCTION schedule_session_mailbox_retries(p_limit integer)
        RETURNS integer
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
        DECLARE
            rec record;
            next_generation bigint;
            processed integer := 0;
        BEGIN
            IF p_limit < 1 OR p_limit > 1000 THEN
                RAISE EXCEPTION 'recovery limit must be between 1 and 1000';
            END IF;
            FOR rec IN
                SELECT m.tenant_id, m.session_id, m.queue_generation,
                       i.priority, i.trace_id, i.inbound_id
                  FROM public.session_mailboxes AS m
                  JOIN public.session_mailbox_items AS i
                    ON i.tenant_id=m.tenant_id
                   AND i.session_id=m.session_id
                   AND i.sequence=m.resolved_sequence+1
                 WHERE m.status='RETRY_WAIT'
                   AND m.retry_at IS NOT NULL
                   AND m.retry_at <= clock_timestamp()
                 ORDER BY m.retry_at, m.tenant_id, m.session_id
                 LIMIT p_limit
                 FOR UPDATE OF m SKIP LOCKED
            LOOP
                UPDATE public.session_mailboxes
                   SET status='QUEUED', retry_at=NULL,
                       queue_generation=queue_generation+1,
                       updated_at=clock_timestamp()
                 WHERE tenant_id=rec.tenant_id AND session_id=rec.session_id
                RETURNING queue_generation INTO next_generation;
                INSERT INTO public.outbox_events (
                    tenant_id, aggregate_type, aggregate_id, event_type, payload_json
                ) VALUES (
                    rec.tenant_id, 'session', rec.session_id, 'session.ready.v2',
                    jsonb_build_object(
                        'generation', next_generation,
                        'priority', rec.priority,
                        'trace_id', coalesce(rec.trace_id, rec.inbound_id::text),
                        'created_at', to_char(clock_timestamp() AT TIME ZONE 'UTC',
                            'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
                    )
                ) ON CONFLICT DO NOTHING;
                processed := processed + 1;
            END LOOP;
            RETURN processed;
        END
        $function$;
        REVOKE ALL ON FUNCTION schedule_session_mailbox_retries(integer) FROM PUBLIC;

        CREATE OR REPLACE FUNCTION reconcile_session_mailboxes(p_limit integer)
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
                    -- A long-lived queued mailbox already owns this
                    -- generation.  Re-open its durable event for replay only
                    -- when it was published and is not currently claimed;
                    -- an available/claimed event is already on its way and
                    -- must not be reset.  This keeps Redis loss recoverable
                    -- without generation churn or outbox row growth.
                    UPDATE public.outbox_events
                       SET published_at=NULL,
                           claimed_by=NULL,
                           claim_expires_at=NULL,
                           available_at=clock_timestamp(),
                           last_error_type=NULL
                     WHERE tenant_id=rec.tenant_id
                       AND aggregate_type='session'
                       AND aggregate_id=rec.session_id
                       AND event_type='session.ready.v2'
                       AND (payload_json->>'generation')::bigint=next_generation
                       AND published_at IS NOT NULL
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
        REVOKE ALL ON FUNCTION reconcile_session_mailboxes(integer) FROM PUBLIC;

        DO $grant$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_runtime') THEN
                GRANT EXECUTE ON FUNCTION sweep_expired_session_leases(integer)
                    TO trpc_runtime;
                GRANT EXECUTE ON FUNCTION schedule_session_mailbox_retries(integer)
                    TO trpc_runtime;
                GRANT EXECUTE ON FUNCTION reconcile_session_mailboxes(integer)
                    TO trpc_runtime;
            END IF;
        END
        $grant$;
        """
    )


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS reconcile_session_mailboxes(integer)")
    op.execute("DROP FUNCTION IF EXISTS schedule_session_mailbox_retries(integer)")
    op.execute("DROP FUNCTION IF EXISTS sweep_expired_session_leases(integer)")
    op.execute("DROP INDEX IF EXISTS uq_outbox_session_ready_v2_generation")
    op.execute(
        "ALTER TABLE outbox_events DROP CONSTRAINT IF EXISTS outbox_session_ready_v2_payload_check"
    )
    op.execute("DROP TABLE IF EXISTS session_mailbox_items CASCADE")
    op.execute("DROP TABLE IF EXISTS session_mailboxes CASCADE")
