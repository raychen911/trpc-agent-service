"""Initial multi-tenant production runtime schema.

Revision ID: 0001_production_runtime
Revises: None
"""

from __future__ import annotations

from alembic import op

revision = "0001_production_runtime"
down_revision = None
branch_labels = None
depends_on = None


_TABLES = (
    "tenants",
    "agent_apps",
    "config_revisions",
    "storage_profiles",
    "tenant_policies",
    "admin_idempotency",
    "channel_bindings",
    "channel_identities",
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
    "migration_checkpoints",
)


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto")
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")
    op.execute(
        """
        CREATE TABLE tenants (
            tenant_id text PRIMARY KEY,
            display_name text NOT NULL,
            status text NOT NULL DEFAULT 'active' CHECK (status IN ('active','suspended')),
            control_version bigint NOT NULL DEFAULT 1 CHECK (control_version > 0),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now()
        );
        CREATE TABLE agent_apps (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            app_id text NOT NULL,
            display_name text NOT NULL,
            active_config_version bigint NOT NULL DEFAULT 1,
            candidate_config_version bigint,
            candidate_percent numeric(5,2) NOT NULL DEFAULT 0
                CHECK (candidate_percent BETWEEN 0 AND 100),
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, app_id)
        );
        CREATE TABLE config_revisions (
            tenant_id text NOT NULL,
            app_id text NOT NULL,
            version bigint NOT NULL CHECK (version > 0),
            config_json jsonb NOT NULL,
            checksum text NOT NULL,
            created_by text NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, app_id, version),
            FOREIGN KEY (tenant_id, app_id) REFERENCES agent_apps(tenant_id, app_id)
        );
        CREATE TABLE storage_profiles (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            profile_id text NOT NULL,
            profile_json jsonb NOT NULL,
            embedding_dimension integer NOT NULL DEFAULT 1536 CHECK (embedding_dimension > 0),
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, profile_id)
        );
        CREATE TABLE tenant_policies (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            policy_version bigint NOT NULL CHECK (policy_version > 0),
            policy_json jsonb NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, policy_version)
        );
        CREATE TABLE admin_idempotency (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            idempotency_key text NOT NULL,
            operation text NOT NULL,
            request_hash text NOT NULL,
            response_status integer NOT NULL,
            response_json jsonb NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now(),
            expires_at timestamptz NOT NULL,
            PRIMARY KEY (tenant_id, idempotency_key)
        );
        CREATE TABLE channel_bindings (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            binding_id text NOT NULL,
            app_id text NOT NULL,
            channel text NOT NULL CHECK (channel IN ('wecom_ai_bot','wechat_official')),
            account_id text NOT NULL,
            -- Official Account provider identifiers are distinct from the
            -- tenant agent app_id and from the callback original ID.
            wechat_app_id text,
            wechat_original_id text,
            secret_refs jsonb NOT NULL DEFAULT '{}'::jsonb,
            capabilities jsonb NOT NULL DEFAULT '[]'::jsonb,
            enabled boolean NOT NULL DEFAULT true,
            control_version bigint NOT NULL DEFAULT 1,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, binding_id),
            UNIQUE (binding_id),
            UNIQUE (tenant_id, channel, account_id),
            FOREIGN KEY (tenant_id, app_id) REFERENCES agent_apps(tenant_id, app_id)
        );
        CREATE TABLE channel_identities (
            tenant_id text NOT NULL,
            binding_id text NOT NULL,
            external_user_hash text NOT NULL,
            principal_id text NOT NULL,
            first_seen_at timestamptz NOT NULL DEFAULT now(),
            last_seen_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, binding_id, external_user_hash),
            UNIQUE (tenant_id, principal_id),
            FOREIGN KEY (tenant_id, binding_id)
                REFERENCES channel_bindings(tenant_id, binding_id)
        );
        """
    )
    op.execute(
        """
        CREATE TABLE inbound_messages (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            inbound_id uuid NOT NULL DEFAULT gen_random_uuid(),
            binding_id text NOT NULL,
            app_id text NOT NULL,
            config_version bigint NOT NULL,
            channel text NOT NULL,
            account_id text NOT NULL,
            external_message_id text NOT NULL,
            principal_id text NOT NULL,
            session_id text NOT NULL,
            request_id text NOT NULL,
            trace_id text NOT NULL,
            envelope_json jsonb NOT NULL,
            status text NOT NULL DEFAULT 'accepted'
                CHECK (status IN ('accepted','processing','committed','failed')),
            accepted_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, inbound_id),
            UNIQUE (tenant_id, channel, account_id, external_message_id),
            FOREIGN KEY (tenant_id, binding_id)
                REFERENCES channel_bindings(tenant_id, binding_id),
            FOREIGN KEY (tenant_id, app_id, config_version)
                REFERENCES config_revisions(tenant_id, app_id, version)
        );
        CREATE INDEX ix_inbound_tenant_session_order
            ON inbound_messages (tenant_id, session_id, accepted_at, inbound_id);
        CREATE TABLE outbound_messages (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            outbound_id uuid NOT NULL,
            binding_id text NOT NULL,
            session_id text NOT NULL,
            channel text NOT NULL,
            target_id text NOT NULL,
            in_reply_to text,
            payload_json jsonb NOT NULL,
            trace_headers jsonb NOT NULL DEFAULT '{}'::jsonb,
            status text NOT NULL DEFAULT 'pending'
                CHECK (status IN ('pending','sending','delivered','failed','ambiguous')),
            provider_message_id text,
            last_error_type text,
            manual_replay_approved boolean NOT NULL DEFAULT false,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, outbound_id),
            FOREIGN KEY (tenant_id, binding_id)
                REFERENCES channel_bindings(tenant_id, binding_id)
        );
        CREATE INDEX ix_outbound_tenant_status_created
            ON outbound_messages (tenant_id, status, created_at);
        CREATE TABLE delivery_attempts (
            tenant_id text NOT NULL,
            attempt_id uuid NOT NULL DEFAULT gen_random_uuid(),
            outbound_id uuid NOT NULL,
            attempt_number integer NOT NULL,
            status text NOT NULL,
            provider_code text,
            latency_ms integer,
            started_at timestamptz NOT NULL DEFAULT now(),
            completed_at timestamptz,
            PRIMARY KEY (tenant_id, attempt_id),
            UNIQUE (tenant_id, outbound_id, attempt_number),
            FOREIGN KEY (tenant_id, outbound_id)
                REFERENCES outbound_messages(tenant_id, outbound_id)
        );
        """
    )
    op.execute(
        """
        CREATE TABLE sessions (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            session_id text NOT NULL,
            app_id text NOT NULL,
            principal_id text NOT NULL,
            state_json jsonb NOT NULL DEFAULT '{}'::jsonb,
            version bigint NOT NULL DEFAULT 0,
            next_sequence bigint NOT NULL DEFAULT 1,
            lease_epoch bigint NOT NULL DEFAULT 0,
            lease_owner text,
            lease_expires_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, session_id),
            FOREIGN KEY (tenant_id, app_id) REFERENCES agent_apps(tenant_id, app_id)
        );
        CREATE TABLE session_turns (
            tenant_id text NOT NULL,
            turn_id uuid NOT NULL,
            session_id text NOT NULL,
            inbound_id uuid NOT NULL,
            config_version bigint NOT NULL,
            status text NOT NULL CHECK (
                status IN ('processing','committed','failed','needs_confirmation')
            ),
            fencing_token bigint NOT NULL,
            attempt integer NOT NULL DEFAULT 1,
            error_type text,
            started_at timestamptz NOT NULL DEFAULT now(),
            committed_at timestamptz,
            PRIMARY KEY (tenant_id, turn_id),
            UNIQUE (tenant_id, inbound_id),
            FOREIGN KEY (tenant_id, session_id) REFERENCES sessions(tenant_id, session_id),
            FOREIGN KEY (tenant_id, inbound_id)
                REFERENCES inbound_messages(tenant_id, inbound_id)
        );
        CREATE INDEX ix_turns_tenant_session_status
            ON session_turns (tenant_id, session_id, status, started_at);
        CREATE TABLE turn_intents (
            tenant_id text NOT NULL,
            turn_id uuid NOT NULL,
            intent_key text NOT NULL,
            intent_json jsonb NOT NULL,
            status text NOT NULL DEFAULT 'pending',
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, turn_id, intent_key),
            FOREIGN KEY (tenant_id, turn_id) REFERENCES session_turns(tenant_id, turn_id)
        );
        CREATE TABLE session_events (
            tenant_id text NOT NULL,
            session_id text NOT NULL,
            sequence bigint NOT NULL,
            event_id text NOT NULL,
            turn_id uuid NOT NULL,
            author text NOT NULL,
            event_timestamp double precision NOT NULL,
            event_json jsonb NOT NULL,
            state_delta jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, session_id, sequence),
            UNIQUE (tenant_id, session_id, event_id),
            FOREIGN KEY (tenant_id, session_id) REFERENCES sessions(tenant_id, session_id),
            FOREIGN KEY (tenant_id, turn_id) REFERENCES session_turns(tenant_id, turn_id)
        );
        CREATE TABLE session_summaries (
            tenant_id text NOT NULL,
            session_id text NOT NULL,
            up_to_sequence bigint NOT NULL,
            summary_json jsonb NOT NULL,
            version bigint NOT NULL DEFAULT 1,
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, session_id),
            FOREIGN KEY (tenant_id, session_id) REFERENCES sessions(tenant_id, session_id)
        );
        """
    )
    op.execute(
        """
        CREATE TABLE memories (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            memory_id uuid NOT NULL DEFAULT gen_random_uuid(),
            principal_id text NOT NULL,
            session_id text,
            source_sequence bigint,
            memory_json jsonb NOT NULL,
            projection_status text NOT NULL DEFAULT 'pending',
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, memory_id)
        );
        CREATE INDEX ix_memories_tenant_principal_created
            ON memories (tenant_id, principal_id, created_at DESC);
        CREATE TABLE artifacts (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            artifact_id text NOT NULL,
            session_id text,
            object_key text NOT NULL,
            checksum text NOT NULL,
            size_bytes bigint NOT NULL CHECK (size_bytes >= 0),
            status text NOT NULL CHECK (status IN ('staged','committed','deleted')),
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, artifact_id),
            UNIQUE (tenant_id, object_key)
        );
        CREATE TABLE knowledge_items (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            item_id text NOT NULL,
            profile_id text NOT NULL,
            source_uri text,
            content_checksum text NOT NULL,
            metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
            projection_status text NOT NULL DEFAULT 'pending',
            created_at timestamptz NOT NULL DEFAULT now(),
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, item_id),
            FOREIGN KEY (tenant_id, profile_id)
                REFERENCES storage_profiles(tenant_id, profile_id)
        );
        CREATE TABLE knowledge_embeddings (
            tenant_id text NOT NULL,
            item_id text NOT NULL,
            chunk_id text NOT NULL,
            embedding vector(1536) NOT NULL,
            metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, item_id, chunk_id),
            FOREIGN KEY (tenant_id, item_id)
                REFERENCES knowledge_items(tenant_id, item_id) ON DELETE CASCADE
        );
        CREATE INDEX ix_knowledge_embeddings_tenant_item
            ON knowledge_embeddings (tenant_id, item_id);
        """
    )
    op.execute(
        """
        CREATE TABLE outbox_events (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            outbox_id uuid NOT NULL DEFAULT gen_random_uuid(),
            aggregate_type text NOT NULL,
            aggregate_id text NOT NULL,
            event_type text NOT NULL,
            payload_json jsonb NOT NULL,
            trace_headers jsonb NOT NULL DEFAULT '{}'::jsonb,
            attempts integer NOT NULL DEFAULT 0,
            available_at timestamptz NOT NULL DEFAULT now(),
            claimed_by text,
            claim_expires_at timestamptz,
            published_at timestamptz,
            last_error_type text,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, outbox_id)
        );
        CREATE INDEX ix_outbox_claim
            ON outbox_events (event_type, available_at, created_at)
            WHERE published_at IS NULL;
        CREATE TABLE dead_letters (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            dead_letter_id uuid NOT NULL DEFAULT gen_random_uuid(),
            source_type text NOT NULL,
            source_id text NOT NULL,
            reason text NOT NULL,
            payload_json jsonb NOT NULL,
            status text NOT NULL DEFAULT 'open' CHECK (status IN ('open','replayed','discarded')),
            created_at timestamptz NOT NULL DEFAULT now(),
            resolved_at timestamptz,
            PRIMARY KEY (tenant_id, dead_letter_id)
        );
        CREATE TABLE tool_executions (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            execution_key text NOT NULL,
            turn_id uuid NOT NULL,
            tool_name text NOT NULL,
            classification text NOT NULL
                CHECK (classification IN ('idempotent','non_idempotent','unknown')),
            arguments_hash text NOT NULL,
            status text NOT NULL,
            result_ref text,
            error_type text,
            started_at timestamptz NOT NULL DEFAULT now(),
            completed_at timestamptz,
            PRIMARY KEY (tenant_id, execution_key),
            FOREIGN KEY (tenant_id, turn_id) REFERENCES session_turns(tenant_id, turn_id)
        );
        CREATE TABLE confirmation_challenges (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            challenge_id uuid NOT NULL DEFAULT gen_random_uuid(),
            principal_id text NOT NULL,
            session_id text NOT NULL,
            tool_name text NOT NULL,
            arguments_hash text NOT NULL,
            token_hash text NOT NULL,
            expires_at timestamptz NOT NULL,
            consumed_at timestamptz,
            created_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, challenge_id),
            UNIQUE (tenant_id, token_hash)
        );
        CREATE TABLE audit_logs (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            audit_id uuid NOT NULL DEFAULT gen_random_uuid(),
            occurred_at timestamptz NOT NULL DEFAULT now(),
            channel text,
            user_id text,
            session_id text,
            agent_name text,
            tool_name text,
            decision text NOT NULL,
            latency_ms integer,
            error_type text,
            cost_units bigint NOT NULL DEFAULT 0,
            trace_id text NOT NULL,
            config_version bigint,
            policy_version bigint,
            idempotency_key text,
            redaction_applied boolean NOT NULL DEFAULT true,
            metadata_json jsonb NOT NULL DEFAULT '{}'::jsonb,
            PRIMARY KEY (tenant_id, audit_id)
        );
        CREATE INDEX ix_audit_tenant_cursor
            ON audit_logs (tenant_id, occurred_at DESC, audit_id DESC);
        CREATE TABLE migration_checkpoints (
            tenant_id text NOT NULL REFERENCES tenants(tenant_id),
            migration_id text NOT NULL,
            phase text NOT NULL,
            batch_key text NOT NULL,
            source_count bigint NOT NULL DEFAULT 0,
            target_count bigint NOT NULL DEFAULT 0,
            checksum text,
            differences jsonb NOT NULL DEFAULT '[]'::jsonb,
            status text NOT NULL,
            updated_at timestamptz NOT NULL DEFAULT now(),
            PRIMARY KEY (tenant_id, migration_id, phase, batch_key)
        );
        """
    )

    for table in _TABLES:
        op.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
        op.execute(
            f"""
            CREATE POLICY tenant_isolation_{table} ON {table}
            USING (tenant_id = nullif(current_setting('app.tenant_id', true), ''))
            WITH CHECK (tenant_id = nullif(current_setting('app.tenant_id', true), ''))
            """
        )

    op.execute(
        """
        CREATE OR REPLACE FUNCTION resolve_channel_binding(p_binding_id text)
        RETURNS TABLE (
            tenant_id text,
            binding_id text,
            app_id text,
            channel text,
            account_id text,
            wechat_app_id text,
            wechat_original_id text,
            secret_refs jsonb,
            capabilities jsonb,
            enabled boolean,
            control_version bigint,
            tenant_active boolean,
            active_config_version bigint,
            candidate_config_version bigint,
            candidate_percent numeric
        )
        LANGUAGE sql
        SECURITY DEFINER
        STABLE
        SET search_path = pg_catalog, public
        AS $$
            SELECT b.tenant_id, b.binding_id, b.app_id, b.channel, b.account_id,
                   b.wechat_app_id, b.wechat_original_id, b.secret_refs,
                   b.capabilities, b.enabled, b.control_version,
                   (t.status = 'active'), a.active_config_version,
                   a.candidate_config_version, a.candidate_percent
              FROM public.channel_bindings b
              JOIN public.tenants t ON t.tenant_id = b.tenant_id
              JOIN public.agent_apps a
                ON a.tenant_id = b.tenant_id AND a.app_id = b.app_id
             WHERE b.binding_id = p_binding_id
        $$;
        REVOKE ALL ON FUNCTION resolve_channel_binding(text) FROM PUBLIC;

        CREATE OR REPLACE FUNCTION list_channel_bindings(p_channel text)
        RETURNS SETOF public.channel_bindings
        LANGUAGE sql
        SECURITY DEFINER
        STABLE
        SET search_path = pg_catalog, public
        AS $$
            SELECT b.* FROM public.channel_bindings b
            JOIN public.tenants t ON t.tenant_id=b.tenant_id
            WHERE b.channel=p_channel AND b.enabled AND t.status='active'
        $$;
        REVOKE ALL ON FUNCTION list_channel_bindings(text) FROM PUBLIC;

        CREATE OR REPLACE FUNCTION claim_outbox_events(
            p_event_type text,
            p_owner_id text,
            p_limit integer,
            p_lease_seconds integer
        )
        RETURNS SETOF public.outbox_events
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $$
        BEGIN
            RETURN QUERY
            WITH candidates AS (
                SELECT tenant_id, outbox_id
                  FROM public.outbox_events
                 WHERE event_type = p_event_type
                   AND published_at IS NULL
                   AND available_at <= now()
                   AND (claim_expires_at IS NULL OR claim_expires_at <= now())
                 ORDER BY available_at, created_at
                 FOR UPDATE SKIP LOCKED
                 LIMIT greatest(1, least(p_limit, 1000))
            )
            UPDATE public.outbox_events AS target
               SET claimed_by = p_owner_id,
                   claim_expires_at = now() + make_interval(secs => p_lease_seconds),
                   attempts = target.attempts + 1
              FROM candidates
             WHERE target.tenant_id = candidates.tenant_id
               AND target.outbox_id = candidates.outbox_id
            RETURNING target.*;
        END
        $$;
        REVOKE ALL ON FUNCTION claim_outbox_events(text,text,integer,integer) FROM PUBLIC;
        """
    )
    op.execute(
        """
        DO $block$
        BEGIN
            IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_runtime') THEN
                GRANT USAGE ON SCHEMA public TO trpc_runtime;
                GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public
                    TO trpc_runtime;
                GRANT EXECUTE ON FUNCTION resolve_channel_binding(text) TO trpc_runtime;
                GRANT EXECUTE ON FUNCTION list_channel_bindings(text) TO trpc_runtime;
                GRANT EXECUTE ON FUNCTION claim_outbox_events(text,text,integer,integer)
                    TO trpc_runtime;
            END IF;
        END
        $block$;
        """
    )


def downgrade() -> None:
    op.execute("DROP FUNCTION IF EXISTS claim_outbox_events(text,text,integer,integer)")
    op.execute("DROP FUNCTION IF EXISTS list_channel_bindings(text)")
    op.execute("DROP FUNCTION IF EXISTS resolve_channel_binding(text)")
    for table in reversed(_TABLES):
        op.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
