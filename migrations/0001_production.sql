CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id text PRIMARY KEY,
    name text NOT NULL,
    status text NOT NULL DEFAULT 'active',
    active_version bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS tenant_revisions (
    tenant_id text NOT NULL,
    version bigint NOT NULL,
    config_json jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,version)
);
CREATE TABLE IF NOT EXISTS inbound_messages (
    tenant_id text NOT NULL,
    inbox_key text NOT NULL,
    channel text NOT NULL,
    account_id text NOT NULL,
    external_message_id text NOT NULL,
    session_id text NOT NULL,
    trace_id text,
    payload_json jsonb NOT NULL,
    status text NOT NULL,
    result_json jsonb,
    received_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,inbox_key),
    UNIQUE (tenant_id,channel,account_id,external_message_id)
);
DROP INDEX IF EXISTS ix_inbound_reconcile;
CREATE INDEX ix_inbound_reconcile ON inbound_messages(status,updated_at)
    WHERE status IN ('accepted','queued');
CREATE TABLE IF NOT EXISTS outbox_events (
    tenant_id text NOT NULL,
    outbox_id text NOT NULL,
    event_type text NOT NULL,
    aggregate_id text NOT NULL,
    payload_json jsonb NOT NULL,
    trace_id text,
    attempts integer NOT NULL DEFAULT 0,
    available_at timestamptz NOT NULL DEFAULT now(),
    claimed_by text,
    claim_expires_at timestamptz,
    published_at timestamptz,
    dead_lettered_at timestamptz,
    last_error text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,outbox_id)
);
CREATE INDEX IF NOT EXISTS ix_outbox_ready ON outbox_events(event_type,available_at)
    WHERE published_at IS NULL AND dead_lettered_at IS NULL;
CREATE TABLE IF NOT EXISTS idempotency_keys (
    tenant_id text NOT NULL,
    idempotency_key text NOT NULL,
    status text NOT NULL,
    result_json jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,idempotency_key)
);
CREATE TABLE IF NOT EXISTS sessions (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    app_id text NOT NULL,
    user_id text NOT NULL,
    state_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    version bigint NOT NULL DEFAULT 0,
    lease_owner text,
    lease_expires_at timestamptz,
    fencing_epoch bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,session_id)
);
CREATE TABLE IF NOT EXISTS session_events (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    sequence bigint NOT NULL,
    event_id text NOT NULL,
    event_type text NOT NULL,
    payload_json jsonb NOT NULL,
    trace_id text,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,session_id,sequence),
    UNIQUE (tenant_id,event_id)
);
CREATE TABLE IF NOT EXISTS memories (
    tenant_id text NOT NULL,
    user_id text NOT NULL,
    memory_id text NOT NULL,
    content text NOT NULL,
    source_version bigint NOT NULL DEFAULT 0,
    projection_status text NOT NULL DEFAULT 'pending',
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,memory_id)
);
CREATE INDEX IF NOT EXISTS ix_memories_user ON memories(tenant_id,user_id,created_at);
CREATE TABLE IF NOT EXISTS summaries (
    tenant_id text NOT NULL,
    session_id text NOT NULL,
    source_version bigint NOT NULL,
    content text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,session_id)
);
CREATE TABLE IF NOT EXISTS knowledge_documents (
    tenant_id text NOT NULL,
    collection text NOT NULL,
    item_id text NOT NULL,
    content text NOT NULL,
    source_version bigint NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,collection,item_id)
);
CREATE TABLE IF NOT EXISTS knowledge_vectors (
    tenant_id text NOT NULL,
    item_id text NOT NULL,
    collection text NOT NULL DEFAULT 'default',
    content text NOT NULL,
    embedding vector NOT NULL,
    source_version bigint NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,item_id)
);
ALTER TABLE knowledge_vectors ADD COLUMN IF NOT EXISTS collection text NOT NULL DEFAULT 'default';
ALTER TABLE knowledge_vectors ALTER COLUMN embedding TYPE vector USING embedding::vector;
CREATE INDEX IF NOT EXISTS ix_knowledge_collection ON knowledge_vectors(tenant_id,collection);
CREATE TABLE IF NOT EXISTS artifacts (
    tenant_id text NOT NULL,
    artifact_id text NOT NULL,
    object_key text NOT NULL,
    checksum text NOT NULL,
    size_bytes bigint NOT NULL,
    content_type text NOT NULL,
    status text NOT NULL DEFAULT 'ready',
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,artifact_id)
);
CREATE TABLE IF NOT EXISTS outbound_messages (
    tenant_id text NOT NULL,
    outbound_id text NOT NULL,
    channel text NOT NULL,
    account_id text NOT NULL,
    in_reply_to text,
    part integer NOT NULL,
    payload_json jsonb NOT NULL,
    status text NOT NULL,
    provider_message_id text,
    error_type text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,outbound_id),
    UNIQUE (tenant_id,channel,account_id,in_reply_to,part)
);
CREATE TABLE IF NOT EXISTS audit_logs (
    tenant_id text NOT NULL,
    audit_id text NOT NULL,
    channel text,
    user_id text,
    session_id text,
    agent_name text,
    tool_name text,
    decision text NOT NULL,
    latency_ms double precision,
    error_type text,
    cost numeric NOT NULL DEFAULT 0,
    trace_id text,
    detail_json jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,audit_id)
);
CREATE INDEX IF NOT EXISTS ix_audit_query ON audit_logs(tenant_id,occurred_at DESC);
CREATE TABLE IF NOT EXISTS migration_checkpoints (
    tenant_id text NOT NULL,
    migration_id text NOT NULL,
    phase text NOT NULL,
    source_count bigint NOT NULL DEFAULT 0,
    target_count bigint NOT NULL DEFAULT 0,
    checksum text,
    differences jsonb NOT NULL DEFAULT '[]'::jsonb,
    status text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,migration_id)
);
CREATE TABLE IF NOT EXISTS tenant_budget_usage (
    tenant_id text NOT NULL,
    usage_day date NOT NULL,
    used_tokens bigint NOT NULL DEFAULT 0,
    reserved_tokens bigint NOT NULL DEFAULT 0,
    PRIMARY KEY (tenant_id,usage_day)
);
CREATE TABLE IF NOT EXISTS budget_reservations (
    tenant_id text NOT NULL,
    reservation_id text NOT NULL,
    usage_day date NOT NULL,
    reserved_tokens bigint NOT NULL,
    expires_at timestamptz NOT NULL,
    settled_at timestamptz,
    PRIMARY KEY (tenant_id,reservation_id)
);
CREATE TABLE IF NOT EXISTS tool_executions (
    tenant_id text NOT NULL,
    execution_key text NOT NULL,
    session_id text NOT NULL,
    turn_id text NOT NULL,
    tool_name text NOT NULL,
    arguments_hash text NOT NULL,
    status text NOT NULL,
    result_json jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,execution_key)
);
CREATE TABLE IF NOT EXISTS tool_confirmations (
    tenant_id text NOT NULL,
    token_id text NOT NULL,
    user_id text NOT NULL,
    session_id text NOT NULL,
    tool_name text NOT NULL,
    arguments_hash text NOT NULL,
    expires_at timestamptz NOT NULL,
    consumed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id,token_id)
);

-- The deployment should use a non-owner runtime role. These policies are a
-- second line of defence behind the mandatory tenant predicates in code.
DO $$
DECLARE table_name text;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'tenant_revisions','inbound_messages','outbox_events','idempotency_keys','sessions',
    'session_events','memories','summaries','knowledge_vectors','artifacts','outbound_messages',
    'knowledge_documents',
    'audit_logs','migration_checkpoints','tenant_budget_usage','budget_reservations','tool_executions',
    'tool_confirmations'
  ] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', table_name);
    IF NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename=table_name
                   AND policyname='tenant_isolation') THEN
      EXECUTE format(
        'CREATE POLICY tenant_isolation ON %I USING (tenant_id = nullif(current_setting(''app.tenant_id'', true), '''')) WITH CHECK (tenant_id = nullif(current_setting(''app.tenant_id'', true), ''''))',
        table_name
      );
    END IF;
    -- The control role is intentionally cross-tenant, but only for these
    -- service tables. This avoids granting cluster-wide BYPASSRLS.
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_control')
       AND NOT EXISTS (SELECT 1 FROM pg_policies WHERE schemaname='public' AND tablename=table_name
                       AND policyname='tenant_control') THEN
      EXECUTE format(
        'CREATE POLICY tenant_control ON %I FOR ALL TO trpc_control USING (true) WITH CHECK (true)',
        table_name
      );
    END IF;
  END LOOP;
END $$;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_runtime') THEN
    GRANT USAGE ON SCHEMA public TO trpc_runtime;
    GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO trpc_runtime;
  END IF;
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='trpc_control') THEN
    GRANT USAGE ON SCHEMA public TO trpc_control;
    GRANT SELECT,INSERT,UPDATE,DELETE ON ALL TABLES IN SCHEMA public TO trpc_control;
  END IF;
END $$;
