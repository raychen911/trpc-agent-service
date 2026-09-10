CREATE TABLE IF NOT EXISTS schema_migrations (
    version integer PRIMARY KEY CHECK (version > 0),
    name varchar(120) NOT NULL,
    checksum char(64) NOT NULL,
    applied_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS tenants (
    tenant_id varchar(64) PRIMARY KEY,
    display_name varchar(120) NOT NULL,
    status varchar(16) NOT NULL CHECK (status IN ('active', 'disabled')),
    config_version bigint NOT NULL CHECK (config_version > 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS agent_applications (
    tenant_id varchar(64) NOT NULL REFERENCES tenants(tenant_id),
    agent_id varchar(64) NOT NULL,
    agent_name varchar(120) NOT NULL,
    status varchar(16) NOT NULL CHECK (status IN ('active', 'disabled')),
    model_profile varchar(64) NOT NULL,
    instruction text NOT NULL,
    config_version bigint NOT NULL CHECK (config_version > 0),
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (tenant_id, agent_id)
);

CREATE TABLE IF NOT EXISTS channel_bindings (
    binding_id varchar(96) PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    agent_id varchar(64) NOT NULL,
    channel varchar(32) NOT NULL,
    status varchar(16) NOT NULL CHECK (status IN ('active', 'disabled')),
    secret_ref varchar(128) NOT NULL,
    signature_version varchar(16) NOT NULL,
    config_version bigint NOT NULL CHECK (config_version > 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    FOREIGN KEY (tenant_id, agent_id)
      REFERENCES agent_applications(tenant_id, agent_id)
);

CREATE TABLE IF NOT EXISTS persistent_audit_records (
    audit_id uuid PRIMARY KEY,
    audit_kind varchar(16) NOT NULL,
    decision varchar(40) NOT NULL,
    trace_id uuid NOT NULL,
    first_claim_trace_id uuid,
    owner_trace_id uuid,
    execution_trace_id uuid,
    tenant_id varchar(64),
    node_id varchar(64) NOT NULL,
    process_instance_id uuid NOT NULL,
    binding_id_digest char(71) NOT NULL,
    external_message_digest char(71),
    platform_session_id varchar(69),
    message_generation bigint,
    session_generation bigint,
    rejected_generation bigint,
    current_generation bigint,
    error_type varchar(64),
    result_digest char(64),
    latency_ms numeric NOT NULL CHECK (latency_ms >= 0),
    cost numeric NOT NULL CHECK (cost >= 0),
    recovery_status varchar(32),
    created_at timestamptz NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_audit_tenant_trace
  ON persistent_audit_records(tenant_id, trace_id, created_at);
CREATE INDEX IF NOT EXISTS ix_audit_tenant_session
  ON persistent_audit_records(tenant_id, platform_session_id, created_at);
CREATE INDEX IF NOT EXISTS ix_audit_tenant_message
  ON persistent_audit_records(tenant_id, external_message_digest, created_at);

CREATE TABLE IF NOT EXISTS recovery_markers (
    recovery_id uuid PRIMARY KEY,
    tenant_id varchar(64) NOT NULL,
    binding_id_digest char(71) NOT NULL,
    external_message_digest char(71) NOT NULL,
    platform_session_id varchar(69) NOT NULL,
    idempotency_key_digest char(64) NOT NULL,
    message_generation bigint NOT NULL CHECK (message_generation > 0),
    session_generation bigint NOT NULL CHECK (session_generation > 0),
    execution_trace_id uuid NOT NULL,
    state varchar(32) NOT NULL,
    result_status varchar(32) NOT NULL,
    result_payload jsonb NOT NULL,
    result_digest char(64) NOT NULL,
    replay_allowed boolean NOT NULL DEFAULT false CHECK (replay_allowed = false),
    failure_stage varchar(64),
    created_at timestamptz NOT NULL,
    reconciled_at timestamptz,
    UNIQUE (tenant_id, binding_id_digest, external_message_digest, execution_trace_id)
);
