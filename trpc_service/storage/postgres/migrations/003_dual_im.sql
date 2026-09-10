ALTER TABLE channel_bindings
  ADD COLUMN IF NOT EXISTS provider_tenant_key varchar(128);

ALTER TABLE channel_bindings
  ADD COLUMN IF NOT EXISTS provider_app_or_bot_id varchar(128);

ALTER TABLE channel_bindings
  ADD COLUMN IF NOT EXISTS channel_identity_digest char(64);

CREATE UNIQUE INDEX IF NOT EXISTS uq_channel_bindings_provider_identity
  ON channel_bindings(channel, provider_tenant_key, provider_app_or_bot_id)
  WHERE provider_tenant_key IS NOT NULL
    AND provider_app_or_bot_id IS NOT NULL
    AND status = 'active';

CREATE TABLE IF NOT EXISTS delivery_records (
    delivery_id uuid PRIMARY KEY,
    tenant_id varchar(64) NOT NULL REFERENCES tenants(tenant_id),
    binding_id varchar(96) NOT NULL REFERENCES channel_bindings(binding_id),
    channel varchar(32) NOT NULL,
    idempotency_key_digest char(64) NOT NULL,
    execution_trace_id uuid NOT NULL,
    reply_context jsonb NOT NULL,
    result_digest char(64) NOT NULL,
    status varchar(32) NOT NULL CHECK (
      status IN ('pending', 'sending', 'retry_wait', 'delivered', 'delivery_failed', 'delivery_unknown')
    ),
    adapter_generation bigint NOT NULL CHECK (adapter_generation > 0),
    next_attempt_at timestamptz,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    CONSTRAINT uq_delivery_execution_scope UNIQUE (
      tenant_id, idempotency_key_digest, channel, binding_id
    )
);

CREATE INDEX IF NOT EXISTS ix_delivery_due
  ON delivery_records(tenant_id, status, next_attempt_at);

CREATE INDEX IF NOT EXISTS ix_delivery_execution_trace
  ON delivery_records(tenant_id, execution_trace_id);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    attempt_id uuid PRIMARY KEY,
    delivery_id uuid NOT NULL REFERENCES delivery_records(delivery_id),
    attempt_no integer NOT NULL CHECK (attempt_no BETWEEN 1 AND 4),
    trace_id uuid NOT NULL,
    adapter_node_id varchar(64) NOT NULL,
    adapter_generation bigint NOT NULL CHECK (adapter_generation > 0),
    started_at timestamptz NOT NULL,
    finished_at timestamptz,
    outcome varchar(32),
    safe_error_code varchar(64),
    retry_delay_seconds integer CHECK (retry_delay_seconds IN (1, 2, 4)),
    CONSTRAINT uq_delivery_attempt_number UNIQUE (delivery_id, attempt_no)
);

ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS adapter_node_id varchar(64);

ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS adapter_generation bigint;

ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS channel_identity_digest char(64);

ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS provider_message_digest char(71);

ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS delivery_id uuid;

ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS delivery_attempt_no integer;

ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS delivery_status varchar(32);

CREATE INDEX IF NOT EXISTS ix_audit_tenant_delivery
  ON persistent_audit_records(tenant_id, delivery_id, created_at);
