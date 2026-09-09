-- Platform-owned control-plane schema. SDK Session/Memory services manage their
-- own backend tables/keys; those are intentionally not duplicated here.

BEGIN;

CREATE TABLE IF NOT EXISTS tenant (
    tenant_id              VARCHAR(64) PRIMARY KEY,
    name                   VARCHAR(128) NOT NULL,
    status                 VARCHAR(16) NOT NULL CHECK (status IN ('active', 'suspended', 'deleted')),
    active_config_version  BIGINT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tenant_config_version (
    tenant_id       VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    version         BIGINT NOT NULL,
    config_json     JSONB NOT NULL,
    config_sha256   CHAR(64) NOT NULL,
    published_by    VARCHAR(128) NOT NULL,
    published_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, version),
    UNIQUE (tenant_id, config_sha256)
);

ALTER TABLE tenant DROP CONSTRAINT IF EXISTS fk_tenant_active_config;
ALTER TABLE tenant ADD CONSTRAINT fk_tenant_active_config
    FOREIGN KEY (tenant_id, active_config_version)
    REFERENCES tenant_config_version(tenant_id, version)
    DEFERRABLE INITIALLY DEFERRED;

CREATE TABLE IF NOT EXISTS agent_app (
    tenant_id       VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id          VARCHAR(64) NOT NULL,
    config_version  BIGINT NOT NULL,
    name            VARCHAR(128) NOT NULL,
    agent_name      VARCHAR(128) NOT NULL,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    model_config    JSONB NOT NULL,
    tool_policy     JSONB NOT NULL,
    runtime_policy  JSONB NOT NULL,
    PRIMARY KEY (tenant_id, app_id, config_version),
    FOREIGN KEY (tenant_id, config_version)
        REFERENCES tenant_config_version(tenant_id, version)
);

CREATE TABLE IF NOT EXISTS channel_binding (
    binding_id          VARCHAR(128) PRIMARY KEY,
    tenant_id           VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id              VARCHAR(64) NOT NULL,
    channel             VARCHAR(32) NOT NULL,
    external_account_id VARCHAR(256) NOT NULL DEFAULT '',
    secret_ref          VARCHAR(512) NOT NULL DEFAULT '',
    webhook_secret_ref  VARCHAR(512) NOT NULL DEFAULT '',
    options             JSONB NOT NULL DEFAULT '{}'::jsonb,
    enabled             BOOLEAN NOT NULL DEFAULT TRUE,
    config_version      BIGINT NOT NULL,
    UNIQUE (tenant_id, channel, external_account_id),
    FOREIGN KEY (tenant_id, app_id, config_version)
        REFERENCES agent_app(tenant_id, app_id, config_version)
);

CREATE TABLE IF NOT EXISTS identity_mapping (
    identity_id             BIGSERIAL PRIMARY KEY,
    tenant_id               VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    binding_id              VARCHAR(128) NOT NULL REFERENCES channel_binding(binding_id),
    external_user_hash      CHAR(64) NOT NULL,
    internal_user_id        VARCHAR(256) NOT NULL,
    encrypted_external_user BYTEA,
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, binding_id, external_user_hash),
    UNIQUE (tenant_id, internal_user_id)
);

CREATE TABLE IF NOT EXISTS session_summary (
    tenant_id          VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id             VARCHAR(64) NOT NULL,
    user_id            VARCHAR(256) NOT NULL,
    session_id         VARCHAR(256) NOT NULL,
    summary_event_id   VARCHAR(128) NOT NULL,
    covers_through_seq BIGINT NOT NULL,
    summary_sha256     CHAR(64) NOT NULL,
    inherited_from     VARCHAR(256),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, app_id, user_id, session_id)
);

CREATE TABLE IF NOT EXISTS artifact (
    artifact_id       VARCHAR(128) PRIMARY KEY,
    tenant_id         VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id            VARCHAR(64) NOT NULL,
    session_id        VARCHAR(256),
    object_uri        TEXT NOT NULL,
    mime_type         VARCHAR(256) NOT NULL,
    size_bytes        BIGINT NOT NULL CHECK (size_bytes >= 0),
    checksum_sha256   CHAR(64) NOT NULL,
    metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS knowledge_source (
    source_id         VARCHAR(128) PRIMARY KEY,
    tenant_id         VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id            VARCHAR(64) NOT NULL,
    source_type       VARCHAR(32) NOT NULL,
    object_uri        TEXT,
    vector_namespace VARCHAR(256) NOT NULL,
    embedding_model   VARCHAR(128) NOT NULL,
    status            VARCHAR(32) NOT NULL,
    metadata          JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS idempotency_record (
    tenant_id       VARCHAR(64) NOT NULL,
    idempotency_key VARCHAR(512) NOT NULL,
    request_id      VARCHAR(64) NOT NULL,
    state           VARCHAR(16) NOT NULL CHECK (state IN ('processing', 'completed', 'failed')),
    result_ref      TEXT,
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, idempotency_key)
);

CREATE TABLE IF NOT EXISTS outbound_message (
    outbound_id        CHAR(64) PRIMARY KEY,
    tenant_id          VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    binding_id         VARCHAR(128) NOT NULL REFERENCES channel_binding(binding_id),
    request_id         VARCHAR(64) NOT NULL,
    payload            JSONB NOT NULL,
    state              VARCHAR(16) NOT NULL CHECK (state IN ('pending', 'sending', 'delivered', 'retry', 'dead')),
    attempts           INTEGER NOT NULL DEFAULT 0,
    next_attempt_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by          VARCHAR(128),
    locked_at          TIMESTAMPTZ,
    external_message_id VARCHAR(256),
    last_error_code    VARCHAR(128),
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at       TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_outbound_due
    ON outbound_message (next_attempt_at, created_at)
    WHERE state IN ('pending', 'retry');

CREATE TABLE IF NOT EXISTS usage_ledger (
    usage_id       BIGSERIAL PRIMARY KEY,
    tenant_id      VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id         VARCHAR(64) NOT NULL,
    request_id     VARCHAR(64) NOT NULL,
    model_name     VARCHAR(128) NOT NULL,
    input_tokens   BIGINT NOT NULL DEFAULT 0,
    output_tokens  BIGINT NOT NULL DEFAULT 0,
    cost_usd       NUMERIC(18, 8) NOT NULL DEFAULT 0,
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, request_id, model_name)
);

CREATE INDEX IF NOT EXISTS idx_usage_tenant_time ON usage_ledger (tenant_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS tool_approval (
    approval_id       VARCHAR(128) PRIMARY KEY,
    tenant_id         VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    request_id        VARCHAR(64) NOT NULL,
    session_id        VARCHAR(256) NOT NULL,
    tool_name         VARCHAR(128) NOT NULL,
    arguments_sha256  CHAR(64) NOT NULL,
    status            VARCHAR(16) NOT NULL CHECK (status IN ('pending', 'approved', 'denied', 'expired', 'used')),
    confirmation_hash CHAR(64),
    expires_at        TIMESTAMPTZ NOT NULL,
    decided_by        VARCHAR(256),
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    decided_at        TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS audit_log (
    audit_id      BIGSERIAL PRIMARY KEY,
    occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    tenant_id    VARCHAR(64) NOT NULL,
    channel      VARCHAR(32) NOT NULL,
    user_id      VARCHAR(256) NOT NULL,
    session_id   VARCHAR(256) NOT NULL,
    agent_name   VARCHAR(128) NOT NULL,
    tool_name    VARCHAR(128),
    action       VARCHAR(64) NOT NULL,
    decision     VARCHAR(32) NOT NULL,
    latency_ms   DOUBLE PRECISION NOT NULL DEFAULT 0,
    error_type   VARCHAR(128),
    cost_usd     NUMERIC(18, 8) NOT NULL DEFAULT 0,
    trace_id     VARCHAR(64),
    request_id   VARCHAR(64) NOT NULL,
    details      JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_audit_tenant_time ON audit_log (tenant_id, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_trace ON audit_log (trace_id) WHERE trace_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS migration_job (
    job_id            VARCHAR(128) PRIMARY KEY,
    tenant_id         VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    resource_type     VARCHAR(32) NOT NULL,
    source_backend    JSONB NOT NULL,
    target_backend    JSONB NOT NULL,
    phase             VARCHAR(32) NOT NULL,
    checkpoint        JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_count      BIGINT NOT NULL DEFAULT 0,
    target_count      BIGINT NOT NULL DEFAULT 0,
    mismatch_count    BIGINT NOT NULL DEFAULT 0,
    started_at        TIMESTAMPTZ,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at      TIMESTAMPTZ,
    last_error        TEXT
);

CREATE INDEX IF NOT EXISTS idx_idempotency_expiry ON idempotency_record (expires_at);
CREATE INDEX IF NOT EXISTS idx_artifact_tenant_session ON artifact (tenant_id, session_id);
CREATE INDEX IF NOT EXISTS idx_knowledge_tenant_app ON knowledge_source (tenant_id, app_id, status);

COMMIT;
