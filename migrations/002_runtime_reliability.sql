-- Runtime reliability tables and idempotency lifecycle introduced by 方案v3.
BEGIN;

CREATE TABLE IF NOT EXISTS request_execution (
    request_id       VARCHAR(64) NOT NULL,
    tenant_id        VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    state            VARCHAR(32) NOT NULL CHECK (state IN
        ('reserved','queued','running','succeeded','retryable_failed','failed')),
    payload_hash     CHAR(64) NOT NULL,
    config_version   BIGINT NOT NULL,
    attempts         INTEGER NOT NULL DEFAULT 0,
    result_json      JSONB,
    error_code       VARCHAR(128) NOT NULL DEFAULT '',
    retryable        BOOLEAN NOT NULL DEFAULT FALSE,
    request_json     JSONB,
    idempotency_key  VARCHAR(512) NOT NULL DEFAULT '',
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, request_id)
);

CREATE INDEX IF NOT EXISTS idx_request_execution_repair
    ON request_execution (state, updated_at)
    WHERE state IN ('reserved','queued','running','retryable_failed');

ALTER TABLE idempotency_record
    ADD COLUMN IF NOT EXISTS payload_hash CHAR(64),
    ADD COLUMN IF NOT EXISTS result_json JSONB,
    ADD COLUMN IF NOT EXISTS error_code VARCHAR(128) NOT NULL DEFAULT '';

ALTER TABLE idempotency_record DROP CONSTRAINT IF EXISTS idempotency_record_state_check;
ALTER TABLE idempotency_record ADD CONSTRAINT idempotency_record_state_check CHECK
    (state IN ('reserved','queued','running','succeeded','retryable_failed','failed',
               'processing','completed'));

ALTER TABLE tool_approval
    ADD COLUMN IF NOT EXISTS user_id VARCHAR(256) NOT NULL DEFAULT '';
ALTER TABLE tool_approval ALTER COLUMN request_id SET DEFAULT '';

CREATE TABLE IF NOT EXISTS tool_execution (
    execution_id     VARCHAR(128) PRIMARY KEY,
    tenant_id        VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    request_id       VARCHAR(64) NOT NULL,
    tool_name        VARCHAR(128) NOT NULL,
    arguments_sha256 CHAR(64) NOT NULL,
    state            VARCHAR(32) NOT NULL CHECK (state IN
        ('reserved','running','succeeded','failed','unknown')),
    result_json      JSONB,
    error_code       VARCHAR(128),
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (tenant_id, request_id, tool_name, arguments_sha256)
);

CREATE TABLE IF NOT EXISTS knowledge_document (
    document_id VARCHAR(128) NOT NULL,
    tenant_id   VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    app_id      VARCHAR(64) NOT NULL,
    title       VARCHAR(512) NOT NULL,
    body        TEXT NOT NULL,
    metadata    JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, document_id)
);
CREATE INDEX IF NOT EXISTS idx_knowledge_document_tenant_app
    ON knowledge_document (tenant_id, app_id);

CREATE INDEX IF NOT EXISTS idx_outbound_sending_lease
    ON outbound_message (locked_at)
    WHERE state = 'sending';

COMMIT;
