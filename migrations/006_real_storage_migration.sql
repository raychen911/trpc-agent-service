-- Online Redis -> PostgreSQL SDK Session/Memory migration control state.
BEGIN;

ALTER TABLE migration_job
    ADD COLUMN IF NOT EXISTS revision BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS source_config_version BIGINT,
    ADD COLUMN IF NOT EXISTS target_config_version BIGINT,
    ADD COLUMN IF NOT EXISTS route_version BIGINT NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS batch_size INTEGER NOT NULL DEFAULT 100,
    ADD COLUMN IF NOT EXISTS shadow_sample_rate DOUBLE PRECISION NOT NULL DEFAULT 0.1,
    ADD COLUMN IF NOT EXISTS rollback_window_seconds INTEGER NOT NULL DEFAULT 3600,
    ADD COLUMN IF NOT EXISTS rollback_deadline TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS lease_owner VARCHAR(128),
    ADD COLUMN IF NOT EXISTS lease_until TIMESTAMPTZ;

CREATE UNIQUE INDEX IF NOT EXISTS uq_migration_job_active_resource
    ON migration_job (tenant_id, resource_type)
    WHERE phase NOT IN ('completed', 'rolled_back');

CREATE TABLE IF NOT EXISTS storage_migration_route (
    tenant_id          VARCHAR(64) NOT NULL REFERENCES tenant(tenant_id),
    route_version      BIGINT NOT NULL,
    job_id             VARCHAR(128) REFERENCES migration_job(job_id),
    mode               VARCHAR(32) NOT NULL CHECK (mode IN
        ('source_only','dual_write','shadow_read','target_primary_mirror','target_only')),
    source_backend     VARCHAR(32) NOT NULL,
    target_backend     VARCHAR(32) NOT NULL,
    config_version     BIGINT NOT NULL,
    shadow_sample_rate DOUBLE PRECISION NOT NULL DEFAULT 0.1,
    admission_paused   BOOLEAN NOT NULL DEFAULT FALSE,
    active             BOOLEAN NOT NULL DEFAULT FALSE,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, route_version)
);

ALTER TABLE storage_migration_route
    ADD COLUMN IF NOT EXISTS shadow_sample_rate DOUBLE PRECISION NOT NULL DEFAULT 0.1;

CREATE UNIQUE INDEX IF NOT EXISTS uq_storage_migration_route_active
    ON storage_migration_route (tenant_id) WHERE active;

CREATE TABLE IF NOT EXISTS migration_item (
    job_id          VARCHAR(128) NOT NULL REFERENCES migration_job(job_id) ON DELETE CASCADE,
    resource_kind   VARCHAR(32) NOT NULL,
    resource_key    TEXT NOT NULL,
    source_hash     CHAR(64),
    target_hash     CHAR(64),
    state           VARCHAR(32) NOT NULL CHECK (state IN ('pending','written','verified','dirty','failed')),
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      VARCHAR(128),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, resource_kind, resource_key)
);

CREATE INDEX IF NOT EXISTS idx_migration_item_state
    ON migration_item (job_id, state, updated_at);

CREATE TABLE IF NOT EXISTS migration_dirty_key (
    job_id          VARCHAR(128) NOT NULL REFERENCES migration_job(job_id) ON DELETE CASCADE,
    resource_kind   VARCHAR(32) NOT NULL,
    resource_key    TEXT NOT NULL,
    last_error      VARCHAR(128) NOT NULL DEFAULT '',
    attempts        INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, resource_kind, resource_key)
);

ALTER TABLE request_execution
    ADD COLUMN IF NOT EXISTS storage_route_version BIGINT NOT NULL DEFAULT 0;

COMMIT;
