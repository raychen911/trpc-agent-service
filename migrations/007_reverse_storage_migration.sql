-- PostgreSQL -> Redis migration target quarantine. Existing migrations remain immutable.
BEGIN;

CREATE TABLE IF NOT EXISTS migration_target_backup (
    job_id          VARCHAR(128) NOT NULL REFERENCES migration_job(job_id) ON DELETE CASCADE,
    resource_kind   VARCHAR(32) NOT NULL,
    resource_key    TEXT NOT NULL,
    redis_key       TEXT NOT NULL,
    redis_type      VARCHAR(16) NOT NULL CHECK (redis_type IN ('string','hash','list')),
    payload         JSONB NOT NULL,
    ttl_milliseconds BIGINT NOT NULL DEFAULT -1,
    restored        BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, redis_key)
);

CREATE INDEX IF NOT EXISTS idx_migration_target_backup_job
    ON migration_target_backup (job_id, restored, created_at);

COMMIT;
