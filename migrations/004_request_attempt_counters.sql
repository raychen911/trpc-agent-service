-- Separate Worker execution, observed model rounds and recovery scheduling.
BEGIN;

ALTER TABLE request_execution
    ADD COLUMN IF NOT EXISTS model_attempts INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS recovery_count INTEGER NOT NULL DEFAULT 0;

COMMIT;
