-- Distinguish started model calls from complete, error-free model calls.
BEGIN;

ALTER TABLE request_execution
    ADD COLUMN IF NOT EXISTS successful_model_calls INTEGER NOT NULL DEFAULT 0;

COMMIT;
