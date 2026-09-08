-- Promote message/config correlation to indexed audit columns.
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS message_id VARCHAR(255);
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS turn_id VARCHAR(64);
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS config_revision BIGINT;
-- MySQL DDL auto-commits. Make index creation safe when a process crashes after
-- CREATE INDEX but before schema_migration is recorded.
SET @idx_audit_message_exists = (
  SELECT COUNT(*) FROM information_schema.statistics
  WHERE table_schema = DATABASE()
    AND table_name = 'audit_log'
    AND index_name = 'idx_audit_message'
);
SET @idx_audit_message_sql = IF(
  @idx_audit_message_exists = 0,
  'CREATE INDEX idx_audit_message ON audit_log (tenant_id, channel, message_id)',
  'SELECT 1'
);
PREPARE idx_audit_message_stmt FROM @idx_audit_message_sql;
EXECUTE idx_audit_message_stmt;
DEALLOCATE PREPARE idx_audit_message_stmt;

-- Preserve execution identity alongside event payloads in older installations.
ALTER TABLE message_event ADD COLUMN IF NOT EXISTS turn_id VARCHAR(64);
ALTER TABLE message_event ADD COLUMN IF NOT EXISTS config_revision BIGINT;
ALTER TABLE message_event ADD COLUMN IF NOT EXISTS fencing_token BIGINT;
