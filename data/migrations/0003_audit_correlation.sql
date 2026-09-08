-- Promote message/config correlation to indexed audit columns.
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS message_id VARCHAR(255);
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS turn_id VARCHAR(64);
ALTER TABLE audit_log ADD COLUMN IF NOT EXISTS config_revision BIGINT;
CREATE INDEX idx_audit_message ON audit_log (tenant_id, channel, message_id);

-- Preserve execution identity alongside event payloads in older installations.
ALTER TABLE message_event ADD COLUMN IF NOT EXISTS turn_id VARCHAR(64);
ALTER TABLE message_event ADD COLUMN IF NOT EXISTS config_revision BIGINT;
ALTER TABLE message_event ADD COLUMN IF NOT EXISTS fencing_token BIGINT;
