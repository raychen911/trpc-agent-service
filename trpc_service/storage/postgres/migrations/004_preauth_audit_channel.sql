ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS channel varchar(32) NOT NULL DEFAULT 'local_http';

CREATE INDEX IF NOT EXISTS ix_audit_channel_identity
  ON persistent_audit_records(channel, channel_identity_digest, created_at);
