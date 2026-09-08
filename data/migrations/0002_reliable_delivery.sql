-- Upgrade the original receipt table without replacing existing data.
ALTER TABLE inbound_receipt ADD COLUMN IF NOT EXISTS turn_id VARCHAR(64);
ALTER TABLE inbound_receipt ADD COLUMN IF NOT EXISTS config_revision BIGINT;
ALTER TABLE inbound_receipt ADD COLUMN IF NOT EXISTS lease_owner VARCHAR(255);
ALTER TABLE inbound_receipt ADD COLUMN IF NOT EXISTS lease_until TIMESTAMP(6) NULL;
ALTER TABLE inbound_receipt ADD COLUMN IF NOT EXISTS fencing_token BIGINT NOT NULL DEFAULT 0;
ALTER TABLE inbound_receipt ADD COLUMN IF NOT EXISTS attempt_count INT NOT NULL DEFAULT 0;
ALTER TABLE inbound_receipt ADD COLUMN IF NOT EXISTS last_error VARCHAR(1024);
