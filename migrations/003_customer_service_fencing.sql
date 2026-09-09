-- Storage-native execution leases and durable customer-service synchronization.
BEGIN;
CREATE TABLE IF NOT EXISTS platform_execution_lease (
    lease_key TEXT PRIMARY KEY,
    token TEXT NOT NULL,
    epoch BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS customer_service_state (
    binding_id VARCHAR(128) PRIMARY KEY REFERENCES channel_binding(binding_id),
    state JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE outbound_message DROP CONSTRAINT IF EXISTS outbound_message_state_check;
ALTER TABLE outbound_message ADD CONSTRAINT outbound_message_state_check
    CHECK (state IN ('pending','sending','delivered','retry','dead','unknown'));
CREATE INDEX IF NOT EXISTS idx_idempotency_request ON idempotency_record(tenant_id,request_id);
-- Preserve old request identities when moving the authority from Redis to PG.
INSERT INTO idempotency_record(tenant_id,idempotency_key,request_id,state,payload_hash,expires_at)
SELECT DISTINCT ON (tenant_id,idempotency_key)
       tenant_id,idempotency_key,request_id,state,payload_hash,now()+interval '7 days'
FROM request_execution WHERE idempotency_key<>''
ORDER BY tenant_id,idempotency_key,created_at
ON CONFLICT (tenant_id,idempotency_key) DO NOTHING;
COMMIT;
