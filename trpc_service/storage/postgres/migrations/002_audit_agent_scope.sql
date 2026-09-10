ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS agent_id varchar(64);

CREATE INDEX IF NOT EXISTS ix_audit_tenant_agent
  ON persistent_audit_records(tenant_id, agent_id, created_at);
