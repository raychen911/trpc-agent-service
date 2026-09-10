CREATE TABLE IF NOT EXISTS governance_policy_versions (
  policy_id varchar(36) PRIMARY KEY,
  tenant_id varchar(64) NOT NULL,
  scope_type varchar(16) NOT NULL,
  scope_id varchar(128) NOT NULL,
  version bigint NOT NULL CHECK (version > 0),
  status varchar(16) NOT NULL CHECK (status IN ('draft','active','superseded','disabled')),
  policy_document jsonb NOT NULL,
  created_by_digest varchar(64) NOT NULL,
  created_at timestamptz NOT NULL,
  activated_at timestamptz,
  disabled_at timestamptz,
  UNIQUE (tenant_id, scope_type, scope_id, version)
);

CREATE TABLE IF NOT EXISTS governance_policy_active (
  tenant_id varchar(64) NOT NULL,
  scope_type varchar(16) NOT NULL,
  scope_id varchar(128) NOT NULL,
  policy_id varchar(36) NOT NULL REFERENCES governance_policy_versions(policy_id),
  version bigint NOT NULL CHECK (version > 0),
  activation_generation bigint NOT NULL CHECK (activation_generation >= 0),
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, scope_type, scope_id)
);

CREATE TABLE IF NOT EXISTS principal_grants (
  grant_id varchar(36) PRIMARY KEY,
  tenant_id varchar(64) NOT NULL,
  channel varchar(32) NOT NULL,
  binding_id varchar(96) NOT NULL,
  provider_subject_digest varchar(64) NOT NULL,
  agent_name varchar(128),
  status varchar(16) NOT NULL CHECK (status IN ('active','disabled')),
  permissions jsonb NOT NULL,
  valid_from timestamptz,
  expires_at timestamptz,
  created_at timestamptz NOT NULL,
  revoked_at timestamptz
);

CREATE INDEX IF NOT EXISTS ix_principal_grants_lookup
  ON principal_grants(tenant_id, channel, binding_id, provider_subject_digest);

CREATE TABLE IF NOT EXISTS budget_accounts (
  tenant_id varchar(64) NOT NULL,
  policy_id varchar(36) NOT NULL,
  period_key varchar(64) NOT NULL,
  dimension varchar(32) NOT NULL,
  hard_limit numeric NOT NULL CHECK (hard_limit >= 0),
  reserved_amount numeric NOT NULL DEFAULT 0 CHECK (reserved_amount >= 0),
  settled_amount numeric NOT NULL DEFAULT 0 CHECK (settled_amount >= 0),
  PRIMARY KEY (tenant_id, policy_id, period_key, dimension),
  CHECK (reserved_amount + settled_amount <= hard_limit)
);

CREATE TABLE IF NOT EXISTS budget_reservations (
  reservation_id varchar(36) PRIMARY KEY,
  tenant_id varchar(64) NOT NULL,
  execution_id varchar(128) NOT NULL,
  policy_id varchar(36) NOT NULL,
  state varchar(32) NOT NULL CHECK (state IN ('reserved','settled','released','review_required')),
  maximum_usage jsonb NOT NULL,
  actual_usage jsonb NOT NULL,
  execution_started boolean NOT NULL DEFAULT false,
  owner_generation bigint NOT NULL CHECK (owner_generation >= 0),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  UNIQUE (tenant_id, execution_id)
);

CREATE TABLE IF NOT EXISTS governance_recovery_markers (
  marker_id varchar(36) PRIMARY KEY,
  tenant_id varchar(64) NOT NULL,
  execution_id varchar(128) NOT NULL,
  stage varchar(32) NOT NULL,
  owner_node_id varchar(128),
  owner_generation bigint NOT NULL CHECK (owner_generation >= 0),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL
);

ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS policy_id varchar(36);
ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS policy_version bigint;
ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS principal_digest varchar(64);
ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS reservation_id varchar(36);
ALTER TABLE persistent_audit_records
  ADD COLUMN IF NOT EXISTS governance_decision varchar(40);
