-- Phase-eight forward-only schema.  Tenant-scoped relations keep tenant_id as
-- the first composite key column; immutable relations have no update business
-- path and mutable projections carry revision/fence CAS keys (DEC-004).

CREATE TABLE IF NOT EXISTS configuration_snapshots (
  tenant_id varchar(64) NOT NULL,
  snapshot_id varchar(36) NOT NULL,
  sequence bigint NOT NULL CHECK (sequence > 0),
  contract_version varchar(32) NOT NULL,
  min_runtime_contract varchar(32) NOT NULL,
  agent_config_ref varchar(128) NOT NULL,
  governance_policy_ref varchar(128) NOT NULL,
  data_backend_profile_ref varchar(128) NOT NULL,
  payload_digest varchar(64) NOT NULL,
  change_summary varchar(512) NOT NULL,
  created_by_digest varchar(64) NOT NULL,
  payload jsonb NOT NULL DEFAULT '{}',
  created_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, snapshot_id),
  UNIQUE (tenant_id, sequence)
);

-- Platform-scoped release registry: one release spans its cohort tenants.
CREATE TABLE IF NOT EXISTS configuration_releases (
  release_id varchar(36) NOT NULL,
  candidate_snapshot_id varchar(36) NOT NULL,
  rollback_snapshot_id varchar(36) NOT NULL,
  cohorts jsonb NOT NULL DEFAULT '[]',
  observation_window_seconds bigint NOT NULL CHECK (observation_window_seconds > 0),
  minimum_sample bigint NOT NULL CHECK (minimum_sample > 0),
  quality_gates jsonb NOT NULL DEFAULT '[]',
  hard_gate_types jsonb NOT NULL DEFAULT '[]',
  state varchar(40) NOT NULL CHECK (state IN (
    'draft', 'validated', 'canary', 'completed',
    'paused_quality', 'paused_insufficient_sample',
    'rolling_back', 'rolled_back', 'failed', 'failed_requires_repair'
  )),
  revision bigint NOT NULL CHECK (revision > 0),
  owner_fence_generation bigint NOT NULL DEFAULT 0 CHECK (owner_fence_generation >= 0),
  created_by_digest varchar(64) NOT NULL,
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (release_id)
);

CREATE TABLE IF NOT EXISTS release_targets (
  tenant_id varchar(64) NOT NULL,
  release_id varchar(36) NOT NULL,
  target_role varchar(16) NOT NULL CHECK (target_role IN ('stable', 'candidate')),
  added_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, release_id)
);

CREATE TABLE IF NOT EXISTS tenant_config_routes (
  tenant_id varchar(64) NOT NULL,
  stable_snapshot_id varchar(36) NOT NULL,
  candidate_snapshot_id varchar(36),
  release_id varchar(36),
  route_generation bigint NOT NULL CHECK (route_generation > 0),
  owner_fence_generation bigint NOT NULL DEFAULT 0 CHECK (owner_fence_generation >= 0),
  hard_gate_latched boolean NOT NULL DEFAULT false,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id)
);

CREATE TABLE IF NOT EXISTS execution_config_pins (
  tenant_id varchar(64) NOT NULL,
  idempotency_key_digest varchar(64) NOT NULL,
  content_fingerprint varchar(64) NOT NULL,
  snapshot_id varchar(36) NOT NULL,
  route_generation bigint NOT NULL CHECK (route_generation > 0),
  release_id varchar(36),
  created_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, idempotency_key_digest)
);

CREATE TABLE IF NOT EXISTS release_gate_signals (
  tenant_id varchar(64) NOT NULL,
  signal_id varchar(36) NOT NULL,
  release_id varchar(36) NOT NULL,
  signal_digest varchar(64) NOT NULL,
  gate_type varchar(64) NOT NULL,
  severity varchar(16) NOT NULL CHECK (severity IN ('hard', 'quality')),
  observation_window_seconds bigint NOT NULL CHECK (observation_window_seconds > 0),
  sample_count bigint NOT NULL DEFAULT 0 CHECK (sample_count >= 0),
  observed_value double precision,
  evidence_digest varchar(64),
  observed_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, signal_id),
  UNIQUE (tenant_id, signal_digest)
);

-- Append-only transition journal; formal audit rows share its transaction.
CREATE TABLE IF NOT EXISTS release_transition_events (
  event_id varchar(36) NOT NULL,
  release_id varchar(36) NOT NULL,
  command_id varchar(128) NOT NULL,
  from_state varchar(40) NOT NULL,
  to_state varchar(40) NOT NULL,
  from_revision bigint NOT NULL CHECK (from_revision > 0),
  to_revision bigint NOT NULL CHECK (to_revision > 0),
  actor_digest varchar(64) NOT NULL,
  reason_code varchar(64) NOT NULL,
  evidence_digest varchar(64),
  occurred_at timestamptz NOT NULL,
  PRIMARY KEY (event_id),
  UNIQUE (release_id, command_id, to_revision)
);

-- Deduplicated alert incidents; tenant scoping flows through scope_digest.
CREATE TABLE IF NOT EXISTS alert_incidents (
  incident_id varchar(36) NOT NULL,
  fingerprint varchar(71) NOT NULL,
  rule_id varchar(64) NOT NULL,
  severity varchar(16) NOT NULL,
  scope_digest varchar(64) NOT NULL,
  state varchar(16) NOT NULL CHECK (state IN ('pending', 'firing', 'recovering', 'resolved')),
  first_observed_at timestamptz NOT NULL,
  last_observed_at timestamptz NOT NULL,
  state_version bigint NOT NULL CHECK (state_version > 0),
  occurrence_count bigint NOT NULL DEFAULT 1 CHECK (occurrence_count >= 0),
  evidence_digest varchar(64),
  last_notification_id varchar(96),
  resolved_at timestamptz,
  PRIMARY KEY (incident_id),
  UNIQUE (fingerprint)
);
CREATE INDEX IF NOT EXISTS ix_alert_incidents_scope_state ON alert_incidents(scope_digest, state);

-- Rollback decisions recorded atomically with release transitions (DEC-004).
CREATE TABLE IF NOT EXISTS rollback_decisions (
  decision_id varchar(36) NOT NULL,
  release_id varchar(36) NOT NULL,
  command_id varchar(128) NOT NULL,
  actor_digest varchar(64) NOT NULL,
  reason_code varchar(64) NOT NULL,
  target_snapshot_id varchar(36) NOT NULL,
  affected_tenant_count bigint NOT NULL DEFAULT 0 CHECK (affected_tenant_count >= 0),
  from_revision bigint NOT NULL CHECK (from_revision > 0),
  to_revision bigint NOT NULL CHECK (to_revision > 0),
  created_at timestamptz NOT NULL,
  PRIMARY KEY (decision_id),
  UNIQUE (release_id, command_id)
);
