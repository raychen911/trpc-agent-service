-- Phase-seven forward-only schema.  Every relation is tenant scoped and all
-- statements are safe to run more than once during initialization.
CREATE TABLE IF NOT EXISTS session_streams (
  tenant_id varchar(64) NOT NULL,
  session_key varchar(256) NOT NULL,
  watermark bigint NOT NULL DEFAULT 0 CHECK (watermark >= 0),
  authority varchar(32) NOT NULL DEFAULT 'POSTGRES',
  rollback_eligible boolean NOT NULL DEFAULT true,
  generation bigint NOT NULL DEFAULT 1 CHECK (generation >= 0),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, session_key)
);

CREATE TABLE IF NOT EXISTS session_events (
  tenant_id varchar(64) NOT NULL,
  session_key varchar(256) NOT NULL,
  event_id varchar(128) NOT NULL,
  sequence bigint NOT NULL,
  event_type varchar(64) NOT NULL,
  payload jsonb NOT NULL,
  content_digest varchar(64) NOT NULL,
  trace_id varchar(36),
  owner_trace_id varchar(36),
  execution_trace_id varchar(36),
  created_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, session_key, event_id),
  UNIQUE (tenant_id, session_key, sequence),
  FOREIGN KEY (tenant_id, session_key) REFERENCES session_streams(tenant_id, session_key)
);
CREATE INDEX IF NOT EXISTS ix_session_events_scope_sequence ON session_events(tenant_id, session_key, sequence);

CREATE TABLE IF NOT EXISTS memory_records (
  tenant_id varchar(64) NOT NULL,
  namespace varchar(128) NOT NULL,
  memory_key varchar(256) NOT NULL,
  content jsonb NOT NULL,
  content_digest varchar(64) NOT NULL,
  byte_size bigint NOT NULL CHECK (byte_size >= 0),
  version bigint NOT NULL CHECK (version > 0),
  source_event_watermark bigint CHECK (source_event_watermark IS NULL OR source_event_watermark >= 0),
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, namespace, memory_key)
);
CREATE INDEX IF NOT EXISTS ix_memory_tenant_namespace ON memory_records(tenant_id, namespace);

CREATE TABLE IF NOT EXISTS summary_records (
  tenant_id varchar(64) NOT NULL,
  session_key varchar(256) NOT NULL,
  content jsonb NOT NULL,
  content_digest varchar(64) NOT NULL,
  event_watermark bigint NOT NULL CHECK (event_watermark >= 0),
  version bigint NOT NULL CHECK (version > 0),
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, session_key),
  FOREIGN KEY (tenant_id, session_key) REFERENCES session_streams(tenant_id, session_key)
);

CREATE TABLE IF NOT EXISTS artifact_metadata (
  tenant_id varchar(64) NOT NULL,
  artifact_id varchar(128) NOT NULL,
  storage_ref varchar(512) NOT NULL,
  content_digest varchar(64) NOT NULL,
  byte_size bigint NOT NULL CHECK (byte_size >= 0),
  media_type varchar(128) NOT NULL,
  status varchar(32) NOT NULL,
  version bigint NOT NULL CHECK (version > 0),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, artifact_id)
);

CREATE TABLE IF NOT EXISTS artifact_uploads (
  tenant_id varchar(64) NOT NULL,
  upload_id varchar(128) NOT NULL,
  artifact_id varchar(128) NOT NULL,
  temp_ref_digest varchar(64) NOT NULL,
  expected_digest varchar(64) NOT NULL,
  status varchar(32) NOT NULL,
  expected_metadata_version bigint,
  expires_at timestamptz NOT NULL,
  generation bigint NOT NULL DEFAULT 1 CHECK (generation >= 0),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, upload_id)
);

CREATE TABLE IF NOT EXISTS knowledge_documents (
  tenant_id varchar(64) NOT NULL,
  document_id varchar(128) NOT NULL,
  metadata jsonb NOT NULL,
  content_digest varchar(64) NOT NULL,
  embedding_ref varchar(512),
  index_status varchar(32) NOT NULL,
  version bigint NOT NULL CHECK (version > 0),
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, document_id)
);
CREATE INDEX IF NOT EXISTS ix_knowledge_tenant_status ON knowledge_documents(tenant_id, index_status);

CREATE TABLE IF NOT EXISTS migration_states (
  tenant_id varchar(64) NOT NULL,
  stream varchar(256) NOT NULL,
  state varchar(48) NOT NULL,
  authority varchar(32) NOT NULL,
  source_watermark bigint,
  copied_watermark bigint NOT NULL DEFAULT 0 CHECK (copied_watermark >= 0),
  source_digest varchar(64),
  target_digest varchar(64),
  rollback_eligible boolean NOT NULL DEFAULT true,
  generation bigint NOT NULL DEFAULT 1 CHECK (generation >= 0),
  lease_owner_digest varchar(64),
  failure_code varchar(64),
  created_at timestamptz NOT NULL,
  updated_at timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, stream)
);

CREATE TABLE IF NOT EXISTS data_recovery_markers (
  marker_id varchar(128) PRIMARY KEY,
  tenant_id varchar(64) NOT NULL,
  operation varchar(64) NOT NULL,
  stage varchar(64) NOT NULL,
  result_digest varchar(64),
  generation bigint NOT NULL DEFAULT 1 CHECK (generation >= 0),
  review_reason varchar(128),
  confirmed boolean NOT NULL DEFAULT false,
  created_at timestamptz NOT NULL,
  UNIQUE (tenant_id, operation, stage, result_digest)
);
