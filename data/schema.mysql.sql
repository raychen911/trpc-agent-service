-- Minimal MySQL 8 tenant-owned persistence schema.
-- Sensitive config values are encrypted by TenantConfigCodec before insert.

CREATE TABLE tenant (
    tenant_id VARCHAR(128) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    status VARCHAR(32) NOT NULL,
    config_version BIGINT NOT NULL DEFAULT 1,
    config_snapshot JSON NOT NULL,
    encrypted_secrets LONGTEXT,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;

CREATE TABLE tenant_config_version (
    tenant_id VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL,
    config_snapshot JSON NOT NULL,
    encrypted_secrets LONGTEXT,
    created_by VARCHAR(128) NOT NULL,
    reason VARCHAR(512) NOT NULL DEFAULT '',
    rolled_back BOOLEAN NOT NULL DEFAULT FALSE,
    rolled_back_to BIGINT,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, version),
    KEY idx_tenant_config_time (tenant_id, created_at)
) ENGINE=InnoDB;

CREATE TABLE config_outbox (
    event_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(128) NOT NULL,
    config_version BIGINT NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSON NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at TIMESTAMP(6),
    KEY idx_outbox_pending (status, created_at),
    KEY idx_outbox_tenant (tenant_id)
) ENGINE=InnoDB;

CREATE TABLE agent_app (
    tenant_id VARCHAR(128) NOT NULL,
    app_id VARCHAR(128) NOT NULL,
    agent_name VARCHAR(128) NOT NULL,
    instruction TEXT,
    config_version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, app_id),
    CONSTRAINT fk_app_tenant FOREIGN KEY (tenant_id) REFERENCES tenant(tenant_id)
) ENGINE=InnoDB;

CREATE TABLE agent_session (
    tenant_id VARCHAR(128) NOT NULL,
    app_id VARCHAR(128) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(128) NOT NULL,
    state JSON NOT NULL,
    version BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, app_id, user_id, session_id),
    CONSTRAINT fk_session_app FOREIGN KEY (tenant_id, app_id)
        REFERENCES agent_app(tenant_id, app_id)
) ENGINE=InnoDB;

CREATE TABLE message_event (
    tenant_id VARCHAR(128) NOT NULL,
    app_id VARCHAR(128) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(128) NOT NULL,
    sequence_no BIGINT NOT NULL,
    event_id VARCHAR(128) NOT NULL,
    role VARCHAR(32) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    payload JSON NOT NULL,
    idempotency_key VARCHAR(255),
    trace_id VARCHAR(64),
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, app_id, user_id, session_id, sequence_no),
    UNIQUE KEY uq_event_idempotency (tenant_id, idempotency_key),
    CONSTRAINT fk_event_session FOREIGN KEY (tenant_id, app_id, user_id, session_id)
        REFERENCES agent_session(tenant_id, app_id, user_id, session_id)
) ENGINE=InnoDB;

CREATE TABLE memory (
    tenant_id VARCHAR(128) NOT NULL,
    memory_id VARCHAR(128) NOT NULL,
    app_id VARCHAR(128) NOT NULL,
    user_id VARCHAR(255),
    session_id VARCHAR(128),
    content JSON NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, memory_id),
    KEY idx_memory_lookup (tenant_id, app_id, user_id, updated_at)
) ENGINE=InnoDB;

CREATE TABLE summary (
    tenant_id VARCHAR(128) NOT NULL,
    app_id VARCHAR(128) NOT NULL,
    user_id VARCHAR(255) NOT NULL,
    session_id VARCHAR(128) NOT NULL,
    summary_id VARCHAR(128) NOT NULL,
    source_version BIGINT NOT NULL,
    content LONGTEXT NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, summary_id),
    KEY idx_summary_session
        (tenant_id, app_id, user_id, session_id, source_version),
    CONSTRAINT fk_summary_session FOREIGN KEY
        (tenant_id, app_id, user_id, session_id)
        REFERENCES agent_session(tenant_id, app_id, user_id, session_id)
) ENGINE=InnoDB;

CREATE TABLE channel_binding (
    tenant_id VARCHAR(128) NOT NULL,
    channel VARCHAR(64) NOT NULL,
    account_id VARCHAR(255) NOT NULL,
    webhook_path VARCHAR(512) NOT NULL,
    secret_ref VARCHAR(512) NOT NULL,
    identity_rules JSON NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, channel, account_id),
    UNIQUE KEY uq_channel_account (channel, account_id),
    CONSTRAINT fk_channel_tenant FOREIGN KEY (tenant_id) REFERENCES tenant(tenant_id)
) ENGINE=InnoDB;

CREATE TABLE inbound_receipt (
    tenant_id VARCHAR(128) NOT NULL,
    channel VARCHAR(64) NOT NULL,
    message_id VARCHAR(255) NOT NULL,
    request_id VARCHAR(128) NOT NULL,
    trace_id VARCHAR(64),
    task_id VARCHAR(128),
    status VARCHAR(32) NOT NULL,
    result_checksum VARCHAR(64),
    reply_status VARCHAR(32),
    expires_at TIMESTAMP(6) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, channel, message_id),
    KEY idx_receipt_expiry (expires_at),
    KEY idx_receipt_trace (trace_id)
) ENGINE=InnoDB;

CREATE TABLE artifact (
    tenant_id VARCHAR(128) NOT NULL,
    artifact_id VARCHAR(128) NOT NULL,
    version BIGINT NOT NULL,
    app_id VARCHAR(128) NOT NULL,
    user_id VARCHAR(255),
    session_id VARCHAR(128),
    filename VARCHAR(512) NOT NULL,
    storage_backend VARCHAR(32) NOT NULL,
    object_key VARCHAR(1024) NOT NULL,
    checksum VARCHAR(64) NOT NULL,
    size_bytes BIGINT NOT NULL,
    mime_type VARCHAR(255),
    metadata JSON NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'ready',
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, artifact_id, version),
    UNIQUE KEY uq_artifact_object (tenant_id, storage_backend, object_key),
    KEY idx_artifact_scope (tenant_id, app_id, user_id, session_id, filename)
) ENGINE=InnoDB;

CREATE TABLE knowledge_document (
    tenant_id VARCHAR(128) NOT NULL,
    document_id VARCHAR(128) NOT NULL,
    app_id VARCHAR(128) NOT NULL,
    source_uri VARCHAR(1024),
    checksum VARCHAR(64) NOT NULL,
    version BIGINT NOT NULL,
    status VARCHAR(32) NOT NULL,
    metadata JSON NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    updated_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6)
        ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, document_id),
    KEY idx_knowledge_app (tenant_id, app_id, status)
) ENGINE=InnoDB;

CREATE TABLE knowledge_chunk (
    tenant_id VARCHAR(128) NOT NULL,
    document_id VARCHAR(128) NOT NULL,
    chunk_id VARCHAR(128) NOT NULL,
    ordinal_no INT NOT NULL,
    content LONGTEXT NOT NULL,
    vector_backend VARCHAR(32) NOT NULL,
    vector_id VARCHAR(255) NOT NULL,
    embedding_model VARCHAR(255) NOT NULL,
    embedding_version VARCHAR(128) NOT NULL,
    metadata JSON NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (tenant_id, document_id, chunk_id),
    UNIQUE KEY uq_vector_record (tenant_id, vector_backend, vector_id),
    CONSTRAINT fk_chunk_document FOREIGN KEY (tenant_id, document_id)
        REFERENCES knowledge_document(tenant_id, document_id)
        ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE TABLE storage_outbox (
    event_id VARCHAR(64) PRIMARY KEY,
    tenant_id VARCHAR(128) NOT NULL,
    entity_type VARCHAR(32) NOT NULL,
    entity_id VARCHAR(255) NOT NULL,
    operation VARCHAR(32) NOT NULL,
    target_backend VARCHAR(32) NOT NULL,
    payload JSON NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'pending',
    retry_count INT NOT NULL DEFAULT 0,
    available_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    last_error VARCHAR(1024),
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    published_at TIMESTAMP(6),
    KEY idx_storage_outbox_pending (status, available_at),
    KEY idx_storage_outbox_entity (tenant_id, entity_type, entity_id)
) ENGINE=InnoDB;

CREATE TABLE audit_log (
    id VARCHAR(255) NOT NULL,
    tenant_id VARCHAR(128) NOT NULL,
    channel VARCHAR(64),
    user_id VARCHAR(255),
    session_id VARCHAR(128),
    agent_name VARCHAR(128),
    tool_name VARCHAR(128),
    decision VARCHAR(64) NOT NULL,
    latency_ms BIGINT,
    error_type VARCHAR(128),
    cost DECIMAL(20, 8),
    trace_id VARCHAR(64),
    detail JSON NOT NULL,
    created_at TIMESTAMP(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    KEY idx_audit_tenant_time (tenant_id, created_at),
    KEY idx_audit_trace (trace_id)
) ENGINE=InnoDB;

-- Every query must include tenant_id. MySQL has no PostgreSQL-style RLS, so
-- application predicates, tenant-scoped repositories and DB grants are mandatory.
