"""Create the production PostgreSQL schema baseline.

Revision ID: 20260903_01
Revises: None
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260903_01"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


UPGRADE_STATEMENTS = (
    """
    CREATE TABLE IF NOT EXISTS tenant_config (
      tenant_id TEXT NOT NULL,
      version INTEGER NOT NULL,
      config_json JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tenant_active (
      tenant_id TEXT PRIMARY KEY,
      active_version INTEGER NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS session_state (
      tenant_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      state_version INTEGER NOT NULL DEFAULT 0,
      latest_event_seq INTEGER NOT NULL DEFAULT 0,
      state_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      PRIMARY KEY (tenant_id, session_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS message_event (
      tenant_id TEXT NOT NULL,
      event_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      seq INTEGER NOT NULL,
      idempotency_key TEXT,
      event_type TEXT NOT NULL,
      payload_json JSONB NOT NULL,
      trace_id TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, event_id),
      UNIQUE (tenant_id, session_id, seq),
      UNIQUE (tenant_id, session_id, idempotency_key, event_type)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS memory (
      tenant_id TEXT NOT NULL,
      memory_id TEXT NOT NULL,
      scope_key TEXT NOT NULL,
      content TEXT NOT NULL,
      version INTEGER NOT NULL,
      metadata_json JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, memory_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS summary (
      tenant_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      summary_version INTEGER NOT NULL,
      source_event_seq INTEGER NOT NULL,
      content TEXT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, session_id, summary_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_log (
      audit_id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      channel TEXT,
      user_id TEXT,
      session_id TEXT,
      agent_name TEXT,
      tool_name TEXT,
      decision TEXT NOT NULL,
      latency_ms INTEGER,
      error_type TEXT,
      token_usage INTEGER,
      cost DOUBLE PRECISION,
      trace_id TEXT NOT NULL,
      metadata_json JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS idempotency (
      tenant_id TEXT NOT NULL,
      key TEXT NOT NULL,
      status TEXT NOT NULL,
      response_ref TEXT,
      result_json JSONB,
      trace_id TEXT,
      attempt INTEGER NOT NULL DEFAULT 1,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, key)
    )
    """,
    "ALTER TABLE idempotency ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 1",
    """
    CREATE TABLE IF NOT EXISTS compensation_task (
      task_id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      operation TEXT NOT NULL,
      payload_json JSONB NOT NULL,
      status TEXT NOT NULL,
      attempt INTEGER NOT NULL DEFAULT 0,
      available_at TIMESTAMPTZ NOT NULL,
      last_error TEXT,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS session_fence (
      tenant_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      fencing_token BIGINT NOT NULL DEFAULT 0,
      PRIMARY KEY (tenant_id, session_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS session_lease (
      tenant_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      owner TEXT NOT NULL,
      fencing_token BIGINT NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, session_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS inbox_message (
      message_id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      dedupe_key TEXT NOT NULL,
      session_id TEXT NOT NULL,
      payload_json JSONB NOT NULL,
      status TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 1,
      owner TEXT,
      lease_until TIMESTAMPTZ,
      result_json JSONB,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      UNIQUE (tenant_id, dedupe_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS outbox_message (
      event_id TEXT PRIMARY KEY,
      tenant_id TEXT NOT NULL,
      topic TEXT NOT NULL,
      aggregate_id TEXT NOT NULL,
      payload_json JSONB NOT NULL,
      status TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      available_at TIMESTAMPTZ NOT NULL,
      locked_by TEXT,
      locked_until TIMESTAMPTZ,
      last_error TEXT,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS mailbox_message (
      tenant_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      sequence BIGINT NOT NULL,
      message_id TEXT NOT NULL,
      dedupe_key TEXT NOT NULL,
      payload_json JSONB NOT NULL,
      status TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      owner TEXT,
      fencing_token BIGINT NOT NULL DEFAULT 0,
      lease_until TIMESTAMPTZ,
      available_at TIMESTAMPTZ NOT NULL,
      last_error TEXT,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, session_id, sequence),
      UNIQUE (tenant_id, dedupe_key),
      UNIQUE (tenant_id, message_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS mailbox_fence (
      tenant_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      fencing_token BIGINT NOT NULL DEFAULT 0,
      PRIMARY KEY (tenant_id, session_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS session_mailbox (
      tenant_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      status TEXT NOT NULL,
      accepted_sequence BIGINT NOT NULL DEFAULT 0,
      resolved_sequence BIGINT NOT NULL DEFAULT 0,
      processing_sequence BIGINT,
      processing_message_id TEXT,
      queue_generation BIGINT NOT NULL DEFAULT 0,
      lease_owner TEXT,
      lease_epoch BIGINT NOT NULL DEFAULT 0,
      lease_until TIMESTAMPTZ,
      retry_count INTEGER NOT NULL DEFAULT 0,
      attempt INTEGER NOT NULL DEFAULT 0,
      priority INTEGER NOT NULL DEFAULT 0,
      retry_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, session_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS session_mailbox_item (
      tenant_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      sequence BIGINT NOT NULL,
      message_id TEXT NOT NULL,
      trace_id TEXT NOT NULL,
      priority INTEGER NOT NULL DEFAULT 0,
      retry_count INTEGER NOT NULL DEFAULT 0,
      attempt INTEGER NOT NULL DEFAULT 0,
      retry_at TIMESTAMPTZ,
      accepted_at TIMESTAMPTZ NOT NULL,
      resolved_at TIMESTAMPTZ,
      PRIMARY KEY (tenant_id, session_id, sequence),
      UNIQUE (tenant_id, session_id, message_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tool_approval (
      tenant_id TEXT NOT NULL,
      approval_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      request_id TEXT NOT NULL,
      tool_name TEXT NOT NULL,
      arguments_hash TEXT NOT NULL,
      status TEXT NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      approved_by TEXT,
      consumed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, approval_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tool_budget (
      tenant_id TEXT NOT NULL,
      request_id TEXT NOT NULL,
      total_calls INTEGER NOT NULL DEFAULT 0,
      side_effect_calls INTEGER NOT NULL DEFAULT 0,
      max_calls INTEGER NOT NULL,
      max_side_effect_calls INTEGER NOT NULL,
      call_keys_json JSONB NOT NULL,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, request_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS tool_executions (
      tenant_id TEXT NOT NULL,
      execution_id TEXT NOT NULL,
      request_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      tool_name TEXT NOT NULL,
      call_key TEXT NOT NULL,
      arguments_hash TEXT NOT NULL,
      side_effect BOOLEAN NOT NULL DEFAULT FALSE,
      status TEXT NOT NULL,
      attempt INTEGER NOT NULL DEFAULT 1,
      fencing_token BIGINT,
      result_json JSONB,
      error_type TEXT,
      error_message TEXT,
      started_at TIMESTAMPTZ NOT NULL,
      completed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, execution_id),
      UNIQUE (tenant_id, call_key)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS knowledge_chunk (
      tenant_id TEXT NOT NULL,
      collection TEXT NOT NULL,
      chunk_id TEXT NOT NULL,
      text TEXT NOT NULL,
      metadata_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      PRIMARY KEY (tenant_id, collection, chunk_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS migration_lease (
      tenant_id TEXT NOT NULL,
      migration_id TEXT NOT NULL,
      owner_id TEXT NOT NULL,
      owner_instance TEXT NOT NULL,
      lease_epoch BIGINT NOT NULL DEFAULT 1,
      expires_at TIMESTAMPTZ NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, migration_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS migration_write_barrier (
      tenant_id TEXT NOT NULL,
      migration_id TEXT NOT NULL,
      owner_instance TEXT NOT NULL,
      lease_epoch BIGINT NOT NULL,
      mode TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, migration_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS migration_checkpoint (
      tenant_id TEXT NOT NULL,
      migration_id TEXT NOT NULL,
      phase TEXT NOT NULL,
      batch_key TEXT NOT NULL,
      cursor_json JSONB NOT NULL DEFAULT '{}'::jsonb,
      source_count BIGINT NOT NULL DEFAULT 0,
      target_count BIGINT NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'running',
      checksum TEXT NOT NULL,
      updated_at TIMESTAMPTZ NOT NULL,
      PRIMARY KEY (tenant_id, migration_id, phase, batch_key)
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_message_event_created ON message_event (tenant_id, created_at)",
    "CREATE INDEX IF NOT EXISTS idx_idempotency_cleanup ON idempotency (tenant_id, status, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_compensation_cleanup ON compensation_task (tenant_id, status, updated_at)",
    "CREATE INDEX IF NOT EXISTS idx_outbox_ready ON outbox_message (status, available_at, created_at)",
    """
    CREATE INDEX IF NOT EXISTS idx_mailbox_ready
      ON mailbox_message (tenant_id, session_id, status, available_at, sequence)
    """,
    "CREATE INDEX IF NOT EXISTS idx_session_mailbox_ready ON session_mailbox (status, retry_at, updated_at)",
    """
    CREATE INDEX IF NOT EXISTS idx_migration_checkpoint_status
      ON migration_checkpoint (tenant_id, migration_id, status, updated_at)
    """,
)


DROP_ORDER = (
    "migration_checkpoint",
    "migration_write_barrier",
    "migration_lease",
    "knowledge_chunk",
    "tool_executions",
    "tool_budget",
    "tool_approval",
    "session_mailbox_item",
    "session_mailbox",
    "mailbox_fence",
    "mailbox_message",
    "outbox_message",
    "inbox_message",
    "session_lease",
    "session_fence",
    "compensation_task",
    "idempotency",
    "audit_log",
    "summary",
    "memory",
    "message_event",
    "session_state",
    "tenant_active",
    "tenant_config",
)


def upgrade() -> None:
    for statement in UPGRADE_STATEMENTS:
        op.execute(statement)


def downgrade() -> None:
    for table in DROP_ORDER:
        op.execute(f'DROP TABLE IF EXISTS "{table}" CASCADE')
