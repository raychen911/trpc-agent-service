"""SQLAlchemy Core table definitions for tenant configuration storage."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB

metadata = sa.MetaData()

tenant_configs = sa.Table(
    "tenant_configs",
    metadata,
    sa.Column("tenant_id", sa.VARCHAR(64), primary_key=True),
    sa.Column("enabled", sa.BOOLEAN, nullable=False),
    sa.Column("version", sa.BIGINT, nullable=False),
    sa.Column("app_id", sa.TEXT, nullable=False),
    sa.Column("instruction", sa.TEXT, nullable=False),
    sa.Column("model_profile", sa.TEXT, nullable=False),
    sa.Column("allowed_tools", JSONB, nullable=False),
    sa.Column(
        "created_at",
        sa.TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    ),
    sa.Column(
        "updated_at",
        sa.TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    ),
    sa.CheckConstraint("version >= 1", name="tenant_configs_version_positive"),
    sa.CheckConstraint(
        "jsonb_typeof(allowed_tools) = 'array'",
        name="tenant_configs_tools_is_array",
    ),
    sa.Column("governance", JSONB, nullable=False),
    sa.CheckConstraint(
        "jsonb_typeof(governance) = 'object'",
        name="tenant_configs_governance_is_object",
    ),
    sa.Column("backend_profile", JSONB, nullable=False),
    sa.CheckConstraint(
        "jsonb_typeof(backend_profile) = 'object'",
        name="tenant_configs_backend_profile_is_object",
    ),
    sa.Column("audit_policy", JSONB, nullable=False),
    sa.CheckConstraint(
        "jsonb_typeof(audit_policy) = 'object'",
        name="tenant_configs_audit_policy_is_object",
    ),
    sa.CheckConstraint(
        "tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="tenant_configs_tenant_id_format",
    ),
    sa.CheckConstraint(
        "btrim(app_id) <> ''",
        name="tenant_configs_app_id_not_blank",
    ),
    sa.CheckConstraint(
        "btrim(instruction) <> ''",
        name="tenant_configs_instruction_not_blank",
    ),
    sa.CheckConstraint(
        "btrim(model_profile) <> ''",
        name="tenant_configs_model_profile_not_blank",
    ),
)

tenant_config_versions = sa.Table(
    "tenant_config_versions",
    metadata,
    sa.Column("tenant_id", sa.VARCHAR(64), primary_key=True),
    sa.Column("version", sa.BIGINT, primary_key=True),
    sa.Column("enabled", sa.BOOLEAN, nullable=False),
    sa.Column("app_id", sa.TEXT, nullable=False),
    sa.Column("instruction", sa.TEXT, nullable=False),
    sa.Column("model_profile", sa.TEXT, nullable=False),
    sa.Column("allowed_tools", JSONB, nullable=False),
    sa.Column(
        "recorded_at",
        sa.TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    ),
    sa.CheckConstraint("version >= 1", name="tenant_config_versions_version_positive"),
    sa.CheckConstraint(
        "jsonb_typeof(allowed_tools) = 'array'",
        name="tenant_config_versions_tools_is_array",
    ),
    sa.Column("governance", JSONB, nullable=False),
    sa.CheckConstraint(
        "jsonb_typeof(governance) = 'object'",
        name="tenant_config_versions_governance_is_object",
    ),
    sa.Column("backend_profile", JSONB, nullable=False),
    sa.CheckConstraint(
        "jsonb_typeof(backend_profile) = 'object'",
        name="tenant_config_versions_backend_profile_is_object",
    ),
    sa.Column("audit_policy", JSONB, nullable=False),
    sa.CheckConstraint(
        "jsonb_typeof(audit_policy) = 'object'",
        name="tenant_config_versions_audit_policy_is_object",
    ),
    sa.ForeignKeyConstraint(
        ["tenant_id"],
        ["tenant_configs.tenant_id"],
        ondelete="RESTRICT",
        name="tenant_config_versions_tenant_fk",
    ),
    sa.CheckConstraint(
        "btrim(app_id) <> ''",
        name="tenant_config_versions_app_id_not_blank",
    ),
    sa.CheckConstraint(
        "btrim(instruction) <> ''",
        name="tenant_config_versions_instruction_not_blank",
    ),
    sa.CheckConstraint(
        "btrim(model_profile) <> ''",
        name="tenant_config_versions_model_profile_not_blank",
    ),
)

tenant_config_rollouts = sa.Table(
    "tenant_config_rollouts",
    metadata,
    sa.Column("rollout_id", sa.UUID, primary_key=True),
    sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
    sa.Column("active_version", sa.BIGINT, nullable=False),
    sa.Column("candidate_version", sa.BIGINT, nullable=False),
    sa.Column("candidate_percent", sa.SMALLINT, nullable=False),
    sa.Column("status", sa.TEXT, nullable=False),
    sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
    sa.ForeignKeyConstraint(["tenant_id"], ["tenant_configs.tenant_id"],
                            ondelete="RESTRICT",
                            name="tenant_config_rollouts_tenant_fk"),
    sa.CheckConstraint("active_version >= 1", name="tenant_config_rollouts_active_version_positive"),
    sa.CheckConstraint("candidate_version >= 1", name="tenant_config_rollouts_candidate_version_positive"),
    sa.CheckConstraint("active_version <> candidate_version", name="tenant_config_rollouts_versions_differ"),
    sa.CheckConstraint("candidate_percent BETWEEN 1 AND 99", name="tenant_config_rollouts_candidate_percent_range"),
    sa.CheckConstraint("status IN ('running', 'promoted', 'aborted')", name="tenant_config_rollouts_status_valid"),
)

message_receipts = sa.Table(
    "message_receipts",
    metadata,
    sa.Column("receipt_id", sa.UUID, primary_key=True),
    sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
    sa.Column("channel", sa.TEXT, nullable=False),
    sa.Column("user_id", sa.TEXT, nullable=False),
    sa.Column("session_id", sa.TEXT, nullable=False),
    sa.Column("message_id", sa.TEXT, nullable=False),
    sa.Column("app_id", sa.TEXT, nullable=False),
    sa.Column("config_version", sa.BIGINT, nullable=False),
    sa.Column("request_id", sa.UUID, nullable=False),
    sa.Column("message_digest", sa.CHAR(64), nullable=False),
    sa.Column("state", sa.TEXT, nullable=False),
    sa.Column("response_text", sa.TEXT, nullable=True),
    sa.Column("error_code", sa.TEXT, nullable=True),
    sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("latency_ms", sa.BIGINT, nullable=True),
    sa.UniqueConstraint(
        "tenant_id",
        "channel",
        "user_id",
        "session_id",
        "message_id",
        name="message_receipts_business_key",
    ),
    sa.UniqueConstraint(
        "receipt_id",
        "tenant_id",
        "request_id",
        "config_version",
        name="message_receipts_execution_identity",
    ),
    sa.CheckConstraint(
        "state IN ('processing', 'completed', 'failed')",
        name="message_receipts_state_valid",
    ),
    sa.CheckConstraint(
        "config_version >= 1",
        name="message_receipts_config_version_positive",
    ),
    sa.CheckConstraint(
        "message_digest ~ '^[0-9a-f]{64}$'",
        name="message_receipts_digest_format",
    ),
    sa.CheckConstraint(
        "(state = 'completed' AND response_text IS NOT NULL) OR (state != 'completed')",
        name="message_receipts_completed_has_response",
    ),
    sa.CheckConstraint(
        "(state = 'failed' AND error_code IS NOT NULL) OR (state != 'failed')",
        name="message_receipts_failed_has_error",
    ),
    sa.CheckConstraint(
        "(state = 'processing' AND response_text IS NULL AND error_code IS NULL) OR (state != 'processing')",
        name="message_receipts_processing_no_terminal_data",
    ),
)

message_audit_events = sa.Table(
    "message_audit_events",
    metadata,
    sa.Column("audit_id", sa.UUID, primary_key=True),
    sa.Column("receipt_id", sa.UUID, nullable=False),
    sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
    sa.Column("app_id", sa.TEXT, nullable=False),
    sa.Column("channel", sa.TEXT, nullable=False),
    sa.Column("user_id", sa.TEXT, nullable=False),
    sa.Column("session_id", sa.TEXT, nullable=False),
    sa.Column("message_id", sa.TEXT, nullable=False),
    sa.Column("event_type", sa.TEXT, nullable=False),
    sa.Column("request_id", sa.UUID, nullable=False),
    sa.Column("config_version", sa.BIGINT, nullable=False),
    sa.Column("error_code", sa.TEXT, nullable=True),
    sa.Column("latency_ms", sa.BIGINT, nullable=True),
    sa.Column("message_digest", sa.CHAR(64), nullable=False),
    sa.Column("response_digest", sa.CHAR(64), nullable=True),
    sa.Column("occurred_at", sa.TIMESTAMP(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(
        ["receipt_id"],
        ["message_receipts.receipt_id"],
        ondelete="RESTRICT",
        name="message_audit_events_receipt_fk",
    ),
    sa.CheckConstraint(
        "event_type IN ('accepted', 'completed', 'failed')",
        name="message_audit_events_type_valid",
    ),
    sa.CheckConstraint(
        "config_version >= 1",
        name="message_audit_events_config_version_positive",
    ),
    sa.CheckConstraint(
        "message_digest ~ '^[0-9a-f]{64}$'",
        name="message_audit_events_digest_format",
    ),
    sa.CheckConstraint(
        "(response_digest IS NULL OR response_digest ~ '^[0-9a-f]{64}$')",
        name="message_audit_events_response_digest_format",
    ),
)

tool_approval_requests = sa.Table(
    "tool_approval_requests",
    metadata,
    sa.Column("approval_id", sa.UUID, primary_key=True),
    sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
    sa.Column("app_id", sa.TEXT, nullable=False),
    sa.Column("config_version", sa.BIGINT, nullable=False),
    sa.Column("channel", sa.TEXT, nullable=False),
    sa.Column("user_id", sa.TEXT, nullable=False),
    sa.Column("session_id", sa.TEXT, nullable=False),
    sa.Column("receipt_id", sa.UUID, nullable=False),
    sa.Column("function_call_id", sa.TEXT, nullable=False),
    sa.Column("tool_name", sa.TEXT, nullable=False),
    sa.Column("tool_args", JSONB, nullable=False),
    sa.Column("args_digest", sa.CHAR(64), nullable=False),
    sa.Column("state", sa.TEXT, nullable=False, server_default=sa.text("'pending'")),
    sa.Column("decision", sa.TEXT, nullable=True),
    sa.Column("decision_message_id", sa.TEXT, nullable=True),
    sa.Column("response_text", sa.TEXT, nullable=True),
    sa.Column(
        "created_at",
        sa.TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    ),
    sa.Column("decided_at", sa.TIMESTAMP(timezone=True), nullable=True),
    sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
    sa.ForeignKeyConstraint(
        ["receipt_id"],
        ["message_receipts.receipt_id"],
        ondelete="RESTRICT",
        name="tool_approval_requests_receipt_fk",
    ),
    sa.UniqueConstraint(
        "receipt_id",
        "function_call_id",
        name="tool_approval_requests_receipt_call_uk",
    ),
    sa.CheckConstraint(
        "tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="tool_approval_requests_tenant_id_format",
    ),
    sa.CheckConstraint("config_version >= 1", name="tool_approval_requests_version_positive"),
    sa.CheckConstraint("btrim(app_id) <> ''", name="tool_approval_requests_app_not_blank"),
    sa.CheckConstraint("btrim(channel) <> ''", name="tool_approval_requests_channel_not_blank"),
    sa.CheckConstraint("btrim(user_id) <> ''", name="tool_approval_requests_user_not_blank"),
    sa.CheckConstraint("btrim(session_id) <> ''", name="tool_approval_requests_session_not_blank"),
    sa.CheckConstraint(
        "btrim(function_call_id) <> ''",
        name="tool_approval_requests_call_id_not_blank",
    ),
    sa.CheckConstraint("btrim(tool_name) <> ''", name="tool_approval_requests_tool_not_blank"),
    sa.CheckConstraint(
        "jsonb_typeof(tool_args) = 'object'",
        name="tool_approval_requests_args_is_object",
    ),
    sa.CheckConstraint(
        "args_digest ~ '^[0-9a-f]{64}$'",
        name="tool_approval_requests_digest_format",
    ),
    sa.CheckConstraint(
        "state IN ('pending', 'executing', 'completed', 'rejected', 'failed')",
        name="tool_approval_requests_state_valid",
    ),
    sa.CheckConstraint(
        "decision IS NULL OR decision IN ('approve', 'reject')",
        name="tool_approval_requests_decision_valid",
    ),
    sa.CheckConstraint(
        "(state = 'pending' AND decision IS NULL AND decided_at IS NULL"
        " AND finished_at IS NULL AND response_text IS NULL)"
        " OR state != 'pending'",
        name="tool_approval_requests_pending_no_decision",
    ),
    sa.CheckConstraint(
        "state != 'executing' OR decision IS NOT NULL",
        name="tool_approval_requests_executing_has_decision",
    ),
    sa.CheckConstraint(
        "(state IN ('completed', 'rejected', 'failed') AND finished_at IS NOT NULL"
        " AND decided_at IS NOT NULL) OR state NOT IN ('completed', 'rejected', 'failed')",
        name="tool_approval_requests_terminal_has_timestamps",
    ),
    sa.CheckConstraint(
        "(state IN ('completed', 'rejected') AND response_text IS NOT NULL)"
        " OR state NOT IN ('completed', 'rejected')",
        name="tool_approval_requests_terminal_has_response",
    ),
)

tool_approval_audit_events = sa.Table(
    "tool_approval_audit_events",
    metadata,
    sa.Column("audit_id", sa.UUID, primary_key=True),
    sa.Column("approval_id", sa.UUID, nullable=False),
    sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
    sa.Column("event_type", sa.TEXT, nullable=False),
    sa.Column("decision", sa.TEXT, nullable=True),
    sa.Column("message_id", sa.TEXT, nullable=True),
    sa.Column("args_digest", sa.CHAR(64), nullable=False),
    sa.Column("occurred_at", sa.TIMESTAMP(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(
        ["approval_id"],
        ["tool_approval_requests.approval_id"],
        ondelete="RESTRICT",
        name="tool_approval_audit_events_approval_fk",
    ),
    sa.CheckConstraint(
        "event_type IN ('created', 'decided', 'completed', 'rejected', 'failed')",
        name="tool_approval_audit_events_type_valid",
    ),
    sa.CheckConstraint(
        "decision IS NULL OR decision IN ('approve', 'reject')",
        name="tool_approval_audit_events_decision_valid",
    ),
    sa.CheckConstraint(
        "args_digest ~ '^[0-9a-f]{64}$'",
        name="tool_approval_audit_events_digest_format",
    ),
)

execution_audit_events = sa.Table(
    "execution_audit_events",
    metadata,
    sa.Column("audit_id", sa.UUID, primary_key=True),
    sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
    sa.Column("receipt_id", sa.UUID, nullable=True),
    sa.Column("request_id", sa.UUID, nullable=False),
    sa.Column("config_version", sa.BIGINT, nullable=False),
    sa.Column("trace_id", sa.TEXT, nullable=True),
    sa.Column("event_type", sa.TEXT, nullable=False),
    sa.Column("outcome", sa.TEXT, nullable=False),
    sa.Column("category", sa.TEXT, nullable=True),
    sa.Column("tool_name", sa.TEXT, nullable=True),
    sa.Column("error_code", sa.TEXT, nullable=True),
    sa.Column("latency_ms", sa.BIGINT, nullable=True),
    sa.Column("occurred_at", sa.TIMESTAMP(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(
        ["receipt_id", "tenant_id", "request_id", "config_version"],
        [
            "message_receipts.receipt_id",
            "message_receipts.tenant_id",
            "message_receipts.request_id",
            "message_receipts.config_version",
        ],
        ondelete="RESTRICT",
        name="execution_audit_events_receipt_identity_fk",
    ),
    sa.CheckConstraint(
        "tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="execution_audit_events_tenant_id_format",
    ),
    sa.CheckConstraint(
        "config_version >= 1",
        name="execution_audit_events_config_version_positive",
    ),
    sa.CheckConstraint(
        "(trace_id IS NULL OR trace_id ~ '^[0-9a-f]{32}$')",
        name="execution_audit_events_trace_format",
    ),
    sa.CheckConstraint(
        "(receipt_id IS NOT NULL OR event_type = 'delivery_result')",
        name="execution_audit_events_receipt_required",
    ),
    sa.CheckConstraint(
        "event_type IN ('content_decision', 'agent_result', 'tool_decision', 'delivery_result')",
        name="execution_audit_events_type_valid",
    ),
    sa.CheckConstraint(
        "outcome IN ('allow', 'blocked', 'success', 'error', 'deny_blocked',"
        " 'review_pending', 'delivered', 'failed')",
        name="execution_audit_events_outcome_valid",
    ),
    sa.CheckConstraint(
        "(category IS NULL OR category IN ('none', 'credential', 'private_key', 'credential_dsn'))",
        name="execution_audit_events_category_valid",
    ),
    sa.CheckConstraint(
        "(tool_name IS NULL OR (tool_name = btrim(tool_name) AND tool_name <> ''"
        " AND char_length(tool_name) <= 200))",
        name="execution_audit_events_tool_name_valid",
    ),
    # Fixed event/outcome pairing plus category/tool_name presence — the same
    # invariants the ExecutionAuditEvent pydantic model enforces in Python.
    sa.CheckConstraint(
        "(event_type = 'content_decision' AND outcome IN ('allow', 'blocked') AND category IS NOT NULL)"
        " OR (event_type = 'agent_result' AND outcome IN ('success', 'error') AND category IS NULL)"
        " OR (event_type = 'tool_decision' AND outcome IN ('allow', 'deny_blocked', 'review_pending')"
        " AND category IS NULL AND tool_name IS NOT NULL)"
        " OR (event_type = 'delivery_result' AND outcome IN ('delivered', 'failed') AND category IS NULL)",
        name="execution_audit_events_pairing_valid",
    ),
    sa.CheckConstraint(
        "(event_type = 'tool_decision' OR tool_name IS NULL)",
        name="execution_audit_events_tool_name_scoped",
    ),
    sa.CheckConstraint(
        "(error_code IS NULL OR error_code IN ("
        "'tenant_config_mismatch', 'tenant_agent_configuration', 'model_configuration', 'model_runtime',"
        " 'worker_unavailable', 'worker_timeout', 'invalid_worker_response', 'session_busy',"
        " 'tenant_repository_unavailable', 'message_in_progress', 'idempotency_conflict',"
        " 'approval_not_available', 'approval_in_progress', 'approval_conflict', 'approval_config_stale',"
        " 'approval_repository_unavailable', 'approval_execution_failed', 'content_input_blocked',"
        " 'usage_budget_exceeded', 'channel_delivery_failed'))",
        name="execution_audit_events_error_code_valid",
    ),
    sa.CheckConstraint(
        "(((event_type = 'agent_result' AND outcome = 'error')"
        " OR (event_type = 'delivery_result' AND outcome = 'failed')) = (error_code IS NOT NULL))",
        name="execution_audit_events_error_code_required",
    ),
    sa.CheckConstraint(
        "(latency_ms IS NULL OR latency_ms >= 0)",
        name="execution_audit_events_latency_non_negative",
    ),
    sa.Index(
        "execution_audit_events_receipt_time_idx",
        "tenant_id",
        "receipt_id",
        "occurred_at",
    ),
    sa.Index(
        "execution_audit_events_request_time_idx",
        "tenant_id",
        "request_id",
        "occurred_at",
    ),
)

tenant_usage_daily = sa.Table(
    "tenant_usage_daily",
    metadata,
    sa.Column("usage_date", sa.Date, primary_key=True),
    sa.Column("tenant_id", sa.VARCHAR(64), primary_key=True),
    sa.Column("model_profile", sa.TEXT, primary_key=True),
    sa.Column("requests", sa.BIGINT, nullable=False),
    sa.Column("input_tokens", sa.BIGINT, nullable=True),
    sa.Column("output_tokens", sa.BIGINT, nullable=True),
    sa.Column("cost_microunits", sa.BIGINT, nullable=True),
    sa.Column(
        "updated_at",
        sa.TIMESTAMP(timezone=True),
        nullable=False,
        server_default=sa.text("now()"),
    ),
    # NULL means UNKNOWN (never a fabricated zero); known values are
    # non-negative integer micro-currency units / token counts.
    sa.CheckConstraint("requests >= 0", name="tenant_usage_daily_requests_non_negative"),
    sa.CheckConstraint("(input_tokens IS NULL OR input_tokens >= 0)", name="tenant_usage_daily_input_tokens_valid"),
    sa.CheckConstraint("(output_tokens IS NULL OR output_tokens >= 0)", name="tenant_usage_daily_output_tokens_valid"),
    sa.CheckConstraint("(cost_microunits IS NULL OR cost_microunits >= 0)",
                       name="tenant_usage_daily_cost_microunits_valid"),
    sa.CheckConstraint(
        "tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'",
        name="tenant_usage_daily_tenant_id_format",
    ),
    sa.CheckConstraint("btrim(model_profile) <> ''", name="tenant_usage_daily_profile_not_blank"),
)

request_usage_records = sa.Table(
    "request_usage_records",
    metadata,
    sa.Column("tenant_id", sa.VARCHAR(64), primary_key=True),
    sa.Column("request_id", sa.UUID, primary_key=True),
    sa.Column("receipt_id", sa.UUID, nullable=True),
    sa.Column("config_version", sa.BIGINT, nullable=False),
    sa.Column("model_profile", sa.TEXT, nullable=False),
    sa.Column("input_tokens", sa.BIGINT, nullable=True),
    sa.Column("output_tokens", sa.BIGINT, nullable=True),
    sa.Column("cost_microunits", sa.BIGINT, nullable=True),
    sa.Column("occurred_at", sa.TIMESTAMP(timezone=True), nullable=False),
    sa.ForeignKeyConstraint(["receipt_id", "tenant_id", "request_id", "config_version"], [
        "message_receipts.receipt_id", "message_receipts.tenant_id", "message_receipts.request_id",
        "message_receipts.config_version"
    ],
                            ondelete="RESTRICT",
                            name="request_usage_records_receipt_identity_fk"),
    sa.CheckConstraint("config_version >= 1", name="request_usage_records_config_version_positive"),
    sa.CheckConstraint("btrim(model_profile) <> ''", name="request_usage_records_profile_not_blank"),
    sa.Index("request_usage_records_tenant_time_idx", "tenant_id", "occurred_at"),
)

# R1C: object bytes live in shared S3/MinIO.  PostgreSQL stores only the
# tenant-scoped version catalogue so version assignment remains transactional.
artifact_metadata = sa.Table(
    "artifact_metadata",
    metadata,
    sa.Column("tenant_id", sa.VARCHAR(64), primary_key=True),
    sa.Column("artifact_path", sa.TEXT, primary_key=True),
    sa.Column("state", sa.TEXT, nullable=False),
    sa.Column("deleted_at", sa.TIMESTAMP(timezone=True)),
    sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.ForeignKeyConstraint(["tenant_id"], ["tenant_configs.tenant_id"],
                            ondelete="RESTRICT",
                            name="artifact_metadata_tenant_fk"),
    sa.CheckConstraint("tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'", name="artifact_metadata_tenant_id_format"),
    sa.CheckConstraint("btrim(artifact_path) <> ''", name="artifact_metadata_path_not_blank"),
    sa.CheckConstraint("state IN ('active', 'deleted')", name="artifact_metadata_state_valid"),
)

artifact_versions = sa.Table(
    "artifact_versions",
    metadata,
    sa.Column("tenant_id", sa.VARCHAR(64), primary_key=True),
    sa.Column("artifact_path", sa.TEXT, primary_key=True),
    sa.Column("version", sa.BIGINT, primary_key=True),
    sa.Column("object_key", sa.TEXT, nullable=False),
    sa.Column("content_digest", sa.CHAR(64), nullable=False),
    sa.Column("size_bytes", sa.BIGINT, nullable=False),
    sa.Column("mime_type", sa.TEXT, nullable=False),
    sa.Column("custom_metadata", JSONB, nullable=False),
    sa.Column("state", sa.TEXT, nullable=False),
    sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.ForeignKeyConstraint(["tenant_id", "artifact_path"],
                            ["artifact_metadata.tenant_id", "artifact_metadata.artifact_path"],
                            ondelete="RESTRICT",
                            name="artifact_versions_metadata_fk"),
    sa.UniqueConstraint("object_key", name="artifact_versions_object_key_uk"),
    sa.CheckConstraint("version >= 0", name="artifact_versions_version_non_negative"),
    sa.CheckConstraint("btrim(object_key) <> ''", name="artifact_versions_object_key_not_blank"),
    sa.CheckConstraint("content_digest ~ '^[0-9a-f]{64}$'", name="artifact_versions_digest_format"),
    sa.CheckConstraint("size_bytes >= 0", name="artifact_versions_size_non_negative"),
    sa.CheckConstraint("btrim(mime_type) <> ''", name="artifact_versions_mime_type_not_blank"),
    sa.CheckConstraint("jsonb_typeof(custom_metadata) = 'object'", name="artifact_versions_metadata_is_object"),
    sa.CheckConstraint("state IN ('pending', 'available', 'deleted')", name="artifact_versions_state_valid"),
    sa.Index("artifact_versions_tenant_path_state_idx", "tenant_id", "artifact_path", "state"),
)

knowledge_documents = sa.Table(
    "knowledge_documents",
    metadata,
    sa.Column("tenant_id", sa.VARCHAR(64), primary_key=True),
    sa.Column("document_id", sa.TEXT, primary_key=True),
    sa.Column("content", sa.TEXT, nullable=False),
    sa.Column("metadata", JSONB, nullable=False),
    sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.ForeignKeyConstraint(["tenant_id"], ["tenant_configs.tenant_id"],
                            ondelete="RESTRICT",
                            name="knowledge_documents_tenant_fk"),
    sa.CheckConstraint("tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'", name="knowledge_documents_tenant_id_format"),
    sa.CheckConstraint("btrim(document_id) <> ''", name="knowledge_documents_id_not_blank"),
    sa.CheckConstraint("btrim(content) <> ''", name="knowledge_documents_content_not_blank"),
    sa.CheckConstraint("jsonb_typeof(metadata) = 'object'", name="knowledge_documents_metadata_is_object"),
)

# R2A: a channel account is globally bound to one tenant, while immutable
# history preserves the exact binding used by prior deliveries.  Secret
# material never enters these tables: only an environment-backed reference is
# permitted.
channel_bindings = sa.Table(
    "channel_bindings",
    metadata,
    sa.Column("binding_id", sa.UUID, primary_key=True),
    sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
    sa.Column("app_id", sa.TEXT, nullable=False),
    sa.Column("channel", sa.TEXT, nullable=False),
    sa.Column("external_account_id", sa.TEXT, nullable=False),
    sa.Column("secret_ref", sa.TEXT, nullable=False),
    sa.Column("webhook_token_ref", sa.TEXT, nullable=True),
    sa.Column("webhook_aes_key_ref", sa.TEXT, nullable=True),
    sa.Column("enabled", sa.BOOLEAN, nullable=False),
    sa.Column("version", sa.BIGINT, nullable=False),
    sa.Column("created_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.Column("updated_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.ForeignKeyConstraint(["tenant_id"], ["tenant_configs.tenant_id"],
                            ondelete="RESTRICT",
                            name="channel_bindings_tenant_fk"),
    sa.UniqueConstraint("channel", "external_account_id", name="channel_bindings_channel_account_uk"),
    sa.UniqueConstraint("binding_id", "tenant_id", name="channel_bindings_binding_tenant_uk"),
    sa.CheckConstraint("tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'", name="channel_bindings_tenant_id_format"),
    sa.CheckConstraint("btrim(app_id) <> ''", name="channel_bindings_app_id_not_blank"),
    sa.CheckConstraint("channel IN ('wecom', 'feishu')", name="channel_bindings_channel_valid"),
    sa.CheckConstraint("external_account_id = btrim(external_account_id) AND external_account_id <> ''",
                       name="channel_bindings_account_normalized"),
    sa.CheckConstraint("secret_ref ~ '^env:TRPC_[A-Z0-9_]+$'", name="channel_bindings_secret_ref_valid"),
    sa.CheckConstraint("webhook_token_ref IS NULL OR webhook_token_ref ~ '^env:TRPC_[A-Z0-9_]+$'",
                       name="channel_bindings_webhook_token_ref_valid"),
    sa.CheckConstraint("webhook_aes_key_ref IS NULL OR webhook_aes_key_ref ~ '^env:TRPC_[A-Z0-9_]+$'",
                       name="channel_bindings_webhook_aes_key_ref_valid"),
    sa.CheckConstraint("version >= 1", name="channel_bindings_version_positive"),
)

channel_binding_versions = sa.Table(
    "channel_binding_versions",
    metadata,
    sa.Column("binding_id", sa.UUID, primary_key=True),
    sa.Column("version", sa.BIGINT, primary_key=True),
    sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
    sa.Column("app_id", sa.TEXT, nullable=False),
    sa.Column("channel", sa.TEXT, nullable=False),
    sa.Column("external_account_id", sa.TEXT, nullable=False),
    sa.Column("secret_ref", sa.TEXT, nullable=False),
    sa.Column("webhook_token_ref", sa.TEXT, nullable=True),
    sa.Column("webhook_aes_key_ref", sa.TEXT, nullable=True),
    sa.Column("enabled", sa.BOOLEAN, nullable=False),
    sa.Column("recorded_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
    sa.ForeignKeyConstraint(["tenant_id"], ["tenant_configs.tenant_id"],
                            ondelete="RESTRICT",
                            name="channel_binding_versions_tenant_fk"),
    sa.ForeignKeyConstraint(["binding_id", "tenant_id"], ["channel_bindings.binding_id", "channel_bindings.tenant_id"],
                            ondelete="RESTRICT",
                            name="channel_binding_versions_binding_tenant_fk"),
    sa.CheckConstraint("tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'", name="channel_binding_versions_tenant_id_format"),
    sa.CheckConstraint("btrim(app_id) <> ''", name="channel_binding_versions_app_id_not_blank"),
    sa.CheckConstraint("channel IN ('wecom', 'feishu')", name="channel_binding_versions_channel_valid"),
    sa.CheckConstraint("external_account_id = btrim(external_account_id) AND external_account_id <> ''",
                       name="channel_binding_versions_account_normalized"),
    sa.CheckConstraint("secret_ref ~ '^env:TRPC_[A-Z0-9_]+$'", name="channel_binding_versions_secret_ref_valid"),
    sa.CheckConstraint("webhook_token_ref IS NULL OR webhook_token_ref ~ '^env:TRPC_[A-Z0-9_]+$'",
                       name="channel_binding_versions_webhook_token_ref_valid"),
    sa.CheckConstraint("webhook_aes_key_ref IS NULL OR webhook_aes_key_ref ~ '^env:TRPC_[A-Z0-9_]+$'",
                       name="channel_binding_versions_webhook_aes_key_ref_valid"),
    sa.CheckConstraint("version >= 1", name="channel_binding_versions_version_positive"),
    sa.Index("channel_binding_versions_tenant_time_idx", "tenant_id", "recorded_at"),
)

__all__ = [
    "artifact_metadata",
    "artifact_versions",
    "channel_binding_versions",
    "channel_bindings",
    "execution_audit_events",
    "metadata",
    "message_audit_events",
    "message_receipts",
    "knowledge_documents",
    "tenant_config_versions",
    "tenant_config_rollouts",
    "tenant_configs",
    "tenant_usage_daily",
    "request_usage_records",
    "tool_approval_audit_events",
    "tool_approval_requests",
]
