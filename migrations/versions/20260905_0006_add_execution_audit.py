"""add append-only execution audit events (Stage 6B2 Task 1)

Revision ID: 0006_add_execution_audit
Revises: 0005_add_tool_approvals
Create Date: 2026-09-05

Creates ``execution_audit_events``: the immutable governance trail for
content decisions, agent results, tool decisions and channel delivery
results.  Every vocabulary (event_type, outcome, category, error scope,
trace-id format, tenant format) is a database CHECK, and the event/outcome
pairing CHECK mirrors the pydantic model in trpc_service/audit/models.py.
The table carries no body/reply/tool-args column at all — the schema cannot
store what the design forbids.  A BEFORE UPDATE OR DELETE trigger raises,
making the table append-only at the database level: not even a superuser
statement can rewrite history.  Downgrade drops the trigger, function,
index and table symmetrically.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0006_add_execution_audit"
down_revision: Union[str, None] = "0005_add_tool_approvals"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_APPEND_ONLY_FUNCTION = "trpc_execution_audit_events_reject_mutation"
_APPEND_ONLY_TRIGGER = "execution_audit_events_append_only"


def upgrade() -> None:
    disabled_policy = '{"enabled": false, "input_action": "block", "output_action": "block"}'
    for table in ("tenant_configs", "tenant_config_versions"):
        op.execute(
            f"UPDATE {table} SET governance = jsonb_set(governance, '{{content_policy}}', "
            f"'{disabled_policy}'::jsonb, true) WHERE NOT governance ? 'content_policy'")

    op.create_unique_constraint(
        "message_receipts_execution_identity",
        "message_receipts",
        ["receipt_id", "tenant_id", "request_id", "config_version"],
    )
    op.create_table(
        "execution_audit_events",
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
            " 'approval_repository_unavailable', 'approval_execution_failed', 'content_input_blocked'))",
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
    )
    op.create_index(
        "execution_audit_events_receipt_time_idx",
        "execution_audit_events",
        ["tenant_id", "receipt_id", "occurred_at"],
    )
    op.create_index(
        "execution_audit_events_request_time_idx",
        "execution_audit_events",
        ["tenant_id", "request_id", "occurred_at"],
    )
    op.execute(f"""
        CREATE FUNCTION {_APPEND_ONLY_FUNCTION}() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'execution_audit_events is append-only';
        END;
        $$ LANGUAGE plpgsql;
    """)
    op.execute(f"""
        CREATE TRIGGER {_APPEND_ONLY_TRIGGER}
        BEFORE UPDATE OR DELETE ON execution_audit_events
        FOR EACH ROW EXECUTE FUNCTION {_APPEND_ONLY_FUNCTION}();
    """)


def downgrade() -> None:
    op.execute(f"DROP TRIGGER IF EXISTS {_APPEND_ONLY_TRIGGER} ON execution_audit_events")
    op.execute(f"DROP FUNCTION IF EXISTS {_APPEND_ONLY_FUNCTION}()")
    op.drop_index("execution_audit_events_receipt_time_idx", table_name="execution_audit_events")
    op.drop_index("execution_audit_events_request_time_idx", table_name="execution_audit_events")
    op.drop_table("execution_audit_events")
    op.drop_constraint("message_receipts_execution_identity", "message_receipts", type_="unique")
    for table in ("tenant_configs", "tenant_config_versions"):
        op.execute(f"UPDATE {table} SET governance = governance - 'content_policy'")
