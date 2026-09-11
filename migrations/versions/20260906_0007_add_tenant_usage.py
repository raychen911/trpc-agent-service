"""add tenant_usage_daily + governance limits + budget error code (Stage 6C Task 2)

Revision ID: 0007_add_tenant_usage
Revises: 0006_add_execution_audit
Create Date: 2026-09-06

Creates ``tenant_usage_daily``: the per-UTC-day, per-tenant, per-model-profile
usage fact source (requests, tokens, integer micro-currency cost).  Token and
cost columns are NULLABLE and NULL means UNKNOWN — the CHECKs forbid negative
values, and the application upsert relies on SQL NULL arithmetic so a day
that ever saw unknown usage stays unknown instead of undercounting.

Backfills every pre-6C governance JSON (head and history) with an explicit
``"limits": null`` so old tenants keep the pre-6C unlimited behavior at an
unchanged version number; rows that already carry the key are untouched, and
original ``version`` numbers never change.  Mirrors the 0006 content_policy
migration semantics reviewed in Stage 6B2 Checkpoint A.

Replaces ``execution_audit_events_error_code_valid`` with the extended fixed
vocabulary (adds ``usage_budget_exceeded``), matching the pydantic model and
``storage/schema.py``.  Downgrade is fully symmetric.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0007_add_tenant_usage"
down_revision: Union[str, None] = "0006_add_execution_audit"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_ERROR_CODE_CHECK_NAME = "execution_audit_events_error_code_valid"
_ERROR_CODE_SQL = (
    "(error_code IS NULL OR error_code IN ("
    "'tenant_config_mismatch', 'tenant_agent_configuration', 'model_configuration', 'model_runtime',"
    " 'worker_unavailable', 'worker_timeout', 'invalid_worker_response', 'session_busy',"
    " 'tenant_repository_unavailable', 'message_in_progress', 'idempotency_conflict',"
    " 'approval_not_available', 'approval_in_progress', 'approval_conflict', 'approval_config_stale',"
    " 'approval_repository_unavailable', 'approval_execution_failed', 'content_input_blocked',"
    " 'usage_budget_exceeded'))")


def upgrade() -> None:
    # 1. Explicit unlimited-preserving backfill over legacy governance rows.
    for table in ("tenant_configs", "tenant_config_versions"):
        op.execute(
            f"UPDATE {table} SET governance = jsonb_set(governance, '{{limits}}', 'null'::jsonb, true) "
            f"WHERE NOT governance ? 'limits'")

    # 2. Usage fact source.
    op.create_table(
        "tenant_usage_daily",
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
        sa.CheckConstraint("requests >= 0", name="tenant_usage_daily_requests_non_negative"),
        sa.CheckConstraint("(input_tokens IS NULL OR input_tokens >= 0)",
                           name="tenant_usage_daily_input_tokens_valid"),
        sa.CheckConstraint("(output_tokens IS NULL OR output_tokens >= 0)",
                           name="tenant_usage_daily_output_tokens_valid"),
        sa.CheckConstraint("(cost_microunits IS NULL OR cost_microunits >= 0)",
                           name="tenant_usage_daily_cost_microunits_valid"),
        sa.CheckConstraint(
            "tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'",
            name="tenant_usage_daily_tenant_id_format",
        ),
        sa.CheckConstraint("btrim(model_profile) <> ''", name="tenant_usage_daily_profile_not_blank"),
    )

    # 3. Extended fixed audit error vocabulary (budget terminal).
    op.drop_constraint(_ERROR_CODE_CHECK_NAME, "execution_audit_events", type_="check")
    op.create_check_constraint(_ERROR_CODE_CHECK_NAME, "execution_audit_events", _ERROR_CODE_SQL)


def downgrade() -> None:
    op.drop_constraint(_ERROR_CODE_CHECK_NAME, "execution_audit_events", type_="check")
    op.create_check_constraint(
        _ERROR_CODE_CHECK_NAME,
        "execution_audit_events",
        "(error_code IS NULL OR error_code IN ("
        "'tenant_config_mismatch', 'tenant_agent_configuration', 'model_configuration', 'model_runtime',"
        " 'worker_unavailable', 'worker_timeout', 'invalid_worker_response', 'session_busy',"
        " 'tenant_repository_unavailable', 'message_in_progress', 'idempotency_conflict',"
        " 'approval_not_available', 'approval_in_progress', 'approval_conflict', 'approval_config_stale',"
        " 'approval_repository_unavailable', 'approval_execution_failed', 'content_input_blocked'))",
    )
    op.drop_table("tenant_usage_daily")
    for table in ("tenant_configs", "tenant_config_versions"):
        op.execute(f"UPDATE {table} SET governance = governance - 'limits'")
