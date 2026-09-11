"""add versioned ChannelBinding and tenant audit policy (R2A)."""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0010_channel_binding_audit"
down_revision: Union[str, None] = "0009_add_artifact_knowledge"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DEFAULT_AUDIT_POLICY_JSON = '{"retention_days":365,"delivery_events":"all"}'
_ERROR_CODE_CHECK_NAME = "execution_audit_events_error_code_valid"
_ERROR_CODE_SQL = ("(error_code IS NULL OR error_code IN ("
                   "'tenant_config_mismatch', 'tenant_agent_configuration', 'model_configuration', 'model_runtime',"
                   " 'worker_unavailable', 'worker_timeout', 'invalid_worker_response', 'session_busy',"
                   " 'tenant_repository_unavailable', 'message_in_progress', 'idempotency_conflict',"
                   " 'approval_not_available', 'approval_in_progress', 'approval_conflict', 'approval_config_stale',"
                   " 'approval_repository_unavailable', 'approval_execution_failed', 'content_input_blocked',"
                   " 'usage_budget_exceeded', 'channel_delivery_failed'))")
_PREVIOUS_ERROR_CODE_SQL = (
    "(error_code IS NULL OR error_code IN ("
    "'tenant_config_mismatch', 'tenant_agent_configuration', 'model_configuration', 'model_runtime',"
    " 'worker_unavailable', 'worker_timeout', 'invalid_worker_response', 'session_busy',"
    " 'tenant_repository_unavailable', 'message_in_progress', 'idempotency_conflict',"
    " 'approval_not_available', 'approval_in_progress', 'approval_conflict', 'approval_config_stale',"
    " 'approval_repository_unavailable', 'approval_execution_failed', 'content_input_blocked',"
    " 'usage_budget_exceeded'))")
_HISTORY_FUNCTION = "trpc_channel_binding_versions_reject_mutation"
_HISTORY_TRIGGER = "channel_binding_versions_append_only"


def upgrade() -> None:
    for table in ("tenant_configs", "tenant_config_versions"):
        op.add_column(table, sa.Column("audit_policy", JSONB, nullable=True))
        op.execute(
            sa.text(f"UPDATE {table} SET audit_policy = CAST(:policy AS jsonb) WHERE audit_policy IS NULL").bindparams(
                policy=_DEFAULT_AUDIT_POLICY_JSON))
        op.alter_column(table, "audit_policy", nullable=False)
        op.create_check_constraint(f"{table}_audit_policy_is_object", table, "jsonb_typeof(audit_policy) = 'object'")

    op.create_table(
        "channel_bindings",
        sa.Column("binding_id", sa.UUID, primary_key=True),
        sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
        sa.Column("app_id", sa.TEXT, nullable=False),
        sa.Column("channel", sa.TEXT, nullable=False),
        sa.Column("external_account_id", sa.TEXT, nullable=False),
        sa.Column("secret_ref", sa.TEXT, nullable=False),
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
        sa.CheckConstraint("external_account_id = lower(btrim(external_account_id)) AND external_account_id <> ''",
                           name="channel_bindings_account_normalized"),
        sa.CheckConstraint("secret_ref ~ '^env:TRPC_[A-Z0-9_]+$'", name="channel_bindings_secret_ref_valid"),
        sa.CheckConstraint("version >= 1", name="channel_bindings_version_positive"),
    )
    op.create_table(
        "channel_binding_versions",
        sa.Column("binding_id", sa.UUID, nullable=False),
        sa.Column("version", sa.BIGINT, nullable=False),
        sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
        sa.Column("app_id", sa.TEXT, nullable=False),
        sa.Column("channel", sa.TEXT, nullable=False),
        sa.Column("external_account_id", sa.TEXT, nullable=False),
        sa.Column("secret_ref", sa.TEXT, nullable=False),
        sa.Column("enabled", sa.BOOLEAN, nullable=False),
        sa.Column("recorded_at", sa.TIMESTAMP(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenant_configs.tenant_id"],
                                ondelete="RESTRICT",
                                name="channel_binding_versions_tenant_fk"),
        sa.ForeignKeyConstraint(["binding_id", "tenant_id"],
                                ["channel_bindings.binding_id", "channel_bindings.tenant_id"],
                                ondelete="RESTRICT",
                                name="channel_binding_versions_binding_tenant_fk"),
        sa.PrimaryKeyConstraint("binding_id", "version"),
        sa.CheckConstraint("tenant_id ~ '^[a-z][a-z0-9_-]{0,63}$'", name="channel_binding_versions_tenant_id_format"),
        sa.CheckConstraint("btrim(app_id) <> ''", name="channel_binding_versions_app_id_not_blank"),
        sa.CheckConstraint("channel IN ('wecom', 'feishu')", name="channel_binding_versions_channel_valid"),
        sa.CheckConstraint("external_account_id = lower(btrim(external_account_id)) AND external_account_id <> ''",
                           name="channel_binding_versions_account_normalized"),
        sa.CheckConstraint("secret_ref ~ '^env:TRPC_[A-Z0-9_]+$'", name="channel_binding_versions_secret_ref_valid"),
        sa.CheckConstraint("version >= 1", name="channel_binding_versions_version_positive"),
    )
    op.create_index("channel_binding_versions_tenant_time_idx", "channel_binding_versions",
                    ["tenant_id", "recorded_at"])
    op.execute(f"CREATE FUNCTION {_HISTORY_FUNCTION}() RETURNS trigger AS $$ "
               "BEGIN RAISE EXCEPTION 'channel binding history is immutable'; END; $$ LANGUAGE plpgsql")
    op.execute(f"CREATE TRIGGER {_HISTORY_TRIGGER} BEFORE UPDATE OR DELETE ON channel_binding_versions "
               f"FOR EACH ROW EXECUTE FUNCTION {_HISTORY_FUNCTION}()")

    op.drop_constraint(_ERROR_CODE_CHECK_NAME, "execution_audit_events", type_="check")
    op.create_check_constraint(_ERROR_CODE_CHECK_NAME, "execution_audit_events", _ERROR_CODE_SQL)


def downgrade() -> None:
    op.drop_constraint(_ERROR_CODE_CHECK_NAME, "execution_audit_events", type_="check")
    op.create_check_constraint(_ERROR_CODE_CHECK_NAME, "execution_audit_events", _PREVIOUS_ERROR_CODE_SQL)
    op.execute(f"DROP TRIGGER {_HISTORY_TRIGGER} ON channel_binding_versions")
    op.execute(f"DROP FUNCTION {_HISTORY_FUNCTION}()")
    op.drop_index("channel_binding_versions_tenant_time_idx", table_name="channel_binding_versions")
    op.drop_table("channel_binding_versions")
    op.drop_table("channel_bindings")
    for table in ("tenant_config_versions", "tenant_configs"):
        op.drop_constraint(f"{table}_audit_policy_is_object", table, type_="check")
        op.drop_column(table, "audit_policy")
