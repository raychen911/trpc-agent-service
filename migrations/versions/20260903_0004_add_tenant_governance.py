"""add tenant governance policy columns (Stage 6A1)

Revision ID: 0004_add_tenant_governance
Revises: 0003_add_message_receipts_audit
Create Date: 2026-09-03

Adds a non-null JSONB ``governance`` column to HEAD and immutable history,
backfills existing rows with the fixed default policy (all four product
channels, no user allowlist, no tool overrides — preserving pre-6A1
behavior), then drops the server default so every future write must carry an
explicit policy. The column check constraint is added while the backfill
default exists and is kept afterwards. Downgrade drops both columns.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0004_add_tenant_governance"
down_revision: Union[str, None] = "0003_add_message_receipts_audit"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DEFAULT_POLICY_JSON = ('{"allowed_channels": ["web", "web_console", "wecom", "feishu"],'
                        ' "allowed_user_ids": [], "tool_decisions": {}}')


def upgrade() -> None:
    # A constant server default backfills existing rows during the NOT NULL
    # add; the default is then dropped so every write must carry an explicit
    # policy (no silent drift if application code forgets the column).
    op.add_column(
        "tenant_configs",
        sa.Column(
            "governance",
            JSONB,
            nullable=False,
            server_default=sa.text(f"'{_DEFAULT_POLICY_JSON}'::jsonb"),
        ),
    )
    op.add_column(
        "tenant_config_versions",
        sa.Column(
            "governance",
            JSONB,
            nullable=False,
            server_default=sa.text(f"'{_DEFAULT_POLICY_JSON}'::jsonb"),
        ),
    )
    op.execute("ALTER TABLE tenant_configs ALTER COLUMN governance DROP DEFAULT")
    op.execute("ALTER TABLE tenant_config_versions ALTER COLUMN governance DROP DEFAULT")

    op.create_check_constraint(
        "tenant_configs_governance_is_object",
        "tenant_configs",
        "jsonb_typeof(governance) = 'object'",
    )
    op.create_check_constraint(
        "tenant_config_versions_governance_is_object",
        "tenant_config_versions",
        "jsonb_typeof(governance) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "tenant_config_versions_governance_is_object",
        "tenant_config_versions",
        type_="check",
    )
    op.drop_constraint("tenant_configs_governance_is_object", "tenant_configs", type_="check")
    op.drop_column("tenant_config_versions", "governance")
    op.drop_column("tenant_configs", "governance")
