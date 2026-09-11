"""add tenant_config_versions history table

Revision ID: 0002_add_tenant_config_versions
Revises: 0001_create_tenant_configs
Create Date: 2026-09-01

Creating the table and backfilling existing tenant_configs rows happen in the
same migration (single transaction on PostgreSQL), so an interrupted upgrade
can never leave the history table missing rows for existing heads.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0002_add_tenant_config_versions"
down_revision: Union[str, None] = "0001_create_tenant_configs"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tenant_config_versions",
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
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenant_configs.tenant_id"],
            ondelete="RESTRICT",
            name="tenant_config_versions_tenant_fk",
        ),
        sa.CheckConstraint("version >= 1", name="tenant_config_versions_version_positive"),
        sa.CheckConstraint(
            "jsonb_typeof(allowed_tools) = 'array'",
            name="tenant_config_versions_tools_is_array",
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
    op.execute(
        "INSERT INTO tenant_config_versions"
        " (tenant_id, version, enabled, app_id, instruction, model_profile, allowed_tools)"
        " SELECT tenant_id, version, enabled, app_id, instruction, model_profile, allowed_tools"
        " FROM tenant_configs"
        " ON CONFLICT (tenant_id, version) DO NOTHING"
    )


def downgrade() -> None:
    op.drop_table("tenant_config_versions")
