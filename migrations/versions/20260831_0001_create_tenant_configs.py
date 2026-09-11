"""create tenant_configs table

Revision ID: 0001_create_tenant_configs
Revises: None
Create Date: 2026-08-31
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0001_create_tenant_configs"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tenant_configs",
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


def downgrade() -> None:
    op.drop_table("tenant_configs")
