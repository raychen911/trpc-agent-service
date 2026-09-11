"""add tenant backend_profile columns (Stage R1A)

Revision ID: 0008_add_tenant_backend_profile
Revises: 0007_add_tenant_usage
Create Date: 2026-09-07

Adds a non-null JSONB ``backend_profile`` column to HEAD and immutable
history, backfills existing rows with the explicit profile
``{"state_backend": "redis", "artifact_backend": "s3", "knowledge_backend":
"sql", "audit_backend": "sql"}`` — exactly the behavior every pre-R1A tenant
has today, and no version number changes — then drops the server default so
every future write must carry an explicit profile (a stored configuration can
never gain a backend by omission). The JSON-object checks are added while the
backfill default exists and are kept afterwards. Downgrade is fully
symmetric (drops both constraints and both columns).
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "0008_add_tenant_backend_profile"
down_revision: Union[str, None] = "0007_add_tenant_usage"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_DEFAULT_BACKEND_PROFILE_JSON = ('{"state_backend": "redis", "artifact_backend": "s3",'
                                 ' "knowledge_backend": "sql", "audit_backend": "sql"}')


def upgrade() -> None:
    # A constant server default backfills existing rows during the NOT NULL
    # add; the default is then dropped so every write must carry an explicit
    # profile (mirrors the 0004 governance-column pattern).
    op.add_column(
        "tenant_configs",
        sa.Column(
            "backend_profile",
            JSONB,
            nullable=False,
            server_default=sa.text(f"'{_DEFAULT_BACKEND_PROFILE_JSON}'::jsonb"),
        ),
    )
    op.add_column(
        "tenant_config_versions",
        sa.Column(
            "backend_profile",
            JSONB,
            nullable=False,
            server_default=sa.text(f"'{_DEFAULT_BACKEND_PROFILE_JSON}'::jsonb"),
        ),
    )
    op.execute("ALTER TABLE tenant_configs ALTER COLUMN backend_profile DROP DEFAULT")
    op.execute("ALTER TABLE tenant_config_versions ALTER COLUMN backend_profile DROP DEFAULT")

    op.create_check_constraint(
        "tenant_configs_backend_profile_is_object",
        "tenant_configs",
        "jsonb_typeof(backend_profile) = 'object'",
    )
    op.create_check_constraint(
        "tenant_config_versions_backend_profile_is_object",
        "tenant_config_versions",
        "jsonb_typeof(backend_profile) = 'object'",
    )


def downgrade() -> None:
    op.drop_constraint(
        "tenant_config_versions_backend_profile_is_object",
        "tenant_config_versions",
        type_="check",
    )
    op.drop_constraint("tenant_configs_backend_profile_is_object", "tenant_configs", type_="check")
    op.drop_column("tenant_config_versions", "backend_profile")
    op.drop_column("tenant_configs", "backend_profile")
