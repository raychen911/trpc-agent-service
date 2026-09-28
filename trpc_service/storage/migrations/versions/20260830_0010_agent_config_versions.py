"""Add immutable Agent configuration snapshots and rollout pointers.

Revision ID: 20260830_0010
Revises: 20260829_0009
Create Date: 2026-08-30
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "20260830_0010"
down_revision: str | None = "20260829_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Backfill version 1 before enabling stable and canary pointers."""

    op.add_column(
        "agent_app",
        sa.Column(
            "stable_config_version",
            sa.BigInteger(),
            nullable=False,
            server_default="1",
        ),
    )
    op.add_column(
        "agent_app",
        sa.Column("canary_config_version", sa.BigInteger(), nullable=True),
    )
    op.add_column(
        "agent_app",
        sa.Column("canary_percent", sa.Integer(), nullable=False, server_default="0"),
    )
    op.create_check_constraint(
        "agent_app_canary_percent",
        "agent_app",
        "canary_percent >= 0 AND canary_percent <= 99",
    )
    op.create_check_constraint(
        "agent_app_canary_pointer",
        "agent_app",
        "(canary_config_version IS NULL AND canary_percent = 0) OR "
        "(canary_config_version IS NOT NULL AND canary_percent > 0)",
    )
    op.create_table(
        "agent_config_version",
        sa.Column("tenant_id", sa.Uuid(), nullable=False),
        sa.Column("agent_app_id", sa.Uuid(), nullable=False),
        sa.Column("version", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column(
            "snapshot",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column("created_by", sa.String(length=255), nullable=False),
        sa.Column("reason", sa.String(length=500), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint("version > 0", name="agent_config_version_positive"),
        sa.CheckConstraint(
            "status IN ('draft', 'released', 'retired')",
            name="agent_config_version_status",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "agent_app_id"],
            ["agent_app.tenant_id", "agent_app.agent_app_id"],
            name="fk_agent_config_version_tenant_agent",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint(
            "tenant_id",
            "agent_app_id",
            "version",
            name=op.f("pk_agent_config_version"),
        ),
    )
    # Existing mutable rows become the released version-1 snapshot. PostgreSQL
    # performs the conversion in the database, so no Secret values enter logs.
    op.execute("""
        INSERT INTO agent_config_version (
            tenant_id, agent_app_id, version, status, snapshot, created_by, reason
        )
        SELECT
            tenant_id,
            agent_app_id,
            1,
            'released',
            jsonb_build_object(
                'model_profile_id', CASE
                    WHEN model_profile_id IS NULL THEN NULL
                    ELSE model_profile_id::text
                END,
                'application_config', application_config,
                'model_settings', model_config,
                'tool_permissions', tool_permissions,
                'knowledge_config', knowledge_config,
                'backend_config', backend_config
            ),
            'migration:20260830_0010',
            'backfilled initial Agent configuration'
        FROM agent_app
        """)


def downgrade() -> None:
    """Remove rollout history only after operators select the retained snapshot."""

    op.drop_table("agent_config_version")
    op.drop_constraint("agent_app_canary_pointer", "agent_app", type_="check")
    op.drop_constraint("agent_app_canary_percent", "agent_app", type_="check")
    op.drop_column("agent_app", "canary_percent")
    op.drop_column("agent_app", "canary_config_version")
    op.drop_column("agent_app", "stable_config_version")
