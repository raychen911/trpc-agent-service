"""add deterministic tenant configuration rollout records."""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "0012_tenant_config_rollouts"
down_revision: Union[str, None] = "0011_request_usage_audit"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "tenant_config_rollouts",
        sa.Column("rollout_id", sa.UUID, primary_key=True),
        sa.Column("tenant_id", sa.VARCHAR(64), nullable=False),
        sa.Column("active_version", sa.BIGINT, nullable=False),
        sa.Column("candidate_version", sa.BIGINT, nullable=False),
        sa.Column("candidate_percent", sa.SMALLINT, nullable=False),
        sa.Column("status", sa.TEXT, nullable=False),
        sa.Column("started_at", sa.TIMESTAMP(timezone=True), nullable=False),
        sa.Column("finished_at", sa.TIMESTAMP(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["tenant_id"], ["tenant_configs.tenant_id"],
                                ondelete="RESTRICT", name="tenant_config_rollouts_tenant_fk"),
        sa.CheckConstraint("active_version >= 1", name="tenant_config_rollouts_active_version_positive"),
        sa.CheckConstraint("candidate_version >= 1", name="tenant_config_rollouts_candidate_version_positive"),
        sa.CheckConstraint("active_version <> candidate_version", name="tenant_config_rollouts_versions_differ"),
        sa.CheckConstraint("candidate_percent BETWEEN 1 AND 99", name="tenant_config_rollouts_candidate_percent_range"),
        sa.CheckConstraint("status IN ('running', 'promoted', 'aborted')", name="tenant_config_rollouts_status_valid"),
    )
    op.create_index("tenant_config_rollouts_one_running", "tenant_config_rollouts", ["tenant_id"],
                    unique=True, postgresql_where=sa.text("status = 'running'"))
    op.create_index("tenant_config_rollouts_tenant_started", "tenant_config_rollouts", ["tenant_id", "started_at"])


def downgrade() -> None:
    op.drop_index("tenant_config_rollouts_tenant_started", table_name="tenant_config_rollouts")
    op.drop_index("tenant_config_rollouts_one_running", table_name="tenant_config_rollouts")
    op.drop_table("tenant_config_rollouts")
