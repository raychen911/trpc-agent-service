"""Add desired-state Worker Pool scaling and graceful drain status.

Revision ID: 20260908_0022
Revises: 20260907_0021
Create Date: 2026-09-08
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260908_0022"
down_revision: str | None = "20260907_0021"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Persist desired capacity and expose the non-terminal drain state."""

    with op.batch_alter_table("runtime_node") as batch:
        batch.drop_constraint("runtime_node_role", type_="check")
        batch.create_check_constraint(
            "runtime_node_role",
            "role IN ('api', 'worker', 'api_worker', 'channel', 'supervisor')",
        )
    with op.batch_alter_table("runtime_node") as batch:
        batch.drop_constraint("runtime_node_status", type_="check")
        batch.create_check_constraint(
            "runtime_node_status",
            "status IN ('active', 'draining', 'stopped')",
        )
    op.create_table(
        "worker_pool_control",
        sa.Column("pool_name", sa.String(length=100), nullable=False),
        sa.Column("desired_nodes", sa.Integer(), nullable=False),
        sa.Column("generation", sa.BigInteger(), nullable=False, server_default="1"),
        sa.Column("updated_by", sa.String(length=255), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "desired_nodes BETWEEN 1 AND 64",
            name="worker_pool_desired_range",
        ),
        sa.CheckConstraint("generation >= 1", name="worker_pool_generation_positive"),
        sa.PrimaryKeyConstraint("pool_name", name=op.f("pk_worker_pool_control")),
    )


def downgrade() -> None:
    """Remove scaling state after converting draining records to stopped."""

    op.drop_table("worker_pool_control")
    op.execute(sa.text("DELETE FROM runtime_node WHERE role = 'supervisor'"))
    op.execute(sa.text("UPDATE runtime_node SET status = 'stopped' WHERE status = 'draining'"))
    with op.batch_alter_table("runtime_node") as batch:
        batch.drop_constraint("runtime_node_status", type_="check")
        batch.create_check_constraint(
            "runtime_node_status",
            "status IN ('active', 'stopped')",
        )
    with op.batch_alter_table("runtime_node") as batch:
        batch.drop_constraint("runtime_node_role", type_="check")
        batch.create_check_constraint(
            "runtime_node_role",
            "role IN ('api', 'worker', 'api_worker', 'channel')",
        )
