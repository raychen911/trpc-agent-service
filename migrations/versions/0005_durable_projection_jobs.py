"""Add durable, fenced post-turn projection jobs.

Revision ID: 0005
Revises: 0004
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0005"
down_revision: str | None = "0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create the job ledger and backfill already-successful runs."""

    op.create_table(
        "projection_job",
        sa.Column("job_id", sa.String(length=32), nullable=False),
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("run_id", sa.String(length=32), nullable=False),
        sa.Column("session_id", sa.String(length=128), nullable=False),
        sa.Column("config_revision", sa.Integer(), nullable=False),
        sa.Column("through_seq", sa.BigInteger(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("fencing_token", sa.BigInteger(), nullable=False),
        sa.Column("claimed_by", sa.String(length=128), nullable=True),
        sa.Column("claim_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("result_hash", sa.String(length=64), nullable=True),
        sa.Column("last_error_type", sa.String(length=128), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("attempt_count >= 0", name="ck_projection_job_attempt_count"),
        sa.CheckConstraint("fencing_token >= 0", name="ck_projection_job_fencing_token"),
        sa.CheckConstraint(
            "status IN ('pending', 'processing', 'retry_wait', 'succeeded', 'dead_letter')",
            name="ck_projection_job_status",
        ),
        sa.CheckConstraint("through_seq >= 0", name="ck_projection_job_through_seq"),
        sa.ForeignKeyConstraint(
            ["tenant_id", "config_revision"],
            ["tenant_config_revision.tenant_id", "tenant_config_revision.revision"],
            name="fk_projection_job_config_revision",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "run_id"],
            ["agent_run.tenant_id", "agent_run.run_id"],
            name="fk_projection_job_run",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id", "session_id"],
            ["session.tenant_id", "session.session_id"],
            name="fk_projection_job_session",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("job_id"),
        sa.UniqueConstraint("tenant_id", "run_id"),
    )
    op.create_index(
        "ix_projection_job_claim",
        "projection_job",
        ["status", "next_attempt_at", "created_at"],
        unique=False,
    )
    op.create_index(
        op.f("ix_projection_job_run_id"),
        "projection_job",
        ["run_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_projection_job_session_id"),
        "projection_job",
        ["session_id"],
        unique=False,
    )
    op.create_index(
        op.f("ix_projection_job_status"),
        "projection_job",
        ["status"],
        unique=False,
    )
    op.create_index(
        op.f("ix_projection_job_tenant_id"),
        "projection_job",
        ["tenant_id"],
        unique=False,
    )

    # A run identifier is already a globally unique 32-character key, so it is a
    # deterministic job identifier for upgrade backfill and finalize retries.
    op.execute(
        """
        INSERT INTO projection_job (
            job_id, tenant_id, run_id, session_id, config_revision, through_seq,
            status, attempt_count, fencing_token, next_attempt_at, created_at
        )
        SELECT
            agent_run.run_id,
            agent_run.tenant_id,
            agent_run.run_id,
            agent_run.session_id,
            inbox_message.config_revision,
            agent_run.last_seq,
            'pending',
            0,
            0,
            COALESCE(agent_run.completed_at, agent_run.started_at),
            COALESCE(agent_run.completed_at, agent_run.started_at)
        FROM agent_run
        JOIN inbox_message
          ON inbox_message.tenant_id = agent_run.tenant_id
         AND inbox_message.inbox_id = agent_run.inbox_id
        WHERE agent_run.status = 'succeeded'
        """
    )

    if op.get_bind().dialect.name == "postgresql":
        op.execute('ALTER TABLE "projection_job" ENABLE ROW LEVEL SECURITY')
        op.execute('ALTER TABLE "projection_job" FORCE ROW LEVEL SECURITY')
        op.execute(
            """
            CREATE POLICY tenant_isolation ON projection_job
            USING (
                tenant_id = NULLIF(current_setting('app.tenant_id', true), '')
            )
            WITH CHECK (
                tenant_id = NULLIF(current_setting('app.tenant_id', true), '')
            )
            """
        )
        op.execute(
            """
            CREATE INDEX ix_projection_job_ready_postgresql
            ON projection_job (next_attempt_at, created_at, tenant_id, job_id)
            WHERE status IN ('pending', 'retry_wait', 'processing')
            """
        )


def downgrade() -> None:
    """Remove the projection job ledger and its PostgreSQL isolation policy."""

    if op.get_bind().dialect.name == "postgresql":
        op.execute("DROP INDEX IF EXISTS ix_projection_job_ready_postgresql")
        op.execute("DROP POLICY IF EXISTS tenant_isolation ON projection_job")
        op.execute('ALTER TABLE "projection_job" NO FORCE ROW LEVEL SECURITY')
        op.execute('ALTER TABLE "projection_job" DISABLE ROW LEVEL SECURITY')
    op.drop_table("projection_job")
