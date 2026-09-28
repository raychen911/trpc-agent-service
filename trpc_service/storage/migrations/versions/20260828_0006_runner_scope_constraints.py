"""Ensure Runner requests retain tenant-aware ownership constraints.

Revision ID: 20260828_0006
Revises: 20260828_0005
Create Date: 2026-08-28
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260828_0006"
down_revision: str | None = "20260828_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Add constraints when upgrading a database that already ran early P0."""

    # The guards make this migration a no-op on fresh databases where revision
    # 0005 already creates the corrected constraints.
    op.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'uq_inbox_message_tenant_id'
            ) THEN
                ALTER TABLE inbox_message
                ADD CONSTRAINT uq_inbox_message_tenant_id
                UNIQUE (tenant_id, inbox_id);
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_runner_request_tenant_inbox'
            ) THEN
                ALTER TABLE runner_request
                ADD CONSTRAINT fk_runner_request_tenant_inbox
                FOREIGN KEY (tenant_id, inbox_id)
                REFERENCES inbox_message (tenant_id, inbox_id)
                ON DELETE RESTRICT;
            END IF;
            IF NOT EXISTS (
                SELECT 1 FROM pg_constraint
                WHERE conname = 'fk_runner_request_tenant_agent'
            ) THEN
                ALTER TABLE runner_request
                ADD CONSTRAINT fk_runner_request_tenant_agent
                FOREIGN KEY (tenant_id, agent_app_id)
                REFERENCES agent_app (tenant_id, agent_app_id)
                ON DELETE RESTRICT NOT VALID;
            END IF;
        END
        $$
    """)


def downgrade() -> None:
    """Keep constraints because revision 0005 defines the same target schema."""

    # This compatibility revision repairs databases upgraded before 0005 was
    # finalized. Removing the constraints would make the 0005 schema incorrect.
    return None
