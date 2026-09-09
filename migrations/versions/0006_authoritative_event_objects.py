"""Persist encrypted SDK events in tenant-isolated authoritative SQL.

Revision ID: 0006
Revises: 0005
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0006"
down_revision: str | None = "0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create an append-only ciphertext table required for session replay."""

    op.create_table(
        "event_object",
        sa.Column("tenant_id", sa.String(length=64), nullable=False),
        sa.Column("object_key", sa.String(length=160), nullable=False),
        sa.Column("ciphertext", sa.Text(), nullable=False),
        sa.Column("ciphertext_sha256", sa.String(length=64), nullable=False),
        sa.Column("size_bytes", sa.BigInteger(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("size_bytes > 0", name="ck_event_object_size"),
        sa.CheckConstraint(
            "length(ciphertext_sha256) = 64",
            name="ck_event_object_digest_length",
        ),
        sa.ForeignKeyConstraint(
            ["tenant_id"],
            ["tenant.tenant_id"],
            name="fk_event_object_tenant",
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("tenant_id", "object_key"),
    )
    op.create_index(
        "ix_event_object_tenant_created",
        "event_object",
        ["tenant_id", "created_at"],
        unique=False,
    )

    if op.get_bind().dialect.name == "postgresql":
        op.execute('ALTER TABLE "event_object" ENABLE ROW LEVEL SECURITY')
        op.execute('ALTER TABLE "event_object" FORCE ROW LEVEL SECURITY')
        op.execute(
            """
            CREATE POLICY tenant_isolation ON event_object
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
            CREATE FUNCTION reject_event_object_mutation() RETURNS trigger
            LANGUAGE plpgsql AS $$
            BEGIN
                RAISE EXCEPTION 'event_object is append-only';
            END;
            $$
            """
        )
        op.execute(
            """
            CREATE TRIGGER event_object_append_only
            BEFORE UPDATE OR DELETE ON event_object
            FOR EACH ROW EXECUTE FUNCTION reject_event_object_mutation()
            """
        )
        op.execute("REVOKE UPDATE, DELETE ON event_object FROM PUBLIC")


def downgrade() -> None:
    """Remove authoritative event objects and PostgreSQL hardening objects."""

    if op.get_bind().dialect.name == "postgresql":
        op.execute("DROP TRIGGER IF EXISTS event_object_append_only ON event_object")
        op.execute("DROP FUNCTION IF EXISTS reject_event_object_mutation()")
        op.execute("DROP POLICY IF EXISTS tenant_isolation ON event_object")
        op.execute('ALTER TABLE "event_object" NO FORCE ROW LEVEL SECURITY')
        op.execute('ALTER TABLE "event_object" DISABLE ROW LEVEL SECURITY')
    op.drop_table("event_object")
