"""remove unsafe automatic usage-reservation RLS

Revision ID: d4e5f607a1b2
Revises: 8b6d1e4f2a90
Create Date: 2026-08-30
"""

from collections.abc import Sequence

from alembic import op

revision: str = "d4e5f607a1b2"
down_revision: str | Sequence[str] | None = "8b6d1e4f2a90"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    if op.get_bind().dialect.name == "postgresql":
        op.execute("DROP POLICY IF EXISTS tenant_scope ON usage_reservations")
        op.execute("ALTER TABLE usage_reservations NO FORCE ROW LEVEL SECURITY")
        op.execute("ALTER TABLE usage_reservations DISABLE ROW LEVEL SECURITY")


def downgrade() -> None:
    # Intentionally do not recreate the unsafe implicit policy. Schema downgrade
    # must not reintroduce a production outage when no runtime tenant GUC exists.
    return None
