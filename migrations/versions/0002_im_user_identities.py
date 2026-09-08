"""Add tenant-scoped IM user identity mappings.

Revision ID: 0002
Revises: 0001
"""

from alembic import op

from trpc_service.storage.models import ImUserIdentity

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    ImUserIdentity.__table__.create(bind=op.get_bind(), checkfirst=True)


def downgrade() -> None:
    ImUserIdentity.__table__.drop(bind=op.get_bind(), checkfirst=True)
