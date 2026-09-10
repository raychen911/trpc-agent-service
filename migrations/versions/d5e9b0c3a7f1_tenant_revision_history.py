"""tenant revision column and tenant_revision history table

Revision ID: d5e9b0c3a7f1
Revises: b3d7f2a91c4e
Create Date: 2026-09-08 15:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'd5e9b0c3a7f1'
down_revision: Union[str, Sequence[str], None] = 'b3d7f2a91c4e'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """租户配置版本化：tenant 表加 revision 列 + 历史版本表。"""
    op.add_column(
        'tenant',
        sa.Column('revision', sa.Integer(), nullable=False, server_default='1'),
    )
    op.create_table(
        'tenant_revision',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('tenant_id', sa.String(length=36), nullable=False),
        sa.Column('revision', sa.Integer(), nullable=False),
        sa.Column('config', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('tenant_id', 'revision', name='uk_tenant_revision'),
        mysql_charset='utf8mb4',
        mysql_engine='InnoDB',
    )
    op.create_index('ix_tenant_revision_tenant_id', 'tenant_revision', ['tenant_id'])


def downgrade() -> None:
    """只回滚本迁移的变更。"""
    op.drop_index('ix_tenant_revision_tenant_id', table_name='tenant_revision')
    op.drop_table('tenant_revision')
    op.drop_column('tenant', 'revision')
