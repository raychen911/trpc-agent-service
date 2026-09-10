"""tenant and agent_app tables

Revision ID: b3d7f2a91c4e
Revises: 1499ea92dff1
Create Date: 2026-09-08 10:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = 'b3d7f2a91c4e'
down_revision: Union[str, Sequence[str], None] = '1499ea92dff1'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """租户持久层：tenant（配置快照）+ agent_app（应用投影）。

    与 audit_log 等平台表同库；框架自身表（sessions/events/...）
    不在本迁移管辖范围内。
    """
    op.create_table(
        'tenant',
        sa.Column('tenant_id', sa.String(length=36), nullable=False),
        sa.Column('name', sa.String(length=128), nullable=False),
        sa.Column('status', sa.String(length=16), nullable=False),
        sa.Column('config', sa.JSON(), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.Column('updated_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('tenant_id'),
        mysql_charset='utf8mb4',
        mysql_engine='InnoDB',
    )
    op.create_table(
        'agent_app',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('app_name', sa.String(length=64), nullable=False),
        sa.Column('tenant_id', sa.String(length=36), nullable=False),
        sa.Column('instruction', sa.Text(), nullable=False),
        sa.Column('model_name', sa.String(length=64), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('app_name', name='uk_agent_app_name'),
        mysql_charset='utf8mb4',
        mysql_engine='InnoDB',
    )
    op.create_index('ix_agent_app_tenant_id', 'agent_app', ['tenant_id'])


def downgrade() -> None:
    """只回滚本迁移创建的两张表。"""
    op.drop_index('ix_agent_app_tenant_id', table_name='agent_app')
    op.drop_table('agent_app')
    op.drop_table('tenant')
