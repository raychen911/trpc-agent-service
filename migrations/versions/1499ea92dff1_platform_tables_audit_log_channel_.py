"""platform tables: audit_log, channel_binding, idempotency

Revision ID: 1499ea92dff1
Revises:
Create Date: 2026-09-06 13:50:06.375745

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '1499ea92dff1'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """创建平台侧三张表。

    注意：autogenerate 曾把框架的 sessions/events/mem_events/user_states/
    app_states 检测为 "removed"（它们在库中但不在平台 metadata 里）——
    那不是本迁移的管辖范围，相关 drop_table 已人工剔除。
    autogenerate 产物必须逐行审查后再执行。
    """
    op.create_table(
        'audit_log',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('tenant_id', sa.String(length=36), nullable=False),
        sa.Column('channel', sa.String(length=16), nullable=False),
        sa.Column('user_id', sa.String(length=64), nullable=False),
        sa.Column('session_id', sa.String(length=64), nullable=False),
        sa.Column('agent_name', sa.String(length=64), nullable=False),
        sa.Column('tool_name', sa.String(length=128), nullable=False),
        sa.Column('decision', sa.String(length=16), nullable=False),
        sa.Column('latency_ms', sa.Integer(), nullable=False),
        sa.Column('error_type', sa.String(length=64), nullable=False),
        sa.Column('cost', sa.Numeric(precision=10, scale=4), nullable=False),
        sa.Column('trace_id', sa.String(length=32), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        mysql_charset='utf8mb4',
        mysql_engine='InnoDB',
    )
    op.create_table(
        'channel_binding',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('tenant_id', sa.String(length=36), nullable=False),
        sa.Column('channel_type', sa.String(length=16), nullable=False),
        sa.Column('external_user_id', sa.String(length=128), nullable=False),
        sa.Column('chat_id', sa.String(length=128), nullable=False),
        sa.Column('session_id', sa.String(length=64), nullable=False),
        sa.Column('status', sa.String(length=16), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('tenant_id', 'channel_type', 'external_user_id', 'chat_id', name='uk_binding'),
        mysql_charset='utf8mb4',
        mysql_engine='InnoDB',
    )
    op.create_table(
        'idempotency',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('idempotency_key', sa.String(length=128), nullable=False),
        sa.Column('dedupe_status', sa.String(length=16), nullable=False),
        sa.Column('created_at', sa.DateTime(), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('idempotency_key', name='uk_idem'),
        mysql_charset='utf8mb4',
        mysql_engine='InnoDB',
    )


def downgrade() -> None:
    """只回滚平台自己的三张表（框架表不属于本迁移管辖）。"""
    op.drop_table('idempotency')
    op.drop_table('channel_binding')
    op.drop_table('audit_log')
