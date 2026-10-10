"""Add Feishu as a supported channel binding.

Revision ID: 0005_add_feishu_channel
Revises: 0004_wechat_binding_identifiers
"""

from __future__ import annotations

from alembic import op

revision = "0005_add_feishu_channel"
down_revision = "0004_wechat_binding_identifiers"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        ALTER TABLE channel_bindings
            DROP CONSTRAINT IF EXISTS channel_bindings_channel_check;
        ALTER TABLE channel_bindings
            ADD CONSTRAINT channel_bindings_channel_check
            CHECK (channel IN ('wecom_ai_bot','wechat_official','feishu'));
        """
    )


def downgrade() -> None:
    op.execute(
        """
        ALTER TABLE channel_bindings
            DROP CONSTRAINT IF EXISTS channel_bindings_channel_check;
        ALTER TABLE channel_bindings
            ADD CONSTRAINT channel_bindings_channel_check
            CHECK (channel IN ('wecom_ai_bot','wechat_official'));
        """
    )
