"""Install WeCom adapter metadata and the Channel runtime role.

Revision ID: 20260831_0011
Revises: 20260830_0010
Create Date: 2026-08-31
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260831_0011"
down_revision: str | None = "20260830_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Expose WeCom to binding CRUD and permit its connector node role."""

    op.drop_constraint("runtime_node_role", "runtime_node", type_="check")
    op.create_check_constraint(
        "runtime_node_role",
        "runtime_node",
        "role IN ('api', 'worker', 'api_worker', 'channel')",
    )
    op.execute(
        sa.text("INSERT INTO channel_adapter_type "
                "(channel_type, display_name, adapter_version, config_schema, secret_schema, "
                "capabilities, status) VALUES "
                "('wecom', '企业微信智能机器人', '1.0', :config, :secrets, "
                ":capabilities, 'active')").bindparams(
                    sa.bindparam(
                        "config",
                        value={
                            "type": "object",
                            "properties": {
                                "bot_id": {
                                    "type": "string",
                                    "minLength": 1
                                },
                                "thinking_message": {
                                    "type": "string",
                                    "maxLength": 200
                                },
                            },
                            "required": ["bot_id"],
                            "additionalProperties": False,
                        },
                        type_=sa.JSON(),
                    ),
                    sa.bindparam(
                        "secrets",
                        value={
                            "type": "object",
                            "properties": {
                                "bot_secret": {
                                    "type": "string",
                                    "minLength": 1
                                }
                            },
                            "required": ["bot_secret"],
                            "additionalProperties": False,
                        },
                        type_=sa.JSON(),
                    ),
                    sa.bindparam(
                        "capabilities",
                        value={
                            "text": True,
                            "private_chat": True,
                            "group_mention": True,
                            "transport": "websocket",
                        },
                        type_=sa.JSON(),
                    ),
                ))


def downgrade() -> None:
    """Remove the connector metadata after stopping Channel processes."""

    op.execute(sa.text("DELETE FROM channel_adapter_type WHERE channel_type = 'wecom'"))
    op.execute(sa.text("DELETE FROM runtime_node WHERE role = 'channel'"))
    op.drop_constraint("runtime_node_role", "runtime_node", type_="check")
    op.create_check_constraint(
        "runtime_node_role",
        "runtime_node",
        "role IN ('api', 'worker', 'api_worker')",
    )
