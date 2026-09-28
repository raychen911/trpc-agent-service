"""Complete tenant-visible form contracts for built-in IM adapters.

Revision ID: 20260908_0023
Revises: 20260908_0022
Create Date: 2026-09-08
"""

from collections.abc import Sequence

from alembic import op
import sqlalchemy as sa

revision: str = "20260908_0023"
down_revision: str | None = "20260908_0022"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_THINKING_MESSAGE = "正在思考，请稍候…"

WECOM_CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "bot_id": {
            "type": "string",
            "title": "机器人 Bot ID",
            "description": "企业微信智能机器人 API 模式的 Bot ID",
            "minLength": 1,
        },
        "thinking_message": {
            "type": "string",
            "title": "快速响应提示语（可选）",
            "description": "收到消息后立即发送的处理中提示；留空则不发送",
            "default": _THINKING_MESSAGE,
            "maxLength": 200,
        },
    },
    "required": ["bot_id"],
    "additionalProperties": False,
}
WECOM_SECRET_SCHEMA = {
    "type": "object",
    "properties": {
        "bot_secret": {
            "type": "string",
            "title": "机器人 Secret",
            "description": "仅加密保存，页面不会回显",
            "minLength": 1,
        },
    },
    "required": ["bot_secret"],
    "additionalProperties": False,
}
FEISHU_CONFIG_SCHEMA = {
    "type": "object",
    "properties": {
        "app_id": {
            "type": "string",
            "title": "App ID",
            "description": "飞书开放平台应用的 App ID",
            "minLength": 1,
        },
        "thinking_message": {
            "type": "string",
            "title": "快速响应提示语（可选）",
            "description": "收到消息后立即发送的卡片提示；留空则不发送",
            "default": _THINKING_MESSAGE,
            "maxLength": 200,
        },
    },
    "required": ["app_id"],
    "additionalProperties": False,
}
FEISHU_SECRET_SCHEMA = {
    "type": "object",
    "properties": {
        "app_secret": {
            "type": "string",
            "title": "App Secret",
            "description": "仅加密保存，页面不会回显",
            "minLength": 1,
        },
    },
    "required": ["app_secret"],
    "additionalProperties": False,
}

_OLD_WECOM_CONFIG_SCHEMA = {
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
}
_OLD_WECOM_SECRET_SCHEMA = {
    "type": "object",
    "properties": {
        "bot_secret": {
            "type": "string",
            "minLength": 1
        }
    },
    "required": ["bot_secret"],
    "additionalProperties": False,
}
_OLD_FEISHU_CONFIG_SCHEMA = {"type": "object", "required": ["app_id"]}
_OLD_FEISHU_SECRET_SCHEMA = {"required": ["app_secret"]}


def _update_contract(channel_type: str, config: dict[str, object], secrets: dict[str,
                                                                                 object]) -> None:
    """Write JSON values through typed binds so credentials never enter SQL."""

    statement = sa.text("UPDATE channel_adapter_type "
                        "SET config_schema = :config, secret_schema = :secrets, updated_at = now() "
                        "WHERE channel_type = :channel_type").bindparams(
                            sa.bindparam("config", type_=sa.JSON()),
                            sa.bindparam("secrets", type_=sa.JSON()),
                        )
    op.get_bind().execute(statement, {
        "channel_type": channel_type,
        "config": config,
        "secrets": secrets,
    })


def upgrade() -> None:
    """Expose every required Feishu and WeCom credential in tenant forms."""

    _update_contract("wecom", WECOM_CONFIG_SCHEMA, WECOM_SECRET_SCHEMA)
    _update_contract("feishu", FEISHU_CONFIG_SCHEMA, FEISHU_SECRET_SCHEMA)


def downgrade() -> None:
    """Restore the adapter contracts that preceded this UI compatibility fix."""

    _update_contract("wecom", _OLD_WECOM_CONFIG_SCHEMA, _OLD_WECOM_SECRET_SCHEMA)
    _update_contract("feishu", _OLD_FEISHU_CONFIG_SCHEMA, _OLD_FEISHU_SECRET_SCHEMA)
