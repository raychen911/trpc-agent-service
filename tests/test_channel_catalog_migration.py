"""Regression tests for tenant-visible built-in IM form contracts."""

from importlib import import_module


def test_builtin_channel_form_migration_exposes_required_credentials() -> None:
    """Every required field must have a property for dynamic form rendering."""

    migration = import_module(
        "trpc_service.storage.migrations.versions.20260908_0023_channel_form_contracts")

    assert migration.FEISHU_CONFIG_SCHEMA["properties"]["app_id"]["title"] == "App ID"
    assert migration.FEISHU_SECRET_SCHEMA["properties"]["app_secret"]["title"] == "App Secret"
    assert migration.WECOM_CONFIG_SCHEMA["properties"]["bot_id"]["title"] == "机器人 Bot ID"
    assert migration.WECOM_SECRET_SCHEMA["properties"]["bot_secret"]["title"] == "机器人 Secret"
    assert migration.WECOM_CONFIG_SCHEMA["properties"]["thinking_message"]["default"] == "正在思考，请稍候…"
