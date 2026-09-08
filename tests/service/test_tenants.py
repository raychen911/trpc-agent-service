# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Unit tests for the enterprise tenant models and config manager."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from trpc_service import to_agent_name
from trpc_service.tenant import AppConfig
from trpc_service.tenant import AppInfo
from trpc_service.tenant import BudgetConfig
from trpc_service.tenant import DingTalkChannelConfig
from trpc_service.tenant import FeishuChannelConfig
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import ModelPricingConfig
from trpc_service.tenant import QQChannelConfig
from trpc_service.tenant import StorageBackendConfig
from trpc_service.tenant import Tenant
from trpc_service.tenant import TenantConfigManager
from trpc_service.tenant import TenantStatus
from trpc_service.tenant import ToolPermissions
from trpc_service.tenant import WeComChannelConfig
from trpc_service.tenant import WechatCustomerServiceChannelConfig
from trpc_service.tenant import load_tenants


def _make_tenant(tenant_id: str, name: str = "t") -> Tenant:
    return Tenant(
        tenant_id=tenant_id,
        name=name,
        model=ModelEndpoint(model_name="gpt-4o"),
    )


def test_tenant_minimal_construction():
    tenant = _make_tenant("tenant_a")
    assert tenant.tenant_id == "tenant_a"
    assert tenant.status == TenantStatus.ACTIVE
    assert tenant.model.model_name == "gpt-4o"
    assert tenant.tool_permissions.tool_whitelist == []


def test_model_pricing_and_budget_validation():
    endpoint = ModelEndpoint(
        model_name="gpt-4o",
        pricing={"gpt-4o": ModelPricingConfig(input_per_mtok=2.5, output_per_mtok=10)},
    )
    assert endpoint.pricing["gpt-4o"].output_per_mtok == 10
    with pytest.raises(ValidationError):
        ModelPricingConfig(input_per_mtok=-1)
    with pytest.raises(ValidationError):
        BudgetConfig(daily_token_budget=0)
    with pytest.raises(ValidationError):
        BudgetConfig(daily_cost_limit=0)


def test_audit_backend_is_mysql_only():
    assert StorageBackendConfig().audit_backend == "mysql"
    with pytest.raises(ValidationError, match="literal_error"):
        StorageBackendConfig(audit_backend="redis")


@pytest.mark.parametrize("backend", ["redis", "mysql"])
def test_summary_backend_follows_session_backend(backend):
    storage = StorageBackendConfig(
        session_backend=backend,
        summary_backend="mysql" if backend == "redis" else "redis",
    )
    assert storage.summary_backend == backend


def test_tenant_id_rejects_key_delimiters():
    with pytest.raises(ValidationError):
        Tenant(tenant_id="a:b", name="x", model=ModelEndpoint(model_name="m"))
    with pytest.raises(ValidationError):
        Tenant(tenant_id="a/b", name="x", model=ModelEndpoint(model_name="m"))


def test_to_agent_name():
    assert to_agent_name("tenant_a") == "tenant_a"
    assert to_agent_name("tenant-a") == "tenant_a"
    assert to_agent_name("123abc") == "t_123abc"
    assert to_agent_name("") == "tenant"


def test_channel_config_secret_not_leaked_in_repr():
    tenant = _make_tenant("tenant_a")
    tenant.channel_configs["wecom"] = WeComChannelConfig(
        token="supersecret",
        aes_key="aes-secret",
        corp_id="corp",
        agent_id="1",
    )
    repr_text = repr(tenant)
    assert "supersecret" not in repr_text
    assert tenant.channel_configs["wecom"].token.get_secret_value() == "supersecret"


@pytest.mark.parametrize(
    ("channel", "payload", "expected_type"),
    [
        (
            "wecom",
            {
                "channel_type": "wecom",
                "token": "token",
                "aes_key": "aes",
                "corp_id": "corp",
                "agent_id": "1",
            },
            WeComChannelConfig,
        ),
        (
            "wechat_kf",
            {
                "channel_type": "wechat_kf",
                "token": "token",
                "aes_key": "aes",
                "corp_id": "corp",
                "open_kfid": "wk-1",
            },
            WechatCustomerServiceChannelConfig,
        ),
        (
            "dingtalk",
            {
                "channel_type": "dingtalk",
                "app_id": "app",
                "robot_code": "robot",
                "secret": "secret",
            },
            DingTalkChannelConfig,
        ),
        (
            "feishu",
            {
                "channel_type": "feishu",
                "app_id": "app",
                "verification_token": "token",
            },
            FeishuChannelConfig,
        ),
        (
            "qq",
            {
                "channel_type": "qq",
                "app_id": "app",
                "secret": "secret",
            },
            QQChannelConfig,
        ),
    ],
)
def test_tenant_discriminates_platform_channel_configs(channel, payload, expected_type):
    tenant = Tenant(
        tenant_id="tenant_a",
        name="A",
        model=ModelEndpoint(model_name="model"),
        channel_configs={channel: payload},
    )

    assert isinstance(tenant.channel_configs[channel], expected_type)


def test_channel_config_rejects_cross_platform_and_missing_fields():
    base = {
        "tenant_id": "tenant_a",
        "name": "A",
        "model": {
            "model_name": "model"
        },
    }
    with pytest.raises(ValidationError, match="extra_forbidden"):
        Tenant.model_validate({
            **base,
            "channel_configs": {
                "qq": {
                    "channel_type": "qq",
                    "app_id": "app",
                    "secret": "secret",
                    "robot_code": "not-a-qq-field",
                }
            },
        })
    with pytest.raises(ValidationError, match="Field required"):
        Tenant.model_validate({
            **base,
            "channel_configs": {
                "wecom": {
                    "channel_type": "wecom",
                    "token": "token"
                }
            },
        })
    with pytest.raises(ValidationError, match="union_tag_invalid"):
        Tenant.model_validate({
            **base,
            "channel_configs": {
                "slack": {
                    "channel_type": "slack"
                }
            },
        })


def test_feishu_channel_requires_callback_credential():
    with pytest.raises(ValidationError, match="verification_token or encrypt_key"):
        FeishuChannelConfig(app_id="app")


def test_manager_register_get_list_delete():
    manager = TenantConfigManager()
    manager.register(_make_tenant("tenant_a"))
    manager.register(_make_tenant("tenant_b"))

    assert manager.get("tenant_a").tenant_id == "tenant_a"
    assert {t.tenant_id for t in manager.list()} == {"tenant_a", "tenant_b"}

    manager.delete("tenant_a")
    assert manager.get("tenant_a") is None
    assert {t.tenant_id for t in manager.list()} == {"tenant_b"}


def test_manager_duplicate_register_raises():
    manager = TenantConfigManager()
    manager.register(_make_tenant("tenant_a"))
    with pytest.raises(ValueError):
        manager.register(_make_tenant("tenant_a"))


def test_manager_versioning_and_rollback():
    manager = TenantConfigManager()
    manager.register(_make_tenant("tenant_a", name="v1"))

    updated = _make_tenant("tenant_a", name="v2")
    updated.tool_permissions = ToolPermissions(tool_whitelist=["query_order"])
    manager.update(updated, by="admin", reason="allow query_order")

    assert manager.get("tenant_a").name == "v2"
    history = manager.history("tenant_a")
    assert [v.version for v in history] == [1, 2]

    restored = manager.rollback("tenant_a", to_version=1)
    assert restored.name == "v1"
    assert restored.tool_permissions.tool_whitelist == []

    # rollback itself is recorded as a new version
    history = manager.history("tenant_a")
    assert history[-1].version == 3
    assert history[-1].rolled_back is True
    assert history[-1].rolled_back_to == 1


def test_manager_rollback_restores_secret_without_exposing_history():
    manager = TenantConfigManager()
    tenant = _make_tenant("tenant_a")
    tenant.channel_configs["feishu"] = FeishuChannelConfig(
        app_id="app",
        verification_token="verification-token",
        secret="original-secret",
    )
    manager.register(tenant)
    updated = manager.get("tenant_a")
    updated.channel_configs["feishu"].secret = "new-secret"
    manager.update(updated)

    history_json = manager.history("tenant_a")[0].model_dump_json()
    assert "original-secret" not in history_json
    restored = manager.rollback("tenant_a", 1)
    assert restored.channel_configs["feishu"].secret.get_secret_value() == "original-secret"


def test_manager_rollback_missing_version_raises():
    manager = TenantConfigManager()
    manager.register(_make_tenant("tenant_a"))
    with pytest.raises(ValueError):
        manager.rollback("tenant_a", to_version=99)


def test_manager_change_listener_notified():
    manager = TenantConfigManager()
    events: list[tuple[str, str | None]] = []
    manager.subscribe(lambda tid, tenant: events.append((tid, tenant.name if tenant else None)))

    manager.register(_make_tenant("tenant_a", name="a"))
    manager.delete("tenant_a")

    assert events == [("tenant_a", "a"), ("tenant_a", None)]


def test_load_tenants_from_yaml_with_env_expansion(tmp_path, monkeypatch):
    monkeypatch.setenv("WECOM_TOKEN", "env-secret")
    monkeypatch.setenv("QDRANT_API_KEY", "qdrant-secret")
    config_file = tmp_path / "tenants.yaml"
    config_file.write_text(
        """
tenants:
  - tenant_id: tenant_a
    name: Company A
    model:
      model_name: gpt-4o
    channel_configs:
      wecom:
        channel_type: wecom
        token: ${WECOM_TOKEN}
        aes_key: env-aes
        corp_id: env-corp
        agent_id: "1"
    storage_config:
      vector:
        backend: qdrant
        url: https://qdrant.example
        api_key: ${QDRANT_API_KEY}
        dimensions: 1536
      object:
        backend: s3
        bucket: tenant-artifacts
        access_key: object-access
        secret_key: object-secret
""",
        encoding="utf-8",
    )

    tenants = load_tenants(config_file)
    assert len(tenants) == 1
    tenant = tenants[0]
    assert tenant.tenant_id == "tenant_a"
    assert tenant.channel_configs["wecom"].token.get_secret_value() == "env-secret"
    assert tenant.storage_config.vector.dimensions == 1536
    assert tenant.storage_config.vector.api_key.get_secret_value() == "qdrant-secret"
    assert tenant.storage_config.object.backend == "s3"
    assert "env-secret" not in repr(tenant)
    assert "qdrant-secret" not in repr(tenant)
    assert "object-secret" not in repr(tenant)


def test_load_tenants_missing_key_raises(tmp_path):
    config_file = tmp_path / "tenants.yaml"
    config_file.write_text("name: no tenants here\n", encoding="utf-8")
    with pytest.raises(ValueError):
        load_tenants(config_file)


def test_app_config_and_channel_binding_validation():
    with pytest.raises(ValueError, match="duplicate app_id"):
        AppConfig(app_list=[AppInfo(app_id="same"), AppInfo(app_id="same")])
    with pytest.raises(ValueError, match="default_app_id"):
        AppConfig(app_list=[AppInfo(app_id="one")], default_app_id="missing")
    with pytest.raises(ValueError, match="unknown agent_app_id"):
        Tenant(
            tenant_id="tenant_a",
            name="A",
            model=ModelEndpoint(model_name="m"),
            app_config=AppConfig(app_list=[AppInfo(app_id="one")]),
            channel_configs={
                "support":
                WeComChannelConfig(
                    token="token",
                    aes_key="aes",
                    corp_id="corp",
                    agent_id="1",
                    agent_app_id="missing",
                )
            },
        )
