# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.

import os

import pytest

from trpc_service.config import EnvironmentSecretResolver
from trpc_service.config import SecretResolutionError
from trpc_service.config import load_environment_file
from trpc_service.config import load_tenant_configs
from trpc_service.config import TenantConfig


def test_load_tenant_configs_accepts_list_root(tmp_path):
    path = tmp_path / "tenants.yaml"
    path.write_text("- tenant_id: demo\n  apps: {}\n", encoding="utf-8")
    configs = load_tenant_configs(path)
    assert [item.tenant_id for item in configs] == ["demo"]


def test_load_tenant_configs_rejects_invalid_root(tmp_path):
    path = tmp_path / "tenants.yaml"
    path.write_text("tenants: wrong\n", encoding="utf-8")
    with pytest.raises(ValueError, match="tenants"):
        load_tenant_configs(path)


def test_environment_placeholder_supports_default(tmp_path, monkeypatch):
    monkeypatch.delenv("TEST_MODEL_NAME", raising=False)
    path = tmp_path / "tenants.yaml"
    path.write_text(
        """tenants:
  - tenant_id: demo
    apps:
      assistant:
        app_id: assistant
        model:
          model_name: ${TEST_MODEL_NAME:-fallback-model}
""",
        encoding="utf-8",
    )
    configs = load_tenant_configs(path)
    assert configs[0].apps["assistant"].model.model_name == "fallback-model"


def test_load_environment_file_for_model_switching(tmp_path, monkeypatch):
    monkeypatch.delenv("TEST_DOTENV_MODEL", raising=False)
    path = tmp_path / ".env"
    path.write_text("TEST_DOTENV_MODEL=switched-model\n", encoding="utf-8")
    assert load_environment_file(path)
    assert os.environ["TEST_DOTENV_MODEL"] == "switched-model"


@pytest.mark.asyncio
async def test_environment_secret_resolver(monkeypatch):
    monkeypatch.setenv("TEST_SERVICE_SECRET", "value")
    resolver = EnvironmentSecretResolver()
    assert await resolver.resolve("env://TEST_SERVICE_SECRET") == "value"
    with pytest.raises(SecretResolutionError):
        await resolver.resolve("plain-text")


def test_duplicate_binding_ids_are_rejected():
    binding = {"binding_id": "same", "channel": "telegram", "app_id": "assistant"}
    with pytest.raises(ValueError, match="binding_id"):
        TenantConfig.model_validate({
            "tenant_id": "demo",
            "apps": {
                "assistant": {
                    "app_id": "assistant"
                }
            },
            "channels": [binding, binding],
        })
