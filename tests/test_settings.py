from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi import HTTPException

from trpc_service.agent.factory import create_executor_from_env
from trpc_service.config import ServiceSettings, load_environment, resolve_secret
from trpc_service.tenant import AgentApp, TenantConfig
from trpc_service.web.admin import parse_config, public_config, validate_secret_references


def test_load_environment_reads_dotenv_file(tmp_path, monkeypatch) -> None:
    monkeypatch.delenv("TRPC_SERVICE_PORT", raising=False)
    env_file = tmp_path / ".env"
    env_file.write_text("TRPC_SERVICE_PORT=9123\n", encoding="utf-8")

    assert load_environment(env_file)
    assert ServiceSettings.from_env().port == 9123


def test_load_environment_preserves_exported_value(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("TRPC_SERVICE_PORT", "9124")
    env_file = tmp_path / ".env"
    env_file.write_text("TRPC_SERVICE_PORT=9123\n", encoding="utf-8")

    assert load_environment(env_file)
    assert ServiceSettings.from_env().port == 9124


def test_file_secret_and_migration_dsn(tmp_path, monkeypatch) -> None:
    secret_file = tmp_path / "secret"
    secret_file.write_text("value\n", encoding="utf-8")
    monkeypatch.setenv("TRPC_SERVICE_MIGRATION_DATABASE_URL", "postgresql://migration")

    assert resolve_secret(f"file://{secret_file}") == "value"
    assert ServiceSettings.from_env().migration_database_url == "postgresql://migration"


def test_app_model_secret_refs_are_redacted_and_preserved_on_update() -> None:
    payload = {
        "tenant_id": "acme",
        "name": "Acme",
        "apps": {
            "default": {
                "app_id": "default",
                "name": "agent",
                "model_name": "tenant-model",
                "model_api_key_ref": "env://TENANT_MODEL_KEY",
                "fallback_model_name": "fallback-model",
                "fallback_api_key_ref": "file:///run/secrets/fallback",
                "timeout_seconds": 30,
            }
        },
        "channels": {},
    }
    config = parse_config(payload)
    visible = public_config(config)

    assert visible["apps"]["default"]["model_api_key_ref"] == "[SECRET_REF]"
    assert visible["apps"]["default"]["fallback_api_key_ref"] == "[SECRET_REF]"
    updated = parse_config(visible, existing=config)
    assert updated.apps["default"].model_api_key_ref == "env://TENANT_MODEL_KEY"
    assert updated.apps["default"].fallback_api_key_ref == "file:///run/secrets/fallback"


def test_production_rejects_unsafe_app_model_configuration() -> None:
    config = TenantConfig(
        "acme",
        "Acme",
        {
            "default": AgentApp(
                "default",
                "agent",
                model_api_key_ref="literal://secret",
                model_base_url="http://model.internal/v1",
            )
        },
        {},
    )
    with pytest.raises(HTTPException):
        validate_secret_references(config, production=True)


def test_tenant_model_credentials_override_global_defaults(monkeypatch) -> None:
    monkeypatch.setenv("TENANT_MODEL_KEY", "tenant-key")
    monkeypatch.setenv("TRPC_AGENT_API_KEY", "global-key")
    monkeypatch.setenv("TRPC_AGENT_MODEL_NAME", "global-model")
    app = AgentApp(
        "default",
        "agent",
        model_name="tenant-model",
        model_api_key_ref="env://TENANT_MODEL_KEY",
    )
    config = TenantConfig("acme", "Acme", {"default": app}, {})
    sentinel = object()

    with patch("trpc_service.agent.factory._create_trpc_executor", return_value=sentinel) as create:
        assert create_executor_from_env(config) is sentinel

    assert create.call_args.args[2:4] == ("tenant-key", "tenant-model")
