from pathlib import Path
import subprocess
import sys
from typing import Any
from uuid import uuid4

import pytest
from pydantic import SecretStr, ValidationError

from trpc_service.config import LeasedWorkerConfig
from trpc_service.config.models import SecretRef, validate_secret_bearing_config
from trpc_service.config.secret_scope import (
    tenant_model_env_prefix,
    validate_platform_model_secret_ref,
    validate_tenant_channel_secret_ref,
    validate_tenant_model_secret_ref,
)
from trpc_service.config.settings import Settings
from trpc_service.tenant.context import TenantContext


def test_settings_support_a_clean_process_import() -> None:
    """Prevent package re-exports from reintroducing a config import cycle."""

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "from trpc_service.config import Settings; Settings(_env_file=None)",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr


def test_mcp_models_support_a_clean_process_import() -> None:
    """Keep Alembic model discovery independent from runtime adapter imports."""

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            "import trpc_service.mcp.models",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr


def test_agent_adapter_compatibility_exports_are_lazy_and_importable() -> None:
    """Preserve package exports without eagerly rebuilding the import cycle."""

    result = subprocess.run(
        [
            sys.executable,
            "-c",
            ("from trpc_service.agent.adapters import "
             "TRPCAgentRunner, TRPCToolBridge"),
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode == 0, result.stderr


def test_secret_ref_accepts_reference_uris_without_resolving_a_secret() -> None:
    reference = SecretRef(uri="vault://tenant-a/models/primary")

    assert reference.uri == "vault://tenant-a/models/primary"


@pytest.mark.parametrize(
    "value",
    ["plain-text-token", "https://example.com/secret", "vault://", "env://"],
)
def test_secret_ref_rejects_plaintext_and_incomplete_references(value: str) -> None:
    with pytest.raises(ValidationError):
        SecretRef(uri=value)


def test_tenant_context_is_immutable_and_explicit() -> None:
    context = TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-1",
        trace_id="trace-1",
    )

    with pytest.raises(ValidationError):
        context.config_version = 2


@pytest.mark.parametrize(
    "override",
    [
        {
            "lease_seconds": 2
        },
        {
            "poll_interval_seconds": 0
        },
        {
            "retry_base_seconds": -1
        },
        {
            "retry_max_seconds": 0
        },
        {
            "retry_jitter_ratio": 0.6
        },
        {
            "max_attempts": 0
        },
    ],
)
def test_leased_worker_config_rejects_unsafe_runtime_values(
    override: dict[str, int | float], ) -> None:
    values: dict[str, Any] = {
        "lease_seconds": 30,
        "poll_interval_seconds": 0.1,
        "retry_base_seconds": 1,
        "retry_max_seconds": 60,
        "retry_jitter_ratio": 0.2,
        "max_attempts": 5,
    }
    values.update(override)

    with pytest.raises(ValueError):
        LeasedWorkerConfig(**values)


def test_database_password_can_be_injected_from_a_secret_file(tmp_path: Path) -> None:
    secret_file = tmp_path / "database_password"
    secret_file.write_text("password with spaces\n", encoding="utf-8")
    settings = Settings(
        database_url="postgresql+asyncpg://trpc@postgres:5432/trpc_agent",
        database_password_file=secret_file,
    )

    assert settings.resolved_database_url.password == "password with spaces"


def test_settings_resolve_admin_token_and_reject_empty_secret_files(tmp_path: Path) -> None:
    database_password = tmp_path / "database-password"
    bootstrap_token = tmp_path / "admin-token"
    database_password.write_text("database-secret\n", encoding="utf-8")
    bootstrap_token.write_text("bootstrap-secret\n", encoding="utf-8")
    settings = Settings(
        _env_file=None,
        database_url="postgresql+asyncpg://trpc@localhost:5432/trpc_agent",
        database_password_file=database_password,
        admin_bootstrap_token_file=bootstrap_token,
    )

    assert settings.resolved_database_url.password == "database-secret"
    assert settings.resolved_admin_bootstrap_token == "bootstrap-secret"
    assert Settings(
        _env_file=None,
        admin_bootstrap_token=SecretStr("direct-secret"),
        admin_bootstrap_token_file=bootstrap_token,
    ).resolved_admin_bootstrap_token == "direct-secret"
    assert Settings(_env_file=None).resolved_admin_bootstrap_token == ""

    database_password.write_text("", encoding="utf-8")
    bootstrap_token.write_text("", encoding="utf-8")
    with pytest.raises(ValueError, match="database password file is empty"):
        _ = settings.resolved_database_url
    with pytest.raises(ValueError, match="admin bootstrap token file is empty"):
        _ = settings.resolved_admin_bootstrap_token


def test_delivery_channels_are_isolated_by_runtime_role() -> None:
    assert Settings(
        _env_file=None,
        runtime_role="channel",
        worker_concurrency=0,
    ).resolved_delivery_channel_types == ("wecom", "feishu")
    assert Settings(
        _env_file=None,
        runtime_role="api",
        worker_concurrency=0,
    ).resolved_delivery_channel_types == ()
    assert Settings(
        _env_file=None,
        runtime_role="worker",
        worker_concurrency=1,
    ).resolved_delivery_channel_types == ()
    assert Settings(
        _env_file=None,
        runtime_role="worker",
        delivery_channel_types="wecom,feishu,wecom",
    ).resolved_delivery_channel_types == ("wecom", "feishu")


def test_model_secret_refs_are_scoped_by_credential_owner() -> None:
    tenant_id = uuid4()
    prefix = tenant_model_env_prefix(tenant_id)

    assert validate_platform_model_secret_ref(
        "env://DASHSCOPE_API_KEY") == "env://DASHSCOPE_API_KEY"
    assert validate_platform_model_secret_ref(
        "file:///run/secrets/platform/qwen-max") == "file:///run/secrets/platform/qwen-max"
    assert validate_tenant_model_secret_ref(f"env://{prefix}PRIMARY_KEY",
                                            tenant_id) == f"env://{prefix}PRIMARY_KEY"
    assert validate_tenant_model_secret_ref(f"file:///run/secrets/tenants/{tenant_id}/primary",
                                            tenant_id).endswith("/primary")

    with pytest.raises(ValueError, match="platform credential scope"):
        validate_platform_model_secret_ref("env://TENANT_MODEL_KEY")
    with pytest.raises(ValueError, match="tenant's credential scope"):
        validate_tenant_model_secret_ref("env://DASHSCOPE_API_KEY", tenant_id)
    with pytest.raises(ValueError, match="tenant's credential scope"):
        validate_tenant_model_secret_ref(
            f"file:///run/secrets/tenants/{tenant_id}/../another-tenant/key", tenant_id)


def test_channel_secret_refs_are_scoped_to_tenant_namespace() -> None:
    tenant_id = uuid4()
    environment_prefix = (f"TRPC_TENANT_{str(tenant_id).replace('-', '_').upper()}_CHANNEL_")

    assert validate_tenant_channel_secret_ref(f"env://{environment_prefix}WECOM_TOKEN",
                                              tenant_id).endswith("WECOM_TOKEN")
    assert validate_tenant_channel_secret_ref(
        f"file:///run/secrets/tenants/{tenant_id}/channels/wecom", tenant_id).endswith("wecom")
    assert validate_tenant_channel_secret_ref(f"vault://tenants/{tenant_id}/channels/wecom",
                                              tenant_id).startswith("vault://")
    assert validate_tenant_channel_secret_ref(
        f"secret-manager://tenants/{tenant_id}/channels/telegram",
        tenant_id).startswith("secret-manager://")
    with pytest.raises(ValueError, match="channel SecretRef"):
        validate_tenant_channel_secret_ref("env://DASHSCOPE_API_KEY", tenant_id)
    with pytest.raises(ValueError, match="channel SecretRef"):
        validate_tenant_channel_secret_ref(f"vault://tenants/{tenant_id}/channels/%2e%2e/other/key",
                                           tenant_id)


def test_flexible_configuration_rejects_non_reference_secret_values() -> None:
    with pytest.raises(ValueError, match="must be a SecretRef URI"):
        validate_secret_bearing_config({"api_key": 123})
    with pytest.raises(ValueError, match="values must be SecretRef URIs"):
        validate_secret_bearing_config({"secret_ref_map": {"token": 123}})
    assert validate_secret_bearing_config({"nested": [{
        "token": "env://SAFE_TOKEN"
    }]}) == {
        "nested": [{
            "token": "env://SAFE_TOKEN"
        }]
    }
