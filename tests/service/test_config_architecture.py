"""Tests for the unified settings, SecretRef, pricing, and preflight boundaries."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import SecretStr

from trpc_service.config import DefaultModelPricing
from trpc_service.config import ProductionTenantPreflight
from trpc_service.config import SecretResolver
from trpc_service.config import ServiceSettings
from trpc_service.config import is_secret_ref
from trpc_service.config import resolve_model_secrets
from trpc_service.log import SecretMasker
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import ModelPricingConfig
from trpc_service.tenant import ObjectBackendConfig
from trpc_service.tenant import QQChannelConfig
from trpc_service.tenant import Tenant
from trpc_service.tenant import VectorBackendConfig


def test_settings_load_only_canonical_typed_environment():
    settings = ServiceSettings.from_env({
        "REDIS_URL": "redis://ignored",
        "TRPC_SERVICE_ENVIRONMENT": "test",
        "TRPC_SERVICE_ROLE": "worker",
        "TRPC_SERVICE_HOST": "127.0.0.1",
        "TRPC_SERVICE_PORT": "9090",
        "TRPC_SERVICE_REDIS_URL": "redis://canonical",
        "TRPC_SERVICE_MYSQL_URL": "mysql://canonical",
        "TRPC_SERVICE_QUEUE_ENABLED": "off",
        "TRPC_SERVICE_DURABLE_DELIVERY_ENABLED": "yes",
        "TRPC_SERVICE_DEEPSEEK_INPUT_PRICE_PER_MTOK": "0.3",
        "TRPC_SERVICE_DEEPSEEK_OUTPUT_PRICE_PER_MTOK": "0.9",
        "TRPC_SERVICE_OUTBOX_POLL_INTERVAL_SECONDS": "2.5",
        "TRPC_SERVICE_OUTBOX_MAX_ATTEMPTS": "5",
    })

    assert settings.environment == "test"
    assert settings.role == "worker"
    assert settings.host == "127.0.0.1"
    assert settings.port == 9090
    assert ServiceSettings.reveal(settings.redis_url) == "redis://canonical"
    assert settings.queue_enabled is False
    assert settings.durable_delivery_enabled is True
    assert settings.deepseek_input_price_per_mtok == 0.3
    assert settings.outbox_poll_interval_seconds == 2.5
    assert settings.outbox_max_attempts == 5
    assert ServiceSettings.reveal(None) is None


def test_settings_reject_invalid_flags_and_missing_role_dependencies():
    with pytest.raises(ValueError, match="QUEUE_ENABLED"):
        ServiceSettings.from_env({"TRPC_SERVICE_QUEUE_ENABLED": "sometimes"})
    with pytest.raises(ValueError, match="REDIS_URL"):
        ServiceSettings(role="worker")
    with pytest.raises(ValueError, match="MYSQL_URL"):
        ServiceSettings(role="outbox")
    with pytest.raises(ValueError, match="TEST_API_KEY"):
        ServiceSettings(test_api_enabled=True)
    with pytest.raises(ValueError, match="explicit runtime role"):
        ServiceSettings(environment="production")
    with pytest.raises(ValueError, match="ADMIN_API_KEY"):
        ServiceSettings(environment="production", role="gateway", redis_url="redis://queue", mysql_url="mysql://db")
    with pytest.raises(ValueError, match="MYSQL_URL"):
        ServiceSettings(role="worker", redis_url="redis://queue", durable_delivery_enabled=True)
    with pytest.raises(ValueError, match="durable delivery"):
        ServiceSettings(
            environment="production",
            role="worker",
            redis_url="redis://queue",
            mysql_url="mysql://db",
        )
    production_gateway = ServiceSettings(
        environment="production",
        role="gateway",
        redis_url="redis://queue",
        mysql_url="mysql://db",
        admin_api_key="secret",
    )
    assert production_gateway.role == "gateway"


def test_main_examples_use_the_canonical_configuration_contract():
    example_root = Path(__file__).resolve().parents[2] / "examples/multi_tenant_saas"
    python_sources = "\n".join(path.read_text(encoding="utf-8") for path in example_root.glob("*.py"))
    tenant_config = (example_root / "tenants.yaml").read_text(encoding="utf-8")

    assert "TRPC_AGENT_API_KEY" not in python_sources
    assert 'os.environ.get("TRPC_SERVICE_MODEL_API_KEY")' in python_sources
    assert "${REDIS_URL}" not in tenant_config
    assert "${MYSQL_URL}" not in tenant_config
    assert "env://TRPC_SERVICE_REDIS_URL" in tenant_config


def test_secret_resolver_env_custom_literal_and_exact_redaction():
    resolver = SecretResolver(environ={"MODEL_KEY": "unique-secret-value"})
    assert resolver.resolve("env://MODEL_KEY") == "unique-secret-value"
    assert SecretMasker.mask_value("got unique-secret-value") == "got ***"
    assert resolver.resolve("redis://host/0") == "redis://host/0"
    resolver.register("vault", lambda target, tenant: f"{tenant}:{target}")
    assert resolver.resolve("vault://models/key", tenant_id="tenant-a") == "tenant-a:models/key"
    with pytest.raises(ValueError, match="scheme is invalid"):
        resolver.register("BAD SCHEME", lambda target, tenant: target)


def test_secret_resolver_rejects_missing_invalid_empty_and_unconfigured_refs():
    resolver = SecretResolver(environ={}, backends={"empty": lambda target, tenant: ""})
    with pytest.raises(ValueError, match="not set"):
        resolver.resolve("env://MISSING")
    with pytest.raises(ValueError, match="environment variable name"):
        resolver.resolve("env://NOT/AN/ENV")
    with pytest.raises(ValueError, match="unsupported secret"):
        resolver.resolve("vault://secret/key")
    with pytest.raises(ValueError, match="empty value"):
        resolver.resolve("empty://value")
    assert is_secret_ref("env://KEY") is True
    assert is_secret_ref("https://example.com") is False


def test_file_secret_resolution_is_tenant_bounded(tmp_path: Path):
    tenant_root = tmp_path / "tenant-a"
    tenant_root.mkdir()
    secret = tenant_root / "model-key"
    secret.write_text("file-secret\n", encoding="utf-8")
    outside = tmp_path / "outside"
    outside.write_text("outside", encoding="utf-8")
    resolver = SecretResolver(file_root=tmp_path)

    assert resolver.resolve("file://model-key", tenant_id="tenant-a") == "file-secret"
    with pytest.raises(ValueError, match="stay below"):
        resolver.resolve("file://../outside", tenant_id="tenant-a")
    with pytest.raises(ValueError):
        resolver.resolve("file://missing", tenant_id="tenant-a")
    with pytest.raises(ValueError, match="tenant id"):
        resolver.resolve("file://model-key", tenant_id="../tenant-a")


def test_resolve_model_secrets_returns_copy_without_mutating_config():
    tenant = Tenant(
        tenant_id="tenant-a",
        name="A",
        model=ModelEndpoint(model_name="m"),
        channel_configs={
            "qq": QQChannelConfig(app_id="app", secret="env://QQ_SECRET"),
        },
    )
    original = tenant.channel_configs["qq"]
    resolved = resolve_model_secrets(
        original,
        tenant_id=tenant.tenant_id,
        resolver=SecretResolver(environ={"QQ_SECRET": "resolved-value"}),
    )

    assert resolved.secret.get_secret_value() == "resolved-value"
    assert original.secret.get_secret_value() == "env://QQ_SECRET"


def test_pricing_policy_prefers_tenant_values_and_defaults_deepseek_only():
    policy = DefaultModelPricing(deepseek_input_per_mtok=1, deepseek_output_per_mtok=2)
    deepseek = Tenant(
        tenant_id="d",
        name="D",
        model=ModelEndpoint(model_name="deepseek-chat", fallback_model="other"),
    )
    assert policy.for_tenant(deepseek)["deepseek-chat"].output_per_mtok == 2
    assert "other" not in policy.for_tenant(deepseek)
    deepseek.model.pricing["custom"] = ModelPricingConfig(input_per_mtok=3, output_per_mtok=4)
    assert policy.for_tenant(deepseek) == deepseek.model.pricing


def test_production_preflight_rejects_local_backends_and_inline_secrets():
    production = ProductionTenantPreflight(
        ServiceSettings(environment="production", role="outbox", mysql_url="mysql://db"))
    tenant = Tenant(tenant_id="p", name="P", model=ModelEndpoint(model_name="m"))
    with pytest.raises(ValueError, match="in-memory vector"):
        production(tenant)

    tenant.storage_config.vector = VectorBackendConfig(backend="qdrant", url="env://QDRANT_URL")
    with pytest.raises(ValueError, match="local object"):
        production(tenant)

    tenant.storage_config.object = ObjectBackendConfig(
        backend="s3",
        bucket="bucket",
        access_key="env://S3_ACCESS",
        secret_key="env://S3_SECRET",
    )
    tenant.channel_configs["qq"] = QQChannelConfig(app_id="app", secret="inline-secret")
    with pytest.raises(ValueError, match="SecretRef"):
        production(tenant)
    tenant.channel_configs["qq"].secret = SecretStr("env://QQ_SECRET")
    production(tenant)
    ProductionTenantPreflight(ServiceSettings(environment="test"))(Tenant(
        tenant_id="test",
        name="T",
        model=ModelEndpoint(model_name="m"),
    ))
