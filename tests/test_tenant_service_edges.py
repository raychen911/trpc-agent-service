"""Tenant validation, versioning, and gray-release behavior."""

from __future__ import annotations

from copy import deepcopy

import pytest

from trpc_service.tenant.models import ChannelBinding, default_demo_config
from trpc_service.tenant.repository import (
    InMemoryTenantRepository,
    TenantNotFound,
    TenantRepositoryConflict,
)
from trpc_service.tenant.service import TenantConfigConflict, TenantService, TenantValidationError


def config():
    value = default_demo_config()
    value.apps[0].model_config.model = "test-model"
    value.channel_bindings = [value.channel_bindings[-1]]
    return value


def test_tenant_versioning_and_runtime_gray_release():
    repository = InMemoryTenantRepository()
    service = TenantService(repository)
    original = config()
    assert service.create_tenant(original).tenant_id == "tenant_demo"
    current = service.get_tenant("tenant_demo")
    saved = service.create_config_version("tenant_demo", deepcopy(current), expected_version=1)
    assert saved.config_version == 2
    assert service.publish("tenant_demo", 2, expected_version=1).config_version == 2
    with pytest.raises(TenantConfigConflict):
        service.check_expected_version("tenant_demo", 1)
    with pytest.raises(TenantConfigConflict):
        service.create_config_version("tenant_demo", deepcopy(current), expected_version=1)
    with pytest.raises(TenantNotFound):
        service.get_tenant("missing")

    candidate = service.get_tenant("tenant_demo")
    candidate.gray_release.enabled = True
    candidate.gray_release.candidate_version = 2
    candidate.gray_release.percent = 100
    service.validate(candidate)
    assert service.resolve_runtime_config("tenant_demo", "session-1").config_version == 2
    candidate.gray_release.session_overrides = {"session-1": 1}
    override = repository.save_version(candidate)
    repository.publish("tenant_demo", override.config_version)
    assert service.resolve_runtime_config("tenant_demo", "session-1").config_version == 1
    with pytest.raises(TenantValidationError):
        service.configure_gray_release("tenant_demo", None, 10)
    service.configure_gray_release("tenant_demo", None, 0, enabled=False)


@pytest.mark.parametrize(
    "mutate, message",
    [
        (lambda c: setattr(c, "tenant_id", "bad id"), "tenant_id"),
        (lambda c: c.apps.clear(), "agent app"),
        (lambda c: setattr(c.quota_policy, "qps_limit", 0), "qps_limit"),
        (lambda c: setattr(c.gray_release, "percent", 101), "gray_release.percent"),
        (lambda c: setattr(c.apps[0].model_config, "model", ""), "model provider"),
        (lambda c: c.channel_bindings.append(deepcopy(c.channel_bindings[0])), "binding already exists"),
    ],
)
def test_tenant_validation_rejects_invalid_configs(mutate, message):
    service = TenantService(InMemoryTenantRepository())
    value = config()
    mutate(value)
    with pytest.raises(TenantValidationError, match=message):
        service.validate(value)


def test_tenant_validation_rejects_secrets_retired_channels_and_cross_tenant_conflicts(monkeypatch):
    repository = InMemoryTenantRepository()
    service = TenantService(repository)
    first = config()
    service.create_tenant(first)

    second = config()
    second.tenant_id = "tenant-two"
    second.channel_bindings[0].tenant_id = "tenant-two"
    with pytest.raises(TenantValidationError, match="another tenant"):
        service.validate(second)

    secret = config()
    secret.channel_bindings[0].config = {"webhook_url": "https://x/?token=plain"}
    with pytest.raises(TenantValidationError, match="secret-bearing URL"):
        service.validate(secret)
    secret = config()
    secret.storage_profile.sql_dsn = "postgresql://user:password@db/service"
    with pytest.raises(TenantValidationError, match="plaintext database"):
        service.validate(secret)
    retired = config()
    retired.channel_bindings[0].channel = "wechat_official_account"
    with pytest.raises(TenantValidationError, match="retired"):
        service.validate(retired)

    monkeypatch.setenv("STRICT_CHANNEL_CONFIG", "1")
    strict = config()
    strict.channel_bindings[0] = ChannelBinding("tenant_demo", "b", "telegram", "a", "app_support")
    with pytest.raises(TenantValidationError, match="strict channel"):
        service.validate(strict)


def test_in_memory_repository_conflicts_binding_lookup_and_rollback():
    repository = InMemoryTenantRepository()
    value = config()
    repository.create(value)
    with pytest.raises(Exception):
        repository.create(value)
    with pytest.raises(TenantRepositoryConflict):
        repository.save_version(value, expected_version=99)
    with pytest.raises(TenantNotFound):
        repository.get("tenant_demo", version=99)
    assert repository.find_binding("WEB", "web_demo").channel == "web"
    with pytest.raises(TenantNotFound):
        repository.find_binding("telegram", "missing")
    snapshot = repository.save_version(repository.get("tenant_demo"))
    assert repository.rollback("tenant_demo", snapshot.config_version).config_version == snapshot.config_version
    assert len(repository.all_active()) == 1
