"""Deep protocol coverage for tenant validation and configuration rollout."""

from __future__ import annotations

from copy import deepcopy

import pytest

from trpc_service.tenant.models import ChannelBinding, TenantStatus, default_demo_config
from trpc_service.tenant.repository import InMemoryTenantRepository
from trpc_service.tenant.service import TenantConfigConflict, TenantService, TenantValidationError


def valid_config():
    config = default_demo_config()
    config.apps[0].model_config.model = "test-model"
    config.channel_bindings = [config.channel_bindings[-1]]
    return config


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda config: config.apps.append(deepcopy(config.apps[0])), "unique"),
        (lambda config: setattr(config.apps[0].model_config, "provider", ""), "provider"),
        (lambda config: setattr(config.apps[0].tool_policy, "risk_levels", {"tool": "unknown"}), "risk levels"),
        (lambda config: setattr(config.apps[0].tool_policy, "require_confirmation_for_risk", ["unknown"]), "confirmation"),
        (lambda config: setattr(config.apps[0].tool_policy, "max_calls_per_request", 0), "max_calls"),
        (lambda config: setattr(config.apps[0].tool_policy, "max_side_effect_calls_per_request", -1), "negative"),
        (lambda config: setattr(config.apps[0].tool_policy, "max_side_effect_calls_per_request", 99), "exceed"),
        (lambda config: setattr(config.gray_release, "candidate_version", 0), "positive"),
    ],
)
def test_validation_rejects_unsafe_application_and_rollout_settings(mutate, message):
    service = TenantService(InMemoryTenantRepository())
    config = valid_config()
    mutate(config)
    with pytest.raises(TenantValidationError, match=message):
        service.validate(config)


def test_validation_rejects_identity_binding_and_non_json_values():
    service = TenantService(InMemoryTenantRepository())
    config = valid_config()
    config.apps[0].metadata["non_json"] = object()
    with pytest.raises(TenantValidationError, match="strict JSON"):
        service.validate(config)

    config = valid_config()
    config.channel_bindings[0].account_id = "bad account"
    with pytest.raises(TenantValidationError, match="account_id"):
        service.validate(config)

    config = valid_config()
    config.channel_bindings[0].tenant_id = "other"
    with pytest.raises(TenantValidationError, match="tenant_id mismatch"):
        service.validate(config)

    config = valid_config()
    config.channel_bindings[0].agent_app_id = "missing-app"
    with pytest.raises(TenantValidationError, match="missing app"):
        service.validate(config)


def test_validation_strict_channel_profiles_and_secret_reference_checks(monkeypatch):
    repository = InMemoryTenantRepository()
    service = TenantService(repository)
    monkeypatch.setenv("STRICT_CHANNEL_CONFIG", "1")

    config = valid_config()
    config.channel_bindings[0] = ChannelBinding("tenant_demo", "web", "telegram", "account", "app_support")
    with pytest.raises(TenantValidationError, match="telegram.token_ref"):
        service.validate(config)

    config.channel_bindings[0].token_ref = "secret://tenant/token"
    with pytest.raises(TenantValidationError, match="telegram.secret_ref"):
        service.validate(config)

    config.channel_bindings[0].secret_ref = "secret://tenant/secret"
    monkeypatch.setenv("VALIDATE_SECRETS", "1")
    calls = []
    service.secrets = type("Secrets", (), {"validate": lambda self, value: calls.append(value)})()
    service.validate(config)
    assert calls == ["secret://tenant/token", "secret://tenant/secret"]

    config.channel_bindings[0].token_ref = "plain-token"
    with pytest.raises(TenantValidationError, match="secret://"):
        service.validate(config)


def test_gray_release_and_publish_boundaries_are_deterministic(monkeypatch):
    repository = InMemoryTenantRepository()
    service = TenantService(repository)
    service.create_tenant(valid_config())
    current = service.get_tenant("tenant_demo")
    candidate = deepcopy(current)
    candidate.config_version = 2
    candidate.apps[0].prompt = "candidate"
    repository.save_version(candidate)

    with pytest.raises(TenantValidationError, match="between"):
        service.configure_gray_release("tenant_demo", 2, 101)
    with pytest.raises(TenantValidationError, match="candidate_version"):
        service.configure_gray_release("tenant_demo", None, 10)
    with pytest.raises(TenantValidationError, match="active"):
        inactive = deepcopy(current)
        inactive.status = TenantStatus.DISABLED
        inactive_saved = repository.save_version(inactive)
        service.publish("tenant_demo", inactive_saved.config_version)

    service.configure_gray_release("tenant_demo", 2, 25, expected_version=1)
    monkeypatch.setattr(service, "_bucket", staticmethod(lambda tenant, route: 99))
    active_version = service.get_tenant("tenant_demo").config_version
    assert service.resolve_runtime_config("tenant_demo", "route").config_version == active_version
    monkeypatch.setattr(service, "_bucket", staticmethod(lambda tenant, route: 1))
    assert service.resolve_runtime_config("tenant_demo", "route").config_version == 2
    service.configure_gray_release("tenant_demo", None, 0, enabled=False)
    assert service.resolve_runtime_config("tenant_demo", "route").gray_release.enabled is False


def test_configuration_mutations_translate_repository_conflicts_and_validate_bindings():
    repository = InMemoryTenantRepository()
    service = TenantService(repository)
    service.create_tenant(valid_config())
    current = service.get_tenant("tenant_demo")
    with pytest.raises(TenantConfigConflict):
        service.check_expected_version("tenant_demo", current.config_version + 1)

    binding = ChannelBinding("tenant_demo", "new", "web", "new-account", "app_support")
    with pytest.raises(TenantConfigConflict):
        service.add_channel_binding("tenant_demo", binding, expected_version=99)
    wrong = ChannelBinding("other", "new", "web", "other-account", "app_support")
    with pytest.raises(TenantValidationError, match="binding tenant_id"):
        service.add_channel_binding("tenant_demo", wrong)
    assert service.add_channel_binding("tenant_demo", binding).config_version == 2
    with pytest.raises(TenantValidationError, match="already exists"):
        service.add_channel_binding("tenant_demo", binding)

    with pytest.raises(TenantValidationError, match="differ"):
        service.create_config_version("other", valid_config())
    with pytest.raises(TenantConfigConflict):
        service._save_version(valid_config(), expected_version=99)
    with pytest.raises(TenantConfigConflict):
        service._save_and_publish(valid_config(), expected_version=99)
