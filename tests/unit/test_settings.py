from __future__ import annotations

import pytest

from trpc_service.config.settings import ConfigurationError, build_demo_settings, load_settings


def test_demo_settings_define_two_isolated_tenants_agents_and_bindings() -> None:
    settings = build_demo_settings()

    assert {tenant.tenant_id for tenant in settings.tenants} == {"tenant-alpha", "tenant-beta"}
    assert len(settings.agents) == 2
    assert len(settings.bindings) == 2
    assert len({binding.secret_ref for binding in settings.bindings}) == 2
    assert all(agent.model_profile == "deterministic-offline" for agent in settings.agents)


def test_runtime_secrets_are_required_but_never_stored(runtime_secret_env: dict[str, str]) -> None:
    settings = load_settings(runtime_secret_env)
    rendered = repr(settings)

    assert all(secret not in rendered for secret in runtime_secret_env.values())
    assert all(not hasattr(binding, "secret") for binding in settings.bindings)


@pytest.mark.parametrize(
    "environ",
    [
        {},
        {"TRPC_DEMO_ALPHA_SECRET": "", "TRPC_DEMO_BETA_SECRET": "present"},
    ],
)
def test_missing_or_empty_binding_secret_fails_with_non_disclosing_error(
    environ: dict[str, str],
) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        load_settings(environ)

    assert str(exc_info.value) == "Required binding secret is unavailable."
    assert "ALPHA" not in str(exc_info.value)
    assert "BETA" not in str(exc_info.value)


def test_model_provider_credentials_are_not_required(runtime_secret_env: dict[str, str]) -> None:
    settings = load_settings(runtime_secret_env)
    assert settings.model_credentials_required is False
