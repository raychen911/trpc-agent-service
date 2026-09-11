import asyncio
import importlib.util
import logging

import pytest
from trpc_agent_sdk.models import shared_http_client_provider_factory

from trpc_service.config import model as model_config


def test_model_configuration_module_exists():
    assert importlib.util.find_spec("trpc_service.config.model") is not None


def test_model_name_is_required():
    error_type = getattr(model_config, "ModelConfigurationError", RuntimeError)

    with pytest.raises(error_type, match="TRPC_MODEL_NAME"):
        model_config.ModelSettings.from_env({})


def test_settings_load_openai_compatible_environment():
    settings = model_config.ModelSettings.from_env({
        "TRPC_MODEL_PROVIDER": "openai-compatible",
        "TRPC_MODEL_NAME": "deepseek-chat",
        "TRPC_MODEL_BASE_URL": "https://model.example/v1",
        "TRPC_MODEL_API_KEY_ENV": "TEST_PROVIDER_KEY",
    })

    assert settings.provider == "openai-compatible"
    assert settings.model_name == "deepseek-chat"
    assert settings.base_url == "https://model.example/v1"
    assert settings.api_key_env == "TEST_PROVIDER_KEY"


def test_api_key_is_required_but_never_stored_in_settings_repr():
    settings = model_config.ModelSettings.from_env({
        "TRPC_MODEL_NAME": "deepseek-chat",
    })
    error_type = getattr(model_config, "ModelConfigurationError", RuntimeError)

    with pytest.raises(error_type, match="TRPC_MODEL_API_KEY"):
        settings.resolve_api_key({})

    api_key = settings.resolve_api_key({"TRPC_MODEL_API_KEY": "top-secret-value"})
    assert api_key == "top-secret-value"
    assert "top-secret-value" not in repr(settings)


def test_unsupported_provider_is_rejected():
    error_type = getattr(model_config, "ModelConfigurationError", RuntimeError)

    with pytest.raises(error_type, match="TRPC_MODEL_PROVIDER"):
        model_config.ModelSettings.from_env({
            "TRPC_MODEL_PROVIDER": "unknown-provider",
            "TRPC_MODEL_NAME": "demo",
        })


def test_build_model_returns_real_sdk_model():
    settings = model_config.ModelSettings.from_env({
        "TRPC_MODEL_NAME": "deepseek-chat",
        "TRPC_MODEL_BASE_URL": "https://model.example/v1",
    })

    model = model_config.build_model(
        settings,
        {"TRPC_MODEL_API_KEY": "test-key-not-used-on-network"},
    )

    assert model.name == "deepseek-chat"
    assert model.__class__.__name__ == "OpenAIModel"


# ---------------------------------------------------------------------------
# HTTP connection reuse: shared client provider injection + bounded cleanup
# ---------------------------------------------------------------------------


def _connection_reuse_settings():
    return model_config.ModelSettings.from_env({
        "TRPC_MODEL_NAME": "deepseek-chat",
        "TRPC_MODEL_BASE_URL": "https://model.example/v1",
    })


def test_build_model_injects_shared_http_client_provider_factory(monkeypatch):
    captured = {}

    class CapturingModel:

        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(model_config, "OpenAIModel", CapturingModel)

    model_config.build_model(
        _connection_reuse_settings(),
        {"TRPC_MODEL_API_KEY": "top-secret-value"},
    )

    # The core assertion is on the public SDK factory identity, not on any
    # private OpenAIModel state; the API key is handed to the SDK as before.
    assert captured["http_client_provider_factory"] is shared_http_client_provider_factory
    assert captured["api_key"] == "top-secret-value"

    # Behavioural proof of "not the temporary default": the injected factory's
    # provider yields a real reusable client (temporary yields None).
    client = captured["http_client_provider_factory"]().create_http_client()
    assert client is not None
    asyncio.run(model_config.close_model_http_clients())


def test_close_model_http_clients_calls_sdk_cleanup_and_is_repeatable(monkeypatch):
    calls = []

    async def fake_close_shared_http_clients():
        calls.append(1)

    monkeypatch.setattr(model_config, "close_shared_http_clients", fake_close_shared_http_clients)

    asyncio.run(model_config.close_model_http_clients())
    asyncio.run(model_config.close_model_http_clients())

    assert calls == [1, 1]


def test_close_model_http_clients_swallows_sdk_failure_without_leaking(monkeypatch, caplog):

    async def exploding_close():
        raise RuntimeError("POST https://model.example/v1 Authorization: Bearer top-secret-value")

    monkeypatch.setattr(model_config, "close_shared_http_clients", exploding_close)

    with caplog.at_level(logging.WARNING, logger="trpc_service.config.model"):
        asyncio.run(model_config.close_model_http_clients())  # must not raise

    text = caplog.text
    assert "model http client cleanup failed" in text
    assert "top-secret-value" not in text
    assert "model.example" not in text


def test_close_model_http_clients_is_bounded_when_cleanup_hangs(monkeypatch, caplog):

    async def hanging_close():
        await asyncio.sleep(30)

    monkeypatch.setattr(model_config, "close_shared_http_clients", hanging_close)
    monkeypatch.setattr(model_config, "HTTP_CLIENT_CLOSE_TIMEOUT_SECONDS", 0.05)

    with caplog.at_level(logging.WARNING, logger="trpc_service.config.model"):
        asyncio.run(model_config.close_model_http_clients())  # returns within ~timeout

    assert "model http client cleanup failed" in caplog.text


def test_shared_provider_reuses_one_client_until_cleanup():
    # Verification over the SDK's public API only: consecutive providers from
    # the injected factory share one keep-alive-capable client, the temporary
    # default would return None instead, and a fresh client is usable again
    # after the bounded cleanup.
    provider_one = shared_http_client_provider_factory()
    provider_two = shared_http_client_provider_factory()

    client_a = provider_one.create_http_client()
    client_b = provider_two.create_http_client()

    assert client_a is not None
    assert client_a is client_b

    asyncio.run(model_config.close_model_http_clients())

    provider_three = shared_http_client_provider_factory()
    client_c = provider_three.create_http_client()
    try:
        assert client_c is not client_a
        assert not client_c.is_closed
    finally:
        asyncio.run(model_config.close_model_http_clients())
