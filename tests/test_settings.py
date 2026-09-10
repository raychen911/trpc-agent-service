"""Configuration safety tests."""

import pytest
from pydantic import ValidationError

from trpc_service.config.secrets import SecretResolver
from trpc_service.config.settings import AppEnvironment, ServiceSettings


def test_test_environment_can_start_without_external_credentials() -> None:
    settings = ServiceSettings(_env_file=None)
    assert settings.app_env == AppEnvironment.TEST
    assert settings.model_provider == "test"


def test_development_rejects_test_model() -> None:
    with pytest.raises(ValidationError, match="real model provider"):
        ServiceSettings(
            _env_file=None,
            app_env="development",
            model_provider="test",
        )


def test_development_requires_secret_references() -> None:
    with pytest.raises(ValidationError, match="secret references"):
        ServiceSettings(
            _env_file=None,
            app_env="development",
            model_provider="openai",
            model_base_url="https://model.example.com/v1",
        )


def test_development_accepts_safe_configuration() -> None:
    settings = ServiceSettings(
        _env_file=None,
        app_env="development",
        model_provider="openai",
        model_name="company-model",
        model_base_url="https://model.example.com/v1/",
        model_api_key_ref="env://MODEL_KEY",
        admin_api_key_ref="env://ADMIN_KEY",
        session_hmac_key_ref="file://secrets/session_hmac",
    )
    assert settings.model_base_url == "https://model.example.com/v1"


def test_absolute_file_secret_reference_is_accepted() -> None:
    settings = ServiceSettings(_env_file=None, session_hmac_key_ref="file:///secrets/hmac-key")
    assert settings.session_hmac_key_ref == "file:///secrets/hmac-key"


def test_rejects_literal_secret_and_insecure_model_url() -> None:
    with pytest.raises(ValidationError):
        ServiceSettings(_env_file=None, model_api_key_ref="literal://secret")
    with pytest.raises(ValidationError, match="HTTPS"):
        ServiceSettings(_env_file=None, model_base_url="http://model.example.com")


def test_postgresql_async_url_is_accepted() -> None:
    settings = ServiceSettings(
        _env_file=None,
        database_url="postgresql+asyncpg://service:secret@postgres/service",
    )
    assert settings.sqlite_path is None


def test_secret_resolver_reads_dotenv_without_export(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("DOTENV_ONLY_SECRET", raising=False)
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("DOTENV_ONLY_SECRET=test-dotenv-secret\n", encoding="utf-8")

    assert SecretResolver(dotenv_path).resolve("env://DOTENV_ONLY_SECRET") == "test-dotenv-secret"


def test_process_environment_overrides_dotenv(monkeypatch, tmp_path) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("PRIORITY_SECRET=dotenv-value\n", encoding="utf-8")
    monkeypatch.setenv("PRIORITY_SECRET", "injected-value")

    assert SecretResolver(dotenv_path).resolve("env://PRIORITY_SECRET") == "injected-value"
