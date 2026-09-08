import pytest
from pydantic import ValidationError

from trpc_service.config import Settings


def test_settings_defaults() -> None:
    settings = Settings(_env_file=None)

    assert settings.app_name == "trpc-agent-service"
    assert settings.port == 8000
    assert settings.environment == "development"


def test_settings_rejects_invalid_port() -> None:
    with pytest.raises(ValidationError):
        Settings(port=0, _env_file=None)


def test_settings_rejects_production_inmemory_conversation() -> None:
    with pytest.raises(ValidationError, match="conversation_backend=sql"):
        Settings(
            environment="production",
            conversation_backend="inmemory",
            gateway_internal_secret="changed-in-production",
            _env_file=None,
        )
