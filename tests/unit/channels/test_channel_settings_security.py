from __future__ import annotations

import pytest

from trpc_service.channels.contracts import Channel
from trpc_service.config.settings import (
    ChannelCredentialSettings,
    EnvironmentSecretProvider,
    load_channel_credentials,
)
from trpc_service.storage.contracts import SecretBytes, SecretUnavailable


def test_channel_credentials_load_only_through_environment_secret_provider() -> None:
    source = {
        "LARK_APP_ID": "app-sensitive-value",
        "LARK_APP_SECRET": "secret-sensitive-value",
    }
    settings = load_channel_credentials(
        Channel.FEISHU,
        EnvironmentSecretProvider(source),
    )

    assert isinstance(settings, ChannelCredentialSettings)
    assert isinstance(settings.app_or_bot_id, SecretBytes)
    assert isinstance(settings.secret, SecretBytes)
    assert settings.app_or_bot_id.reveal() == b"app-sensitive-value"
    assert settings.secret.reveal() == b"secret-sensitive-value"
    rendered = repr(settings)
    assert "app-sensitive-value" not in rendered
    assert "secret-sensitive-value" not in rendered


@pytest.mark.parametrize("channel", [Channel.FEISHU, Channel.WECOM])
def test_missing_channel_credentials_fail_closed_without_disclosure(channel: Channel) -> None:
    with pytest.raises(SecretUnavailable) as exc_info:
        load_channel_credentials(channel, EnvironmentSecretProvider({}))

    assert str(exc_info.value) == "Channel credential is unavailable."
    assert "LARK" not in str(exc_info.value)
    assert "WECOM" not in str(exc_info.value)


def test_secret_provider_and_errors_never_print_values_or_variable_names() -> None:
    provider = EnvironmentSecretProvider({"LARK_APP_SECRET": "secret-sensitive-value"})
    assert "secret-sensitive-value" not in repr(provider)
    assert "LARK_APP_SECRET" not in repr(provider)
    with pytest.raises(SecretUnavailable) as exc_info:
        provider.resolve("WECOM_BOT_SECRET")
    assert "WECOM_BOT_SECRET" not in str(exc_info.value)
