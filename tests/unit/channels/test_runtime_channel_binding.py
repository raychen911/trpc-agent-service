from __future__ import annotations

import json

import pytest

from trpc_service import _cli
from trpc_service.channels.contracts import Channel
from trpc_service.config.settings import ConfigurationError, build_runtime_channel_binding


def _environment() -> dict[str, str]:
    return {
        "LARK_TENANT_KEY": "tenant-key-test",
        "LARK_APP_ID": "cli-app-test",
        "LARK_APP_SECRET": "lark-secret-must-not-be-stored",
        "WECOM_CORP_ID": "corp-id-test",
        "WECOM_BOT_ID": "wecom-bot-test",
        "WECOM_BOT_SECRET": "wecom-secret-must-not-be-stored",
    }


@pytest.mark.parametrize(
    ("channel", "binding_id", "secret_ref"),
    [
        (Channel.FEISHU, "binding-feishu-real", "LARK_APP_SECRET"),
        (Channel.WECOM, "binding-wecom-real", "WECOM_BOT_SECRET"),
    ],
)
def test_runtime_channel_binding_stores_only_secret_reference(
    channel: Channel, binding_id: str, secret_ref: str
) -> None:
    environment = _environment()

    binding = build_runtime_channel_binding(channel, environment)

    assert binding.binding_id == binding_id
    assert binding.tenant_id == "tenant-alpha"
    assert binding.agent_id == "agent-alpha"
    assert binding.channel == channel
    assert binding.secret_ref == secret_ref
    assert len(binding.channel_identity_digest or "") == 64
    rendered = binding.model_dump_json()
    assert environment[secret_ref] not in rendered


@pytest.mark.parametrize(
    "missing_name",
    [
        "LARK_TENANT_KEY",
        "LARK_APP_ID",
        "LARK_APP_SECRET",
        "WECOM_CORP_ID",
        "WECOM_BOT_ID",
        "WECOM_BOT_SECRET",
    ],
)
def test_runtime_channel_binding_fails_closed_when_configuration_is_missing(
    missing_name: str,
) -> None:
    environment = _environment()
    environment[missing_name] = ""
    channel = Channel.FEISHU if missing_name.startswith("LARK_") else Channel.WECOM

    with pytest.raises(ConfigurationError, match="unavailable"):
        build_runtime_channel_binding(channel, environment)


def test_runtime_channel_binding_rejects_local_http() -> None:
    with pytest.raises(ConfigurationError, match="real IM"):
        build_runtime_channel_binding(Channel.LOCAL_HTTP, _environment())


def test_shared_channel_init_cli_outputs_only_pseudonymous_metadata(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    environment = _environment()
    binding = build_runtime_channel_binding(Channel.FEISHU, environment)

    async def initialize(_channel: Channel):
        return binding

    monkeypatch.setattr(_cli, "_initialize_shared_channel_binding", initialize)

    assert _cli.shared_channel_init_main(["--channel", "feishu"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload == {
        "channel": "feishu",
        "binding_id": "binding-feishu-real",
        "identity_digest": binding.channel_identity_digest,
        "status": "active",
    }
    assert environment["LARK_TENANT_KEY"] not in repr(payload)
    assert environment["LARK_APP_ID"] not in repr(payload)
    assert environment["LARK_APP_SECRET"] not in repr(payload)
