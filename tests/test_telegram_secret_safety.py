from __future__ import annotations

import asyncio

import pytest

from trpc_service.channels import (
    ChannelBinding,
    ChannelError,
    DeliveryCapability,
    ReplyBlock,
    ReplyEnvelope,
    TelegramAdapter,
)
from trpc_service.config import MockSecretProvider, parse_secret_json


def _binding() -> ChannelBinding:
    return ChannelBinding(
        tenant_id="tenant-test",
        binding_id="telegram-test",
        agent_id="agent-test",
        provider="telegram",
        external_account_id="bot-test",
        secret_ref="env://TELEGRAM_SECRET",
    )


def _reply() -> ReplyEnvelope:
    return ReplyEnvelope(
        tenant_id="tenant-test",
        binding_id="telegram-test",
        session_id="session-test",
        recipient_id="recipient-test",
        delivery_id="delivery-test",
        blocks=(ReplyBlock.text_block("probe"),),
        traceparent=f"00-{'a' * 32}-{'0' * 16}-01",
    )


def test_structured_secret_accepts_shell_quoted_json() -> None:
    assert parse_secret_json("'{\"bot_token\": \"token-value\"}'") == {
        "bot_token": "token-value"
    }


def test_telegram_unwraps_shell_quoted_structured_secret() -> None:
    captured: dict[str, str] = {}

    async def http_json(method, url, *, body, headers):
        captured["method"] = method
        captured["url"] = url
        return 200, {"ok": True, "result": {"message_id": 1}}, {}

    adapter = TelegramAdapter(
        MockSecretProvider({"env://TELEGRAM_SECRET": "'{\"bot_token\": \"token-value\"}'"}),
        http_json=http_json,
    )
    result = asyncio.run(adapter.deliver(_binding(), _reply()))

    assert result.status == "accepted"
    assert result.capability is DeliveryCapability.NON_RETRIABLE
    assert captured == {
        "method": "POST",
        "url": "https://api.telegram.org/bottoken-value/sendMessage",
    }


def test_telegram_rejects_malformed_structured_secret_before_http() -> None:
    async def http_json(*args, **kwargs):
        raise AssertionError("malformed credential reached the provider HTTP client")

    adapter = TelegramAdapter(
        MockSecretProvider({"env://TELEGRAM_SECRET": "'{not-json}'"}), http_json=http_json
    )
    with pytest.raises(ChannelError) as caught:
        asyncio.run(adapter.deliver(_binding(), _reply()))

    assert caught.value.code == "credentials_unavailable"
