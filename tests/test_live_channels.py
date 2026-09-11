"""Explicit opt-in probes for real sandbox channel credentials.

These tests never run in the regular suite. They only send an outbound message
when ``RUN_LIVE_CHANNEL_TESTS=1`` and the provider-specific sandbox variables
are present.
"""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest

from trpc_service.channels import (
    ChannelBinding,
    ReplyBlock,
    ReplyEnvelope,
    TelegramAdapter,
    WeComAdapter,
)
from trpc_service.config import EnvironmentSecretProvider

pytestmark = pytest.mark.live


def _enabled() -> None:
    if os.environ.get("RUN_LIVE_CHANNEL_TESTS") != "1":
        pytest.skip("set RUN_LIVE_CHANNEL_TESTS=1 to permit outbound sandbox probes")


def _reply(provider: str, recipient_id: str) -> ReplyEnvelope:
    return ReplyEnvelope(
        tenant_id="live-sandbox",
        binding_id=f"live-{provider}",
        session_id="live-probe",
        recipient_id=recipient_id,
        delivery_id=f"live-{provider}-{uuid4().hex}",
        blocks=(ReplyBlock.text_block("tRPC-Agent sandbox delivery probe"),),
        traceparent=f"00-{'a' * 32}-{'0' * 16}-01",
    )


def test_live_telegram_sandbox_delivery():
    _enabled()
    recipient = os.environ.get("TRPC_LIVE_TELEGRAM_RECIPIENT")
    if not recipient or not os.environ.get("TRPC_LIVE_TELEGRAM_SECRET"):
        pytest.skip("Telegram sandbox recipient and secret are not configured")
    binding = ChannelBinding(
        tenant_id="live-sandbox",
        binding_id="live-telegram",
        agent_id="live-agent",
        provider="telegram",
        external_account_id="live-bot",
        secret_ref="env://TRPC_LIVE_TELEGRAM_SECRET",
    )
    result = asyncio.run(TelegramAdapter(EnvironmentSecretProvider()).deliver(binding, _reply("telegram", recipient)))
    assert result.status == "accepted", result.error_code


def test_live_wecom_sandbox_delivery():
    _enabled()
    recipient = os.environ.get("TRPC_LIVE_WECOM_RECIPIENT")
    agent_id = os.environ.get("TRPC_LIVE_WECOM_AGENT_ID")
    if not recipient or not agent_id or not os.environ.get("TRPC_LIVE_WECOM_SECRET"):
        pytest.skip("WeCom sandbox recipient, agent id, and secret are not configured")
    binding = ChannelBinding(
        tenant_id="live-sandbox",
        binding_id="live-wecom",
        agent_id="live-agent",
        provider="wecom",
        external_account_id="live-corp",
        secret_ref="env://TRPC_LIVE_WECOM_SECRET",
        capabilities={"wecom_agent_id": agent_id},
    )
    result = asyncio.run(WeComAdapter(EnvironmentSecretProvider()).deliver(binding, _reply("wecom", recipient)))
    assert result.status == "accepted", result.error_code
