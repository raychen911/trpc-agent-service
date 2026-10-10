from __future__ import annotations

import pytest

from trpc_service.channels import ChannelCapabilities, RecallEnvelope
from trpc_service.channels.envelopes import (
    DeliveryStatus,
    PayloadKind,
)
from trpc_service.channels.feishu import FeishuAdapter
from trpc_service.channels.wecom import WeComConnector
from trpc_service.config.secrets import LocalSecretProvider
from trpc_service.tenant.models import Channel, ChannelBinding


def _binding(channel: Channel) -> ChannelBinding:
    return ChannelBinding(
        binding_id=f"{channel.value}-binding",
        tenant_id="tenant",
        app_id="app",
        channel=channel,
        account_id="account",
    )


def _recall(channel: Channel) -> RecallEnvelope:
    return RecallEnvelope(
        outbound_id="recall-operation",
        tenant_id="tenant",
        binding_id=f"{channel.value}-binding",
        channel=channel,
        provider_message_id="provider-message",
    )


def test_provider_outbound_capability_matrix_is_explicit() -> None:
    for capabilities in (FeishuAdapter.capabilities, WeComConnector.capabilities):
        assert isinstance(capabilities, ChannelCapabilities)
        assert capabilities.outbound_payloads == frozenset({PayloadKind.TEXT})
        assert capabilities.stream is False
        assert capabilities.card is False
        assert capabilities.media is False
        assert capabilities.recall is False
        assert capabilities.proactive is True
        assert capabilities.text_split is False
        assert capabilities.max_text_bytes is None


@pytest.mark.asyncio
async def test_feishu_recall_fails_closed_without_provider_request() -> None:
    adapter = FeishuAdapter(LocalSecretProvider(allow_literal=True))

    receipt = await adapter.recall(_recall(Channel.FEISHU), _binding(Channel.FEISHU))

    assert receipt.outbound_id == "recall-operation"
    assert receipt.status == DeliveryStatus.FAILED
    assert receipt.provider_code == "unsupported_capability"
    assert receipt.retryable is False


@pytest.mark.asyncio
async def test_wecom_recall_fails_closed_before_connection_lookup() -> None:
    connector = WeComConnector(
        LocalSecretProvider(allow_literal=True),
        object(),  # type: ignore[arg-type]
        owner_id="owner",
    )

    receipt = await connector.recall(
        _recall(Channel.WECOM_AI_BOT),
        _binding(Channel.WECOM_AI_BOT),
    )

    assert receipt.outbound_id == "recall-operation"
    assert receipt.status == DeliveryStatus.FAILED
    assert receipt.provider_code == "unsupported_capability"
    assert receipt.retryable is False
