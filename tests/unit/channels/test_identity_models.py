from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from trpc_service.channels.contracts import Channel
from trpc_service.channels.identity import (
    AuthenticatedSender,
    ChannelIdentity,
    RuntimeBotIdentity,
)


UTC_NOW = datetime(2026, 9, 8, 8, 0, tzinfo=timezone.utc)


def test_channel_identity_is_complete_stable_and_length_prefixed() -> None:
    first = ChannelIdentity(
        channel=Channel.FEISHU,
        provider_tenant_key="ab",
        provider_app_or_bot_id="c",
    )
    second = ChannelIdentity(
        channel=Channel.FEISHU,
        provider_tenant_key="a",
        provider_app_or_bot_id="bc",
    )
    repeated = ChannelIdentity(
        channel=Channel.FEISHU,
        provider_tenant_key="ab",
        provider_app_or_bot_id="c",
    )

    assert first.identity_digest == repeated.identity_digest
    assert first.identity_digest != second.identity_digest
    assert len(first.identity_digest) == 64


@pytest.mark.parametrize(
    "payload",
    [
        {"channel": Channel.LOCAL_HTTP, "provider_tenant_key": "tenant", "provider_app_or_bot_id": "bot"},
        {"channel": Channel.WECOM, "provider_tenant_key": "", "provider_app_or_bot_id": "bot"},
        {"channel": Channel.WECOM, "provider_tenant_key": "corp", "provider_app_or_bot_id": ""},
    ],
)
def test_channel_identity_rejects_incomplete_or_non_im_identity(payload: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        ChannelIdentity(**payload)


def test_runtime_bot_and_sender_are_complete_frozen_and_redacted() -> None:
    identity = ChannelIdentity(
        channel=Channel.WECOM,
        provider_tenant_key="corp-sensitive",
        provider_app_or_bot_id="bot-sensitive",
    )
    bot = RuntimeBotIdentity(
        channel=Channel.WECOM,
        sender_type="bot",
        sender_id="runtime-bot-sensitive",
        channel_identity_digest=identity.identity_digest,
        authenticated_at=UTC_NOW,
    )
    sender = AuthenticatedSender(
        sender_type="user",
        sender_id="sender-sensitive",
        is_bot=False,
    )

    rendered = f"{identity!r} {bot!r} {sender!r}"
    assert "corp-sensitive" not in rendered
    assert "bot-sensitive" not in rendered
    assert "runtime-bot-sensitive" not in rendered
    assert "sender-sensitive" not in rendered
    with pytest.raises(ValidationError):
        sender.sender_id = "changed"


def test_runtime_bot_requires_utc_and_sender_requires_structured_identity() -> None:
    digest = "a" * 64
    with pytest.raises(ValidationError):
        RuntimeBotIdentity(
            channel=Channel.FEISHU,
            sender_type="bot",
            sender_id="bot-id",
            channel_identity_digest=digest,
            authenticated_at=datetime(2026, 9, 8, 8, 0),
        )
    with pytest.raises(ValidationError):
        AuthenticatedSender(sender_type="user", sender_id="", is_bot=None)
