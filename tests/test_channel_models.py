"""Channel-neutral inbound message validation tests."""

from datetime import datetime
from datetime import timezone

import pytest
from pydantic import ValidationError

from trpc_service.channels import ChannelType
from trpc_service.channels import ConversationType
from trpc_service.channels import NormalizedInboundMessage


def message_data(**overrides):
    data = {
        "channel": ChannelType.WECOM,
        "webhook_public_id": "callback_acme",
        "external_message_id": "message_1",
        "external_user_id": "external_user_42",
        "conversation_type": ConversationType.DIRECT,
        "text": "hello",
        "received_at": datetime.now(timezone.utc),
        "request_id": "request_1",
    }
    data.update(overrides)
    return data


def test_group_and_thread_messages_require_chat_identity():
    for conversation_type in (ConversationType.GROUP, ConversationType.THREAD):
        with pytest.raises(ValidationError, match="external_chat_id"):
            NormalizedInboundMessage(**message_data(conversation_type=conversation_type))


def test_thread_message_requires_thread_identity():
    with pytest.raises(ValidationError, match="thread_id"):
        NormalizedInboundMessage(
            **message_data(conversation_type=ConversationType.THREAD, external_chat_id="group_a"))


def test_message_requires_text_or_attachment():
    with pytest.raises(ValidationError, match="text or at least one attachment"):
        NormalizedInboundMessage(**message_data(text=""))
