from __future__ import annotations

from dataclasses import dataclass

import pytest

from trpc_service.channels.envelopes import PayloadKind
from trpc_service.channels.wecom import parse_wecom_frame
from trpc_service.tenant.models import ConversationKind


@dataclass
class Frame:
    body: dict


def test_parse_direct_text_frame() -> None:
    envelope = parse_wecom_frame(
        Frame(
            {
                "msgid": "message-1",
                "chattype": "single",
                "from": {"userid": "user-1"},
                "msgtype": "text",
                "text": {"content": "hello"},
                "create_time": 1787270400,
            }
        ),
        account_id="bot-id",
    )
    assert envelope.conversation_kind == ConversationKind.DIRECT
    assert envelope.text == "hello"
    assert envelope.external_conversation_id is None


def test_parse_group_mixed_frame_preserves_media_boundary() -> None:
    envelope = parse_wecom_frame(
        Frame(
            {
                "msgid": "message-2",
                "chattype": "group",
                "chatid": "chat-1",
                "from": {"userid": "user-1"},
                "msgtype": "mixed",
                "mixed": {
                    "msg_item": [
                        {"msgtype": "text", "text": {"content": "question"}},
                        {
                            "msgtype": "image",
                            "image": {"url": "https://media", "aeskey": "key-ref"},
                        },
                    ]
                },
            }
        ),
        account_id="bot-id",
    )
    assert envelope.conversation_kind == ConversationKind.GROUP
    assert envelope.external_conversation_id == "chat-1"
    assert envelope.payload_kind == PayloadKind.MIXED
    assert envelope.media[0].encryption_key_ref == "key-ref"


def test_invalid_frame_without_sender_is_rejected() -> None:
    with pytest.raises(ValueError, match="sender"):
        parse_wecom_frame(Frame({"msgid": "id"}), account_id="bot")
