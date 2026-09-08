# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
"""DingTalk channel adapter."""

from __future__ import annotations

from typing import Any

from ._models import CHAT_GROUP
from ._models import CHAT_PRIVATE
from ._models import InboundMessage
from ._models import OutboundMessage
from ._webhook_json import JsonWebhookAdapter
from ._webhook_json import json_body
from ._webhook_json import verify_hmac_hex


class DingTalkAdapter(JsonWebhookAdapter):
    """Adapter boundary for DingTalk Stream/Webhook SDK messages."""

    channel = "dingtalk"

    def __init__(self, *, client_id: str = "", robot_code: str = "", **kwargs) -> None:
        kwargs.setdefault("message_limit_chars", 4000)
        super().__init__(**kwargs)
        self.client_id = client_id
        self.robot_code = robot_code

    async def verify_signature(self, payload: Any, headers: dict[str, str], query: dict[str, str]) -> bool:
        signature = headers.get("x-dingtalk-signature") or query.get("signature", "")
        return verify_hmac_hex(self.secret, payload, signature)

    async def parse_message(self, payload: Any) -> InboundMessage:
        body = json_body(payload)
        conversation_type = str(body.get("conversationType", body.get("conversation_type", "1")))
        chat_type = CHAT_GROUP if conversation_type in ("2", "group") else CHAT_PRIVATE
        sender = body.get("senderStaffId") or body.get("senderId") or body.get("sender_id", "")
        chat_id = body.get("conversationId") or body.get("chat_id") or sender
        text_value = body.get("text", "")
        text = text_value.get("content", "") if isinstance(text_value, dict) else str(text_value or "")
        return InboundMessage(
            channel=self.channel,
            chat_id=str(chat_id),
            chat_type=chat_type,
            sender_id=str(sender),
            message_id=str(body.get("msgId") or body.get("message_id") or body.get("eventId", "")),
            text=text.strip(),
            images=[str(value) for value in body.get("images", [])],
            files=[str(value) for value in body.get("files", [])],
            metadata={
                "sender_name": body.get("senderNick"),
                "session_webhook": body.get("sessionWebhook")
            },
            raw=body,
        )

    def render_outbound(self, outbound: OutboundMessage) -> dict[str, Any]:
        return {
            "robotCode": self.robot_code,
            "conversationId": outbound.chat_id,
            "msgKey": "sampleText",
            "msgParam": {
                "content": outbound.text
            },
        }
