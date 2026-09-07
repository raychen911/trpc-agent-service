# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
"""Feishu channel adapter."""

from __future__ import annotations

import json
from typing import Any

from ._models import CHAT_GROUP
from ._models import CHAT_PRIVATE
from ._models import InboundMessage
from ._models import OutboundMessage
from ._webhook_json import JsonWebhookAdapter
from ._webhook_json import json_body
from ._webhook_json import verify_hmac_hex


class FeishuAdapter(JsonWebhookAdapter):
    """Normalize Feishu event callbacks and delegate sending to its SDK."""

    channel = "feishu"

    def __init__(self, *, app_id: str = "", verification_token: str = "", encrypt_key: str = "", **kwargs) -> None:
        kwargs.setdefault("message_limit_chars", 4000)
        super().__init__(**kwargs)
        self.app_id = app_id
        self.verification_token = verification_token
        self.encrypt_key = encrypt_key

    async def verify_signature(self, payload: Any, headers: dict[str, str], query: dict[str, str]) -> bool:
        body = json_body(payload)
        supplied_token = body.get("token") or body.get("header", {}).get("token")
        if self.verification_token and supplied_token == self.verification_token:
            return True
        signature = headers.get("x-lark-signature", "")
        return verify_hmac_hex(self.encrypt_key, payload, signature)

    async def parse_message(self, payload: Any) -> InboundMessage:
        body = json_body(payload)
        event = body.get("event", body)
        message = event.get("message", {})
        sender = event.get("sender", {}).get("sender_id", {})
        sender_id = sender.get("open_id") or sender.get("user_id") or event.get("sender_id", "")
        chat_type = CHAT_GROUP if message.get("chat_type") == "group" else CHAT_PRIVATE
        content = message.get("content", "")
        if isinstance(content, str):
            try:
                content = json.loads(content)
            except json.JSONDecodeError:
                content = {"text": content}
        text = content.get("text", "") if isinstance(content, dict) else ""
        msg_type = message.get("message_type", "text")
        return InboundMessage(
            channel=self.channel,
            chat_id=str(message.get("chat_id") or sender_id),
            chat_type=chat_type,
            sender_id=str(sender_id),
            message_id=str(message.get("message_id") or body.get("header", {}).get("event_id", "")),
            text=text,
            images=[content.get("image_key", "")] if msg_type == "image" and isinstance(content, dict) else [],
            files=[content.get("file_key", "")] if msg_type == "file" and isinstance(content, dict) else [],
            metadata={
                "msg_type": msg_type,
                "tenant_key": body.get("header", {}).get("tenant_key")
            },
            raw=body,
        )

    def render_outbound(self, outbound: OutboundMessage) -> dict[str, Any]:
        return {
            "receive_id": outbound.chat_id,
            "msg_type": "text",
            "content": json.dumps({"text": outbound.text}, ensure_ascii=False),
        }
