# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
"""WeChat Customer Service (微信客服) channel adapter."""

from __future__ import annotations

from typing import Any

from ._models import CHAT_PRIVATE
from ._models import InboundMessage
from ._models import OutboundMessage
from ._webhook_json import JsonWebhookAdapter
from ._webhook_json import json_body
from ._webhook_json import verify_hmac_hex


class WechatCustomerServiceAdapter(JsonWebhookAdapter):
    """Normalize 微信客服 callbacks and delegate sending to an injected SDK.

    The official API obtains messages with a callback token/cursor. The
    adapter accepts the resulting message object so transport/authentication
    remains replaceable without leaking it into Gateway or Worker code.
    """

    channel = "wechat_kf"

    def __init__(self, *, corp_id: str = "", open_kfid: str = "", token: str = "", **kwargs) -> None:
        kwargs.setdefault("message_limit_chars", 2048)
        super().__init__(secret=token, **kwargs)
        self.corp_id = corp_id
        self.open_kfid = open_kfid

    async def verify_signature(self, payload: Any, headers: dict[str, str], query: dict[str, str]) -> bool:
        signature = headers.get("x-wechat-kf-signature") or query.get("signature", "")
        return verify_hmac_hex(self.secret, payload, signature)

    async def parse_message(self, payload: Any) -> InboundMessage:
        body = json_body(payload)
        message = body.get("message") or body.get("msg") or body
        origin = message.get("origin") or message.get("external_userid") or message.get("from_user", "")
        text_value = message.get("text", "")
        text = text_value.get("content", "") if isinstance(text_value, dict) else str(text_value or "")
        msg_type = message.get("msgtype", "text")
        return InboundMessage(
            channel=self.channel,
            chat_id=str(origin),
            chat_type=CHAT_PRIVATE,
            sender_id=str(origin),
            message_id=str(message.get("msgid") or message.get("message_id") or body.get("event_id", "")),
            text=text,
            images=[message.get("image", {}).get("media_id", "")] if msg_type == "image" else [],
            files=[message.get("file", {}).get("media_id", "")] if msg_type == "file" else [],
            metadata={
                "open_kfid": message.get("open_kfid") or self.open_kfid,
                "msg_type": msg_type
            },
            raw=body,
        )

    def render_outbound(self, outbound: OutboundMessage) -> dict[str, Any]:
        return {
            "touser": outbound.chat_id,
            "open_kfid": self.open_kfid,
            "msgtype": "text",
            "text": {
                "content": outbound.text
            },
        }
