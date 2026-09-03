"""
WeChat Customer Service (微信客服 / 企业微信客服) Channel Adapter.
"""

import hashlib
import json
from typing import Any, Dict, Optional
import aiohttp
import logging
from trpc_service.config.models import (
    ChannelType,
    ChannelBindingConfig,
    InboundMessage,
    OutboundMessage,
)
from trpc_service.channels.base import BaseChannelAdapter

logger = logging.getLogger(__name__)


class WeChatKFChannelAdapter(BaseChannelAdapter):
    """
    Adapter for 微信客服 (WeChat Customer Service).
    Processes user inquiries via customer service bot, maps external_userid to session,
    and supports active response via customer service message APIs.
    """

    channel_type = ChannelType.WECHAT_KF
    MAX_TEXT_BYTES = 2048

    def verify_signature(
        self,
        query_params: Dict[str, str],
        headers: Dict[str, str],
        raw_body: bytes,
        token: Optional[str] = None,
        secret: Optional[str] = None,
    ) -> bool:
        """Verify WeChat Customer Service callback signature."""
        if not token:
            return True

        signature = query_params.get("msg_signature") or query_params.get("signature")
        timestamp = query_params.get("timestamp", "")
        nonce = query_params.get("nonce", "")
        echostr = query_params.get("echostr", "")

        if not signature:
            return False

        encrypt_part = echostr or raw_body.decode("utf-8", errors="ignore")
        items = sorted([token, timestamp, nonce, encrypt_part])
        sha1 = hashlib.sha1("".join(items).encode("utf-8")).hexdigest()
        return sha1 == signature

    def parse_inbound_message(
        self,
        tenant_id: str,
        query_params: Dict[str, str],
        headers: Dict[str, str],
        raw_body: bytes,
        aes_key: Optional[str] = None,
    ) -> InboundMessage:
        """Parse incoming WeChat KF message payload."""
        trace_id = self.generate_trace_id()
        body_str = raw_body.decode("utf-8", errors="ignore").strip()

        data: Dict[str, Any] = {}
        try:
            data = json.loads(body_str)
        except Exception:
            pass

        # In WeChat KF, external_userid represents the WeChat user, open_kfid represents the bot
        user_id = data.get("external_userid") or data.get("from_user") or "wechat_kf_user"
        kfid = data.get("open_kfid") or "default_kf"
        msg_id = data.get("msgid") or trace_id

        # Extract text content
        msg_type = data.get("msgtype", "text")
        content = ""
        if msg_type == "text":
            content = data.get("text", {}).get("content") or data.get("content", "")
        elif msg_type == "event":
            event_type = data.get("event", {}).get("event_type")
            content = f"[System Event: {event_type}]"
        else:
            content = f"[{msg_type} message received]"

        return InboundMessage(
            trace_id=trace_id,
            tenant_id=tenant_id,
            channel_type=self.channel_type,
            raw_user_id=user_id,
            raw_chat_id=kfid,
            is_group=False,  # WeChat KF is predominantly 1-on-1 customer service
            message_id=msg_id,
            content=content,
            content_type=msg_type,
            raw_payload=data,
        )

    def format_outbound_payload(
        self,
        outbound: OutboundMessage,
    ) -> Dict[str, Any]:
        """Format outbound payload for WeChat KF send message API."""
        return {
            "touser": outbound.target_user_id,
            "open_kfid": outbound.target_chat_id or "default_kf",
            "msgtype": "text",
            "text": {
                "content": outbound.content,
            },
        }

    async def send_message(
        self,
        outbound: OutboundMessage,
        binding: ChannelBindingConfig,
    ) -> bool:
        """Send message back to WeChat KF via API."""
        chunks = self.split_long_message(outbound.content, self.MAX_TEXT_BYTES)
        success = True

        for chunk in chunks:
            chunk_outbound = outbound.model_copy(update={"content": chunk})
            payload = self.format_outbound_payload(chunk_outbound)

            # Simulated / Real WeChat KF HTTP API call
            if binding.bot_id.startswith("mock_") or not binding.encrypted_secret:
                logger.info("[Mock WeChat KF Push] To: %s | Content: %s", outbound.target_user_id, chunk[:60])
                continue

            api_url = "https://qyapi.weixin.qq.com/cgi-bin/kf/send_msg?access_token=dummy_token"
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(api_url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        res_json = await resp.json()
                        if res_json.get("errcode", 0) != 0:
                            logger.error("WeChat KF send failed: %s", res_json)
                            success = False
            except Exception as e:
                logger.warning("WeChat KF push error: %s", e)
        return success
