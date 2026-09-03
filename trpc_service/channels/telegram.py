"""
Telegram Channel Adapter Implementation.
"""

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


class TelegramChannelAdapter(BaseChannelAdapter):
    """
    Adapter for Telegram Bot API Webhook updates.
    Supports secret token verification, Markdown/HTML formatting,
    message chunking (4096-char limit), and async Telegram Bot API push.
    """

    channel_type = ChannelType.TELEGRAM
    MAX_TEXT_CHARS = 4096

    def verify_signature(
        self,
        query_params: Dict[str, str],
        headers: Dict[str, str],
        raw_body: bytes,
        token: Optional[str] = None,
        secret: Optional[str] = None,
    ) -> bool:
        """
        Verify Telegram X-Telegram-Bot-Api-Secret-Token header.
        """
        if not secret:
            return True  # If no secret configured in dev mode, pass

        received_secret = headers.get("X-Telegram-Bot-Api-Secret-Token") or headers.get("x-telegram-bot-api-secret-token")
        return received_secret == secret

    def parse_inbound_message(
        self,
        tenant_id: str,
        query_params: Dict[str, str],
        headers: Dict[str, str],
        raw_body: bytes,
        aes_key: Optional[str] = None,
    ) -> InboundMessage:
        """Parse incoming Telegram Update JSON object."""
        trace_id = self.generate_trace_id()
        body_str = raw_body.decode("utf-8", errors="ignore").strip()

        update = {}
        try:
            update = json.loads(body_str)
        except Exception as e:
            logger.warning("Error parsing Telegram JSON: %s", e)

        msg = update.get("message") or update.get("edited_message") or {}
        from_user = str(msg.get("from", {}).get("id") or "telegram_user")
        chat_id = str(msg.get("chat", {}).get("id") or from_user)
        chat_type = msg.get("chat", {}).get("type", "private")
        is_group = chat_type in ["group", "supergroup", "channel"]
        text = msg.get("text") or msg.get("caption") or ""
        msg_id = str(msg.get("message_id") or update.get("update_id") or trace_id)

        return InboundMessage(
            trace_id=trace_id,
            tenant_id=tenant_id,
            channel_type=self.channel_type,
            raw_user_id=from_user,
            raw_chat_id=chat_id,
            is_group=is_group,
            message_id=msg_id,
            content=text,
            content_type="text",
            raw_payload=update,
        )

    def format_outbound_payload(
        self,
        outbound: OutboundMessage,
    ) -> Dict[str, Any]:
        """Format Telegram sendMessage API payload."""
        payload = {
            "chat_id": outbound.target_chat_id or outbound.target_user_id,
            "text": outbound.content,
            "parse_mode": "Markdown",
        }
        if outbound.quote_msg_id:
            payload["reply_to_message_id"] = int(outbound.quote_msg_id)
        if outbound.extra_cards:
            payload.update(outbound.extra_cards)
        return payload

    async def send_message(
        self,
        outbound: OutboundMessage,
        binding: ChannelBindingConfig,
    ) -> bool:
        """Push message to Telegram Bot API."""
        chunks = self.split_long_message(outbound.content, self.MAX_TEXT_CHARS)
        success = True

        for chunk in chunks:
            chunk_outbound = outbound.model_copy(update={"content": chunk})
            payload = self.format_outbound_payload(chunk_outbound)

            bot_token = binding.encrypted_token or "mock_token"
            api_url = f"https://api.telegram.org/bot{bot_token}/sendMessage"

            try:
                async with aiohttp.ClientSession() as session:
                    if binding.bot_id.startswith("mock_") or bot_token == "mock_token":
                        logger.info("[Mock TG Push] Chat: %s | Content: %s", payload["chat_id"], chunk[:60])
                        continue

                    async with session.post(api_url, json=payload, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                        res = await resp.json()
                        if not res.get("ok"):
                            logger.error("Telegram send error: %s", res)
                            success = False
            except Exception as e:
                logger.warning("Telegram send exception: %s", e)
        return success
