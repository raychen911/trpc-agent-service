"""Telegram Bot API webhook and delivery adapter."""

from __future__ import annotations

import asyncio
import hmac
import json
from datetime import UTC, datetime
from typing import Any

import httpx

from tenant_agent.channels.base import (
    TELEGRAM_TEXT_MAX_CHARS,
    DeliveryError,
    DeliveryResult,
    ParsedWebhook,
    RateLimited,
    SignatureError,
    UnsupportedMessage,
    WebhookAck,
    WebhookRequest,
    split_text,
)
from tenant_agent.models import (
    Attachment,
    ChannelBindingConfig,
    ChannelType,
    ChatType,
    InboundEnvelope,
    OutboundMessage,
    TenantConfig,
)
from tenant_agent.security import CompositeSecretResolver


class TelegramAdapter:
    name = "telegram"

    def __init__(
        self,
        *,
        api_base: str = "https://api.telegram.org",
        timeout_seconds: float = 10.0,
    ) -> None:
        self.api_base = api_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._stream_messages: dict[str, str] = {}
        self._stream_guard = asyncio.Lock()

    async def parse(
        self,
        request: WebhookRequest,
        *,
        tenant: TenantConfig,
        binding: ChannelBindingConfig,
        secrets: CompositeSecretResolver,
    ) -> ParsedWebhook:
        reference = binding.credential_refs.get("webhook_secret")
        if reference is None:
            raise SignatureError("Telegram webhook secret is not configured")
        expected = await secrets.resolve(reference)
        presented = request.headers.get("x-telegram-bot-api-secret-token", "")
        if not hmac.compare_digest(presented.encode(), expected.encode()):
            raise SignatureError("invalid Telegram webhook signature")
        try:
            update = json.loads(request.body)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise UnsupportedMessage("invalid Telegram JSON") from exc
        if not isinstance(update, dict) or "update_id" not in update:
            raise UnsupportedMessage("Telegram update_id is required")

        message: dict[str, Any] | None = None
        text_override: str | None = None
        update_type = "message"
        for key in ("message", "edited_message", "channel_post", "edited_channel_post"):
            if isinstance(update.get(key), dict):
                message = update[key]
                update_type = key
                break
        if message is None and isinstance(update.get("callback_query"), dict):
            callback = update["callback_query"]
            message = callback.get("message")
            text_override = callback.get("data", "")
            update_type = "callback_query"
        if not isinstance(message, dict):
            return ParsedWebhook((), WebhookAck(body=b"ok"))

        chat = message.get("chat") or {}
        sender = (
            (update.get("callback_query") or {}).get("from")
            or message.get("from")
            or message.get("sender_chat")
            or {}
        )
        if "id" not in chat or "id" not in sender:
            raise UnsupportedMessage("Telegram chat and sender IDs are required")
        raw_chat_type = str(chat.get("type", "private"))
        chat_type = {
            "private": ChatType.DIRECT,
            "group": ChatType.GROUP,
            "supergroup": ChatType.GROUP,
            "channel": ChatType.CHANNEL,
        }.get(raw_chat_type, ChatType.DIRECT)
        attachments: list[Attachment] = []
        photos = message.get("photo") or []
        if photos:
            largest = max(photos, key=lambda item: item.get("file_size", 0))
            attachments.append(
                Attachment(
                    kind="image",
                    external_id=str(largest["file_id"]),
                    size_bytes=largest.get("file_size"),
                )
            )
        for field, kind in (("document", "file"), ("audio", "audio"), ("video", "video")):
            item = message.get(field)
            if isinstance(item, dict) and item.get("file_id"):
                attachments.append(
                    Attachment(
                        kind=kind,
                        external_id=str(item["file_id"]),
                        filename=item.get("file_name"),
                        mime_type=item.get("mime_type"),
                        size_bytes=item.get("file_size"),
                    )
                )
        occurred = (
            datetime.fromtimestamp(int(message.get("date", 0)), UTC)
            if message.get("date")
            else datetime.now(UTC)
        )
        envelope = InboundEnvelope(
            message_id=str(update["update_id"]),
            tenant_id=tenant.tenant_id,
            app_id=binding.app_id,
            binding_id=binding.binding_id,
            channel=ChannelType.TELEGRAM,
            external_account_id=binding.external_account_id,
            external_user_id=str(sender["id"]),
            external_chat_id=str(chat["id"]),
            chat_type=chat_type,
            thread_id=str(message["message_thread_id"]) if message.get("message_thread_id") else None,
            text=str(
                text_override
                if text_override is not None
                else message.get("text") or message.get("caption") or ""
            ),
            attachments=tuple(attachments),
            occurred_at=occurred,
            metadata={
                "platform_message_id": str(message.get("message_id", "")),
                "update_type": update_type,
                "language_code": sender.get("language_code"),
            },
        )
        return ParsedWebhook((envelope,), WebhookAck(body=b"ok"))

    async def deliver(
        self,
        message: OutboundMessage,
        *,
        tenant: TenantConfig,
        binding: ChannelBindingConfig,
        secrets: CompositeSecretResolver,
    ) -> DeliveryResult:
        del tenant
        token_ref = binding.credential_refs.get("bot_token")
        if token_ref is None:
            raise DeliveryError("Telegram bot token is not configured")
        token = await secrets.resolve(token_ref)
        message_ids: list[str] = []
        chunks = (
            split_text(message.text, max_chars=TELEGRAM_TEXT_MAX_CHARS)
            if message.text or message.cards
            else ()
        )
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            for index, chunk in enumerate(chunks):
                payload: dict[str, Any] = {
                    "chat_id": message.external_chat_id,
                    "text": chunk or " ",
                    "disable_web_page_preview": True,
                }
                if message.reply_to_message_id and index == 0:
                    payload["reply_parameters"] = {"message_id": message.reply_to_message_id}
                if message.cards and index == len(chunks) - 1:
                    buttons = message.cards[0].get("buttons", [])
                    if buttons:
                        payload["reply_markup"] = {
                            "inline_keyboard": [
                                [
                                    {
                                        key: button[key]
                                        for key in ("text", "url", "callback_data")
                                        if key in button
                                    }
                                    for button in row
                                ]
                                for row in buttons
                            ]
                        }
                method = "sendMessage"
                existing_id: str | None = None
                if message.stream_key:
                    async with self._stream_guard:
                        existing_id = self._stream_messages.get(message.stream_key)
                    if existing_id:
                        method = "editMessageText"
                        payload["message_id"] = existing_id
                response = await client.post(f"{self.api_base}/bot{token}/{method}", json=payload)
                try:
                    body = response.json()
                except ValueError as exc:
                    raise DeliveryError("Telegram returned a non-JSON response") from exc
                if response.status_code == 429:
                    raise RateLimited(float(body.get("parameters", {}).get("retry_after", 1)))
                if response.status_code >= 500:
                    raise DeliveryError("Telegram is temporarily unavailable")
                if not response.is_success or not body.get("ok"):
                    raise DeliveryError(f"Telegram delivery failed with status {response.status_code}")
                external_id = str(body.get("result", {}).get("message_id", existing_id or ""))
                message_ids.append(external_id)
                if message.stream_key:
                    async with self._stream_guard:
                        if message.is_final:
                            self._stream_messages.pop(message.stream_key, None)
                        elif external_id:
                            self._stream_messages[message.stream_key] = external_id
            media_methods = {
                "image": ("sendPhoto", "photo"),
                "file": ("sendDocument", "document"),
                "audio": ("sendAudio", "audio"),
                "video": ("sendVideo", "video"),
            }
            for attachment in message.attachments:
                method, field = media_methods[attachment.kind]
                payload = {
                    "chat_id": message.external_chat_id,
                    field: attachment.download_url or attachment.external_id,
                }
                if message.reply_to_message_id and not chunks:
                    payload["reply_parameters"] = {"message_id": message.reply_to_message_id}
                response = await client.post(f"{self.api_base}/bot{token}/{method}", json=payload)
                try:
                    body = response.json()
                except ValueError as exc:
                    raise DeliveryError("Telegram returned a non-JSON response") from exc
                if response.status_code == 429:
                    raise RateLimited(float(body.get("parameters", {}).get("retry_after", 1)))
                if response.status_code >= 500:
                    raise DeliveryError("Telegram is temporarily unavailable")
                if not response.is_success or not body.get("ok"):
                    raise DeliveryError(f"Telegram delivery failed with status {response.status_code}")
                message_ids.append(str(body.get("result", {}).get("message_id", "")))
        return DeliveryResult(tuple(message_ids))
