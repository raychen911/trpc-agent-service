# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Telegram Bot API webhook adapter."""

from __future__ import annotations

import hmac
from typing import Any

import httpx

from trpc_service.channels.base import ChannelAuthenticationError
from trpc_service.channels.base import DeliveryResult
from trpc_service.channels.base import UnsupportedMessageError
from trpc_service.channels.base import ChannelTransportError
from trpc_service.config import ChannelType
from trpc_service.gateway.models import NormalizedInboundMessage
from trpc_service.gateway.models import OutboundMessage
from trpc_service.gateway.models import Attachment
from trpc_service.gateway.models import MessageKind


class TelegramChannelAdapter:
    """Validate Telegram's secret header and map Update/message fields."""

    def __init__(self,
                 bot_token: str,
                 webhook_secret: str,
                 client: httpx.AsyncClient | None = None,
                 *,
                 artifacts=None) -> None:
        self._token = bot_token
        self._secret = webhook_secret
        self._client = client or httpx.AsyncClient(timeout=15)
        self._owns_client = client is None
        self._artifacts = artifacts

    async def normalize(self, binding_id: str, payload: dict[str, Any], headers: dict[str,
                                                                                      str]) -> NormalizedInboundMessage:
        supplied = headers.get("x-telegram-bot-api-secret-token", "")
        if not self._secret or not hmac.compare_digest(supplied, self._secret):
            raise ChannelAuthenticationError("invalid Telegram webhook secret")
        message = payload.get("message") or payload.get("edited_message")
        if not isinstance(message, dict):
            raise UnsupportedMessageError("Telegram update does not contain a supported message")
        chat = message.get("chat") or {}
        sender = message.get("from") or {}
        text = message.get("text") or message.get("caption") or ""
        attachments: list[Attachment] = []
        photos = message.get("photo") or []
        if photos:
            photo = photos[-1]
            attachments.append(
                Attachment(
                    attachment_id=str(photo.get("file_unique_id") or photo.get("file_id")),
                    kind=MessageKind.IMAGE,
                    name="telegram-photo.jpg",
                    mime_type="image/jpeg",
                    size_bytes=int(photo.get("file_size") or 0),
                    source_url=f"telegram://file/{photo.get('file_id', '')}",
                ))
        document = message.get("document")
        if isinstance(document, dict):
            attachments.append(
                Attachment(
                    attachment_id=str(document.get("file_unique_id") or document.get("file_id")),
                    kind=MessageKind.FILE,
                    name=str(document.get("file_name") or "telegram-document.bin"),
                    mime_type=str(document.get("mime_type") or "application/octet-stream"),
                    size_bytes=int(document.get("file_size") or 0),
                    source_url=f"telegram://file/{document.get('file_id', '')}",
                ))
        if not text and not attachments:
            raise UnsupportedMessageError("Telegram message has no supported text, photo or document")
        update_id = payload.get("update_id")
        if update_id is None or "id" not in chat or "id" not in sender:
            raise UnsupportedMessageError("Telegram update is missing update, chat or sender identity")
        chat_type = str(chat.get("type", "private"))
        return NormalizedInboundMessage(
            message_id=str(update_id),
            binding_id=binding_id,
            channel=ChannelType.TELEGRAM,
            external_user_id=str(sender["id"]),
            external_conversation_id=str(chat["id"]),
            text=str(text),
            kind=attachments[0].kind if attachments and not text else MessageKind.TEXT,
            attachments=attachments,
            is_group=chat_type in {"group", "supergroup"},
            reply_to_message_id=str(message.get("message_id", "")),
            metadata={
                "chat_type": chat_type,
                "telegram_message_id": message.get("message_id")
            },
        )

    async def deliver(self, message: OutboundMessage) -> DeliveryResult:
        if not self._token:
            return DeliveryResult(delivered=False, retryable=False, error_code="telegram_token_missing")
        try:
            external_id = ""
            chunks = [message.text[index:index + 4096] for index in range(0, len(message.text), 4096)]
            for chunk in chunks:
                result = await self._post("sendMessage",
                                          json={
                                              "chat_id": message.external_conversation_id,
                                              "text": chunk
                                          })
                external_id = str(result.get("message_id", external_id))
            for attachment in message.attachments:
                if self._artifacts is None:
                    raise ChannelTransportError("telegram_artifact_store_missing")
                metadata, content = await self._artifacts.get(message.tenant_id, attachment.attachment_id)
                is_image = metadata.mime_type.startswith("image/")
                method, field = ("sendPhoto", "photo") if is_image else ("sendDocument", "document")
                result = await self._post(method,
                                          data={"chat_id": message.external_conversation_id},
                                          files={field: (metadata.original_name, content, metadata.mime_type)})
                external_id = str(result.get("message_id", external_id))
            if not chunks and not message.attachments:
                raise ChannelTransportError("telegram_empty_message")
            return DeliveryResult(delivered=True, external_message_id=external_id)
        except ChannelTransportError as error:
            return DeliveryResult(delivered=False,
                                  retryable=error.retryable,
                                  uncertain=error.uncertain,
                                  error_code=error.code,
                                  retry_after_seconds=error.retry_after_seconds)

    async def _post(self, method: str, **kwargs) -> dict[str, Any]:
        try:
            response = await self._client.post(f"https://api.telegram.org/bot{self._token}/{method}", **kwargs)
        except (httpx.ConnectError, httpx.ConnectTimeout):
            raise ChannelTransportError("telegram_connect_failed", retryable=True) from None
        except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError):
            raise ChannelTransportError("telegram_delivery_unknown", uncertain=True) from None
        try:
            data = response.json()
        except ValueError:
            data = {}
        retry_after = 0.0
        try:
            retry_after = float((data.get("parameters") or {}).get("retry_after", 0))
        except (TypeError, ValueError):
            pass
        code = int(data.get("error_code") or response.status_code or 0)
        if response.status_code == 429 or code == 429:
            raise ChannelTransportError("telegram_rate_limited", retryable=True,
                                        retry_after_seconds=retry_after) from None
        if response.status_code >= 500 or code >= 500:
            raise ChannelTransportError(f"telegram_http_{code}", retryable=True) from None
        if response.status_code >= 400 or data.get("ok") is False:
            raise ChannelTransportError(f"telegram_api_{code or 'error'}") from None
        if data.get("ok") is not True or not isinstance(data.get("result"), dict):
            raise ChannelTransportError("telegram_invalid_response", retryable=True) from None
        return data["result"]

    async def download_file(self, file_id: str) -> tuple[bytes, str]:
        """Resolve getFile then download bytes; callers persist them in AttachmentStore."""
        result = await self._post("getFile", params={"file_id": file_id})
        file_path = str(result.get("file_path", ""))
        if not file_path:
            raise UnsupportedMessageError("Telegram getFile returned no file_path")
        parts = []
        total = 0
        async with self._client.stream("GET",
                                       f"https://api.telegram.org/file/bot{self._token}/{file_path}") as response:
            response.raise_for_status()
            async for chunk in response.aiter_bytes():
                total += len(chunk)
                if total > 10 * 1024 * 1024:
                    raise UnsupportedMessageError("Telegram attachment exceeds 10 MiB")
                parts.append(chunk)
        return b"".join(parts), file_path

    async def download_attachment(self, attachment: Attachment) -> tuple[bytes, str, str]:
        prefix = "telegram://file/"
        if not attachment.source_url.startswith(prefix):
            raise UnsupportedMessageError("invalid Telegram attachment reference")
        content, file_path = await self.download_file(attachment.source_url.removeprefix(prefix))
        return content, attachment.mime_type, attachment.name or file_path.rsplit("/", 1)[-1]

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()
