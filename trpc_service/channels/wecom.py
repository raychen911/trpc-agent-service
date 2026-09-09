# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Enterprise WeChat intelligent-bot WebSocket integration seam.

The official/community SDK owns authentication, heartbeat, reconnect and frame
acknowledgement.  This adapter receives its decoded message dictionary and an
SDK-backed sender callback, keeping those transport details out of the Worker.
"""

from __future__ import annotations

from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any
import hashlib

from trpc_service.channels.base import DeliveryResult
from trpc_service.channels.base import ChannelAuthenticationError
from trpc_service.channels.base import UnsupportedMessageError
from trpc_service.channels.base import ChannelTransportError
from trpc_service.config import ChannelType
from trpc_service.gateway.models import NormalizedInboundMessage
from trpc_service.gateway.models import OutboundMessage
from trpc_service.gateway.models import Attachment
from trpc_service.gateway.models import MessageKind

WeComSender = Callable[[str, str, str], Awaitable[str]]
WeComStreamSender = Callable[[str, str, bool], Awaitable[str]]


def _utf8_chunks(text: str, max_bytes: int) -> list[str]:
    """Split without cutting a Unicode code point or exceeding a byte limit."""
    chunks: list[str] = []
    current = ""
    for character in text:
        if current and len((current + character).encode("utf-8")) > max_bytes:
            chunks.append(current)
            current = ""
        current += character
    if current or not chunks:
        chunks.append(current)
    return chunks


class WeComChannelAdapter:
    """Map an SDK-decoded intelligent-bot message to the internal contract."""

    def __init__(self,
                 sender: WeComSender | None = None,
                 stream_sender: WeComStreamSender | None = None,
                 *,
                 expected_bot_id: str = "",
                 downloader: Callable[[str, str], Awaitable[tuple[bytes, str]]] | None = None,
                 max_chunk_bytes: int = 2048) -> None:
        self._sender = sender
        self._stream_sender = stream_sender
        self._expected_bot_id = expected_bot_id
        self._downloader = downloader
        self._max_chunk_bytes = max(4, max_chunk_bytes)
        self._pending_media: dict[str, tuple[str, str, str]] = {}

    async def normalize(self, binding_id: str, payload: dict[str, Any], headers: dict[str,
                                                                                      str]) -> NormalizedInboundMessage:
        del headers
        if payload.get("cmd") != "aibot_msg_callback":
            raise UnsupportedMessageError("WeCom frame cmd must be aibot_msg_callback")
        body = payload.get("body")
        frame_headers = payload.get("headers")
        if not isinstance(body, dict) or not isinstance(frame_headers, dict):
            raise UnsupportedMessageError("WeCom frame must contain headers and body")
        request_id = frame_headers.get("req_id")
        sender = body.get("from") if isinstance(body.get("from"), dict) else {}
        message_id = body.get("msgid")
        user_id = sender.get("userid")
        bot_id = body.get("aibotid")
        chat_type = str(body.get("chattype") or "")
        if self._expected_bot_id and bot_id != self._expected_bot_id:
            raise ChannelAuthenticationError("WeCom aibotid does not match channel binding")
        if chat_type not in {"single", "group"}:
            raise UnsupportedMessageError("WeCom chattype must be single or group")
        conversation_id = body.get("chatid") if chat_type == "group" else user_id
        text = ""
        msgtype = str(body.get("msgtype") or "")
        attachments: list[Attachment] = []
        media_items: list[tuple[str, MessageKind, dict[str, Any]]] = []
        if msgtype == "text":
            text = str((body.get("text") or {}).get("content") or "")
        elif msgtype == "voice":
            text = str((body.get("voice") or {}).get("content") or "")
        elif msgtype in {"image", "file"}:
            media_items.append(
                (msgtype, MessageKind.IMAGE if msgtype == "image" else MessageKind.FILE, body.get(msgtype) or {}))
        elif msgtype == "mixed":
            for item in (body.get("mixed") or {}).get("msg_item", []):
                if not isinstance(item, dict):
                    continue
                item_type = item.get("msgtype")
                if item_type == "text":
                    content = str((item.get("text") or {}).get("content") or "")
                    text = f"{text}\n{content}".strip()
                elif item_type == "image":
                    media_items.append(("image", MessageKind.IMAGE, item.get("image") or {}))
        else:
            raise UnsupportedMessageError(f"unsupported WeCom message type: {msgtype or 'missing'}")
        for index, (media_type, kind, media) in enumerate(media_items):
            url, aes_key = str(media.get("url") or ""), str(media.get("aeskey") or "")
            if not url or not aes_key:
                raise UnsupportedMessageError(f"WeCom {media_type} message is missing url or aeskey")
            reference = hashlib.sha256(f"{message_id}:{index}:{url}".encode()).hexdigest()
            name = str(media.get("filename") or f"wecom-{media_type}-{index}")
            self._pending_media[reference] = (url, aes_key, name)
            attachments.append(
                Attachment(attachment_id=reference,
                           kind=kind,
                           name=name,
                           mime_type="image/*" if kind == MessageKind.IMAGE else "application/octet-stream",
                           source_url=f"wecom://pending/{reference}"))
        if not all((request_id, message_id, bot_id, user_id, conversation_id)) or not (text or attachments):
            raise UnsupportedMessageError(
                "WeCom frame must contain req_id, msgid, aibotid, sender, conversation and supported content")
        return NormalizedInboundMessage(
            message_id=str(message_id),
            binding_id=binding_id,
            channel=ChannelType.WECOM,
            external_user_id=str(user_id),
            external_conversation_id=str(conversation_id),
            text=str(text or ""),
            kind=attachments[0].kind if attachments and not text else MessageKind.TEXT,
            attachments=attachments,
            is_group=chat_type in {"group", "group_chat"},
            reply_to_message_id=str(request_id),
            metadata={
                "chat_type": chat_type,
                "request_id": str(request_id),
                "aibotid": str(bot_id),
            },
        )

    async def download_attachment(self, attachment: Attachment) -> tuple[bytes, str, str]:
        prefix = "wecom://pending/"
        if not attachment.source_url.startswith(prefix) or self._downloader is None:
            raise UnsupportedMessageError("WeCom attachment downloader is not configured")
        reference = attachment.source_url.removeprefix(prefix)
        item = self._pending_media.pop(reference, None)
        if item is None:
            raise UnsupportedMessageError("WeCom attachment reference expired")
        url, aes_key, name = item
        content, downloaded_name = await self._downloader(url, aes_key)
        mime = "image/jpeg" if attachment.kind == MessageKind.IMAGE else "application/octet-stream"
        return content, mime, downloaded_name or name

    async def deliver(self, message: OutboundMessage) -> DeliveryResult:
        if message.attachments:
            return DeliveryResult(delivered=False,
                                  retryable=False,
                                  error_code="wecom_outbound_attachment_not_supported")
        chunks = _utf8_chunks(message.text, self._max_chunk_bytes)
        sent_chunks = 0
        try:
            # SDK stream updates replace/extend one reply rather than creating
            # independent messages.  A long answer is therefore sent as
            # bounded active messages instead of misusing delta chunks.
            if len(chunks) == 1 and message.reply_to_message_id and self._stream_sender is not None:
                try:
                    external_id = ""
                    for index, chunk in enumerate(chunks):
                        external_id = await self._stream_sender(message.reply_to_message_id, chunk,
                                                                index == len(chunks) - 1)
                        sent_chunks += 1
                    return DeliveryResult(delivered=True, external_message_id=external_id)
                except KeyError:
                    # Callback-frame TTL elapsed; active send remains valid.
                    if sent_chunks:
                        return DeliveryResult(delivered=False,
                                              uncertain=True,
                                              error_code="wecom_partial_delivery_unknown")
            if self._sender is None:
                return DeliveryResult(delivered=False, retryable=False, error_code="wecom_sender_not_configured")
            external_id = ""
            for chunk in chunks:
                external_id = await self._sender(message.external_conversation_id, chunk, message.reply_to_message_id)
                sent_chunks += 1
            return DeliveryResult(delivered=True, external_message_id=external_id)
        except ChannelTransportError as error:
            if sent_chunks:
                return DeliveryResult(delivered=False, uncertain=True, error_code="wecom_partial_delivery_unknown")
            return DeliveryResult(delivered=False,
                                  retryable=error.retryable,
                                  uncertain=error.uncertain,
                                  error_code=error.code,
                                  retry_after_seconds=error.retry_after_seconds)
        except Exception:
            if sent_chunks:
                return DeliveryResult(delivered=False, uncertain=True, error_code="wecom_partial_delivery_unknown")
            raise

    async def close(self) -> None:
        return None
