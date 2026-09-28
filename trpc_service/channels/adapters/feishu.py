"""Feishu/Lark Channel Adapter over the official long-connection SDK."""

import asyncio
from datetime import datetime, timezone
from collections.abc import Mapping
from typing import Literal, Protocol
from uuid import UUID
import warnings

from pydantic import BaseModel, ConfigDict, Field, model_validator

from trpc_service.channels.contracts import (
    ChannelAdapter,
    ChannelBindingConfig,
    ChannelResponse,
    DeliveryReceipt,
    IncomingEnvelope,
    IncomingMessage,
    MessageKind,
    OutgoingMessage,
)
from trpc_service.channels.media import ChannelMediaStore


def _buffer_media_source(content: bytes) -> object:
    """Create an SDK media source behind a narrowly scoped compatibility guard."""

    with warnings.catch_warnings():
        # lark-channel-sdk 1.4.0 vendors protobuf code that still calls
        # utcfromtimestamp during import on Python 3.12. Keep project-owned
        # deprecations strict while isolating this known third-party warning.
        warnings.filterwarnings(
            "ignore",
            message=r"datetime\.datetime\.utcfromtimestamp\(\) is deprecated.*",
            category=DeprecationWarning,
            module=r"lark_channel\.ws\.pb\.google\.protobuf.*",
        )
        from lark_channel import MediaSource

    return MediaSource(kind="buffer", buffer=content)


class FeishuSendResult(Protocol):
    """Small SDK result surface retained by the project boundary."""

    success: bool
    message_id: str | None
    error: object | None


class FeishuStreamController(Protocol):
    """Append-only surface exposed by the official CardKit stream."""

    async def append(self, content: str) -> None:
        ...


class FeishuClient(Protocol):
    """Official SDK operations used by provider-neutral delivery."""

    async def send(
        self,
        to: str,
        message: object,
        opts: object | None = None,
    ) -> FeishuSendResult:
        ...

    async def upload_media(
        self,
        source: object,
        *,
        kind: str,
        file_name: str | None = None,
    ) -> str:
        ...

    async def stream(
        self,
        to: str,
        spec: dict[str, object],
        opts: object | None = None,
    ) -> FeishuSendResult:
        ...


class FeishuTransportRegistry:
    """Resolve one active SDK channel for each Feishu binding."""

    def __init__(self) -> None:
        self._clients: dict[UUID, FeishuClient] = {}

    def register(self, binding_id: UUID, client: FeishuClient) -> None:
        self._clients[binding_id] = client

    def unregister(self, binding_id: UUID, client: FeishuClient | None = None) -> None:
        current = self._clients.get(binding_id)
        if current is not None and (client is None or current is client):
            self._clients.pop(binding_id, None)

    def resolve(self, binding_id: UUID) -> FeishuClient:
        try:
            return self._clients[binding_id]
        except KeyError as error:
            raise ConnectionError("Feishu binding has no active transport") from error


class _FeishuResource(BaseModel):
    model_config = ConfigDict(extra="ignore")

    type: Literal["image", "file", "audio", "video", "sticker"]
    file_key: str = Field(min_length=1, max_length=512)
    file_name: str | None = Field(default=None, max_length=255)
    duration_ms: int | None = Field(default=None, ge=0)
    cover_image_key: str | None = Field(default=None, max_length=512)


class _FeishuNormalizedFrame(BaseModel):
    """Stable DTO emitted by the SDK wrapper before core normalization."""

    model_config = ConfigDict(extra="ignore")

    message_id: str = Field(min_length=1, max_length=512)
    create_time: int = Field(ge=0)
    chat_id: str = Field(min_length=1, max_length=512)
    chat_type: Literal["p2p", "group", "topic", "unknown"]
    thread_id: str | None = Field(default=None, min_length=1, max_length=512)
    sender_id: str = Field(min_length=1, max_length=512)
    sender_name: str | None = Field(default=None, max_length=255)
    content_text: str = Field(default="", max_length=100_000)
    raw_content_type: str = Field(min_length=1, max_length=80)
    mentioned_bot: bool = False
    resources: list[_FeishuResource] = Field(default_factory=list, max_length=20)
    artifact_refs: list[str] = Field(default_factory=list, max_length=20)

    @model_validator(mode="after")
    def require_group_mention_marker(self) -> "_FeishuNormalizedFrame":
        # The official SDK policy decides whether an unmentioned group message
        # reaches this DTO. Retaining the marker makes that decision auditable.
        return self


class FeishuChannelAdapter(ChannelAdapter):
    """Translate normalized Feishu SDK events into the shared IM contract."""

    def __init__(
        self,
        transports: FeishuTransportRegistry,
        media_store: ChannelMediaStore | None = None,
    ) -> None:
        self._transports = transports
        self._media_store = media_store

    @property
    def channel_type(self) -> str:
        return "feishu"

    @staticmethod
    def app_id(binding: ChannelBindingConfig) -> str:
        value = binding.account_config.get("app_id")
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Feishu binding requires app_id")
        return value.strip()

    @classmethod
    def _require_binding(cls, binding: ChannelBindingConfig) -> None:
        if binding.channel_type != "feishu":
            raise ValueError("Feishu adapter requires a feishu channel binding")
        cls.app_id(binding)

    async def decode(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
    ) -> IncomingMessage:
        self._require_binding(binding)
        frame = _FeishuNormalizedFrame.model_validate_json(envelope.body)
        if frame.thread_id:
            conversation_id = f"thread:{frame.thread_id}"
            conversation_kind = "thread"
        elif frame.chat_type == "p2p":
            conversation_id = f"single:{frame.chat_id}"
            conversation_kind = "direct"
        else:
            conversation_id = f"group:{frame.chat_id}"
            conversation_kind = "group"
        kind = self._message_kind(frame)
        return IncomingMessage(
            external_message_id=frame.message_id,
            principal_id=frame.sender_id,
            conversation_id=conversation_id,
            kind=kind,
            occurred_at=datetime.fromtimestamp(frame.create_time / 1000, timezone.utc),
            text=frame.content_text or None,
            artifact_refs=tuple(frame.artifact_refs),
            attributes={
                "display_name": frame.sender_name or "",
                "conversation_kind": conversation_kind,
                "mentioned_bot": frame.mentioned_bot,
                "provider_media":
                [resource.model_dump(mode="json") for resource in frame.resources],
                "reply_context": {
                    "chat_id": frame.chat_id,
                    "message_id": frame.message_id,
                    "thread_id": frame.thread_id,
                },
            },
        )

    async def acknowledge(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
    ) -> ChannelResponse:
        del envelope
        self._require_binding(binding)
        return ChannelResponse(status_code=202)

    async def send_progress(
        self,
        incoming: IncomingMessage,
        binding: ChannelBindingConfig,
    ) -> None:
        """Send a visible receipt only after the inbound task is durable."""

        self._require_binding(binding)
        raw_context = incoming.attributes.get("reply_context")
        if not isinstance(raw_context, Mapping):
            raise ValueError("Feishu progress reply requires reply_context")
        chat_id = raw_context.get("chat_id")
        reply_to = raw_context.get("message_id")
        if not isinstance(chat_id, str) or not chat_id:
            raise ValueError("Feishu progress reply_context requires chat_id")
        content = binding.account_config.get("thinking_message", "正在思考...")
        if not isinstance(content, str) or not content.strip():
            return
        opts = ({"reply_to": reply_to} if isinstance(reply_to, str) and reply_to else None)
        result = await self._transports.resolve(binding.binding_id).send(
            chat_id,
            {"markdown": content.strip()},
            opts,
        )
        if not result.success:
            raise RuntimeError(f"Feishu progress delivery failed: {type(result.error).__name__}")

    async def deliver(
        self,
        message: OutgoingMessage,
        binding: ChannelBindingConfig,
    ) -> DeliveryReceipt:
        self._require_binding(binding)
        raw_context = message.attributes.get("reply_context")
        if not isinstance(raw_context, Mapping):
            raise ValueError("Feishu delivery requires reply_context")
        chat_id = raw_context.get("chat_id")
        if not isinstance(chat_id, str) or not chat_id:
            raise ValueError("Feishu reply_context requires chat_id")
        reply_to = raw_context.get("message_id")
        opts = ({"reply_to": reply_to} if isinstance(reply_to, str) and reply_to else None)
        client = self._transports.resolve(binding.binding_id)
        if (message.kind is MessageKind.TEXT and message.text is not None
                and binding.capabilities.get("streaming") is True):
            result = await self._stream_text(client, chat_id, message.text, opts)
        else:
            payload = await self._outgoing_payload(client, message, binding)
            result = await client.send(chat_id, payload, opts)
        if not result.success:
            raise RuntimeError(f"Feishu delivery failed: {type(result.error).__name__}")
        external_id = result.message_id or message.delivery_id
        return DeliveryReceipt(
            delivery_id=message.delivery_id,
            external_delivery_id=external_id,
            accepted_at=datetime.now(timezone.utc),
        )

    @staticmethod
    async def _stream_text(
        client: FeishuClient,
        chat_id: str,
        text: str,
        opts: object | None,
    ) -> FeishuSendResult:
        """Render text incrementally through the official CardKit stream."""

        async def produce(controller: FeishuStreamController) -> None:
            # CardKit owns ordering, update throttling, and finalization. Small
            # chunks make progress visible without issuing parallel API calls.
            chunk_size = 64
            for offset in range(0, len(text), chunk_size):
                await controller.append(text[offset:offset + chunk_size])
                await asyncio.sleep(0)

        return await client.stream(chat_id, {"markdown": produce}, opts)

    @staticmethod
    def _message_kind(frame: _FeishuNormalizedFrame) -> MessageKind:
        if frame.raw_content_type == "image":
            return MessageKind.IMAGE
        if frame.raw_content_type in {"file", "audio", "media", "sticker"}:
            return MessageKind.FILE
        if frame.raw_content_type == "interactive":
            return MessageKind.CARD
        return MessageKind.TEXT

    async def _outgoing_payload(
        self,
        client: FeishuClient,
        message: OutgoingMessage,
        binding: ChannelBindingConfig,
    ) -> dict[str, object]:
        if message.kind is MessageKind.TEXT and message.text is not None:
            return {"markdown": message.text}
        if message.kind is MessageKind.CARD:
            card = message.attributes.get("card")
            if not isinstance(card, Mapping):
                raise ValueError("Feishu card delivery requires card attributes")
            return {"card": dict(card)}
        if message.kind in {MessageKind.IMAGE, MessageKind.FILE} and message.artifact_refs:
            key = message.artifact_refs[0]
            media_name = "image" if message.kind is MessageKind.IMAGE else "file"
            if not key.startswith(("img_", "file_")):
                if self._media_store is None:
                    raise ValueError("Feishu local artifact delivery requires a media store")
                content = await self._media_store.read(binding, key)
                filename_value = message.attributes.get("filename")
                filename = filename_value if isinstance(filename_value, str) else None
                key = await client.upload_media(
                    _buffer_media_source(content),
                    kind=media_name,
                    file_name=filename,
                )
            return {media_name: {"source": key}}
        raise ValueError(f"Feishu does not support outgoing kind: {message.kind.value}")
