"""Enterprise WeCom intelligent-robot Channel Adapter."""

from datetime import datetime, timezone
import hashlib
from collections.abc import Mapping
from typing import Literal, Protocol
from uuid import UUID

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
from trpc_service.agent.recovery import ProviderOutcomeUnknown
from trpc_service.channels.media import ChannelMediaStore
from trpc_service.tenant.context import TenantContext


class WeComReplyClient(Protocol):
    """Narrow SDK boundary required by the provider-neutral adapter."""

    async def reply_stream(
        self,
        frame: dict[str, object],
        stream_id: str,
        content: str,
        finish: bool = False,
    ) -> Mapping[str, object]:
        ...

    async def download_file(self, url: str, aes_key: str | None = None) -> Mapping[str, object]:
        ...

    async def reply_media(
        self,
        frame: dict[str, object],
        media_type: str,
        media_id: str,
    ) -> Mapping[str, object]:
        ...

    async def upload_media(
        self,
        file_data: bytes,
        *,
        type: str,
        filename: str,
    ) -> Mapping[str, object]:
        ...


class WeComTransportRegistry:
    """Map a Channel Binding to its one authenticated long connection."""

    def __init__(self) -> None:
        self._clients: dict[UUID, WeComReplyClient] = {}

    def register(self, binding_id: UUID, client: WeComReplyClient) -> None:
        """Publish a connected client only within its owning process."""

        self._clients[binding_id] = client

    def unregister(self, binding_id: UUID, client: WeComReplyClient | None = None) -> None:
        """Remove a stale client without deleting a newer replacement."""

        current = self._clients.get(binding_id)
        if current is not None and (client is None or current is client):
            self._clients.pop(binding_id, None)

    def resolve(self, binding_id: UUID) -> WeComReplyClient:
        """Fail transiently until the binding has an authenticated connection."""

        try:
            return self._clients[binding_id]
        except KeyError as error:
            raise ConnectionError("WeCom binding has no active transport") from error


class _WeComFrameHeaders(BaseModel):
    model_config = ConfigDict(extra="ignore")

    req_id: str = Field(min_length=1, max_length=256)


class _WeComSender(BaseModel):
    model_config = ConfigDict(extra="ignore")

    userid: str = Field(min_length=1, max_length=256)


class _WeComText(BaseModel):
    model_config = ConfigDict(extra="ignore")

    content: str = Field(min_length=1, max_length=20_000)


class _WeComMedia(BaseModel):
    """Encrypted provider media reference delivered over the long connection."""

    model_config = ConfigDict(extra="ignore")

    url: str = Field(min_length=1, max_length=4000)
    aeskey: str | None = Field(default=None, min_length=1, max_length=512)
    filename: str | None = Field(default=None, max_length=255)


class _WeComMixedItem(BaseModel):
    """One provider-defined text or image item in a mixed message."""

    model_config = ConfigDict(extra="ignore")

    msgtype: Literal["text", "image"]
    text: _WeComText | None = None
    image: _WeComMedia | None = None

    @model_validator(mode="after")
    def require_item_payload(self) -> "_WeComMixedItem":
        if getattr(self, self.msgtype) is None:
            raise ValueError("WeCom mixed item requires its typed payload")
        return self


class _WeComMixed(BaseModel):
    """Official ``mixed.msg_item`` payload used for image captions."""

    model_config = ConfigDict(extra="ignore")

    msg_item: list[_WeComMixedItem] = Field(min_length=1, max_length=20)


class _WeComMessageBody(BaseModel):
    """Validated subset of the provider frame retained by the platform."""

    model_config = ConfigDict(extra="ignore")

    msgid: str = Field(min_length=1, max_length=256)
    aibotid: str = Field(min_length=1, max_length=256)
    chattype: Literal["single", "group"]
    chatid: str | None = Field(default=None, min_length=1, max_length=256)
    sender: _WeComSender = Field(alias="from")
    msgtype: Literal["text", "image", "mixed", "file", "voice", "video"]
    create_time: int | None = Field(default=None, ge=0)
    text: _WeComText | None = None
    image: _WeComMedia | None = None
    mixed: _WeComMixed | None = None
    file: _WeComMedia | None = None
    voice: _WeComMedia | None = None
    video: _WeComMedia | None = None

    @model_validator(mode="after")
    def require_message_fields(self) -> "_WeComMessageBody":
        if self.chattype == "group" and self.chatid is None:
            raise ValueError("WeCom group messages require chatid")
        if self.msgtype == "text" and self.text is None:
            raise ValueError("WeCom text message requires text content")
        if self.msgtype != "text" and getattr(self, self.msgtype) is None:
            raise ValueError("WeCom media message requires its media descriptor")
        return self


class _WeComMessageFrame(BaseModel):
    model_config = ConfigDict(extra="ignore")

    cmd: Literal["aibot_msg_callback"]
    headers: _WeComFrameHeaders
    body: _WeComMessageBody


class WeComChannelAdapter(ChannelAdapter):
    """Normalize WeCom frames while keeping its SDK outside Agent code."""

    def __init__(
        self,
        transports: WeComTransportRegistry,
        media_store: ChannelMediaStore | None = None,
    ) -> None:
        self._transports = transports
        self._media_store = media_store

    @property
    def channel_type(self) -> str:
        return "wecom"

    @staticmethod
    def bot_id(binding: ChannelBindingConfig) -> str:
        """Return the validated public robot identity from one binding."""

        value = binding.account_config.get("bot_id")
        if not isinstance(value, str) or not value.strip():
            raise ValueError("WeCom binding requires bot_id")
        return value.strip()

    @classmethod
    def _require_binding(cls, binding: ChannelBindingConfig) -> str:
        if binding.channel_type != "wecom":
            raise ValueError("WeCom adapter requires a wecom channel binding")
        return cls.bot_id(binding)

    async def decode(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
    ) -> IncomingMessage:
        """Validate one SDK frame and expose only provider-neutral fields."""

        return await self._decode(envelope, binding, context=None)

    async def decode_for_tenant(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
        context: TenantContext,
    ) -> IncomingMessage:
        """Decode production media using the request's versioned storage profile."""

        return await self._decode(envelope, binding, context=context)

    async def _decode(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
        *,
        context: TenantContext | None,
    ) -> IncomingMessage:
        """Share frame parsing while production supplies its immutable context."""

        expected_bot_id = self._require_binding(binding)
        frame = _WeComMessageFrame.model_validate_json(envelope.body)
        if frame.body.aibotid != expected_bot_id:
            raise PermissionError("WeCom message does not belong to this binding")
        user_id = frame.body.sender.userid
        conversation_id = (f"single:{user_id}"
                           if frame.body.chattype == "single" else f"group:{frame.body.chatid}")
        occurred_at = (datetime.now(timezone.utc) if frame.body.create_time is None else
                       datetime.fromtimestamp(frame.body.create_time, timezone.utc))
        # The stream ID is deterministic across callback retries and process
        # restarts, so replaying the same provider message updates one reply.
        stream_seed = f"{binding.binding_id}:{frame.body.msgid}".encode()
        stream_id = f"trpc-{hashlib.sha256(stream_seed).hexdigest()[:32]}"
        text = frame.body.text.content if frame.body.text is not None else None
        kind = MessageKind.TEXT
        media_items: list[tuple[str, _WeComMedia]] = []
        if frame.body.msgtype == "mixed":
            if frame.body.mixed is None:
                raise ValueError("WeCom mixed message descriptor is missing")
            # Preserve item order for captions while every image is stored as
            # a tenant-scoped artifact and later supplied to the vision model.
            text_parts = [
                item.text.content for item in frame.body.mixed.msg_item
                if item.msgtype == "text" and item.text is not None
            ]
            text = "\n".join(text_parts).strip() or None
            media_items = [("image", item.image) for item in frame.body.mixed.msg_item
                           if item.msgtype == "image" and item.image is not None]
            kind = MessageKind.IMAGE if media_items else MessageKind.TEXT
        elif frame.body.msgtype != "text":
            descriptor = getattr(frame.body, frame.body.msgtype)
            if not isinstance(descriptor, _WeComMedia):
                raise ValueError("WeCom media descriptor is missing")
            media_items = [(frame.body.msgtype, descriptor)]
            kind = MessageKind.IMAGE if frame.body.msgtype == "image" else MessageKind.FILE

        artifact_refs: tuple[str, ...] = ()
        provider_media = [descriptor.model_dump(mode="json") for _, descriptor in media_items]
        if media_items and self._media_store is not None:
            configured_limit = binding.capabilities.get("max_inbound_media_bytes", 50 * 1024 * 1024)
            if (isinstance(configured_limit, bool) or not isinstance(configured_limit, int)
                    or configured_limit < 1):
                raise ValueError("WeCom media size limit must be a positive integer")
            stored: list[str] = []
            total_bytes = 0
            client = self._transports.resolve(binding.binding_id)
            for index, (media_type, descriptor) in enumerate(media_items):
                downloaded = await client.download_file(descriptor.url, descriptor.aeskey)
                content = downloaded.get("buffer")
                if not isinstance(content, bytes):
                    raise RuntimeError("WeCom media download returned invalid content")
                total_bytes += len(content)
                if total_bytes > configured_limit:
                    raise ValueError("WeCom inbound media exceeds the configured size limit")
                downloaded_name = downloaded.get("filename")
                filename = (descriptor.filename
                            or (downloaded_name if isinstance(downloaded_name, str) else None)
                            or f"{frame.body.msgid}-{index}-{media_type}")
                stored.append(await self._media_store.put(
                    binding,
                    principal_id=f"{binding.tenant_id}:wecom:{user_id}",
                    message_id=frame.body.msgid,
                    content=content,
                    filename=filename,
                    media_type=f"application/x-wecom-{media_type}",
                    context=context,
                ))
            artifact_refs = tuple(stored)
        return IncomingMessage(
            external_message_id=frame.body.msgid,
            principal_id=f"{binding.tenant_id}:wecom:{user_id}",
            conversation_id=conversation_id,
            kind=kind,
            occurred_at=occurred_at,
            text=text,
            artifact_refs=artifact_refs,
            attributes={
                "chat_type": frame.body.chattype,
                "conversation_kind": ("direct" if frame.body.chattype == "single" else "group"),
                "provider_media": provider_media,
                # This context is persisted with the task and copied into the
                # reply Outbox. It intentionally excludes response_url and all
                # credentials.
                "reply_context": {
                    "req_id": frame.headers.req_id,
                    "stream_id": stream_id,
                },
            },
        )

    async def acknowledge(
        self,
        envelope: IncomingEnvelope,
        binding: ChannelBindingConfig,
    ) -> ChannelResponse:
        """Return a logical ACK; the long-connection service sends the frame."""

        del envelope
        self._require_binding(binding)
        return ChannelResponse(status_code=202)

    async def send_progress(
        self,
        incoming: IncomingMessage,
        binding: ChannelBindingConfig,
    ) -> None:
        """Open the reply stream after the inbound task is durably queued."""

        self._require_binding(binding)
        raw_context = incoming.attributes.get("reply_context")
        if not isinstance(raw_context, Mapping):
            raise ValueError("WeCom progress reply requires reply_context")
        req_id = raw_context.get("req_id")
        stream_id = raw_context.get("stream_id")
        if not isinstance(req_id, str) or not isinstance(stream_id, str):
            raise ValueError("WeCom progress reply_context is invalid")
        content = binding.account_config.get("thinking_message", "正在思考...")
        if not isinstance(content, str) or not content.strip():
            return
        await self._transports.resolve(binding.binding_id).reply_stream(
            {"headers": {
                "req_id": req_id
            }},
            stream_id,
            content.strip(),
            finish=False,
        )

    async def deliver(
        self,
        message: OutgoingMessage,
        binding: ChannelBindingConfig,
    ) -> DeliveryReceipt:
        """Finish one provider stream through the binding-owned connection."""

        self._require_binding(binding)
        raw_context = message.attributes.get("reply_context")
        if not isinstance(raw_context, Mapping):
            raise ValueError("WeCom delivery requires reply_context")
        req_id = raw_context.get("req_id")
        stream_id = raw_context.get("stream_id")
        if (not isinstance(req_id, str) or not req_id.strip() or not isinstance(stream_id, str)
                or not stream_id.strip()):
            raise ValueError("WeCom delivery reply_context is invalid")
        client = self._transports.resolve(binding.binding_id)
        try:
            if message.kind is MessageKind.TEXT and message.text is not None:
                await client.reply_stream(
                    {"headers": {
                        "req_id": req_id
                    }},
                    stream_id,
                    message.text,
                    finish=True,
                )
            elif message.kind in {MessageKind.IMAGE, MessageKind.FILE} and message.artifact_refs:
                media_type = "image" if message.kind is MessageKind.IMAGE else "file"
                media_id = message.artifact_refs[0]
                if self._media_store is not None and not media_id.startswith("media_"):
                    content = await self._media_store.read(binding, media_id)
                    filename_value = message.attributes.get("filename")
                    filename = (filename_value if isinstance(filename_value, str) else
                                f"{message.delivery_id}.{media_type}")
                    uploaded = await client.upload_media(
                        content,
                        type=media_type,
                        filename=filename,
                    )
                    uploaded_id = uploaded.get("media_id")
                    if not isinstance(uploaded_id, str) or not uploaded_id:
                        raise RuntimeError("WeCom media upload returned no media ID")
                    media_id = uploaded_id
                await client.reply_media(
                    {"headers": {
                        "req_id": req_id
                    }},
                    media_type,
                    media_id,
                )
            else:
                raise ValueError(f"WeCom does not support outgoing kind: {message.kind.value}")
        except Exception as error:
            # The pinned SDK reports an ACK timeout as a generic exception even
            # though the provider may already have accepted the reply.
            if "reply ack timeout" in str(error).casefold():
                raise ProviderOutcomeUnknown("WeCom reply acknowledgement timed out") from error
            raise
        return DeliveryReceipt(
            delivery_id=message.delivery_id,
            external_delivery_id=req_id,
            accepted_at=datetime.now(timezone.utc),
        )
