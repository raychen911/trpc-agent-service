"""Channel-neutral message models used at the IM boundary."""

from __future__ import annotations

from datetime import datetime
from enum import Enum

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import model_validator


class ChannelType(str, Enum):
    """IM channel types supported by the platform roadmap."""

    WEB = "web"
    WECOM = "wecom"
    TELEGRAM = "telegram"
    FEISHU = "feishu"


class ConversationType(str, Enum):
    """Conversation scopes that affect session identity."""

    DIRECT = "direct"
    GROUP = "group"
    THREAD = "thread"


class MessageType(str, Enum):
    """Normalized inbound message types."""

    TEXT = "text"
    IMAGE = "image"
    FILE = "file"
    EVENT = "event"
    CARD_ACTION = "card_action"


class AttachmentRef(BaseModel):
    """Tenant-scoped reference to an attachment staged by an adapter."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    artifact_id: str = Field(min_length=1, max_length=255)
    filename: str = Field(min_length=1, max_length=255)
    mime_type: str = Field(min_length=1, max_length=255)
    size_bytes: int = Field(ge=0)
    sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class NormalizedInboundMessage(BaseModel):
    """Channel-neutral message produced after callback verification."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    channel: ChannelType
    webhook_public_id: str = Field(min_length=8, max_length=128)
    external_message_id: str = Field(min_length=1, max_length=255)
    external_user_id: str = Field(min_length=1, max_length=255)
    external_chat_id: str | None = Field(default=None, max_length=255)
    conversation_type: ConversationType
    message_type: MessageType = MessageType.TEXT
    text: str = Field(default="", max_length=100_000)
    thread_id: str | None = Field(default=None, max_length=255)
    attachments: tuple[AttachmentRef, ...] = ()
    received_at: datetime
    request_id: str = Field(min_length=1, max_length=255)

    @model_validator(mode="after")
    def validate_conversation_and_payload(self) -> "NormalizedInboundMessage":
        if self.conversation_type in {ConversationType.GROUP, ConversationType.THREAD} and not self.external_chat_id:
            raise ValueError("group and thread conversations require external_chat_id")
        if self.conversation_type is ConversationType.THREAD and not self.thread_id:
            raise ValueError("thread conversations require thread_id")
        if not self.text and not self.attachments:
            raise ValueError("message must contain text or at least one attachment")
        return self


class OutboundMessage(BaseModel):
    """Channel-neutral output that an adapter renders for its IM platform."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str
    channel_binding_id: str
    session_id: str
    text: str = ""
    final: bool = True
    card: dict | None = None
    attachments: tuple[AttachmentRef, ...] = ()
    idempotency_key: str = Field(min_length=1, max_length=255)


class ChannelCapabilities(BaseModel):
    """Features used to select streaming/card/file fallbacks."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_text_length: int = Field(default=4_000, ge=1)
    supports_streaming: bool = False
    supports_message_edit: bool = False
    supports_cards: bool = False
    supports_recall: bool = False
    supports_images: bool = True
    supports_files: bool = True
