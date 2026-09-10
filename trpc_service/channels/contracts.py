"""Strict transport-neutral contracts for channel messages."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, PrivateAttr, field_validator, model_validator


class Channel(StrEnum):
    LOCAL_HTTP = "local_http"
    FEISHU = "feishu"
    WECOM = "wecom"

    @property
    def is_real_im(self) -> bool:
        return self in {Channel.FEISHU, Channel.WECOM}


class ConversationType(StrEnum):
    DIRECT = "direct"
    GROUP = "group"


class ReplyStatus(StrEnum):
    SUCCEEDED = "succeeded"
    DUPLICATE = "duplicate"
    PROCESSING = "processing"
    INVALID_REQUEST = "invalid_request"
    UNAUTHORIZED = "unauthorized"
    ACCESS_DENIED = "access_denied"
    CONFLICT = "conflict"
    FAILED = "failed"


class DeliveryAction(StrEnum):
    DELIVER = "deliver"
    SUPPRESS = "suppress"
    NONE = "none"


class _StrictFrozenModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class InboundMessage(_StrictFrozenModel):
    channel: Channel
    binding_id: str = Field(min_length=1, max_length=96, pattern=r"^[a-z0-9][a-z0-9-]*$")
    external_message_id: str = Field(min_length=1, max_length=128)
    external_user_id: str = Field(min_length=1, max_length=128)
    conversation_type: ConversationType
    external_conversation_id: str = Field(min_length=1, max_length=128)
    channel_identity_digest: str | None = Field(
        default=None, pattern=r"^[0-9a-f]{64}$"
    )
    group_sender_id: str | None = Field(default=None, min_length=1, max_length=128)
    message_type: Literal["text"] = "text"
    text: str = Field(min_length=1, max_length=4000)
    received_at: datetime
    trace_id: UUID

    @field_validator("text", mode="before")
    @classmethod
    def strip_text(cls, value: object) -> object:
        return value.strip() if isinstance(value, str) else value

    @field_validator("received_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None or value.utcoffset().total_seconds() != 0:
            raise ValueError("received_at must be UTC-aware")
        return value

    @model_validator(mode="after")
    def validate_real_im_scope(self) -> Self:
        if self.channel.is_real_im:
            if self.channel_identity_digest is None:
                raise ValueError("real IM message requires a trusted channel identity")
            if self.conversation_type == ConversationType.GROUP and self.group_sender_id is None:
                raise ValueError("group message requires a sender scope")
            if self.conversation_type == ConversationType.DIRECT and self.group_sender_id is not None:
                raise ValueError("direct message cannot contain a group sender scope")
        return self


class ErrorDetail(_StrictFrozenModel):
    code: str = Field(min_length=1, max_length=64, pattern=r"^[a-z][a-z0-9_]*$")
    message: str = Field(min_length=1, max_length=256)
    retryable: bool
    execution_started: bool


class OutboundReply(_StrictFrozenModel):
    status: ReplyStatus
    trace_id: UUID
    original_trace_id: UUID | None = None
    tenant_id: str | None = Field(default=None, pattern=r"^[a-z0-9][a-z0-9-]{0,63}$")
    platform_session_id: str | None = Field(default=None, pattern=r"^sess_[0-9a-f]{64}$")
    external_message_id: str | None = Field(default=None, min_length=1, max_length=128)
    text: str | None = Field(default=None, min_length=1, max_length=4000)
    delivery_action: DeliveryAction
    error: ErrorDetail | None = None

    @model_validator(mode="after")
    def validate_status_payload(self) -> Self:
        if self.status == ReplyStatus.SUCCEEDED:
            required = (self.tenant_id, self.platform_session_id, self.external_message_id, self.text)
            if any(value is None for value in required) or self.delivery_action != DeliveryAction.DELIVER:
                raise ValueError("successful reply requires complete delivery data")
            if self.error is not None:
                raise ValueError("successful reply cannot contain an error")
        elif self.status == ReplyStatus.FAILED and self.error is None:
            raise ValueError("failed reply requires an error")
        return self


class VerifiedBindingScope(_StrictFrozenModel):
    """Capability produced only after authentication succeeds."""

    binding_id: str
    channel: Channel
    _verified: bool = PrivateAttr(default=False)

    def __init__(self, **data: object) -> None:
        raise TypeError("VerifiedBindingScope can only be issued by the authentication adapter")

    @classmethod
    def _issue(cls, *, binding_id: str, channel: Channel) -> VerifiedBindingScope:
        scope = cls.model_construct(binding_id=binding_id, channel=channel)
        scope._verified = True
        return scope


class UnifiedInboundMessage(InboundMessage):
    """SDK-independent inbound message accepted from verified IM adapters."""


class UnifiedReply(OutboundReply):
    """Provider-independent reply returned by the gateway."""
