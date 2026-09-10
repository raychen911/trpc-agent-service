"""Trusted, SDK-independent channel and sender identity models."""

from __future__ import annotations

from datetime import datetime
from hashlib import sha256
from typing import Self

from pydantic import BaseModel, ConfigDict, Field, computed_field, field_validator, model_validator

from trpc_service.channels.contracts import Channel, ConversationType


def length_prefixed_digest(*parts: str) -> str:
    """Hash structured identity parts without delimiter-collision ambiguity."""

    digest = sha256()
    for part in parts:
        encoded = part.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


class _IdentityModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class ChannelIdentity(_IdentityModel):
    channel: Channel
    provider_tenant_key: str = Field(min_length=1, max_length=128, repr=False)
    provider_app_or_bot_id: str = Field(min_length=1, max_length=128, repr=False)

    @model_validator(mode="after")
    def require_real_im_channel(self) -> Self:
        if not self.channel.is_real_im:
            raise ValueError("channel identity is only valid for real IM channels")
        return self

    @computed_field(return_type=str, repr=False)
    @property
    def identity_digest(self) -> str:
        return length_prefixed_digest(
            self.channel.value,
            self.provider_tenant_key,
            self.provider_app_or_bot_id,
        )


class RuntimeBotIdentity(_IdentityModel):
    channel: Channel
    sender_type: str = Field(min_length=1, max_length=64, repr=False)
    sender_id: str = Field(min_length=1, max_length=128, repr=False)
    channel_identity_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    authenticated_at: datetime

    @field_validator("authenticated_at")
    @classmethod
    def require_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None or value.utcoffset().total_seconds() != 0:
            raise ValueError("authenticated_at must be UTC-aware")
        return value

    @model_validator(mode="after")
    def require_real_im_channel(self) -> Self:
        if not self.channel.is_real_im:
            raise ValueError("runtime bot identity requires a real IM channel")
        return self


class AuthenticatedSender(_IdentityModel):
    sender_type: str = Field(min_length=1, max_length=64, repr=False)
    sender_id: str = Field(min_length=1, max_length=128, repr=False)
    is_bot: bool | None = Field(default=None, repr=False)

    def is_same_bot(self, bot: RuntimeBotIdentity) -> bool:
        return self.sender_type == bot.sender_type and self.sender_id == bot.sender_id


class ProviderReplyContext(_IdentityModel):
    channel: Channel
    conversation_type: ConversationType
    reply_target_id: str = Field(min_length=1, max_length=128, repr=False)
    protocol_request_id: str | None = Field(
        default=None, min_length=1, max_length=256, repr=False
    )
    provider_message_id: str = Field(min_length=1, max_length=128, repr=False)

    @model_validator(mode="after")
    def require_real_im_channel(self) -> Self:
        if not self.channel.is_real_im:
            raise ValueError("reply context requires a real IM channel")
        if self.channel != Channel.WECOM and self.protocol_request_id is not None:
            raise ValueError("protocol request id is only supported for WeCom")
        return self

    @computed_field(return_type=str, repr=False)
    @property
    def context_digest(self) -> str:
        return length_prefixed_digest(
            self.channel.value,
            self.conversation_type.value,
            self.reply_target_id,
            self.protocol_request_id or "",
            self.provider_message_id,
        )


__all__ = [
    "AuthenticatedSender",
    "ChannelIdentity",
    "ProviderReplyContext",
    "RuntimeBotIdentity",
    "length_prefixed_digest",
]
