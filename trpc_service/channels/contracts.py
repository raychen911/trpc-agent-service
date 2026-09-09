"""Immutable contracts shared by first-party IM channel adapters.

The contracts intentionally contain only trusted, normalized identifiers. Raw webhook
secrets, response URLs, bot tokens, and external user/chat identifiers must remain in
the channel ingress or encrypted reply-route storage.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator, model_validator


class Channel(StrEnum):
    """Supported first-party channel transports."""

    WECOM = "wecom"
    TELEGRAM = "telegram"


class CallbackKind(StrEnum):
    """How a verified callback should be routed."""

    USER_MESSAGE = "user_message"
    CONTROL_REFRESH = "control_refresh"
    CHANNEL_EVENT = "channel_event"


class ConversationKind(StrEnum):
    """Conversation topology exposed to the Agent platform."""

    PRIVATE = "private"
    GROUP = "group"


class AttachmentKind(StrEnum):
    """Portable attachment categories."""

    IMAGE = "image"
    FILE = "file"
    AUDIO = "audio"
    VOICE = "voice"
    VIDEO = "video"


class ReplyKind(StrEnum):
    """Channel-independent reply semantics."""

    SNAPSHOT = "snapshot"
    FINAL = "final"
    CARD = "card"
    FILE = "file"
    ERROR = "error"


class SensitiveReplyRouteKind(StrEnum):
    """Credential-bearing reply routes that require encrypted persistence."""

    WECOM_RESPONSE_URL = "wecom_response_url"
    TELEGRAM_DELIVERY_CONTEXT = "telegram_delivery_context"


class CallbackRequest(BaseModel):
    """Immutable HTTP callback material after the web framework reads the request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    path_binding_id: str
    headers: tuple[tuple[str, str], ...] = ()
    query: tuple[tuple[str, str], ...] = ()
    body: bytes = b""
    received_at: datetime
    request_id: str
    trace_id: str

    @field_validator("path_binding_id", "request_id", "trace_id")
    @classmethod
    def non_empty_identifier(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("identifier must not be empty")
        return value

    @field_validator("received_at")
    @classmethod
    def timezone_aware_received_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("received_at must be timezone-aware")
        return value

    def header(self, name: str) -> str | None:
        """Return one case-insensitive header, rejecting ambiguous duplicates."""

        matches = [value for key, value in self.headers if key.casefold() == name.casefold()]
        if len(matches) > 1:
            raise ValueError(f"duplicate header: {name}")
        return matches[0] if matches else None

    def query_value(self, name: str) -> str | None:
        """Return one query value, rejecting ambiguous duplicates."""

        matches = [value for key, value in self.query if key == name]
        if len(matches) > 1:
            raise ValueError(f"duplicate query parameter: {name}")
        return matches[0] if matches else None


class TrustedBindingContext(BaseModel):
    """Server-resolved binding identity; external payloads never create this object."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str
    app_id: str
    app_revision: int = Field(ge=1)
    binding_id: str
    binding_revision: int = Field(ge=1)
    channel: Channel
    external_account_id: str
    enabled: bool = True

    @field_validator("tenant_id", "app_id", "binding_id", "external_account_id")
    @classmethod
    def binding_identifier_not_empty(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("binding identifier must not be empty")
        return value


class AttachmentRef(BaseModel):
    """An opaque pointer to attachment retrieval state."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: AttachmentKind
    locator_key: str
    filename: str | None = None
    mime_type: str | None = None
    size_bytes: int | None = Field(default=None, ge=0)

    @field_validator("locator_key")
    @classmethod
    def locator_not_empty(cls, value: str) -> str:
        if not value:
            raise ValueError("locator_key must not be empty")
        return value


class ExternalMessageRef(BaseModel):
    """A channel message reference that contains no credentials."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    delivery_id: str

    @field_validator("delivery_id")
    @classmethod
    def delivery_id_not_empty(cls, value: str) -> str:
        if not value:
            raise ValueError("delivery_id must not be empty")
        return value


class SensitiveReplyRoute(BaseModel):
    """A short-lived credential handed only to the encrypted ingress store.

    ``value`` is deliberately a :class:`SecretStr`: normal repr/str/JSON logging
    displays a mask. Calling ``get_secret_value()`` is an explicit trust-boundary
    operation and should happen only inside the route-encryption repository.
    """

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    route_key: str
    tenant_id: str
    binding_id: str
    delivery_id: str
    channel: Channel
    kind: SensitiveReplyRouteKind
    value: SecretStr
    expires_at: datetime
    max_uses: int | None = Field(default=None, ge=1)

    @field_validator("route_key", "tenant_id", "binding_id", "delivery_id")
    @classmethod
    def route_identifier_not_empty(cls, value: str) -> str:
        if not value or not value.strip():
            raise ValueError("sensitive route identifier must not be empty")
        return value

    @field_validator("value")
    @classmethod
    def route_secret_not_empty(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value():
            raise ValueError("sensitive route value must not be empty")
        return value

    @field_validator("expires_at")
    @classmethod
    def route_expiry_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("expires_at must be timezone-aware")
        return value


class NormalizedInbound(BaseModel):
    """Trusted, channel-neutral Agent input."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: int = Field(default=1, ge=1)
    tenant_id: str
    app_id: str
    binding_id: str
    binding_revision: int = Field(ge=1)
    channel: Channel
    delivery_id: str
    payload_sha256: str
    received_at: datetime
    principal_id: str
    conversation_id: str
    session_id: str
    conversation_kind: ConversationKind
    thread_id: str | None = None
    text: str | None = None
    attachments: tuple[AttachmentRef, ...] = ()
    reply_to: ExternalMessageRef | None = None
    reply_route_key: str
    request_id: str
    trace_id: str

    @field_validator(
        "tenant_id",
        "app_id",
        "binding_id",
        "delivery_id",
        "principal_id",
        "conversation_id",
        "session_id",
        "reply_route_key",
        "request_id",
        "trace_id",
    )
    @classmethod
    def inbound_identifier_not_empty(cls, value: str) -> str:
        if not value:
            raise ValueError("normalized identifier must not be empty")
        return value

    @field_validator("payload_sha256")
    @classmethod
    def valid_payload_digest(cls, value: str) -> str:
        if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
            raise ValueError("payload_sha256 must be a lowercase SHA-256 hex digest")
        return value

    @field_validator("received_at")
    @classmethod
    def inbound_received_at_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("received_at must be timezone-aware")
        return value

    @model_validator(mode="after")
    def input_has_content(self) -> NormalizedInbound:
        if not self.text and not self.attachments:
            raise ValueError("normalized input requires text or an attachment")
        return self


class VerifiedCallback(BaseModel):
    """Result of authenticating and classifying one callback."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: CallbackKind
    channel: Channel
    delivery_id: str
    payload_sha256: str
    inbound: NormalizedInbound | None = None
    control_id: str | None = None

    @model_validator(mode="after")
    def routing_shape_is_consistent(self) -> VerifiedCallback:
        if self.kind is CallbackKind.USER_MESSAGE and self.inbound is None:
            raise ValueError("user message callback requires normalized inbound")
        if self.kind is not CallbackKind.USER_MESSAGE and self.inbound is not None:
            raise ValueError("control/event callback must not invoke the Agent")
        if self.kind is CallbackKind.CONTROL_REFRESH and not self.control_id:
            raise ValueError("control refresh requires control_id")
        return self


class ReplyIntent(BaseModel):
    """Immutable semantic reply emitted by the Agent execution layer."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    intent_id: str
    tenant_id: str
    binding_id: str
    session_id: str
    run_id: str
    in_reply_to_delivery_id: str
    kind: ReplyKind
    text: str | None = None
    attachments: tuple[AttachmentRef, ...] = ()
    revision: int = Field(default=1, ge=1)
    final: bool = True
    idempotency_key: str

    @model_validator(mode="after")
    def reply_shape_is_consistent(self) -> ReplyIntent:
        if self.kind is ReplyKind.SNAPSHOT and self.final:
            raise ValueError("snapshot reply cannot be final")
        if self.kind in {ReplyKind.FINAL, ReplyKind.ERROR} and not self.final:
            raise ValueError("final/error reply must be final")
        if not self.text and not self.attachments:
            raise ValueError("reply requires text or an attachment")
        return self


@runtime_checkable
class ChannelAdapter(Protocol):
    """Minimal contract implemented by concrete channel adapters."""

    channel: Channel

    def verify_and_normalize(
        self,
        request: CallbackRequest,
        binding: TrustedBindingContext,
    ) -> VerifiedCallback:
        """Authenticate, classify, and normalize one callback."""

    def render_text(self, intent: ReplyIntent) -> tuple[str, ...]:
        """Render text into channel-safe chunks."""


@runtime_checkable
class SensitiveRouteChannelAdapter(ChannelAdapter, Protocol):
    """Adapter extension for callbacks carrying a reply credential."""

    def verify_decrypt_and_normalize(
        self,
        request: CallbackRequest,
        binding: TrustedBindingContext,
    ) -> tuple[VerifiedCallback, SensitiveReplyRoute | None]:
        """Return the callback and a separately handled sensitive route, if any."""
