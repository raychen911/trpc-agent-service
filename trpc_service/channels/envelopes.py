"""Provider-neutral, serializable IM envelopes."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from trpc_service.tenant.models import Channel, ConversationKind


class PayloadKind(StrEnum):
    TEXT = "text"
    IMAGE = "image"
    FILE = "file"
    VIDEO = "video"
    VOICE = "voice"
    MIXED = "mixed"
    EVENT = "event"


class DeliveryStatus(StrEnum):
    DELIVERED = "delivered"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


class EnvelopeModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class MediaReference(EnvelopeModel):
    provider_url: str | None = Field(default=None, max_length=8192)
    provider_media_id: str | None = Field(default=None, max_length=16_384)
    encryption_key_ref: str | None = Field(default=None, max_length=512)
    filename: str | None = Field(default=None, max_length=512)
    content_type: str | None = Field(default=None, max_length=256)


class InboundEnvelope(EnvelopeModel):
    """Untrusted provider input; deliberately has no ``tenant_id`` field."""

    channel: Channel
    account_id: str = Field(min_length=1, max_length=256)
    external_message_id: str = Field(min_length=1, max_length=512)
    external_user_id: str = Field(min_length=1, max_length=512)
    conversation_kind: ConversationKind
    external_conversation_id: str | None = Field(default=None, max_length=512)
    payload_kind: PayloadKind
    text: str | None = Field(default=None, max_length=100_000)
    media: tuple[MediaReference, ...] = ()
    event_type: str | None = Field(default=None, max_length=256)
    occurred_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    provider_metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("provider_metadata")
    @classmethod
    def validate_provider_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "target_id",
            "chat_type",
            "message_type",
            "provider_message_id",
            "event_type",
            "event_id",
            "tenant_key",
            "thread_id",
            "root_id",
            "parent_id",
            "sender_type",
            "raw_message_type",
        }
        if len(value) > 16 or any(key not in allowed for key in value):
            raise ValueError("provider metadata contains unsupported fields")
        if any(_sensitive_metadata_key(key) for key in value):
            raise ValueError("provider metadata contains sensitive fields")
        for key, item in value.items():
            if not isinstance(item, (str, int, float, bool)) or (
                isinstance(item, bool) and key != "chat_type"
            ):
                raise ValueError("provider metadata values must be scalar")
            if isinstance(item, str) and len(item.encode("utf-8")) > 512:
                raise ValueError("provider metadata value is too large")
        try:
            size = len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode())
        except (TypeError, ValueError) as exc:
            raise ValueError("provider metadata is invalid") from exc
        if size > 16 * 1024:
            raise ValueError("provider metadata is too large")
        return value


class OutboundEnvelope(EnvelopeModel):
    outbound_id: str
    tenant_id: str
    binding_id: str
    channel: Channel
    target_id: str
    session_id: str
    payload_kind: PayloadKind = PayloadKind.TEXT
    text: str | None = Field(default=None, max_length=100_000)
    media: tuple[MediaReference, ...] = ()
    in_reply_to: str | None = None
    trace_headers: dict[str, str] = Field(default_factory=dict)

    @field_validator("trace_headers")
    @classmethod
    def validate_trace_headers(cls, value: dict[str, str]) -> dict[str, str]:
        allowed = {"traceparent", "tracestate", "baggage"}
        if set(value).difference(allowed):
            raise ValueError("unsupported trace header")
        if len(value) > 3 or any(len(key) > 64 or len(item) > 512 for key, item in value.items()):
            raise ValueError("trace headers are too large")
        return value


class RecallEnvelope(EnvelopeModel):
    """A durable request to recall one provider message."""

    outbound_id: str = Field(min_length=1, max_length=512)
    tenant_id: str = Field(min_length=1, max_length=128)
    binding_id: str = Field(min_length=1, max_length=256)
    channel: Channel
    provider_message_id: str = Field(min_length=1, max_length=512)


class DeliveryReceipt(EnvelopeModel):
    outbound_id: str
    status: DeliveryStatus
    provider_message_id: str | None = None
    provider_code: str | None = None
    retryable: bool = False
    # Provider-advertised backoff; absent means use the dispatcher fallback.
    retry_after_seconds: float | None = Field(default=None, ge=0, le=3600)
    attempts: int = Field(default=1, ge=1)


class VerifiedCallback(EnvelopeModel):
    envelope: InboundEnvelope
    acknowledgement: str = "success"


__all__ = [
    "DeliveryReceipt",
    "DeliveryStatus",
    "InboundEnvelope",
    "MediaReference",
    "OutboundEnvelope",
    "PayloadKind",
    "RecallEnvelope",
    "VerifiedCallback",
]


_SENSITIVE_METADATA = re.compile(
    r"(?:^|_)(?:body|content|prompt|completion|input|output|token|secret|password|authorization|cookie|request|response)(?:_|$)",
    re.IGNORECASE,
)


def _sensitive_metadata_key(key: str) -> bool:
    return not isinstance(key, str) or bool(_SENSITIVE_METADATA.search(key))
