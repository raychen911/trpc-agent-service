"""Typed transport events and receipts; protocol acknowledgements are not read receipts."""

import mimetypes
import re
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .models import NormalizedInboundMessage

MAX_MEDIA_BYTES = 20 * 1024 * 1024


def safe_media_filename(value, kind):
    default = "image.bin" if kind == "image" else "attachment.bin"
    if not isinstance(value, str):
        return default
    name = value.strip().replace("\\", "/").rsplit("/", 1)[-1]
    name = re.sub(r"[\x00-\x1f\x7f]", "", name)
    if name in {"", ".", ".."}:
        return default
    stem, dot, suffix = name.rpartition(".")
    if len(name) > 120:
        name = (stem[:100] + dot + suffix[:16]) if dot else name[:120]
    return name


def media_mime(filename, fallback="application/octet-stream"):
    guessed = mimetypes.guess_type(filename)[0]
    trusted_fallback = (fallback if isinstance(fallback, str)
                        and re.fullmatch(r"[\w.+-]+/[\w.+-]+", fallback) else None)
    value = trusted_fallback if trusted_fallback != "application/octet-stream" else guessed or trusted_fallback
    value = value or "application/octet-stream"
    return value if isinstance(value, str) and re.fullmatch(r"[\w.+-]+/[\w.+-]+", value) else fallback


class PendingMedia(BaseModel):
    """Authenticated provider reference that must never enter durable storage."""

    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    kind: Literal["image", "file"]
    resource_id: str = Field(min_length=1, max_length=512, repr=False)
    filename: str = Field(min_length=1, max_length=255)
    mime_type: str = Field(min_length=1, max_length=255)
    download_url: str | None = Field(default=None, max_length=2048, repr=False)
    encryption_key: str | None = Field(default=None, max_length=512, repr=False)


class TransportEvent(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    event_id: str = Field(min_length=1, max_length=255)
    kind: Literal["chat", "action", "recall", "ignored"]
    message: NormalizedInboundMessage | None = None
    external_actor_id: str | None = None
    external_chat_id: str | None = None
    action_data: str | None = Field(default=None, max_length=256, repr=False)
    recalled_message_id: str | None = Field(default=None, min_length=1, max_length=255)
    pending_media: tuple[PendingMedia, ...] = Field(default=(), repr=False)
    reason: str | None = None
    reply_context: dict = Field(default_factory=dict, repr=False)

    @model_validator(mode="after")
    def validate_event(self):
        if self.kind == "chat":
            if self.message is None or self.message.external_message_id != self.event_id:
                raise ValueError("chat event requires a matching message")
            if (self.external_actor_id, self.external_chat_id) != (self.message.external_user_id,
                                                                   self.message.external_chat_id):
                raise ValueError("event identity does not match message")
        elif self.message is not None:
            raise ValueError("control event cannot contain an agent input")
        if self.kind == "action" and not (self.action_data and self.external_actor_id and self.external_chat_id):
            raise ValueError("action requires an actor, conversation and action reference")
        if self.kind != "action" and self.action_data is not None:
            raise ValueError("action reference is only valid for action events")
        if self.kind == "recall" and not self.recalled_message_id:
            raise ValueError("recall event requires the original message id")
        if self.kind != "recall" and self.recalled_message_id is not None:
            raise ValueError("recalled message id is only valid for recall events")
        if self.pending_media and self.kind != "chat":
            raise ValueError("pending media is only valid for chat events")
        if self.kind == "ignored" and not self.reason:
            raise ValueError("ignored event requires a reason")
        return self


class DeliveryResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    outcome: Literal["accepted", "rejected", "unknown"]
    receipt_kind: Literal["message_id", "protocol_ack", "none"] = "none"
    external_message_id: str | None = None
    provider_request_id: str | None = None
    retry_after: float | None = Field(default=None, ge=0, allow_inf_nan=False)
    error_type: str | None = None
    retryable: bool = False

    @model_validator(mode="after")
    def validate_result(self):
        if self.outcome == "accepted":
            if self.receipt_kind == "none" or self.retryable:
                raise ValueError("accepted delivery requires a receipt")
            if self.receipt_kind == "message_id" and not self.external_message_id:
                raise ValueError("message receipt requires an external message id")
            if self.receipt_kind == "protocol_ack" and not self.provider_request_id:
                raise ValueError("protocol receipt requires a request id")
        elif self.receipt_kind != "none" or self.external_message_id or self.provider_request_id:
            raise ValueError("unconfirmed delivery cannot contain a success receipt")
        if self.outcome == "unknown" and self.retryable:
            raise ValueError("unknown network outcome is not safe to retry")
        return self


def action_command(message, reply_context):
    """Exact control commands never become model input or plaintext durable payloads."""
    if not message.text or not message.text.startswith("/action "):
        return None
    token = message.text.removeprefix("/action ").strip()
    # IM clients often preserve Markdown escaping when users copy a command
    # from a bot card. Normalize presentation-only characters before the
    # cryptographic nonce and conversation checks; the signed token remains
    # mandatory and is never shortened or regenerated.
    if token.startswith("`") and token.endswith("`"):
        token = token[1:-1].strip()
    token = token.replace("\\_", "_")
    if not 1 <= len(token) <= 256:
        return TransportEvent(event_id=message.external_message_id, kind="ignored", reason="invalid_action_command")
    return TransportEvent(event_id=message.external_message_id, kind="action",
                          external_actor_id=message.external_user_id, external_chat_id=message.external_chat_id,
                          action_data=token, reply_context=reply_context)


def split_text(text: str, limit: int, *, unit: Literal["characters", "utf8_bytes"] = "characters") -> tuple[str, ...]:
    """Plain text boundaries are Unicode code points, never slices of encoded UTF-8."""
    if limit < 4:
        raise ValueError("text limit must fit a Unicode character")
    if not text:
        raise ValueError("empty reply")
    chunks, current, size = [], [], 0
    for char in text:
        width = len(char.encode("utf-8")) if unit == "utf8_bytes" else 1
        if current and size + width > limit:
            chunks.append("".join(current))
            current, size = [], 0
        current.append(char)
        size += width
    if current:
        chunks.append("".join(current))
    return tuple(chunks)
