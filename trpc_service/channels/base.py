"""Unified channel adapter contracts."""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

from trpc_service.tenant.models import ChannelBinding
from trpc_service.security.secrets import SecretManager, SecretResolutionError


def channel_now() -> datetime:
    return datetime.now(timezone.utc)


REVOKE_EVENT_TYPES = {"revoke", "revoked", "withdraw", "recall", "delete", "deleted", "message_revoke"}
REVOKE_TARGET_FIELDS = (
    "target_message_id",
    "revoke_message_id",
    "withdraw_message_id",
    "recall_message_id",
    "delete_message_id",
    "MsgId",
    "msg_id",
    "message_id",
)


@dataclass(slots=True)
class Attachment:
    kind: str
    url: str | None = None
    name: str | None = None
    content_type: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class InboundMessage:
    channel: str
    account_id: str
    external_message_id: str
    external_user_id: str
    text: str | None = None
    group_id: str | None = None
    attachments: list[Attachment] = field(default_factory=list)
    received_at: datetime = field(default_factory=channel_now)
    raw_event: dict[str, Any] = field(default_factory=dict)
    internal_user_id: str | None = None

    @property
    def conversation_id(self) -> str:
        return self.group_id or self.external_user_id

    @property
    def effective_user_id(self) -> str:
        return self.internal_user_id or self.external_user_id

    @property
    def event_action(self) -> str:
        return str(self.raw_event.get("normalized_event_type") or "message")

    @property
    def is_revoke(self) -> bool:
        return self.event_action in REVOKE_EVENT_TYPES


@dataclass(slots=True)
class OutboundMessage:
    channel: str
    account_id: str
    session_id: str
    external_user_id: str
    text: str
    group_id: str | None = None
    attachments: list[Attachment] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class SendResult:
    ok: bool
    response_ref: str
    error: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class ChannelVerificationError(ValueError):
    pass


class ChannelAdapter(Protocol):
    channel_name: str

    def verify_callback(self, payload: dict[str, Any], binding: ChannelBinding) -> None: ...

    def parse_event(self, payload: dict[str, Any], binding: ChannelBinding) -> InboundMessage: ...

    def send(self, message: OutboundMessage, binding: ChannelBinding) -> SendResult: ...


def hmac_signature(secret: str, body: str) -> str:
    return hmac.new(secret.encode("utf-8"), body.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_optional_hmac(payload: dict[str, Any], binding: ChannelBinding) -> None:
    try:
        secret = SecretManager().resolve(binding.secret_ref) if binding.secret_ref else None
    except SecretResolutionError as exc:
        if payload.get("signature"):
            raise ChannelVerificationError(str(exc)) from exc
        secret = None
    signature = payload.get("signature")
    if not secret or not signature:
        return
    body = str(payload.get("body", payload.get("text", "")))
    expected = hmac_signature(str(secret), body)
    if not hmac.compare_digest(str(signature), expected):
        raise ChannelVerificationError("invalid callback signature")


def normalize_event_type(payload: dict[str, Any]) -> str:
    for field_name in (
        "normalized_event_type",
        "event_type",
        "action",
        "Event",
        "event",
        "ChangeType",
        "MsgType",
        "msgtype",
    ):
        value = payload.get(field_name)
        if value is None:
            continue
        normalized = str(value).strip().lower()
        if normalized in REVOKE_EVENT_TYPES:
            return "revoke"
        if normalized in {"text", "image", "file", "voice", "video", "event", "message"}:
            continue
    return "message"


def revoke_target_message_id(payload: dict[str, Any]) -> str | None:
    for field_name in REVOKE_TARGET_FIELDS:
        value = payload.get(field_name)
        if value is not None and value != "":
            return str(value)
    return None


def parse_attachments(raw: list[dict[str, Any]] | None) -> list[Attachment]:
    return [
        Attachment(
            kind=str(item.get("kind", item.get("type", "file"))),
            url=item.get("url"),
            name=item.get("name"),
            content_type=item.get("content_type"),
            metadata=dict(item.get("metadata", {})),
        )
        for item in raw or []
    ]


def normalize_attachment(raw: dict[str, Any], *, default_kind: str = "file") -> Attachment:
    """Build a normalized Attachment from provider-specific media fields."""
    metadata = dict(raw.get("metadata", {}))
    for key in (
        "file_id",
        "media_id",
        "mediaid",
        "file_unique_id",
        "message_id",
        "msg_id",
        "update_id",
    ):
        if raw.get(key) is not None and key not in metadata:
            metadata[key] = raw.get(key)
    if raw.get("content_base64") is not None:
        metadata.setdefault("content_base64", raw.get("content_base64"))
    if raw.get("source") is not None:
        metadata.setdefault("source", raw.get("source"))
    return Attachment(
        kind=str(raw.get("kind", raw.get("type", default_kind))),
        url=raw.get("url") or raw.get("file_url") or raw.get("download_url"),
        name=raw.get("name") or raw.get("filename"),
        content_type=raw.get("content_type"),
        metadata=metadata,
    )
