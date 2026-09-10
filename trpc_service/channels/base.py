"""Unified channel adapter contracts."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import parse_qs
from xml.etree import ElementTree

from trpc_service.security.secrets import SecretManager, SecretResolutionError
from trpc_service.tenant.models import ChannelBinding


def channel_now() -> datetime:
    return datetime.now(UTC)


REVOKE_EVENT_TYPES = {"revoke", "revoked", "withdraw", "recall", "delete", "deleted", "message_revoke"}
REVOKE_TARGET_FIELDS = (
    "target_message_id",
    "revoke_message_id",
    "withdraw_message_id",
    "recall_message_id",
    "delete_message_id",
    "MsgId",
    "msg_id",
)

MAX_ATTACHMENTS = 32
MAX_ATTACHMENT_METADATA_BYTES = 32 * 1024
MAX_EVENT_DEPTH = 8
MAX_EVENT_FIELDS = 512
MAX_EVENT_STRING_BYTES = 16 * 1024
SENSITIVE_EVENT_KEY = re.compile(
    r"(?i)(?:^|[_-])(raw[_-]?body|_headers?|authorization|signature|token|secret|password|passwd|api[_-]?key)(?:$|[_-])"
)


@dataclass(frozen=True, slots=True)
class ChannelCapabilities:
    max_text_length: int = 4096
    supports_media: bool = False
    supports_cards: bool = False
    supports_revoke: bool = False
    delivery_mode: str = "webhook"


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
    capabilities: ChannelCapabilities

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
    if raw is None:
        return []
    if not isinstance(raw, list) or len(raw) > MAX_ATTACHMENTS:
        raise ValueError("attachments must be a list of at most 32 items")
    return [normalize_attachment(item) for item in raw]


def normalize_attachment(raw: dict[str, Any], *, default_kind: str = "file") -> Attachment:
    """Build a normalized Attachment from provider-specific media fields."""
    if not isinstance(raw, dict):
        raise ValueError("attachment must be an object")
    raw_metadata = raw.get("metadata", {})
    if not isinstance(raw_metadata, dict):
        raise ValueError("attachment metadata must be an object")
    metadata = dict(raw_metadata)
    encoded_metadata = metadata.get("content_base64")
    if encoded_metadata is not None:
        if (
            not isinstance(encoded_metadata, str)
            or len(encoded_metadata.encode("ascii", errors="ignore")) > 16 * 1024 * 1024
        ):
            raise ValueError("attachment base64 content exceeds limit")
        try:
            base64.b64decode(encoded_metadata, validate=True)
        except (binascii.Error, ValueError, TypeError):
            raise ValueError("attachment base64 content is invalid") from None
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
        encoded = raw.get("content_base64")
        if not isinstance(encoded, str) or len(encoded.encode("ascii", errors="ignore")) > 16 * 1024 * 1024:
            raise ValueError("attachment base64 content exceeds limit")
        try:
            base64.b64decode(encoded, validate=True)
        except (binascii.Error, ValueError, TypeError):
            raise ValueError("attachment base64 content is invalid") from None
        metadata.setdefault("content_base64", encoded)
    if raw.get("source") is not None:
        metadata.setdefault("source", raw.get("source"))
    attachment = Attachment(
        kind=str(raw.get("kind", raw.get("type", default_kind))),
        url=raw.get("url") or raw.get("file_url") or raw.get("download_url"),
        name=raw.get("name") or raw.get("filename"),
        content_type=raw.get("content_type"),
        metadata=metadata,
    )
    metadata_size = len(json.dumps(attachment.metadata, ensure_ascii=False, default=str).encode("utf-8"))
    if metadata_size > MAX_ATTACHMENT_METADATA_BYTES:
        raise ValueError("attachment metadata exceeds limit")
    return attachment


def sanitize_event_metadata(value: object, *, _depth: int = 0, _fields: list[int] | None = None) -> object:
    """Keep diagnostic event metadata while excluding credentials and raw bodies."""
    fields = _fields or [0]
    if _depth > MAX_EVENT_DEPTH:
        return "[depth-limit]"
    if isinstance(value, dict):
        result: dict[str, object] = {}
        for key, item in value.items():
            fields[0] += 1
            if fields[0] > MAX_EVENT_FIELDS:
                break
            key_text = str(key)
            if SENSITIVE_EVENT_KEY.search(key_text) or key_text.casefold() in {
                "encrypt",
                "content_base64",
                "provider_url",
                "aes_key",
            }:
                continue
            result[key_text] = sanitize_event_metadata(item, _depth=_depth + 1, _fields=fields)
        return result
    if isinstance(value, (list, tuple)):
        return [
            sanitize_event_metadata(item, _depth=_depth + 1, _fields=fields)
            for item in list(value)[:MAX_ATTACHMENTS]
        ]
    if isinstance(value, str):
        encoded = value.encode("utf-8")
        if len(encoded) > MAX_EVENT_STRING_BYTES:
            return encoded[:MAX_EVENT_STRING_BYTES].decode("utf-8", errors="ignore") + "...[truncated]"
    return value


def sanitize_event_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Sanitize a callback before it is placed in a queue or session record."""
    if not isinstance(payload, dict):
        raise ValueError("webhook payload must be an object")
    result = sanitize_event_metadata(payload)
    if not isinstance(result, dict):
        raise ValueError("webhook payload must be an object")
    if payload.get("callback_verified"):
        result["callback_verified"] = True
    return result


class WebhookPayloadError(ValueError):
    pass


def parse_webhook_body(body: bytes, content_type: str, *, max_bytes: int | None = None) -> dict[str, Any]:
    """Parse supported callback encodings with a bounded request body."""
    limit = max_bytes or int(os.getenv("WEBHOOK_MAX_BODY_BYTES", str(1024 * 1024)))
    if len(body) > limit:
        raise WebhookPayloadError("webhook body exceeds configured limit")
    if not body:
        return {}
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError:
        raise WebhookPayloadError("webhook body must be UTF-8") from None
    media_type = content_type.split(";", 1)[0].strip().lower()
    if (
        media_type in {"application/json", "text/json"}
        or media_type.endswith("+json")
        or text.lstrip().startswith(("{", "["))
    ):
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            raise WebhookPayloadError("webhook JSON is invalid") from None
        if not isinstance(value, dict):
            raise WebhookPayloadError("webhook JSON must be an object")
        return value
    if media_type in {"application/xml", "text/xml"} or media_type.endswith("+xml") or text.lstrip().startswith("<"):
        try:
            root = ElementTree.fromstring(text)
        except ElementTree.ParseError:
            raise WebhookPayloadError("webhook XML is invalid") from None
        return {"raw_body": text, **{child.tag: child.text or "" for child in root}}
    if media_type == "application/x-www-form-urlencoded":
        return {key: values[-1] for key, values in parse_qs(text, keep_blank_values=True).items()}
    raise WebhookPayloadError("unsupported webhook content type")
