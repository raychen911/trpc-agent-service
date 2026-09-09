"""Telegram webhook authentication and channel-neutral update normalization."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping
from datetime import timedelta
from typing import Any, NoReturn

from pydantic import SecretStr

from trpc_service.channels.contracts import (
    AttachmentKind,
    AttachmentRef,
    CallbackKind,
    CallbackRequest,
    Channel,
    ConversationKind,
    ExternalMessageRef,
    NormalizedInbound,
    ReplyIntent,
    SensitiveReplyRoute,
    SensitiveReplyRouteKind,
    TrustedBindingContext,
    VerifiedCallback,
)
from trpc_service.channels.session import ChannelIdentityDeriver
from trpc_service.channels.text import split_telegram_text

_TELEGRAM_AUTH_HEADER = "X-Telegram-Bot-Api-Secret-Token"
_DEFAULT_MAX_BODY_BYTES = 2_097_152
_TELEGRAM_SECRET_CHARACTERS = frozenset(
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"
)
_DEFAULT_ROUTE_LIFETIME = timedelta(days=7)


class TelegramProtocolError(ValueError):
    """Base error for Telegram authentication or payload validation."""


class TelegramAuthenticationError(TelegramProtocolError):
    """The webhook secret header did not match."""


class TelegramCallbackError(TelegramProtocolError):
    """The update schema or trusted binding was invalid."""


def _reject_json_constant(value: str) -> NoReturn:
    raise ValueError(f"non-finite JSON number is forbidden: {value}")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def verify_telegram_secret(provided: str | None, expected: str) -> None:
    """Verify the official webhook secret header in constant time."""

    if not expected:
        raise ValueError("expected Telegram webhook secret must not be empty")
    if len(expected) > 256 or any(char not in _TELEGRAM_SECRET_CHARACTERS for char in expected):
        raise ValueError("expected Telegram webhook secret violates Bot API syntax")
    if provided is None or not hmac.compare_digest(
        provided.encode("utf-8"),
        expected.encode("ascii"),
    ):
        raise TelegramAuthenticationError("invalid Telegram webhook secret")


class TelegramAdapter:
    """Authenticate and normalize Telegram Bot API Update objects."""

    channel = Channel.TELEGRAM

    def __init__(
        self,
        webhook_secret: str,
        identities: ChannelIdentityDeriver,
        *,
        max_body_bytes: int = _DEFAULT_MAX_BODY_BYTES,
        route_lifetime: timedelta = _DEFAULT_ROUTE_LIFETIME,
    ) -> None:
        try:
            verify_telegram_secret(webhook_secret, webhook_secret)
        except ValueError as exc:
            raise ValueError("invalid webhook_secret") from exc
        if max_body_bytes < 1:
            raise ValueError("max_body_bytes must be positive")
        if route_lifetime <= timedelta(0):
            raise ValueError("route_lifetime must be positive")
        self._webhook_secret = webhook_secret
        self._identities = identities
        self._max_body_bytes = max_body_bytes
        self._route_lifetime = route_lifetime

    def verify_and_normalize(
        self,
        request: CallbackRequest,
        binding: TrustedBindingContext,
    ) -> VerifiedCallback:
        """Verify an Update without exposing its external delivery coordinates.

        Web ingress should call :meth:`verify_decrypt_and_normalize` so routing data
        can be encrypted and committed with the Inbox row before acknowledging.
        """

        callback, _ = self.verify_decrypt_and_normalize(request, binding)
        return callback

    def verify_decrypt_and_normalize(
        self,
        request: CallbackRequest,
        binding: TrustedBindingContext,
    ) -> tuple[VerifiedCallback, SensitiveReplyRoute | None]:
        """Verify once and split log-safe input from sensitive delivery context."""

        self._check_binding(request, binding)
        try:
            secret = request.header(_TELEGRAM_AUTH_HEADER)
        except ValueError as exc:
            raise TelegramAuthenticationError(str(exc)) from exc
        verify_telegram_secret(secret, self._webhook_secret)
        if len(request.body) > self._max_body_bytes:
            raise TelegramCallbackError("Telegram body exceeds configured limit")
        payload = _strict_update_json(request.body)
        update_id = payload.get("update_id")
        if isinstance(update_id, bool) or not isinstance(update_id, int) or update_id < 0:
            raise TelegramCallbackError("update_id must be a non-negative integer")
        delivery_id = str(update_id)
        payload_sha256 = hashlib.sha256(request.body).hexdigest()

        message: Mapping[str, Any] | None = None
        callback_data: str | None = None
        callback_query_id: str | None = None
        if isinstance(payload.get("message"), Mapping):
            message = payload["message"]
        elif isinstance(payload.get("edited_message"), Mapping):
            message = payload["edited_message"]
        elif isinstance(payload.get("callback_query"), Mapping):
            query = payload["callback_query"]
            callback_query_id = _required_string(query, "id")
            candidate = query.get("message")
            if isinstance(candidate, Mapping):
                message = candidate
            data = query.get("data")
            if isinstance(data, str) and data:
                callback_data = data
            else:
                return (
                    VerifiedCallback(
                        kind=CallbackKind.CHANNEL_EVENT,
                        channel=self.channel,
                        delivery_id=delivery_id,
                        payload_sha256=payload_sha256,
                    ),
                    None,
                )
            sender = query.get("from")
            if message is not None and isinstance(sender, Mapping):
                message = dict(message)
                message["from"] = sender
        elif isinstance(payload.get("my_chat_member"), Mapping):
            return (
                VerifiedCallback(
                    kind=CallbackKind.CHANNEL_EVENT,
                    channel=self.channel,
                    delivery_id=delivery_id,
                    payload_sha256=payload_sha256,
                ),
                None,
            )

        if message is None:
            return (
                VerifiedCallback(
                    kind=CallbackKind.CHANNEL_EVENT,
                    channel=self.channel,
                    delivery_id=delivery_id,
                    payload_sha256=payload_sha256,
                ),
                None,
            )
        inbound, route = self._normalize_message(
            message,
            callback_data=callback_data,
            callback_query_id=callback_query_id,
            request=request,
            binding=binding,
            delivery_id=delivery_id,
            payload_sha256=payload_sha256,
        )
        return (
            VerifiedCallback(
                kind=CallbackKind.USER_MESSAGE,
                channel=self.channel,
                delivery_id=delivery_id,
                payload_sha256=payload_sha256,
                inbound=inbound,
            ),
            route,
        )

    def render_text(self, intent: ReplyIntent) -> tuple[str, ...]:
        """Render Telegram text within the Bot API character ceiling."""

        return split_telegram_text(intent.text or "", max_characters=4096)

    def parse_with_aiogram(self, body: bytes, bot: object) -> object:
        """Optionally validate an Update with aiogram without making it mandatory.

        Import is delayed so the protocol core remains testable when aiogram is omitted.
        """

        try:
            from aiogram.types import Update
        except ImportError as exc:
            raise RuntimeError("aiogram is not installed") from exc
        payload = _strict_update_json(body)
        return Update.model_validate(payload, context={"bot": bot})

    def _check_binding(
        self,
        request: CallbackRequest,
        binding: TrustedBindingContext,
    ) -> None:
        if not binding.enabled:
            raise TelegramCallbackError("binding is disabled")
        if binding.channel is not Channel.TELEGRAM:
            raise TelegramCallbackError("binding channel mismatch")
        if request.path_binding_id != binding.binding_id:
            raise TelegramCallbackError("callback path binding mismatch")

    def _normalize_message(
        self,
        message: Mapping[str, Any],
        *,
        callback_data: str | None,
        callback_query_id: str | None,
        request: CallbackRequest,
        binding: TrustedBindingContext,
        delivery_id: str,
        payload_sha256: str,
    ) -> tuple[NormalizedInbound, SensitiveReplyRoute]:
        sender = _required_mapping(message, "from")
        user_id = _required_integer(sender, "id")
        chat = _required_mapping(message, "chat")
        chat_id = _required_integer(chat, "id")
        message_id = _required_integer(message, "message_id")
        if user_id <= 0 or message_id <= 0:
            raise TelegramCallbackError("user id and message_id must be positive")
        chat_type = _required_string(chat, "type")
        if chat_type == "private":
            conversation_kind = ConversationKind.PRIVATE
        elif chat_type in {"group", "supergroup"}:
            conversation_kind = ConversationKind.GROUP
        else:
            raise TelegramCallbackError(f"unsupported chat type: {chat_type}")

        raw_thread_id = message.get("message_thread_id")
        if raw_thread_id is not None and (
            isinstance(raw_thread_id, bool) or not isinstance(raw_thread_id, int)
        ):
            raise TelegramCallbackError("message_thread_id must be an integer")
        if isinstance(raw_thread_id, int) and raw_thread_id <= 0:
            raise TelegramCallbackError("message_thread_id must be positive")
        external_thread_id = str(raw_thread_id) if raw_thread_id is not None else None
        identity = self._identities.derive(
            tenant_id=binding.tenant_id,
            app_id=binding.app_id,
            app_revision=binding.app_revision,
            binding_id=binding.binding_id,
            channel=self.channel,
            conversation_kind=conversation_kind,
            external_user_id=str(user_id),
            external_conversation_id=str(chat_id),
            external_thread_id=external_thread_id,
        )

        text = callback_data
        if text is None:
            candidate = message.get("text", message.get("caption"))
            if candidate is not None and not isinstance(candidate, str):
                raise TelegramCallbackError("message text/caption must be a string")
            text = candidate or None
        attachments = _extract_telegram_attachments(message)
        if not text and not attachments:
            raise TelegramCallbackError("message contains no supported content")

        reply_to: ExternalMessageRef | None = None
        replied = message.get("reply_to_message")
        if isinstance(replied, Mapping):
            reply_message_id = _required_integer(replied, "message_id")
            if reply_message_id <= 0:
                raise TelegramCallbackError("reply message_id must be positive")
            reply_to = ExternalMessageRef(
                delivery_id=self._identities.derive_message_id(
                    tenant_id=binding.tenant_id,
                    app_id=binding.app_id,
                    app_revision=binding.app_revision,
                    binding_id=binding.binding_id,
                    channel=self.channel,
                    external_message_id=str(reply_message_id),
                )
            )

        inbound = NormalizedInbound(
            tenant_id=binding.tenant_id,
            app_id=binding.app_id,
            binding_id=binding.binding_id,
            binding_revision=binding.binding_revision,
            channel=self.channel,
            delivery_id=delivery_id,
            payload_sha256=payload_sha256,
            received_at=request.received_at,
            principal_id=identity.principal_id,
            conversation_id=identity.conversation_id,
            session_id=identity.session_id,
            conversation_kind=conversation_kind,
            thread_id=identity.thread_id,
            text=text,
            attachments=attachments,
            reply_to=reply_to,
            reply_route_key=f"telegram:{binding.binding_id}:{delivery_id}",
            request_id=request.request_id,
            trace_id=request.trace_id,
        )
        route_payload: dict[str, int | str | None] = {
            "chat_id": chat_id,
            "message_thread_id": raw_thread_id,
            "reply_to_message_id": message_id,
        }
        if callback_query_id is not None:
            route_payload["callback_query_id"] = callback_query_id
        route = SensitiveReplyRoute(
            route_key=inbound.reply_route_key,
            tenant_id=binding.tenant_id,
            binding_id=binding.binding_id,
            delivery_id=delivery_id,
            channel=self.channel,
            kind=SensitiveReplyRouteKind.TELEGRAM_DELIVERY_CONTEXT,
            value=SecretStr(json.dumps(route_payload, sort_keys=True, separators=(",", ":"))),
            expires_at=request.received_at + self._route_lifetime,
        )
        return inbound, route


def _strict_update_json(data: bytes) -> dict[str, Any]:
    try:
        value = json.loads(
            data,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise TelegramCallbackError("invalid Telegram JSON") from exc
    if not isinstance(value, dict):
        raise TelegramCallbackError("Telegram JSON root must be an object")
    return value


def _required_mapping(payload: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = payload.get(name)
    if not isinstance(value, Mapping):
        raise TelegramCallbackError(f"{name} must be an object")
    return value


def _required_string(payload: Mapping[str, Any], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise TelegramCallbackError(f"{name} must be a non-empty string")
    return value


def _required_integer(payload: Mapping[str, Any], name: str) -> int:
    value = payload.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TelegramCallbackError(f"{name} must be an integer")
    return value


def _telegram_locator(file_id: str) -> str:
    digest = hashlib.sha256(file_id.encode("utf-8")).hexdigest()
    return f"telegram:{digest}"


def _file_attachment(
    content: Mapping[str, Any],
    *,
    kind: AttachmentKind,
) -> AttachmentRef:
    file_id = _required_string(content, "file_id")
    filename = content.get("file_name")
    mime_type = content.get("mime_type")
    size = content.get("file_size")
    if filename is not None and not isinstance(filename, str):
        raise TelegramCallbackError("file_name must be a string")
    if mime_type is not None and not isinstance(mime_type, str):
        raise TelegramCallbackError("mime_type must be a string")
    if size is not None and (isinstance(size, bool) or not isinstance(size, int) or size < 0):
        raise TelegramCallbackError("file_size must be a non-negative integer")
    return AttachmentRef(
        kind=kind,
        locator_key=_telegram_locator(file_id),
        filename=filename,
        mime_type=mime_type,
        size_bytes=size,
    )


def _extract_telegram_attachments(
    message: Mapping[str, Any],
) -> tuple[AttachmentRef, ...]:
    attachments: list[AttachmentRef] = []
    photos = message.get("photo")
    if photos is not None:
        if not isinstance(photos, list) or not photos:
            raise TelegramCallbackError("photo must be a non-empty array")
        photo = photos[-1]
        if not isinstance(photo, Mapping):
            raise TelegramCallbackError("photo item must be an object")
        attachments.append(_file_attachment(photo, kind=AttachmentKind.IMAGE))

    for field, kind in (
        ("document", AttachmentKind.FILE),
        ("audio", AttachmentKind.AUDIO),
        ("voice", AttachmentKind.VOICE),
        ("video", AttachmentKind.VIDEO),
        ("animation", AttachmentKind.VIDEO),
    ):
        content = message.get(field)
        if content is not None:
            if not isinstance(content, Mapping):
                raise TelegramCallbackError(f"{field} must be an object")
            attachments.append(_file_attachment(content, kind=kind))
    return tuple(attachments)
