"""Enterprise WeChat intelligent-robot JSON callback protocol.

This module deliberately does not implement the legacy XML/CorpId group-robot
protocol. Intelligent-robot callbacks use a JSON envelope and an empty ReceiveId.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
import struct
import time
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Any, NoReturn
from urllib.parse import urlsplit

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from pydantic import SecretStr

from trpc_service.channels.contracts import (
    AttachmentKind,
    AttachmentRef,
    CallbackKind,
    CallbackRequest,
    Channel,
    ConversationKind,
    NormalizedInbound,
    ReplyIntent,
    SensitiveReplyRoute,
    SensitiveReplyRouteKind,
    TrustedBindingContext,
    VerifiedCallback,
)
from trpc_service.channels.session import ChannelIdentityDeriver
from trpc_service.channels.text import split_utf8_bytes

_PAD_BLOCK_BYTES = 32
_AES_BLOCK_BYTES = 16
_PREFIX_BYTES = 20
_DEFAULT_MAX_BODY_BYTES = 1_048_576
_DEFAULT_RESPONSE_HOSTS = frozenset({"qyapi.weixin.qq.com"})
_RESPONSE_ROUTE_LIFETIME = timedelta(hours=1)
_DEFAULT_CALLBACK_SKEW = timedelta(hours=24)


class WeComProtocolError(ValueError):
    """Base error safe to map to an authentication/protocol failure."""


class WeComSignatureError(WeComProtocolError):
    """The callback signature did not match."""


class WeComCryptoError(WeComProtocolError):
    """The encrypted callback could not be safely decoded."""


class WeComCallbackError(WeComProtocolError):
    """The decrypted callback schema or trusted binding was invalid."""


def _reject_json_constant(value: str) -> NoReturn:
    raise ValueError(f"non-finite JSON number is forbidden: {value}")


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _strict_json_object(data: bytes) -> dict[str, Any]:
    try:
        value = json.loads(
            data,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise WeComCallbackError("invalid JSON callback") from exc
    if not isinstance(value, dict):
        raise WeComCallbackError("callback JSON root must be an object")
    return value


class WeComCrypto:
    """Strict implementation of the official SHA1 + AES-CBC callback scheme."""

    def __init__(
        self,
        token: str,
        encoding_aes_key: str,
        *,
        receive_id: str = "",
        random_source: Callable[[int], bytes] = secrets.token_bytes,
    ) -> None:
        if not token:
            raise ValueError("token must not be empty")
        if receive_id != "":
            raise ValueError("intelligent-robot ReceiveId must be an empty string")
        try:
            key = base64.b64decode(encoding_aes_key + "=", validate=True)
        except (ValueError, TypeError) as exc:
            raise ValueError("invalid EncodingAESKey") from exc
        if len(key) != 32:
            raise ValueError("EncodingAESKey must decode to exactly 32 bytes")
        self._token = token
        self._key = key
        self._receive_id = receive_id.encode("utf-8")
        self._random_source = random_source

    def signature(self, *, timestamp: str, nonce: str, ciphertext: str) -> str:
        """Compute the official lexicographically sorted SHA-1 signature."""

        if not timestamp or not nonce or not ciphertext:
            raise WeComProtocolError("signature inputs must not be empty")
        pieces = sorted((self._token, timestamp, nonce, ciphertext))
        return hashlib.sha1("".join(pieces).encode("utf-8"), usedforsecurity=False).hexdigest()

    def verify_signature(
        self,
        *,
        msg_signature: str,
        timestamp: str,
        nonce: str,
        ciphertext: str,
    ) -> None:
        """Verify a callback signature with constant-time comparison."""

        expected = self.signature(timestamp=timestamp, nonce=nonce, ciphertext=ciphertext)
        if not msg_signature or not hmac.compare_digest(expected, msg_signature):
            raise WeComSignatureError("invalid callback signature")

    def verify_url(
        self,
        *,
        msg_signature: str,
        timestamp: str,
        nonce: str,
        echostr: str,
    ) -> bytes:
        """Verify and decrypt URL-validation echostr, returning exact response bytes."""

        self.verify_signature(
            msg_signature=msg_signature,
            timestamp=timestamp,
            nonce=nonce,
            ciphertext=echostr,
        )
        return self.decrypt_ciphertext(echostr)

    def decrypt_callback(
        self,
        body: bytes,
        *,
        msg_signature: str,
        timestamp: str,
        nonce: str,
        max_body_bytes: int = _DEFAULT_MAX_BODY_BYTES,
    ) -> dict[str, Any]:
        """Verify the encrypted JSON envelope and decode the business JSON object."""

        if len(body) > max_body_bytes:
            raise WeComCallbackError("callback body exceeds configured limit")
        envelope = _strict_json_object(body)
        ciphertext = envelope.get("encrypt")
        if not isinstance(ciphertext, str) or not ciphertext:
            raise WeComCallbackError("callback envelope requires string encrypt")
        self.verify_signature(
            msg_signature=msg_signature,
            timestamp=timestamp,
            nonce=nonce,
            ciphertext=ciphertext,
        )
        return _strict_json_object(self.decrypt_ciphertext(ciphertext))

    def encrypt_reply(
        self,
        payload: Mapping[str, Any],
        *,
        nonce: str,
        timestamp: int | str | None = None,
    ) -> bytes:
        """Encrypt a passive JSON reply using the callback nonce."""

        if not nonce:
            raise WeComProtocolError("nonce must not be empty")
        timestamp_text = str(int(time.time()) if timestamp is None else timestamp)
        message = json.dumps(
            dict(payload),
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf-8")
        ciphertext = self.encrypt_plaintext(message)
        signature = self.signature(
            timestamp=timestamp_text,
            nonce=nonce,
            ciphertext=ciphertext,
        )
        envelope = {
            "encrypt": ciphertext,
            "msgsignature": signature,
            "timestamp": int(timestamp_text),
            "nonce": nonce,
        }
        return json.dumps(envelope, separators=(",", ":")).encode("utf-8")

    def encrypt_plaintext(self, message: bytes) -> str:
        """Encrypt plaintext; public for deterministic static-vector verification."""

        random_prefix = self._random_source(16)
        if len(random_prefix) != 16:
            raise WeComCryptoError("random source must return exactly 16 bytes")
        packed = random_prefix + struct.pack("!I", len(message)) + message + self._receive_id
        padded = _pad_32(packed)
        encryptor = Cipher(algorithms.AES(self._key), modes.CBC(self._key[:16])).encryptor()
        encrypted = encryptor.update(padded) + encryptor.finalize()
        return base64.b64encode(encrypted).decode("ascii")

    def decrypt_ciphertext(self, ciphertext: str) -> bytes:
        """Decrypt and strictly validate one official callback ciphertext."""

        try:
            encrypted = base64.b64decode(ciphertext, validate=True)
            if not encrypted or len(encrypted) % _AES_BLOCK_BYTES:
                raise ValueError("ciphertext length")
            decryptor = Cipher(
                algorithms.AES(self._key),
                modes.CBC(self._key[:16]),
            ).decryptor()
            padded = decryptor.update(encrypted) + decryptor.finalize()
            plaintext = _unpad_32(padded)
            if len(plaintext) < _PREFIX_BYTES:
                raise ValueError("plaintext is too short")
            message_length = struct.unpack("!I", plaintext[16:20])[0]
            message_end = _PREFIX_BYTES + message_length
            if message_end > len(plaintext):
                raise ValueError("declared message length exceeds plaintext")
            message = plaintext[_PREFIX_BYTES:message_end]
            receive_id = plaintext[message_end:]
            if not hmac.compare_digest(receive_id, self._receive_id):
                raise ValueError("ReceiveId mismatch")
            return message
        except (ValueError, TypeError) as exc:
            raise WeComCryptoError("invalid encrypted callback") from exc


def _pad_32(data: bytes) -> bytes:
    padding_length = _PAD_BLOCK_BYTES - (len(data) % _PAD_BLOCK_BYTES)
    return data + bytes((padding_length,)) * padding_length


def _unpad_32(data: bytes) -> bytes:
    if not data or len(data) % _AES_BLOCK_BYTES:
        raise ValueError("invalid padded plaintext")
    padding_length = data[-1]
    if padding_length < 1 or padding_length > _PAD_BLOCK_BYTES:
        raise ValueError("invalid padded plaintext")
    if len(data) < padding_length:
        raise ValueError("invalid padded plaintext")
    expected = bytes((padding_length,)) * padding_length
    if not hmac.compare_digest(data[-padding_length:], expected):
        raise ValueError("invalid padded plaintext")
    return data[:-padding_length]


class WeComAdapter:
    """Authenticate and normalize WeCom intelligent-robot callbacks."""

    channel = Channel.WECOM

    def __init__(
        self,
        crypto: WeComCrypto,
        identities: ChannelIdentityDeriver,
        *,
        max_body_bytes: int = _DEFAULT_MAX_BODY_BYTES,
        allowed_response_hosts: frozenset[str] = _DEFAULT_RESPONSE_HOSTS,
        max_callback_skew: timedelta = _DEFAULT_CALLBACK_SKEW,
    ) -> None:
        if max_body_bytes < 1:
            raise ValueError("max_body_bytes must be positive")
        if not allowed_response_hosts or any(not host for host in allowed_response_hosts):
            raise ValueError("allowed_response_hosts must not be empty")
        if max_callback_skew <= timedelta(0):
            raise ValueError("max_callback_skew must be positive")
        self._crypto = crypto
        self._identities = identities
        self._max_body_bytes = max_body_bytes
        self._allowed_response_hosts = frozenset(host.casefold() for host in allowed_response_hosts)
        self._max_callback_skew = max_callback_skew

    def verify_and_normalize(
        self,
        request: CallbackRequest,
        binding: TrustedBindingContext,
    ) -> VerifiedCallback:
        """Verify a callback without exposing its credential-bearing reply route.

        Web ingress should call :meth:`verify_decrypt_and_normalize` so the route can
        be encrypted and committed with its Inbox row before acknowledging WeCom.
        """

        callback, _ = self.verify_decrypt_and_normalize(request, binding)
        return callback

    def verify_decrypt_and_normalize(
        self,
        request: CallbackRequest,
        binding: TrustedBindingContext,
    ) -> tuple[VerifiedCallback, SensitiveReplyRoute | None]:
        """Verify once and separate log-safe input from the sensitive response URL."""

        self._check_binding(request, binding)
        if len(request.body) > self._max_body_bytes:
            raise WeComCallbackError("callback body exceeds configured limit")
        signature = _required_query(request, "msg_signature")
        timestamp = _required_query(request, "timestamp")
        nonce = _required_query(request, "nonce")
        _validate_callback_timestamp(
            timestamp,
            received_at=request.received_at,
            max_skew=self._max_callback_skew,
        )
        payload = self._crypto.decrypt_callback(
            request.body,
            msg_signature=signature,
            timestamp=timestamp,
            nonce=nonce,
            max_body_bytes=self._max_body_bytes,
        )
        payload_sha256 = hashlib.sha256(request.body).hexdigest()
        delivery_id = _required_string(payload, "msgid")
        account_id = _required_string(payload, "aibotid")
        if not hmac.compare_digest(account_id, binding.external_account_id):
            raise WeComCallbackError("callback account does not match binding")

        msg_type = _required_string(payload, "msgtype")
        if msg_type == "stream":
            stream = _required_mapping(payload, "stream")
            control_id = _required_string(stream, "id")
            return (
                VerifiedCallback(
                    kind=CallbackKind.CONTROL_REFRESH,
                    channel=self.channel,
                    delivery_id=delivery_id,
                    payload_sha256=payload_sha256,
                    control_id=control_id,
                ),
                None,
            )
        if msg_type == "event":
            return (
                VerifiedCallback(
                    kind=CallbackKind.CHANNEL_EVENT,
                    channel=self.channel,
                    delivery_id=delivery_id,
                    payload_sha256=payload_sha256,
                ),
                None,
            )

        inbound = self._normalize_user_message(
            payload,
            request=request,
            binding=binding,
            payload_sha256=payload_sha256,
        )
        response_url = _validated_response_url(
            _required_string(payload, "response_url"),
            allowed_hosts=self._allowed_response_hosts,
        )
        callback = VerifiedCallback(
            kind=CallbackKind.USER_MESSAGE,
            channel=self.channel,
            delivery_id=delivery_id,
            payload_sha256=payload_sha256,
            inbound=inbound,
        )
        route = SensitiveReplyRoute(
            route_key=inbound.reply_route_key,
            tenant_id=binding.tenant_id,
            binding_id=binding.binding_id,
            delivery_id=delivery_id,
            channel=self.channel,
            kind=SensitiveReplyRouteKind.WECOM_RESPONSE_URL,
            value=SecretStr(response_url),
            expires_at=request.received_at + _RESPONSE_ROUTE_LIFETIME,
            max_uses=1,
        )
        return callback, route

    def verify_url(self, request: CallbackRequest, binding: TrustedBindingContext) -> bytes:
        """Verify a GET URL-validation callback."""

        self._check_binding(request, binding)
        timestamp = _required_query(request, "timestamp")
        _validate_callback_timestamp(
            timestamp,
            received_at=request.received_at,
            max_skew=self._max_callback_skew,
        )
        return self._crypto.verify_url(
            msg_signature=_required_query(request, "msg_signature"),
            timestamp=timestamp,
            nonce=_required_query(request, "nonce"),
            echostr=_required_query(request, "echostr"),
        )

    def encode_passive_reply(
        self,
        payload: Mapping[str, Any],
        *,
        nonce: str,
        timestamp: int | str | None = None,
    ) -> bytes:
        """Encode an encrypted passive callback response."""

        return self._crypto.encrypt_reply(payload, nonce=nonce, timestamp=timestamp)

    def render_text(self, intent: ReplyIntent) -> tuple[str, ...]:
        """Render WeCom Markdown/stream content within its UTF-8 byte ceiling."""

        return split_utf8_bytes(intent.text or "", max_bytes=20_480)

    def _check_binding(
        self,
        request: CallbackRequest,
        binding: TrustedBindingContext,
    ) -> None:
        if not binding.enabled:
            raise WeComCallbackError("binding is disabled")
        if binding.channel is not Channel.WECOM:
            raise WeComCallbackError("binding channel mismatch")
        if request.path_binding_id != binding.binding_id:
            raise WeComCallbackError("callback path binding mismatch")

    def _normalize_user_message(
        self,
        payload: Mapping[str, Any],
        *,
        request: CallbackRequest,
        binding: TrustedBindingContext,
        payload_sha256: str,
    ) -> NormalizedInbound:
        delivery_id = _required_string(payload, "msgid")
        chat_type = _required_string(payload, "chattype")
        sender = _required_mapping(payload, "from")
        external_user_id = _required_string(sender, "userid")
        if chat_type == "single":
            conversation_kind = ConversationKind.PRIVATE
            external_conversation_id = external_user_id
        elif chat_type == "group":
            conversation_kind = ConversationKind.GROUP
            external_conversation_id = _required_string(payload, "chatid")
        else:
            raise WeComCallbackError("unsupported chattype")

        text, attachments = _extract_wecom_content(payload)
        identity = self._identities.derive(
            tenant_id=binding.tenant_id,
            app_id=binding.app_id,
            app_revision=binding.app_revision,
            binding_id=binding.binding_id,
            channel=self.channel,
            conversation_kind=conversation_kind,
            external_user_id=external_user_id,
            external_conversation_id=external_conversation_id,
        )
        return NormalizedInbound(
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
            text=text,
            attachments=attachments,
            reply_route_key=f"wecom:{binding.binding_id}:{delivery_id}",
            request_id=request.request_id,
            trace_id=request.trace_id,
        )


def _required_query(request: CallbackRequest, name: str) -> str:
    try:
        value = request.query_value(name)
    except ValueError as exc:
        raise WeComCallbackError(str(exc)) from exc
    if value is None or not value:
        raise WeComCallbackError(f"missing query parameter: {name}")
    return value


def _validate_callback_timestamp(
    timestamp: str,
    *,
    received_at: datetime,
    max_skew: timedelta,
) -> None:
    """Reject first-seen signed callbacks captured outside the replay window."""

    if not timestamp.isascii() or not timestamp.isdecimal() or len(timestamp) > 12:
        raise WeComCallbackError("invalid callback timestamp")
    try:
        signed_at = datetime.fromtimestamp(int(timestamp), tz=UTC)
    except (OverflowError, OSError, ValueError) as exc:
        raise WeComCallbackError("invalid callback timestamp") from exc
    if abs(received_at.astimezone(UTC) - signed_at) > max_skew:
        raise WeComCallbackError("callback timestamp is outside the replay window")


def _required_string(payload: Mapping[str, Any], name: str) -> str:
    value = payload.get(name)
    if not isinstance(value, str) or not value:
        raise WeComCallbackError(f"{name} must be a non-empty string")
    return value


def _required_mapping(payload: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = payload.get(name)
    if not isinstance(value, Mapping):
        raise WeComCallbackError(f"{name} must be an object")
    return value


def _opaque_locator(channel: str, raw_locator: str) -> str:
    digest = hashlib.sha256(raw_locator.encode("utf-8")).hexdigest()
    return f"{channel}:{digest}"


def _validated_response_url(raw_url: str, *, allowed_hosts: frozenset[str]) -> str:
    """Reject credential routes that could become an arbitrary server-side request."""

    try:
        parsed = urlsplit(raw_url)
        port = parsed.port
    except ValueError as exc:
        raise WeComCallbackError("invalid response_url") from exc
    if (
        parsed.scheme.casefold() != "https"
        or parsed.hostname is None
        or parsed.hostname.casefold() not in allowed_hosts
        or parsed.username is not None
        or parsed.password is not None
        or port not in {None, 443}
        or parsed.fragment
    ):
        raise WeComCallbackError("response_url is outside the trusted endpoint policy")
    return raw_url


def _extract_wecom_content(
    payload: Mapping[str, Any],
) -> tuple[str | None, tuple[AttachmentRef, ...]]:
    msg_type = _required_string(payload, "msgtype")
    attachments: list[AttachmentRef] = []
    text: str | None = None

    if msg_type == "text":
        text = _required_string(_required_mapping(payload, "text"), "content")
    elif msg_type == "voice":
        text = _required_string(_required_mapping(payload, "voice"), "content")
    elif msg_type == "mixed":
        mixed = _required_mapping(payload, "mixed")
        items = mixed.get("msg_item")
        if not isinstance(items, list) or not items:
            raise WeComCallbackError("mixed.msg_item must be a non-empty array")
        text_parts: list[str] = []
        for item in items:
            if not isinstance(item, Mapping):
                raise WeComCallbackError("mixed item must be an object")
            item_type = _required_string(item, "msgtype")
            if item_type == "text":
                text_parts.append(_required_string(_required_mapping(item, "text"), "content"))
            elif item_type == "image":
                url = _required_string(_required_mapping(item, "image"), "url")
                attachments.append(
                    AttachmentRef(
                        kind=AttachmentKind.IMAGE,
                        locator_key=_opaque_locator("wecom", url),
                    )
                )
        text = "\n".join(text_parts) or None
    elif msg_type in {"image", "file", "video"}:
        content = _required_mapping(payload, msg_type)
        url = _required_string(content, "url")
        kind = {
            "image": AttachmentKind.IMAGE,
            "file": AttachmentKind.FILE,
            "video": AttachmentKind.VIDEO,
        }[msg_type]
        attachments.append(AttachmentRef(kind=kind, locator_key=_opaque_locator("wecom", url)))
    else:
        raise WeComCallbackError(f"unsupported user message type: {msg_type}")

    if not text and not attachments:
        raise WeComCallbackError("message contains no supported content")
    return text, tuple(attachments)
