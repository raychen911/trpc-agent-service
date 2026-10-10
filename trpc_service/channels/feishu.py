"""Feishu encrypted webhook and OpenAPI channel adapter.

The official Feishu Python channel SDK currently requires ``websockets<16``
while the tRPC-Agent OpenClaw dependency requires ``websockets>=16``. This
adapter implements the documented HTTP webhook and OpenAPI surface with the
project's existing dependencies. The durable Inbox/Outbox remains authoritative.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import builtins
import hashlib
import hmac
import json
import math
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any, cast
from urllib.parse import quote, unquote

import httpx
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from trpc_service.channels.base import ChannelCapabilities, WebhookRequest
from trpc_service.channels.envelopes import (
    DeliveryReceipt,
    DeliveryStatus,
    InboundEnvelope,
    MediaReference,
    OutboundEnvelope,
    PayloadKind,
    RecallEnvelope,
)
from trpc_service.config.secrets import SecretProvider, SecretRef
from trpc_service.tenant.models import Channel, ChannelBinding, ConversationKind

_AUTH_PATH = "/open-apis/auth/v3/tenant_access_token/internal"
_MESSAGE_PATH = "/open-apis/im/v1/messages"
_RESOURCE_PATH = f"{_MESSAGE_PATH}/{{message_id}}/resources/{{resource_key}}"
_RETRYABLE_CODES = {99991400, 99991401, 99991402, 99991672}
_TOKEN_INVALID_CODES = {99991663, 99991664, 99991668}
_RESOURCE_TYPES = {"file", "image"}
_DEFAULT_MAX_MEDIA_BYTES = 64 * 1024 * 1024
_DEFAULT_MEDIA_TIMEOUT_SECONDS = 30.0
_DEFAULT_MEDIA_CHUNK_BYTES = 64 * 1024
_MAX_ERROR_BODY_BYTES = 64 * 1024


class FeishuVerificationError(ValueError):
    """Safe callback rejection that never includes request or secret content."""


class _TokenError(RuntimeError):
    def __init__(
        self,
        provider_code: str,
        *,
        retryable: bool,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__("Feishu access token is unavailable")
        self.provider_code = provider_code
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds


class FeishuResourceError(RuntimeError):
    """Safe resource download failure without provider response body content."""

    def __init__(
        self,
        provider_code: str,
        *,
        retryable: bool = False,
        ambiguous: bool = False,
        status_code: int | None = None,
        token_invalid: bool = False,
        retry_after_seconds: float | None = None,
    ) -> None:
        super().__init__("Feishu message resource download failed")
        self.provider_code = provider_code
        self.retryable = retryable
        self.ambiguous = ambiguous
        self.status_code = status_code
        self.token_invalid = token_invalid
        self.retry_after_seconds = retry_after_seconds


FeishuMediaError = FeishuResourceError


@dataclass(frozen=True, slots=True)
class FeishuResource:
    """Downloaded message resource bytes and safe provider metadata."""

    bytes: builtins.bytes
    content_type: str
    filename: str | None
    provider_media_id: str

    @property
    def data(self) -> builtins.bytes:
        return self.bytes

    @property
    def provider_id(self) -> str:
        return self.provider_media_id


@dataclass(frozen=True, slots=True)
class FeishuCallback:
    envelope: InboundEnvelope | None = None
    challenge: str | None = None

    @property
    def acknowledgement(self) -> dict[str, object]:
        if self.challenge is not None:
            return {"challenge": self.challenge}
        return {"msg": "success"}


class FeishuAdapter:
    """Verify Feishu callbacks and deliver durable outbound messages."""

    API_ROOT = "https://open.feishu.cn"
    capabilities = ChannelCapabilities(
        outbound_payloads=frozenset({PayloadKind.TEXT}),
        stream=False,
        card=False,
        media=False,
        recall=False,
        proactive=True,
        text_split=False,
        max_text_bytes=None,
    )

    def __init__(
        self,
        secrets: SecretProvider,
        *,
        http_client: httpx.AsyncClient | None = None,
        api_root: str = API_ROOT,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], float] = time.time,
        refresh_skew_seconds: float = 300,
        max_callback_age_seconds: float = 300,
        max_callback_bytes: int = 2 * 1024 * 1024,
        max_media_bytes: int = _DEFAULT_MAX_MEDIA_BYTES,
        media_timeout_seconds: float = _DEFAULT_MEDIA_TIMEOUT_SECONDS,
        media_chunk_bytes: int = _DEFAULT_MEDIA_CHUNK_BYTES,
    ) -> None:
        if refresh_skew_seconds < 0:
            raise ValueError("refresh skew must not be negative")
        if max_callback_age_seconds <= 0:
            raise ValueError("callback age limit must be positive")
        if max_callback_bytes < 1024:
            raise ValueError("callback body limit is too small")
        if max_media_bytes <= 0:
            raise ValueError("media byte limit must be positive")
        if media_timeout_seconds <= 0:
            raise ValueError("media timeout must be positive")
        if media_chunk_bytes <= 0:
            raise ValueError("media chunk size must be positive")
        self._secrets = secrets
        self._http = http_client or httpx.AsyncClient(timeout=10)
        self._owns_http = http_client is None
        self._api_root = api_root.rstrip("/")
        self._clock = clock
        self._wall_clock = wall_clock
        self._refresh_skew_seconds = refresh_skew_seconds
        self._max_callback_age_seconds = max_callback_age_seconds
        self._max_callback_bytes = max_callback_bytes
        self._max_media_bytes = max_media_bytes
        self._media_timeout_seconds = media_timeout_seconds
        self._media_chunk_bytes = media_chunk_bytes
        self._tokens: dict[tuple[str, str, str], tuple[str, float]] = {}
        self._token_locks: dict[tuple[str, str, str], asyncio.Lock] = {}

    def verify_and_parse(self, request: WebhookRequest, binding: ChannelBinding) -> FeishuCallback:
        if request.method.upper() != "POST":
            raise FeishuVerificationError("Feishu callback method is invalid")
        if binding.channel != Channel.FEISHU:
            raise FeishuVerificationError("Feishu callback binding is invalid")
        if not request.body or len(request.body) > self._max_callback_bytes:
            raise FeishuVerificationError("Feishu callback body size is invalid")

        verification_token = self._required_secret(binding, "verification_token")
        encrypt_ref = binding.secret_refs.get("encrypt_key")
        encrypt_key = self._resolve(encrypt_ref, "encrypt key") if encrypt_ref else None
        payload, signature_verified, encrypted = self._decode_payload(request, encrypt_key)
        self._verify_token(payload, verification_token)

        event_type = _event_type(payload)
        if event_type == "url_verification":
            if encrypt_key is not None and not (encrypted or signature_verified):
                raise FeishuVerificationError("Feishu challenge authentication is invalid")
            challenge = payload.get("challenge")
            if not isinstance(challenge, str) or not challenge:
                raise FeishuVerificationError("Feishu challenge is invalid")
            return FeishuCallback(challenge=challenge)

        if encrypt_key is not None and not signature_verified:
            self._verify_signature(request, encrypt_key)
        if event_type != "im.message.receive_v1":
            return FeishuCallback()
        return FeishuCallback(envelope=self._parse_message(payload, binding))

    async def send(self, envelope: OutboundEnvelope, binding: ChannelBinding) -> DeliveryReceipt:
        if envelope.channel != Channel.FEISHU or binding.channel != Channel.FEISHU:
            return self._failed(envelope, "binding_mismatch")
        if envelope.payload_kind != PayloadKind.TEXT or not envelope.text:
            return self._failed(envelope, "unsupported_payload")

        try:
            token = await self._token_for(binding)
        except _TokenError as exc:
            return self._failed(
                envelope,
                exc.provider_code,
                retryable=exc.retryable,
                retry_after_seconds=exc.retry_after_seconds,
            )

        response = await self._send_request(envelope, token)
        if response is None:
            return DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.AMBIGUOUS,
                provider_code="transport_unknown",
            )
        receipt, token_invalid = self._receipt(envelope, response)
        if not token_invalid:
            return receipt

        self._invalidate_token(binding)
        try:
            token = await self._token_for(binding)
        except _TokenError as exc:
            return self._failed(
                envelope,
                exc.provider_code,
                retryable=exc.retryable,
                retry_after_seconds=exc.retry_after_seconds,
            )
        response = await self._send_request(envelope, token)
        if response is None:
            return DeliveryReceipt(
                outbound_id=envelope.outbound_id,
                status=DeliveryStatus.AMBIGUOUS,
                provider_code="transport_unknown",
                attempts=2,
            )
        refreshed, _ = self._receipt(envelope, response, attempts=2)
        return refreshed

    async def recall(self, envelope: RecallEnvelope, binding: ChannelBinding) -> DeliveryReceipt:
        """Fail closed because this adapter has no audited recall implementation."""

        del binding
        return DeliveryReceipt(
            outbound_id=envelope.outbound_id,
            status=DeliveryStatus.FAILED,
            provider_code="unsupported_capability",
        )

    async def download_resource(
        self,
        binding: ChannelBinding,
        message_id: str,
        resource_key: str,
        *,
        resource_type: str = "file",
        filename: str | None = None,
    ) -> FeishuResource:
        """Download an image or file attached to a Feishu message.

        ``resource_key`` is the ``image_key`` or ``file_key`` from the verified
        callback. The response body is buffered only after both the advertised
        and observed sizes pass ``max_media_bytes``. Provider response bodies
        are consumed only for error-code mapping and are never included in an
        exception or log message.
        """
        if binding.channel != Channel.FEISHU:
            raise FeishuResourceError("binding_mismatch")
        message_id = _resource_identifier(message_id, "message id")
        resource_key = _resource_identifier(resource_key, "resource key")
        resource_type = _resource_type(resource_type)

        try:
            token = await self._token_for(binding)
        except _TokenError as exc:
            raise FeishuResourceError(
                exc.provider_code,
                retryable=exc.retryable,
                retry_after_seconds=exc.retry_after_seconds,
            ) from None

        try:
            return await self._download_resource_once(
                binding,
                message_id,
                resource_key,
                resource_type,
                filename,
                token,
            )
        except FeishuResourceError as exc:
            if not exc.token_invalid:
                raise

        # A resource request is retried exactly once after invalidating the
        # cached tenant token. This mirrors send() and avoids an unbounded
        # refresh loop when credentials or provider state are wrong.
        self._invalidate_token(binding)
        try:
            token = await self._token_for(binding)
        except _TokenError as exc:
            raise FeishuResourceError(
                exc.provider_code,
                retryable=exc.retryable,
                retry_after_seconds=exc.retry_after_seconds,
            ) from None
        try:
            return await self._download_resource_once(
                binding,
                message_id,
                resource_key,
                resource_type,
                filename,
                token,
            )
        except FeishuResourceError as exc:
            if exc.token_invalid:
                raise FeishuResourceError(
                    exc.provider_code,
                    status_code=exc.status_code,
                ) from None
            raise

    async def download_media(
        self,
        binding: ChannelBinding,
        message_id: str,
        media_key: str,
        *,
        media_type: str = "file",
        filename: str | None = None,
        media_reference: MediaReference | None = None,
    ) -> FeishuResource:
        """Compatibility entry point using the channel's media terminology."""
        del media_reference
        return await self.download_resource(
            binding,
            message_id,
            media_key,
            resource_type=media_type,
            filename=filename,
        )

    async def _download_resource_once(
        self,
        binding: ChannelBinding,
        message_id: str,
        resource_key: str,
        resource_type: str,
        filename: str | None,
        token: str,
    ) -> FeishuResource:
        path = _RESOURCE_PATH.format(
            message_id=quote(message_id, safe=""),
            resource_key=quote(resource_key, safe=""),
        )
        headers = {"Authorization": f"Bearer {token}"}
        try:
            async with self._http.stream(
                "GET",
                f"{self._api_root}{path}",
                params={"type": resource_type},
                headers=headers,
                timeout=self._media_timeout_seconds,
            ) as response:
                if not response.is_success:
                    raise await self._resource_response_error(response)

                content_length = _content_length(response.headers.get("Content-Length"))
                if content_length is not None and content_length > self._max_media_bytes:
                    raise FeishuResourceError(
                        "media_too_large",
                        status_code=response.status_code,
                    )

                body = bytearray()
                async for chunk in response.aiter_bytes(chunk_size=self._media_chunk_bytes):
                    if len(body) + len(chunk) > self._max_media_bytes:
                        raise FeishuResourceError(
                            "media_too_large",
                            status_code=response.status_code,
                        )
                    body.extend(chunk)

                content_type = _content_type(response.headers.get("Content-Type"))
                if content_type in {"application/json", "text/json"}:
                    error_code = _json_error_code(bytes(body))
                    if error_code not in (None, 0):
                        raise await self._resource_response_error(response, code=error_code)
                response_filename = _content_disposition_filename(
                    response.headers.get("Content-Disposition")
                )
                return FeishuResource(
                    bytes=bytes(body),
                    content_type=content_type,
                    filename=_safe_filename(filename) or response_filename,
                    provider_media_id=resource_key,
                )
        except FeishuResourceError:
            raise
        except httpx.TimeoutException:
            raise FeishuResourceError("transport_timeout", retryable=True, ambiguous=True) from None
        except (httpx.NetworkError, httpx.ProtocolError):
            raise FeishuResourceError("transport_unknown", retryable=True, ambiguous=True) from None
        except httpx.HTTPError:
            raise FeishuResourceError("transport_error", retryable=True, ambiguous=True) from None

    async def _resource_response_error(
        self, response: httpx.Response, *, code: int | None = None
    ) -> FeishuResourceError:
        if code is None:
            code = await _response_error_code(response)
        token_invalid = response.status_code == 401 or code in _TOKEN_INVALID_CODES
        if token_invalid:
            provider_code = f"token_{code}" if code is not None else "token_invalid"
            return FeishuResourceError(
                provider_code,
                status_code=response.status_code,
                token_invalid=True,
            )
        if response.status_code == 404:
            return FeishuResourceError("resource_not_found", status_code=404)
        if response.status_code == 429:
            return FeishuResourceError(
                "rate_limited",
                retryable=True,
                status_code=429,
                retry_after_seconds=_retry_after_seconds(response),
            )
        retryable = response.status_code >= 500 or code in _RETRYABLE_CODES
        if response.status_code >= 500:
            provider_code = f"http_{response.status_code}"
        elif code is not None and code != response.status_code:
            provider_code = str(code)
        else:
            provider_code = f"http_{response.status_code}"
        return FeishuResourceError(
            provider_code,
            retryable=retryable,
            status_code=response.status_code,
        )

    async def _send_request(self, envelope: OutboundEnvelope, token: str) -> httpx.Response | None:
        content = json.dumps({"text": envelope.text}, ensure_ascii=False, separators=(",", ":"))
        body: dict[str, object] = {
            "msg_type": "text",
            "content": content,
            "uuid": envelope.outbound_id,
        }
        headers = {"Authorization": f"Bearer {token}"}
        try:
            if envelope.in_reply_to:
                return await self._http.post(
                    f"{self._api_root}{_MESSAGE_PATH}/{envelope.in_reply_to}/reply",
                    headers=headers,
                    json=body,
                )
            body["receive_id"] = envelope.target_id
            return await self._http.post(
                f"{self._api_root}{_MESSAGE_PATH}",
                params={"receive_id_type": _receive_id_type(envelope.target_id)},
                headers=headers,
                json=body,
            )
        except httpx.TransportError:
            return None

    def _receipt(
        self,
        envelope: OutboundEnvelope,
        response: httpx.Response,
        *,
        attempts: int = 1,
    ) -> tuple[DeliveryReceipt, bool]:
        try:
            payload = response.json()
        except ValueError:
            if response.is_success:
                return (
                    DeliveryReceipt(
                        outbound_id=envelope.outbound_id,
                        status=DeliveryStatus.AMBIGUOUS,
                        provider_code="invalid_response",
                        attempts=attempts,
                    ),
                    False,
                )
            return (
                self._failed(
                    envelope,
                    "invalid_response",
                    retryable=response.status_code == 429 or response.status_code >= 500,
                    retry_after_seconds=(
                        _retry_after_seconds(response)
                        if response.status_code == 429 or response.status_code >= 500
                        else None
                    ),
                    attempts=attempts,
                ),
                False,
            )
        if not isinstance(payload, Mapping):
            if response.is_success:
                return (
                    DeliveryReceipt(
                        outbound_id=envelope.outbound_id,
                        status=DeliveryStatus.AMBIGUOUS,
                        provider_code="invalid_response",
                        attempts=attempts,
                    ),
                    False,
                )
            retryable = response.status_code == 429 or response.status_code >= 500
            return (
                self._failed(
                    envelope,
                    "invalid_response",
                    retryable=retryable,
                    retry_after_seconds=(
                        _retry_after_seconds(response, payload=payload) if retryable else None
                    ),
                    attempts=attempts,
                ),
                False,
            )

        raw_code = payload.get("code")
        if response.is_success and (isinstance(raw_code, bool) or not isinstance(raw_code, int)):
            return (
                DeliveryReceipt(
                    outbound_id=envelope.outbound_id,
                    status=DeliveryStatus.AMBIGUOUS,
                    provider_code="invalid_response",
                    attempts=attempts,
                ),
                False,
            )

        code = _integer(raw_code, default=response.status_code)
        if response.is_success and code == 0:
            data = payload.get("data")
            message_id = data.get("message_id") if isinstance(data, Mapping) else None
            return (
                DeliveryReceipt(
                    outbound_id=envelope.outbound_id,
                    status=DeliveryStatus.DELIVERED,
                    provider_message_id=message_id if isinstance(message_id, str) else None,
                    provider_code="0",
                    attempts=attempts,
                ),
                False,
            )
        retryable = response.status_code == 429 or response.status_code >= 500
        retryable = retryable or code in _RETRYABLE_CODES
        return (
            self._failed(
                envelope,
                str(code),
                retryable=retryable,
                retry_after_seconds=(
                    _retry_after_seconds(response, payload=payload) if retryable else None
                ),
                attempts=attempts,
            ),
            response.status_code == 401 or code in _TOKEN_INVALID_CODES,
        )

    async def _token_for(self, binding: ChannelBinding) -> str:
        secret_ref = binding.secret_refs.get("app_secret")
        if secret_ref is None:
            raise _TokenError("token_not_configured", retryable=False)
        cache_key = (
            binding.binding_id,
            binding.account_id,
            hashlib.sha256(secret_ref.uri.encode()).hexdigest(),
        )
        now = self._clock()
        cached = self._tokens.get(cache_key)
        if cached is not None and cached[1] - self._refresh_skew_seconds > now:
            return cached[0]

        lock = self._token_locks.setdefault(cache_key, asyncio.Lock())
        async with lock:
            now = self._clock()
            cached = self._tokens.get(cache_key)
            if cached is not None and cached[1] - self._refresh_skew_seconds > now:
                return cached[0]
            app_secret = self._resolve_token_secret(secret_ref)
            try:
                response = await self._http.post(
                    f"{self._api_root}{_AUTH_PATH}",
                    json={"app_id": binding.account_id, "app_secret": app_secret},
                )
                payload = response.json()
            except httpx.TransportError:
                raise _TokenError("token_transport", retryable=True) from None
            except (httpx.HTTPError, ValueError):
                raise _TokenError("token_invalid_response", retryable=True) from None
            if not isinstance(payload, Mapping):
                raise _TokenError("token_invalid_response", retryable=True)
            code = _integer(payload.get("code"), default=response.status_code)
            token = payload.get("tenant_access_token")
            expires = _integer(payload.get("expire"), default=0)
            if not response.is_success or code != 0:
                retryable = (
                    response.status_code == 429
                    or response.status_code >= 500
                    or code in _RETRYABLE_CODES
                )
                raise _TokenError(
                    f"token_{code}",
                    retryable=retryable,
                    retry_after_seconds=(
                        _retry_after_seconds(response, payload=payload) if retryable else None
                    ),
                )
            if not isinstance(token, str) or not token or expires <= 0:
                raise _TokenError("token_invalid_response", retryable=True)
            self._tokens[cache_key] = (token, self._clock() + expires)
            return token

    def _invalidate_token(self, binding: ChannelBinding) -> None:
        for key in tuple(self._tokens):
            if key[0] == binding.binding_id and key[1] == binding.account_id:
                self._tokens.pop(key, None)

    def _decode_payload(
        self, request: WebhookRequest, encrypt_key: str | None
    ) -> tuple[dict[str, Any], bool, bool]:
        outer = _json_object(request.body)
        encrypted = outer.get("encrypt")
        if encrypted is None:
            signature_verified = (
                self._verify_signature_if_present(request, encrypt_key)
                if encrypt_key is not None
                else False
            )
            return outer, signature_verified, False
        if not isinstance(encrypted, str) or not encrypted or encrypt_key is None:
            raise FeishuVerificationError("Feishu encrypted callback configuration is invalid")
        signature_verified = self._verify_signature_if_present(request, encrypt_key)
        try:
            ciphertext = base64.b64decode(encrypted, validate=True)
            if len(ciphertext) < 32 or len(ciphertext) % 16 != 0:
                raise ValueError
            iv, encrypted_body = ciphertext[:16], ciphertext[16:]
            key = hashlib.sha256(encrypt_key.encode()).digest()
            decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
            padded = decryptor.update(encrypted_body) + decryptor.finalize()
            unpadder = padding.PKCS7(128).unpadder()
            plaintext = unpadder.update(padded) + unpadder.finalize()
        except (ValueError, binascii.Error):
            raise FeishuVerificationError("Feishu encrypted callback is invalid") from None
        return _json_object(plaintext), signature_verified, True

    def _verify_signature_if_present(self, request: WebhookRequest, encrypt_key: str) -> bool:
        headers = (
            _header(request.headers, "X-Lark-Request-Timestamp"),
            _header(request.headers, "X-Lark-Request-Nonce"),
            _header(request.headers, "X-Lark-Signature"),
        )
        if not any(headers):
            return False
        self._verify_signature(request, encrypt_key)
        return True

    def _verify_signature(self, request: WebhookRequest, encrypt_key: str) -> None:
        timestamp = _header(request.headers, "X-Lark-Request-Timestamp")
        nonce = _header(request.headers, "X-Lark-Request-Nonce")
        supplied = _header(request.headers, "X-Lark-Signature")
        if not timestamp or not nonce or not supplied:
            raise FeishuVerificationError("Feishu callback signature is missing")
        try:
            signed_at = int(timestamp)
        except ValueError:
            raise FeishuVerificationError("Feishu callback timestamp is invalid") from None
        if abs(self._wall_clock() - signed_at) > self._max_callback_age_seconds:
            raise FeishuVerificationError("Feishu callback timestamp is stale")
        expected = hashlib.sha256(
            timestamp.encode() + nonce.encode() + encrypt_key.encode() + request.body
        ).hexdigest()
        if not hmac.compare_digest(expected, supplied):
            raise FeishuVerificationError("Feishu callback signature is invalid")

    def _verify_token(self, payload: Mapping[str, Any], expected: str) -> None:
        header = payload.get("header")
        supplied = header.get("token") if isinstance(header, Mapping) else payload.get("token")
        if not isinstance(supplied, str) or not hmac.compare_digest(expected, supplied):
            raise FeishuVerificationError("Feishu callback token is invalid")

    def _parse_message(
        self, payload: Mapping[str, Any], binding: ChannelBinding
    ) -> InboundEnvelope | None:
        header = _mapping(payload.get("header"), "header")
        app_id = header.get("app_id")
        if not isinstance(app_id, str) or not hmac.compare_digest(app_id, binding.account_id):
            raise FeishuVerificationError("Feishu callback binding is invalid")
        event = _mapping(payload.get("event"), "event")
        sender = _mapping(event.get("sender"), "sender")
        if sender.get("sender_type") in {"app", "bot"}:
            return None
        sender_id = _mapping(sender.get("sender_id"), "sender")
        open_id = sender_id.get("open_id")
        if not isinstance(open_id, str) or not open_id:
            raise FeishuVerificationError("Feishu callback sender is invalid")
        message = _mapping(event.get("message"), "message")
        message_id = message.get("message_id")
        chat_id = message.get("chat_id")
        message_type = message.get("message_type")
        if not all(
            isinstance(value, str) and value for value in (message_id, chat_id, message_type)
        ):
            raise FeishuVerificationError("Feishu callback message is invalid")
        content_raw = message.get("content")
        if not isinstance(content_raw, str):
            raise FeishuVerificationError("Feishu callback message content is invalid")
        content = _json_object(content_raw.encode())
        raw_mentions = message.get("mentions")
        mentions: list[object] = list(raw_mentions) if isinstance(raw_mentions, list) else []
        payload_kind, text, media = _normalize_content(str(message_type), content, mentions)
        kind = (
            ConversationKind.DIRECT if message.get("chat_type") == "p2p" else ConversationKind.GROUP
        )
        occurred = _timestamp(message.get("create_time") or header.get("create_time"))
        metadata = {
            "target_id": chat_id,
            "event_id": header.get("event_id"),
            "tenant_key": header.get("tenant_key"),
            "chat_type": message.get("chat_type"),
            "thread_id": message.get("thread_id"),
            "root_id": message.get("root_id"),
            "parent_id": message.get("parent_id"),
            "sender_type": sender.get("sender_type"),
            "raw_message_type": message_type,
        }
        return InboundEnvelope(
            channel=Channel.FEISHU,
            account_id=binding.account_id,
            external_message_id=str(message_id),
            external_user_id=open_id,
            conversation_kind=kind,
            external_conversation_id=str(chat_id) if kind == ConversationKind.GROUP else None,
            payload_kind=payload_kind,
            text=text,
            media=media,
            event_type=None if payload_kind != PayloadKind.EVENT else str(message_type),
            occurred_at=occurred,
            provider_metadata={key: value for key, value in metadata.items() if value is not None},
        )

    def _required_secret(self, binding: ChannelBinding, name: str) -> str:
        ref = binding.secret_refs.get(name)
        if ref is None:
            raise FeishuVerificationError(f"Feishu {name.replace('_', ' ')} is not configured")
        return self._resolve(ref, name.replace("_", " "))

    def _resolve(self, ref: SecretRef, label: str) -> str:
        try:
            resolver = getattr(self._secrets, "resolve_tenant", None)
            if ref.uri.startswith("literal://"):
                value = self._secrets.resolve(ref)
            else:
                if resolver is None:
                    raise ValueError("tenant-scoped secret resolver is required")
                value = resolver(ref)
        except Exception:
            raise FeishuVerificationError(f"Feishu {label} is unavailable") from None
        if not value:
            raise FeishuVerificationError(f"Feishu {label} is unavailable")
        return value

    def _resolve_token_secret(self, ref: SecretRef) -> str:
        try:
            resolver = getattr(self._secrets, "resolve_tenant", None)
            if ref.uri.startswith("literal://"):
                value = self._secrets.resolve(ref)
            else:
                if resolver is None:
                    raise ValueError("tenant-scoped secret resolver is required")
                value = resolver(ref)
        except Exception:
            raise _TokenError("token_secret_unavailable", retryable=True) from None
        if not value:
            raise _TokenError("token_secret_unavailable", retryable=True)
        return value

    @staticmethod
    def _failed(
        envelope: OutboundEnvelope,
        provider_code: str,
        *,
        retryable: bool = False,
        retry_after_seconds: float | None = None,
        attempts: int = 1,
    ) -> DeliveryReceipt:
        return DeliveryReceipt(
            outbound_id=envelope.outbound_id,
            status=DeliveryStatus.FAILED,
            provider_code=provider_code,
            retryable=retryable,
            retry_after_seconds=retry_after_seconds,
            attempts=attempts,
        )

    async def close(self) -> None:
        if self._owns_http:
            await self._http.aclose()


def _retry_after_seconds(
    response: httpx.Response,
    *,
    payload: Mapping[str, Any] | None = None,
    now: float | None = None,
) -> float | None:
    """Read a bounded provider backoff from headers or Feishu error data.

    Feishu deployments do not consistently put the retry hint in the HTTP
    header: some responses expose ``retry_after`` in the platform JSON (and
    occasionally under ``error``/``data``).  The adapter accepts either form,
    but never trusts an unbounded or non-finite value.
    """

    candidates: list[object] = [response.headers.get("Retry-After")]
    if payload is None:
        try:
            parsed = response.json()
        except (TypeError, ValueError):
            parsed = None
        if isinstance(parsed, Mapping):
            payload = parsed
    if payload is not None:
        candidates.extend(_retry_after_payload_values(payload))
    for raw in candidates:
        seconds = _coerce_retry_after(raw, now=now)
        if seconds is not None:
            return seconds
    return None


def _retry_after_payload_values(payload: Mapping[str, Any]) -> tuple[object, ...]:
    """Return only retry-hint fields from bounded, known Feishu envelopes."""

    values: list[object] = []
    seen: set[int] = set()
    stack: list[object] = [payload]
    while stack and len(seen) < 16:
        current = stack.pop()
        if not isinstance(current, Mapping) or id(current) in seen:
            continue
        seen.add(id(current))
        for key in ("retry_after", "retry_after_seconds", "retryAfter", "retry-after"):
            if key in current:
                values.append(current[key])
        for key in ("error", "data", "result"):
            nested = current.get(key)
            if isinstance(nested, Mapping):
                stack.append(nested)
    return tuple(values)


def _coerce_retry_after(raw: object, *, now: float | None = None) -> float | None:
    if raw is None or isinstance(raw, bool):
        return None
    try:
        seconds = float(cast(Any, raw))
    except (TypeError, ValueError, OverflowError):
        if not isinstance(raw, str):
            return None
        try:
            retry_at = parsedate_to_datetime(raw)
        except (TypeError, ValueError, OverflowError):
            return None
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=UTC)
        try:
            seconds = retry_at.timestamp() - (time.time() if now is None else now)
        except (OverflowError, OSError, ValueError):
            return None
    if not math.isfinite(seconds) or seconds < 0:
        return None
    return min(seconds, 3600.0)


def _event_type(payload: Mapping[str, Any]) -> str | None:
    header = payload.get("header")
    if isinstance(header, Mapping):
        value = header.get("event_type")
        return value if isinstance(value, str) else None
    value = payload.get("type")
    return value if isinstance(value, str) else None


def _header(headers: Mapping[str, str], name: str) -> str | None:
    wanted = name.casefold()
    for key, value in headers.items():
        if key.casefold() == wanted:
            return value
    return None


def _json_object(value: bytes) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise FeishuVerificationError("Feishu callback JSON is invalid") from None
    if not isinstance(parsed, dict):
        raise FeishuVerificationError("Feishu callback JSON is invalid")
    return parsed


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FeishuVerificationError(f"Feishu callback {label} is invalid")
    return value


def _normalize_content(
    message_type: str,
    content: Mapping[str, Any],
    mentions: list[object],
) -> tuple[PayloadKind, str | None, tuple[MediaReference, ...]]:
    if message_type == "text":
        text = content.get("text")
        if not isinstance(text, str):
            raise FeishuVerificationError("Feishu callback message content is invalid")
        for mention in mentions:
            if isinstance(mention, Mapping) and isinstance(mention.get("key"), str):
                text = text.replace(str(mention["key"]), "")
        return PayloadKind.TEXT, text.strip(), ()
    if message_type == "post":
        return PayloadKind.MIXED, _flatten_post(content), ()

    kind = {
        "image": PayloadKind.IMAGE,
        "sticker": PayloadKind.IMAGE,
        "file": PayloadKind.FILE,
        "audio": PayloadKind.VOICE,
        "media": PayloadKind.VIDEO,
    }.get(message_type)
    if kind is None:
        return PayloadKind.EVENT, None, ()
    media_id = content.get("image_key") or content.get("file_key")
    if not isinstance(media_id, str) or not media_id:
        raise FeishuVerificationError("Feishu callback media is invalid")
    filename = content.get("file_name")
    return (
        kind,
        None,
        (
            MediaReference(
                provider_media_id=media_id,
                filename=filename if isinstance(filename, str) else None,
            ),
        ),
    )


def _flatten_post(content: Mapping[str, Any]) -> str | None:
    values: list[str] = []
    title = content.get("title")
    if isinstance(title, str) and title:
        values.append(title)
    stack: list[object] = [content.get("content")]
    visited = 0
    while stack and visited < 1000:
        visited += 1
        item = stack.pop()
        if isinstance(item, list):
            stack.extend(reversed(item))
        elif isinstance(item, Mapping):
            text = item.get("text")
            if isinstance(text, str) and text:
                values.append(text)
            stack.extend(
                reversed([value for key, value in item.items() if key not in {"text", "tag"}])
            )
    rendered = "\n".join(value.strip() for value in values if value.strip())
    return rendered or None


def _timestamp(raw: object) -> datetime:
    try:
        value = int(str(raw))
    except (TypeError, ValueError):
        return datetime.now(UTC)
    seconds = value / 1000 if value >= 10_000_000_000 else value
    try:
        return datetime.fromtimestamp(seconds, UTC)
    except (OverflowError, OSError, ValueError):
        return datetime.now(UTC)


def _receive_id_type(target_id: str) -> str:
    return "open_id" if target_id.startswith("ou_") else "chat_id"


def _integer(value: object, *, default: int) -> int:
    if isinstance(value, bool):
        return default
    if not isinstance(value, (int, str, bytes, bytearray)):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _resource_identifier(value: object, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-empty string")
    return value.strip()


def _resource_type(value: object) -> str:
    if not isinstance(value, str) or value not in _RESOURCE_TYPES:
        raise ValueError("resource type must be image or file")
    return value


def _content_length(value: str | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _content_type(value: str | None) -> str:
    if not isinstance(value, str):
        return "application/octet-stream"
    value = value.split(";", 1)[0].strip().lower()
    return value or "application/octet-stream"


def _safe_filename(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    value = value.replace("\x00", "").strip().strip('"')
    value = value.replace("\\", "/").rsplit("/", 1)[-1]
    if not value or any(ord(char) < 32 for char in value):
        return None
    return value[:512]


def _content_disposition_filename(value: str | None) -> str | None:
    if not isinstance(value, str):
        return None
    filename_star = re.search(
        r"(?:^|;)\s*filename\*\s*=\s*(?:\"([^\"]*)\"|([^;]+))",
        value,
        flags=re.IGNORECASE,
    )
    if filename_star:
        encoded = filename_star.group(1) or filename_star.group(2) or ""
        pieces = encoded.strip().split("'", 2)
        encoded = pieces[2] if len(pieces) == 3 else encoded.strip()
        safe = _safe_filename(unquote(encoded))
        if safe:
            return safe
    filename = re.search(
        r"(?:^|;)\s*filename\s*=\s*(?:\"([^\"]*)\"|([^;]+))",
        value,
        flags=re.IGNORECASE,
    )
    if not filename:
        return None
    return _safe_filename(filename.group(1) or filename.group(2))


async def _response_error_code(response: httpx.Response) -> int | None:
    body = bytearray()
    try:
        async for chunk in response.aiter_bytes(chunk_size=8192):
            remaining = _MAX_ERROR_BODY_BYTES - len(body)
            if remaining <= 0:
                break
            body.extend(chunk[:remaining])
    except httpx.HTTPError:
        return None
    return _json_error_code(bytes(body))


def _json_error_code(body: bytes) -> int | None:
    try:
        payload = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(payload, Mapping):
        return None
    code = payload.get("code")
    if isinstance(code, bool) or not isinstance(code, (int, str)):
        return None
    try:
        return int(code)
    except (TypeError, ValueError):
        return None


__all__ = [
    "FeishuAdapter",
    "FeishuCallback",
    "FeishuMediaError",
    "FeishuResource",
    "FeishuResourceError",
    "FeishuVerificationError",
]
