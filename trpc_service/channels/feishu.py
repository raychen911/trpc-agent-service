"""Feishu event callback and Open API adapter.

The adapter supports URL verification, encrypted event callbacks, callback
signature/token verification, text-message normalization, tenant token
caching, and asynchronous text replies through Feishu Open API.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import hmac
import json
import logging
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid5

from ..config import resolve_secret
from ..log import redact
from ..tenant.models import ChannelBinding, InboundMessage, OutboundMessage
from .base import ChannelAdapter

AUTH_PATH = "/open-apis/auth/v3/tenant_access_token/internal"
MESSAGES_PATH = "/open-apis/im/v1/messages"
RETRYABLE_CODES = {99991400, 99991401, 99991402, 99991672}
TOKEN_INVALID_CODES = {99991663, 99991664, 99991668}
LOGGER = logging.getLogger(__name__)


class FeishuError(RuntimeError):
    """Safe Feishu error that does not expose credentials or response bodies."""

    def __init__(self, code: str, *, retryable: bool = False) -> None:
        super().__init__(f"Feishu request failed: {code}")
        self.code = code
        self.retryable = retryable


class FeishuVerificationError(ValueError):
    """Raised when a Feishu callback cannot be authenticated."""


@dataclass(frozen=True)
class FeishuCallback:
    message: InboundMessage | None = None
    challenge: str | None = None
    ignored: bool = False


@dataclass(frozen=True)
class HTTPResponse:
    status_code: int
    headers: Mapping[str, str]
    body: bytes

    def json(self) -> dict[str, Any]:
        value = json.loads(self.body.decode("utf-8"))
        if not isinstance(value, dict):
            raise TypeError("response is not a JSON object")
        return value


class AsyncHTTPClient(Protocol):
    async def post(self, url: str, *, headers: Mapping[str, str], payload: Mapping[str, Any]) -> HTTPResponse: ...

    async def get(self, url: str, *, headers: Mapping[str, str]) -> HTTPResponse: ...

    async def delete(self, url: str, *, headers: Mapping[str, str]) -> HTTPResponse: ...

    async def post_bytes(self, url: str, *, headers: Mapping[str, str], body: bytes) -> HTTPResponse: ...


class UrllibAsyncHTTPClient:
    """Small dependency-free async HTTP client using a worker thread."""

    def __init__(self, timeout: float = 10.0) -> None:
        self.timeout = timeout

    async def post(self, url: str, *, headers: Mapping[str, str], payload: Mapping[str, Any]) -> HTTPResponse:
        return await asyncio.to_thread(self._post, url, headers, payload)

    async def get(self, url: str, *, headers: Mapping[str, str]) -> HTTPResponse:
        return await asyncio.to_thread(self._get, url, headers)

    async def delete(self, url: str, *, headers: Mapping[str, str]) -> HTTPResponse:
        return await asyncio.to_thread(self._delete, url, headers)

    async def post_bytes(self, url: str, *, headers: Mapping[str, str], body: bytes) -> HTTPResponse:
        return await asyncio.to_thread(self._post_bytes, url, headers, body)

    def _get(self, url: str, headers: Mapping[str, str]) -> HTTPResponse:
        request = urllib.request.Request(url, headers=dict(headers), method="GET")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return HTTPResponse(response.status, dict(response.headers.items()), response.read())
        except urllib.error.HTTPError as exc:
            return HTTPResponse(exc.code, dict(exc.headers.items()), exc.read())
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise FeishuError("transport", retryable=True) from exc

    def _delete(self, url: str, headers: Mapping[str, str]) -> HTTPResponse:
        request = urllib.request.Request(url, headers=dict(headers), method="DELETE")
        return self._open(request)

    def _post_bytes(self, url: str, headers: Mapping[str, str], body: bytes) -> HTTPResponse:
        request = urllib.request.Request(url, data=body, headers=dict(headers), method="POST")
        return self._open(request)

    def _open(self, request: urllib.request.Request) -> HTTPResponse:
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return HTTPResponse(response.status, dict(response.headers.items()), response.read())
        except urllib.error.HTTPError as exc:
            return HTTPResponse(exc.code, dict(exc.headers.items()), exc.read())
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise FeishuError("transport", retryable=True) from exc

    def _post(self, url: str, headers: Mapping[str, str], payload: Mapping[str, Any]) -> HTTPResponse:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        request_headers = {"Content-Type": "application/json; charset=utf-8", **dict(headers)}
        request = urllib.request.Request(url, data=body, headers=request_headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return HTTPResponse(response.status, dict(response.headers.items()), response.read())
        except urllib.error.HTTPError as exc:
            return HTTPResponse(exc.code, dict(exc.headers.items()), exc.read())
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise FeishuError("transport", retryable=True) from exc


class FeishuAdapter(ChannelAdapter):
    name = "feishu"
    max_message_size = 20_000

    def __init__(
        self,
        http_client: AsyncHTTPClient | None = None,
        *,
        api_base_url: str = "https://open.feishu.cn",
        refresh_skew_seconds: int = 300,
        max_callback_age_seconds: int = 300,
        max_callback_bytes: int = 2 * 1024 * 1024,
    ) -> None:
        self.http_client = http_client or UrllibAsyncHTTPClient()
        self.api_base_url = api_base_url.rstrip("/")
        self.refresh_skew_seconds = refresh_skew_seconds
        self.max_callback_age_seconds = max_callback_age_seconds
        self.max_callback_bytes = max_callback_bytes
        self._tokens: dict[str, tuple[str, float]] = {}
        self._token_locks: dict[str, asyncio.Lock] = {}

    def verify(self, binding: ChannelBinding, headers: dict[str, str], body: bytes) -> bool:
        try:
            self._decode_and_verify(binding, headers, body)
            return True
        except (FeishuVerificationError, ValueError):
            return False

    def parse(self, tenant_id: str, binding: ChannelBinding, payload: Any, trace_id: str) -> InboundMessage:
        callback = self._parse_payload(tenant_id, binding, _mapping(payload, "payload"), trace_id)
        if callback.message is None:
            raise ValueError("Feishu callback does not contain a user message")
        return callback.message

    def handle_callback(
        self,
        tenant_id: str,
        binding: ChannelBinding,
        headers: dict[str, str],
        body: bytes,
        trace_id: str,
    ) -> FeishuCallback:
        payload = self._decode_and_verify(binding, headers, body)
        return self._parse_payload(tenant_id, binding, payload, trace_id)

    def handle_long_connection_event(
        self,
        tenant_id: str,
        binding: ChannelBinding,
        payload: Mapping[str, Any],
        trace_id: str,
    ) -> FeishuCallback:
        """Parse an event already authenticated and decoded by the official SDK."""
        return self._parse_payload(tenant_id, binding, payload, trace_id)

    async def send(self, message: OutboundMessage, binding: ChannelBinding | None = None) -> dict[str, Any]:
        if binding is None:
            receipt = {"ok": False, "code": "binding_missing"}
        elif binding.channel != self.name or binding.account_id != message.account_id:
            receipt = {"ok": False, "code": "binding_mismatch"}
        else:
            try:
                token = await self._tenant_access_token(binding)
                receipt = await self._send_once(message, binding, token)
                if receipt.get("code") in TOKEN_INVALID_CODES or receipt.get("status_code") == 401:
                    self._tokens.pop(self._token_key(binding), None)
                    token = await self._tenant_access_token(binding)
                    receipt = await self._send_once(message, binding, token)
            except FeishuError as exc:
                receipt = {"ok": False, "code": exc.code, "retryable": exc.retryable}
        _log_reply_result(message, receipt)
        return receipt

    async def send_proactive(
        self,
        binding: ChannelBinding,
        receive_id: str,
        content: str | Mapping[str, Any],
        *,
        receive_id_type: str = "open_id",
        msg_type: str = "text",
    ) -> dict[str, Any]:
        token = await self._tenant_access_token(binding)
        normalized = {"text": content} if msg_type == "text" and isinstance(content, str) else content
        response = await self._post_with_retry(
            f"{self._base_url(binding)}{MESSAGES_PATH}?receive_id_type={urllib.parse.quote(receive_id_type)}",
            {"Authorization": f"Bearer {token}"},
            {
                "receive_id": receive_id,
                "msg_type": msg_type,
                "content": json.dumps(normalized, ensure_ascii=False, separators=(",", ":")),
                "uuid": _stable_uuid(binding.account_id, receive_id, msg_type, normalized),
            },
        )
        return _receipt(response)

    async def send_card(
        self, binding: ChannelBinding, receive_id: str, card: Mapping[str, Any], *, receive_id_type: str = "open_id"
    ) -> dict[str, Any]:
        return await self.send_proactive(
            binding, receive_id, card, receive_id_type=receive_id_type, msg_type="interactive"
        )

    async def recall(self, binding: ChannelBinding, message_id: str) -> dict[str, Any]:
        token = await self._tenant_access_token(binding)
        client_delete = getattr(self.http_client, "delete", None)
        if client_delete is None:
            raise FeishuError("delete_unsupported")
        response = await client_delete(
            f"{self._base_url(binding)}{MESSAGES_PATH}/{urllib.parse.quote(message_id, safe='')}",
            headers={"Authorization": f"Bearer {token}"},
        )
        return _receipt(response)

    async def upload_image(self, binding: ChannelBinding, data: bytes, *, image_type: str = "message") -> str:
        return await self._upload(binding, "/open-apis/im/v1/images", "image", "image_type", image_type, data)

    async def upload_file(
        self, binding: ChannelBinding, data: bytes, filename: str, *, file_type: str = "stream"
    ) -> str:
        return await self._upload(binding, "/open-apis/im/v1/files", "file", "file_type", file_type, data, filename)

    async def download_resource(
        self,
        binding: ChannelBinding,
        message_id: str,
        file_key: str,
        *,
        resource_type: str,
        max_bytes: int = 30 * 1024 * 1024,
    ) -> bytes:
        if resource_type not in {"image", "file"}:
            raise ValueError("Feishu resource type must be image or file")
        token = await self._tenant_access_token(binding)
        get = getattr(self.http_client, "get", None)
        if get is None:
            raise FeishuError("download_unsupported")
        url = (
            f"{self._base_url(binding)}{MESSAGES_PATH}/{urllib.parse.quote(message_id, safe='')}"
            f"/resources/{urllib.parse.quote(file_key, safe='')}?type={resource_type}"
        )
        response = await get(url, headers={"Authorization": f"Bearer {token}"})
        if response.status_code >= 400:
            raise FeishuError(str(response.status_code), retryable=response.status_code == 429 or response.status_code >= 500)
        if len(response.body) > max_bytes:
            raise ValueError("Feishu resource exceeds configured size")
        return response.body

    async def _upload(
        self,
        binding: ChannelBinding,
        path: str,
        file_field: str,
        type_field: str,
        type_value: str,
        data: bytes,
        filename: str = "image.png",
    ) -> str:
        if not data or len(data) > 30 * 1024 * 1024:
            raise ValueError("Feishu upload must contain at most 30 MiB")
        token = await self._tenant_access_token(binding)
        boundary = hashlib.sha256(data[:1024] + filename.encode()).hexdigest()
        body = _multipart(boundary, type_field, type_value, file_field, filename, data)
        post_bytes = getattr(self.http_client, "post_bytes", None)
        if post_bytes is None:
            raise FeishuError("upload_unsupported")
        response = await post_bytes(
            self._base_url(binding) + path,
            headers={"Authorization": f"Bearer {token}", "Content-Type": f"multipart/form-data; boundary={boundary}"},
            body=body,
        )
        payload = _response_json(response)
        if response.status_code >= 400 or _integer(payload.get("code"), response.status_code) != 0:
            raise FeishuError(str(payload.get("code", response.status_code)), retryable=response.status_code >= 500)
        data_value = payload.get("data") if isinstance(payload.get("data"), Mapping) else {}
        key = data_value.get("image_key") or data_value.get("file_key")
        if not isinstance(key, str) or not key:
            raise FeishuError("upload_key_missing")
        return key

    def _decode_and_verify(self, binding: ChannelBinding, headers: Mapping[str, str], body: bytes) -> dict[str, Any]:
        if not body or len(body) > self.max_callback_bytes:
            raise FeishuVerificationError("invalid callback body size")
        outer = _json_object(body)
        encrypt_key = self._binding_secret(binding.encrypt_key_ref, "encrypt key", required=False)
        encrypted = outer.get("encrypt")
        signature_verified = False
        if encrypted is not None:
            if not encrypt_key or not isinstance(encrypted, str):
                raise FeishuVerificationError("encrypted callback is not configured")
            signature_verified = self._verify_signature_if_present(headers, body, encrypt_key)
            payload = self._decrypt(encrypted, encrypt_key)
        else:
            payload = outer
            if encrypt_key:
                signature_verified = self._verify_signature_if_present(headers, body, encrypt_key)

        verification_token = self._binding_secret(binding.verify_token, "verification token")
        header = payload.get("header")
        supplied_token = header.get("token") if isinstance(header, Mapping) else payload.get("token")
        if not isinstance(supplied_token, str) or not hmac.compare_digest(verification_token, supplied_token):
            raise FeishuVerificationError("callback verification token is invalid")

        event_type = _event_type(payload)
        if event_type != "url_verification" and encrypt_key and not signature_verified:
            raise FeishuVerificationError("callback signature is required")
        if event_type == "url_verification" and encrypt_key and not (encrypted or signature_verified):
            raise FeishuVerificationError("challenge authentication is invalid")
        return payload

    def _parse_payload(
        self, tenant_id: str, binding: ChannelBinding, payload: Mapping[str, Any], trace_id: str
    ) -> FeishuCallback:
        event_type = _event_type(payload)
        log_feishu_event(
            "received",
            tenant_id=tenant_id,
            account_id=binding.account_id,
            trace_id=trace_id,
            event_type=event_type or "unknown",
        )
        if event_type == "url_verification":
            challenge = payload.get("challenge")
            if not isinstance(challenge, str) or not challenge:
                raise FeishuVerificationError("URL verification challenge is missing")
            return FeishuCallback(challenge=challenge)
        if event_type != "im.message.receive_v1":
            log_feishu_event(
                "ignored",
                tenant_id=tenant_id,
                account_id=binding.account_id,
                trace_id=trace_id,
                event_type=event_type or "unknown",
                reason="unsupported_event_type",
            )
            return FeishuCallback(ignored=True)

        header = _mapping(payload.get("header"), "header")
        callback_app_id = header.get("app_id")
        if callback_app_id and (
            not isinstance(callback_app_id, str) or not hmac.compare_digest(callback_app_id, binding.account_id)
        ):
            raise FeishuVerificationError("callback App ID does not match binding")
        event = _mapping(payload.get("event"), "event")
        sender = _mapping(event.get("sender"), "sender")
        if sender.get("sender_type") in {"app", "bot"}:
            log_feishu_event(
                "ignored",
                tenant_id=tenant_id,
                account_id=binding.account_id,
                trace_id=trace_id,
                event_type=event_type,
                reason="bot_sender",
            )
            return FeishuCallback(ignored=True)
        sender_id = _mapping(sender.get("sender_id"), "sender_id")
        user_id = sender_id.get("open_id") or sender_id.get("union_id") or sender_id.get("user_id")
        message = _mapping(event.get("message"), "message")
        message_id = message.get("message_id")
        chat_id = message.get("chat_id")
        message_type = message.get("message_type")
        if not all(isinstance(value, str) and value for value in (user_id, message_id, chat_id, message_type)):
            raise FeishuVerificationError("message identity is invalid")
        content_raw = message.get("content")
        if not isinstance(content_raw, str):
            raise FeishuVerificationError("message content is invalid")
        content = _json_object(content_raw.encode("utf-8"))
        text = content.get("text")
        if message_type == "image":
            text = "[image]"
            content["normalized_media"] = {"type": "image", "image_key": content.get("image_key")}
        elif message_type == "file":
            text = f"[file: {content.get('file_name') or 'attachment'}]"
            content["normalized_media"] = {"type": "file", "file_key": content.get("file_key")}
        elif message_type != "text":
            log_feishu_event(
                "ignored",
                tenant_id=tenant_id,
                account_id=binding.account_id,
                trace_id=trace_id,
                event_type=event_type,
                message_id=message_id,
                message_type=message_type,
                reason="unsupported_message_type",
            )
            return FeishuCallback(ignored=True)
        if not isinstance(text, str):
            raise FeishuVerificationError("message content is invalid")
        mentions = message.get("mentions")
        if isinstance(mentions, list):
            for mention in mentions:
                if isinstance(mention, Mapping) and isinstance(mention.get("key"), str):
                    text = text.replace(str(mention["key"]), "")

        raw = dict(payload)
        raw.pop("normalized_media", None)
        raw.pop("_artifact_materialized", None)
        if "normalized_media" in content:
            raw["normalized_media"] = content["normalized_media"]
        inbound = InboundMessage(
            tenant_id=tenant_id,
            channel=self.name,
            account_id=binding.account_id,
            external_message_id=message_id,
            external_user_id=str(user_id),
            chat_id=chat_id,
            chat_type="direct" if message.get("chat_type") == "p2p" else "group",
            text=text.strip(),
            trace_id=trace_id,
            raw=raw,
        )
        return FeishuCallback(message=inbound)

    def _verify_signature_if_present(self, headers: Mapping[str, str], body: bytes, encrypt_key: str) -> bool:
        timestamp = _header(headers, "x-lark-request-timestamp")
        nonce = _header(headers, "x-lark-request-nonce")
        supplied = _header(headers, "x-lark-signature")
        if not any((timestamp, nonce, supplied)):
            return False
        if not all((timestamp, nonce, supplied)):
            raise FeishuVerificationError("callback signature headers are incomplete")
        try:
            signed_at = int(timestamp)
        except ValueError as exc:
            raise FeishuVerificationError("callback timestamp is invalid") from exc
        if abs(time.time() - signed_at) > self.max_callback_age_seconds:
            raise FeishuVerificationError("callback timestamp is stale")
        expected = hashlib.sha256(timestamp.encode() + nonce.encode() + encrypt_key.encode() + body).hexdigest()
        if not hmac.compare_digest(expected, supplied):
            raise FeishuVerificationError("callback signature is invalid")
        return True

    @staticmethod
    def _decrypt(encrypted: str, encrypt_key: str) -> dict[str, Any]:
        try:
            from cryptography.hazmat.primitives import padding
            from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
        except ImportError as exc:
            raise FeishuVerificationError("encrypted callbacks require the cryptography package") from exc
        try:
            ciphertext = base64.b64decode(encrypted, validate=True)
            if len(ciphertext) < 32 or len(ciphertext) % 16:
                raise ValueError("invalid ciphertext")
            iv, encrypted_body = ciphertext[:16], ciphertext[16:]
            key = hashlib.sha256(encrypt_key.encode()).digest()
            decryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).decryptor()
            padded = decryptor.update(encrypted_body) + decryptor.finalize()
            unpadder = padding.PKCS7(128).unpadder()
            plaintext = unpadder.update(padded) + unpadder.finalize()
            return _json_object(plaintext)
        except (ValueError, binascii.Error) as exc:
            raise FeishuVerificationError("encrypted callback is invalid") from exc

    async def _tenant_access_token(self, binding: ChannelBinding) -> str:
        token_key = self._token_key(binding)
        cached = self._tokens.get(token_key)
        now = time.monotonic()
        if cached and cached[1] - self.refresh_skew_seconds > now:
            return cached[0]
        lock = self._token_locks.setdefault(token_key, asyncio.Lock())
        async with lock:
            cached = self._tokens.get(token_key)
            now = time.monotonic()
            if cached and cached[1] - self.refresh_skew_seconds > now:
                return cached[0]
            app_secret = self._binding_secret(binding.secret_ref, "App Secret")
            response = await self._post_with_retry(
                self._base_url(binding) + AUTH_PATH,
                {},
                {"app_id": binding.account_id, "app_secret": app_secret},
            )
            payload = _response_json(response)
            code = _integer(payload.get("code"), response.status_code)
            token = payload.get("tenant_access_token")
            expires = _integer(payload.get("expire"), 0)
            if response.status_code >= 400 or code != 0 or not isinstance(token, str) or expires <= 0:
                raise FeishuError(f"token_{code}", retryable=_retryable(response.status_code, code))
            self._tokens[token_key] = (token, time.monotonic() + expires)
            return token

    async def _send_once(self, message: OutboundMessage, binding: ChannelBinding, token: str) -> dict[str, Any]:
        message_id = urllib.parse.quote(message.in_reply_to, safe="")
        url = f"{self._base_url(binding)}{MESSAGES_PATH}/{message_id}/reply"
        outbound_uuid = _stable_uuid(
            message.tenant_id,
            message.account_id,
            message.in_reply_to,
            message.part,
        )
        content: Mapping[str, Any]
        if message.message_type == "image":
            content = {"image_key": message.media_id}
        elif message.message_type == "file":
            content = {"file_key": message.media_id}
        elif message.message_type == "interactive":
            content = message.metadata.get("card", {})
        else:
            content = {"text": message.text}
        response = await self._post_with_retry(
            url,
            {"Authorization": f"Bearer {token}"},
            {
                "msg_type": message.message_type,
                "content": json.dumps(content, ensure_ascii=False, separators=(",", ":")),
                "uuid": outbound_uuid,
            },
        )
        payload = _response_json(response)
        code = _integer(payload.get("code"), response.status_code)
        data = payload.get("data") if isinstance(payload.get("data"), Mapping) else {}
        return {
            "ok": response.status_code < 400 and code == 0,
            "code": code,
            "status_code": response.status_code,
            "provider_message_id": data.get("message_id"),
            "retryable": _retryable(response.status_code, code),
            "field_violations": _safe_field_violations(payload),
        }

    async def _post_with_retry(self, url: str, headers: Mapping[str, str], payload: Mapping[str, Any]) -> HTTPResponse:
        last_error: FeishuError | None = None
        for attempt in range(3):
            try:
                response = await self.http_client.post(url, headers=headers, payload=payload)
            except FeishuError as exc:
                last_error = exc
                if not exc.retryable or attempt == 2:
                    raise
                await asyncio.sleep(0.1 * (2**attempt))
                continue
            try:
                response_payload = _response_json(response)
                code = _integer(response_payload.get("code"), response.status_code)
            except FeishuError:
                code = response.status_code
            if not _retryable(response.status_code, code) or attempt == 2:
                return response
            await asyncio.sleep(0.1 * (2**attempt))
        raise last_error or FeishuError("transport", retryable=True)

    def _base_url(self, binding: ChannelBinding) -> str:
        return (binding.api_base_url or self.api_base_url).rstrip("/")

    @staticmethod
    def _token_key(binding: ChannelBinding) -> str:
        secret_identity = hashlib.sha256((binding.secret_ref or "").encode()).hexdigest()
        return f"{binding.account_id}:{secret_identity}"

    @staticmethod
    def _binding_secret(reference: str | None, name: str, required: bool = True) -> str | None:
        if not reference:
            if required:
                raise FeishuVerificationError(f"{name} is not configured")
            return None
        if reference.startswith(("env://", "file://", "literal://")):
            value = resolve_secret(reference)
        else:
            value = reference
        if required and not value:
            raise FeishuVerificationError(f"{name} is not configured")
        return value


def _json_object(body: bytes) -> dict[str, Any]:
    try:
        value = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FeishuVerificationError("callback JSON is invalid") from exc
    if not isinstance(value, dict):
        raise FeishuVerificationError("callback JSON must be an object")
    return value


def _mapping(value: Any, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise FeishuVerificationError(f"{name} is invalid")
    return value


def _event_type(payload: Mapping[str, Any]) -> str:
    header = payload.get("header")
    if isinstance(header, Mapping) and isinstance(header.get("event_type"), str):
        return str(header["event_type"])
    return str(payload.get("type") or "")


def _header(headers: Mapping[str, str], name: str) -> str:
    lowered = name.lower()
    return next((str(value) for key, value in headers.items() if key.lower() == lowered), "")


def _integer(value: Any, default: int) -> int:
    return value if isinstance(value, int) and not isinstance(value, bool) else default


def _response_json(response: HTTPResponse) -> dict[str, Any]:
    try:
        return response.json()
    except (TypeError, ValueError, UnicodeDecodeError) as exc:
        raise FeishuError("invalid_response", retryable=response.status_code >= 500) from exc


def _retryable(status_code: int, provider_code: int) -> bool:
    return status_code == 429 or status_code >= 500 or provider_code in RETRYABLE_CODES


def _stable_uuid(*values: object) -> str:
    """Return a deterministic RFC 4122 UUID accepted by Feishu's <=50 character field."""
    canonical = json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)
    return str(uuid5(NAMESPACE_URL, canonical))


def log_feishu_event(event: str, *, level: int = logging.INFO, **fields: object) -> None:
    """Emit one privacy-safe JSON log record without message bodies or credentials."""
    record = redact({"event": f"feishu.{event}", "channel": "feishu", **fields})
    LOGGER.log(level, "%s", json.dumps(record, ensure_ascii=False, separators=(",", ":"), default=str))


def _safe_field_violations(payload: Mapping[str, Any]) -> list[dict[str, str]]:
    error = payload.get("error")
    if not isinstance(error, Mapping):
        return []
    violations = error.get("field_violations")
    if not isinstance(violations, list):
        return []
    result: list[dict[str, str]] = []
    for violation in violations[:10]:
        if not isinstance(violation, Mapping):
            continue
        result.append(
            {
                "field": str(redact(str(violation.get("field", ""))))[:128],
                "description": str(redact(str(violation.get("description", ""))))[:512],
            }
        )
    return result


def _log_reply_result(message: OutboundMessage, receipt: Mapping[str, Any]) -> None:
    succeeded = receipt.get("ok") is True
    fields: dict[str, object] = {
        "tenant_id": message.tenant_id,
        "account_id": message.account_id,
        "trace_id": message.trace_id,
        "message_id": message.in_reply_to,
        "part": message.part,
        "total_parts": message.total_parts,
        "code": receipt.get("code", "unknown"),
        "status_code": receipt.get("status_code"),
        "retryable": bool(receipt.get("retryable", False)),
    }
    if succeeded:
        fields["provider_message_id"] = receipt.get("provider_message_id")
    else:
        fields["field_violations"] = receipt.get("field_violations", [])
    log_feishu_event("reply_succeeded" if succeeded else "reply_failed", level=logging.INFO if succeeded else logging.WARNING, **fields)


def _receipt(response: HTTPResponse) -> dict[str, Any]:
    payload = _response_json(response)
    code = _integer(payload.get("code"), response.status_code)
    data = payload.get("data") if isinstance(payload.get("data"), Mapping) else {}
    return {
        "ok": response.status_code < 400 and code == 0,
        "code": code,
        "status_code": response.status_code,
        "provider_message_id": data.get("message_id"),
        "retryable": _retryable(response.status_code, code),
    }


def _multipart(
    boundary: str, type_field: str, type_value: str, file_field: str, filename: str, data: bytes
) -> bytes:
    marker = boundary.encode()
    return b"\r\n".join(
        (
            b"--" + marker,
            f'Content-Disposition: form-data; name="{type_field}"'.encode(),
            b"",
            type_value.encode(),
            b"--" + marker,
            f'Content-Disposition: form-data; name="{file_field}"; filename="{filename}"'.encode(),
            b"Content-Type: application/octet-stream",
            b"",
            data,
            b"--" + marker + b"--",
            b"",
        )
    )
