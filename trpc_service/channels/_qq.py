# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""QQ Bot Open Platform channel adapter.

The adapter implements the HTTP callback transport documented by QQ Bot:

* ``op=13`` callback URL validation with an Ed25519 signature;
* Ed25519 verification of normal callbacks over ``timestamp + raw_body``;
* normalization of C2C, group, guild channel and guild DM messages;
* AppID/AppSecret exchange and caching for OpenAPI access tokens;
* text replies through the matching QQ OpenAPI endpoint.
"""

from __future__ import annotations

import asyncio
import time
from typing import Any
from typing import AsyncIterable
from typing import Awaitable
from typing import Callable
from typing import Optional
from urllib.parse import quote

import httpx
from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from ._base import ChannelAdapter
from ._models import CHAT_GROUP
from ._models import CHAT_PRIVATE
from ._models import InboundMessage
from ._models import OutboundMessage
from ._models import SendResult
from ._models import split_text
from ._webhook_json import json_body
from ._webhook_json import raw_bytes
from trpc_service.log import safe_error_message

QQ_API_BASE = "https://api.bot.qq.com"
QQ_TOKEN_URL = f"{QQ_API_BASE}/app/getAppAccessToken"
QQ_MESSAGE_LIMIT_CHARS = 2000

QQ_SCOPE_C2C = "c2c"
QQ_SCOPE_GROUP = "group"
QQ_SCOPE_GUILD = "guild"
QQ_SCOPE_DM = "dm"

QQ_EVENT_C2C = "C2C_MESSAGE_CREATE"
QQ_EVENT_GROUP_AT = "GROUP_AT_MESSAGE_CREATE"
QQ_EVENT_GROUP = "GROUP_MESSAGE_CREATE"
QQ_EVENT_GUILD_AT = "AT_MESSAGE_CREATE"
QQ_EVENT_DM = "DIRECT_MESSAGE_CREATE"

_QQ_MESSAGE_EVENTS = {
    QQ_EVENT_C2C,
    QQ_EVENT_GROUP_AT,
    QQ_EVENT_GROUP,
    QQ_EVENT_GUILD_AT,
    QQ_EVENT_DM,
}

QQSendHook = Callable[[dict[str, Any]], Awaitable[SendResult]]


def _derive_ed25519_seed(app_secret: str) -> bytes:
    """Derive the 32-byte QQ Bot Ed25519 seed from an AppSecret."""
    secret = app_secret.encode("utf-8")
    if not secret:
        raise ValueError("QQ Bot AppSecret is required")
    repeats = (32 + len(secret) - 1) // len(secret)
    return (secret * repeats)[:32]


def _private_key(app_secret: str) -> Ed25519PrivateKey:
    return Ed25519PrivateKey.from_private_bytes(_derive_ed25519_seed(app_secret))


def sign_validation_response(app_secret: str, plain_token: str, event_ts: str) -> dict[str, str]:
    """Build the response body required by QQ's ``op=13`` challenge."""
    message = f"{event_ts}{plain_token}".encode("utf-8")
    signature = _private_key(app_secret).sign(message).hex()
    return {"plain_token": plain_token, "signature": signature}


def verify_webhook_signature(app_secret: str, body: bytes, timestamp: str, signature: str) -> bool:
    """Verify QQ's Ed25519 signature over ``timestamp + raw request body``."""
    if not app_secret or not timestamp or not signature:
        return False
    try:
        signature_bytes = bytes.fromhex(signature)
        message = timestamp.encode("utf-8") + body
        _private_key(app_secret).public_key().verify(signature_bytes, message)
        return True
    except (InvalidSignature, ValueError):
        return False


def _header(headers: dict[str, str], name: str) -> str:
    expected = name.lower()
    for key, value in headers.items():
        if key.lower() == expected:
            return value
    return ""


def _attachment_urls(event: dict[str, Any]) -> tuple[list[str], list[str]]:
    attachments: list[dict[str, Any]] = []
    for value in event.get("attachments", []):
        if isinstance(value, dict):
            attachments.append(value)
    for element in event.get("msg_elements", []):
        if not isinstance(element, dict):
            continue
        for value in element.get("attachments", []):
            if isinstance(value, dict):
                attachments.append(value)

    images: list[str] = []
    files: list[str] = []
    seen: set[str] = set()
    for attachment in attachments:
        url = str(attachment.get("url") or attachment.get("voice_wav_url") or "")
        if not url or url in seen:
            continue
        seen.add(url)
        content_type = str(attachment.get("content_type", "")).lower()
        if content_type.startswith("image/"):
            images.append(url)
        else:
            files.append(url)
    return images, files


class QQAdapter(ChannelAdapter):
    """QQ Bot webhook and text-message adapter."""

    channel = "qq"

    def __init__(
        self,
        *,
        app_id: str = "",
        app_secret: str = "",
        access_token: Optional[str] = None,
        http_client: Optional[httpx.AsyncClient] = None,
        send_hook: Optional[QQSendHook] = None,
        api_base: str = QQ_API_BASE,
        token_url: str = QQ_TOKEN_URL,
        message_limit_chars: int = QQ_MESSAGE_LIMIT_CHARS,
        max_retries: int = 2,
        retry_backoff: float = 0.2,
    ) -> None:
        self.app_id = app_id
        self.app_secret = app_secret
        self.access_token = access_token
        self.api_base = api_base.rstrip("/")
        self.token_url = token_url
        self.message_limit_chars = message_limit_chars
        self.max_retries = max(0, max_retries)
        self.retry_backoff = max(0.0, retry_backoff)
        self._http_client = http_client
        self._owns_http_client = http_client is None
        self._send_hook = send_hook
        self._token_expires_at = float("inf") if access_token else 0.0
        self._token_lock = asyncio.Lock()

    def _client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=10.0)
        return self._http_client

    async def close(self) -> None:
        if self._owns_http_client and self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None

    async def challenge_response(self, payload: Any) -> Optional[dict[str, Any]]:
        body = json_body(payload)
        if body.get("op") not in (13, "13"):
            return None
        challenge = body.get("d")
        if not isinstance(challenge, dict):
            raise ValueError("QQ validation payload is missing 'd'")
        plain_token = str(challenge.get("plain_token") or "")
        event_ts = str(challenge.get("event_ts") or "")
        if not plain_token or not event_ts:
            raise ValueError("QQ validation payload is missing plain_token or event_ts")
        return sign_validation_response(self.app_secret, plain_token, event_ts)

    async def verify_request(
        self,
        raw_body: bytes,
        payload: Any,
        headers: dict[str, str],
        query: dict[str, str],
    ) -> bool:
        del payload, query
        return verify_webhook_signature(
            self.app_secret,
            raw_body,
            _header(headers, "x-signature-timestamp"),
            _header(headers, "x-signature-ed25519"),
        )

    async def verify_signature(self, payload: Any, headers: dict[str, str], query: dict[str, str]) -> bool:
        del query
        return verify_webhook_signature(
            self.app_secret,
            raw_bytes(payload),
            _header(headers, "x-signature-timestamp"),
            _header(headers, "x-signature-ed25519"),
        )

    def callback_response(self, *, duplicate: bool = False) -> dict[str, Any]:
        del duplicate
        return {"op": 12, "d": 0}

    @staticmethod
    def _infer_event_type(event: dict[str, Any]) -> str:
        if event.get("group_openid"):
            return QQ_EVENT_GROUP
        author = event.get("author") if isinstance(event.get("author"), dict) else {}
        if author.get("user_openid"):
            return QQ_EVENT_C2C
        if event.get("channel_id"):
            return QQ_EVENT_GUILD_AT
        if event.get("guild_id"):
            return QQ_EVENT_DM
        return ""

    async def parse_message(self, payload: Any) -> InboundMessage:
        body = json_body(payload)
        if body.get("op") not in (None, 0, "0"):
            raise ValueError(f"unsupported QQ webhook opcode: {body.get('op')}")

        event = body.get("d", body)
        if not isinstance(event, dict):
            raise ValueError("QQ webhook event data must be an object")
        event_type = str(body.get("t") or body.get("event_type") or self._infer_event_type(event))
        if event_type not in _QQ_MESSAGE_EVENTS:
            raise ValueError(f"unsupported QQ message event: {event_type or 'unknown'}")

        author = event.get("author") if isinstance(event.get("author"), dict) else {}
        if event_type == QQ_EVENT_C2C:
            scope = QQ_SCOPE_C2C
            sender_id = str(author.get("user_openid") or "")
            chat_id = sender_id
            chat_type = CHAT_PRIVATE
        elif event_type in (QQ_EVENT_GROUP_AT, QQ_EVENT_GROUP):
            scope = QQ_SCOPE_GROUP
            sender_id = str(author.get("member_openid") or "")
            chat_id = str(event.get("group_openid") or "")
            chat_type = CHAT_GROUP
        elif event_type == QQ_EVENT_GUILD_AT:
            scope = QQ_SCOPE_GUILD
            sender_id = str(author.get("id") or "")
            chat_id = str(event.get("channel_id") or "")
            chat_type = CHAT_GROUP
        else:
            scope = QQ_SCOPE_DM
            sender_id = str(author.get("id") or "")
            chat_id = str(event.get("guild_id") or "")
            chat_type = CHAT_PRIVATE

        message_id = str(event.get("id") or "")
        if not sender_id or not chat_id or not message_id:
            raise ValueError("QQ message is missing sender, conversation or message id")

        images, files = _attachment_urls(event)
        return InboundMessage(
            channel=self.channel,
            chat_id=chat_id,
            chat_type=chat_type,
            sender_id=sender_id,
            message_id=message_id,
            text=str(event.get("content") or "").strip(),
            images=images,
            files=files,
            metadata={
                "qq_scope": scope,
                "event_type": event_type,
                "timestamp": event.get("timestamp"),
                "guild_id": event.get("guild_id"),
                "channel_id": event.get("channel_id"),
                "sender_name": author.get("username"),
            },
            raw=body,
        )

    async def _post_json(
        self,
        url: str,
        body: dict[str, Any],
        headers: Optional[dict[str, str]] = None,
    ) -> dict[str, Any]:
        for attempt in range(self.max_retries + 1):
            try:
                response = await self._client().post(url, json=body, headers=headers)
                retryable = response.status_code == 429 or response.status_code >= 500
                if retryable:
                    raise httpx.HTTPStatusError(
                        f"retryable status {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                data = response.json()
                if not isinstance(data, dict):
                    raise ValueError("QQ OpenAPI response must be a JSON object")
                return data
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
                response = getattr(exc, "response", None)
                retryable = response is None or response.status_code == 429 or response.status_code >= 500
                if not retryable or attempt >= self.max_retries:
                    raise
                await asyncio.sleep(self.retry_backoff * (2**attempt))

    async def _ensure_access_token(self) -> Optional[str]:
        if self.access_token and time.monotonic() < self._token_expires_at:
            return self.access_token
        if not self.app_id or not self.app_secret:
            return None
        async with self._token_lock:
            if self.access_token and time.monotonic() < self._token_expires_at:
                return self.access_token
            try:
                data = await self._post_json(
                    self.token_url,
                    {
                        "appId": self.app_id,
                        "clientSecret": self.app_secret,
                    },
                )
            except Exception:  # noqa: BLE001 - caller receives a normalized send failure
                return None
            token = data.get("access_token")
            if not token:
                return None
            self.access_token = str(token)
            try:
                expires_in = max(1, int(data.get("expires_in", 7200)))
            except (TypeError, ValueError):
                expires_in = 7200
            self._token_expires_at = time.monotonic() + max(1, expires_in - 60)
            return self.access_token

    @staticmethod
    def _message_path(scope: str, target_id: str) -> str:
        escaped = quote(target_id, safe="")
        if scope == QQ_SCOPE_C2C:
            return f"/v2/users/{escaped}/messages"
        if scope == QQ_SCOPE_GROUP:
            return f"/v2/groups/{escaped}/messages"
        if scope == QQ_SCOPE_GUILD:
            return f"/channels/{escaped}/messages"
        if scope == QQ_SCOPE_DM:
            return f"/dms/{escaped}/messages"
        raise ValueError(f"unsupported QQ message scope: {scope}")

    async def send_message(self, outbound: OutboundMessage) -> SendResult:
        if not outbound.text:
            return SendResult(ok=False, error="QQ message text must not be empty")

        scope = str(outbound.metadata.get("qq_scope") or QQ_SCOPE_C2C)
        reply_to = str(outbound.metadata.get("reply_to_message_id") or "")
        try:
            path = self._message_path(scope, outbound.chat_id)
        except ValueError as exc:
            return SendResult(ok=False, error=safe_error_message(exc))

        body: dict[str, Any] = {"content": outbound.text}
        if scope in (QQ_SCOPE_C2C, QQ_SCOPE_GROUP):
            body["msg_type"] = 0
            body["msg_seq"] = int(outbound.metadata.get("msg_seq", 1))
        if reply_to:
            body["msg_id"] = reply_to
        if self._send_hook is not None:
            return await self._send_hook(body)

        access_token = await self._ensure_access_token()
        if not access_token:
            return SendResult(ok=False, error="QQ AppID/AppSecret or access_token is not configured")

        try:
            data = await self._post_json(
                f"{self.api_base}{path}",
                body,
                headers={"Authorization": f"QQBot {access_token}"},
            )
        except Exception as exc:  # noqa: BLE001 - normalize transport and response errors
            return SendResult(ok=False, error=safe_error_message(exc))

        error_code = data.get("code", data.get("err_code", 0))
        if error_code not in (0, "0", None):
            message = safe_error_message(RuntimeError(str(data.get("message") or "OpenAPI error")))
            return SendResult(ok=False, error=f"qq code={error_code} {message}")
        message_id = data.get("id") or data.get("message_id")
        return SendResult(ok=True, message_id=str(message_id) if message_id else None)

    async def send_stream(self, chat_id: str, stream: AsyncIterable[str]) -> SendResult:
        text = ""
        async for chunk in stream:
            text += chunk
        if not text:
            return SendResult(ok=True)
        return await self.send_message(OutboundMessage(chat_id=chat_id, text=text))

    async def reply_text(self, inbound: InboundMessage, text: str) -> SendResult:
        chunks = split_text(text, self.message_limit_chars)
        if not chunks:
            return SendResult(ok=True)
        last = SendResult(ok=True)
        for sequence, chunk in enumerate(chunks, start=1):
            last = await self.send_message(
                OutboundMessage(
                    chat_id=inbound.chat_id,
                    text=chunk,
                    metadata={
                        "qq_scope": inbound.metadata.get("qq_scope", QQ_SCOPE_C2C),
                        "reply_to_message_id": inbound.message_id,
                        "msg_seq": sequence,
                    },
                ))
            if not last.ok:
                return last
        return last
