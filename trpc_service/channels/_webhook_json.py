# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Shared transport helpers for JSON based enterprise IM adapters."""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
from typing import Any
from typing import AsyncIterable
from typing import Awaitable
from typing import Callable
from typing import Optional

import httpx

from ._base import ChannelAdapter
from ._models import InboundMessage
from ._models import OutboundMessage
from ._models import SendResult
from ._models import split_text
from trpc_service.log import safe_error_message

SendHook = Callable[[dict[str, Any]], Awaitable[SendResult]]


def json_body(payload: Any) -> dict[str, Any]:
    """Return a JSON object from dict/bytes/string payloads."""
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8")
    if isinstance(payload, str):
        value = json.loads(payload)
        if isinstance(value, dict):
            return value
    raise ValueError("IM webhook payload must be a JSON object")


def raw_bytes(payload: Any) -> bytes:
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, bytearray):
        return bytes(payload)
    if isinstance(payload, str):
        return payload.encode("utf-8")
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def verify_hmac_hex(secret: str, payload: Any, signature: str) -> bool:
    if not secret or not signature:
        return False
    expected = hmac.new(secret.encode("utf-8"), raw_bytes(payload), hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


class JsonWebhookAdapter(ChannelAdapter):
    """Reusable outbound transport for JSON webhook/SDK based adapters.

    ``send_hook`` is the production integration seam for a platform's official
    SDK. Tests and the local validation console inject a deterministic hook;
    a plain webhook URL is also supported for platform endpoints that accept
    direct JSON posts.
    """

    channel = "json"

    def __init__(
        self,
        *,
        webhook_url: Optional[str] = None,
        secret: Optional[str] = None,
        http_client: Optional[httpx.AsyncClient] = None,
        send_hook: Optional[SendHook] = None,
        message_limit_chars: int = 4000,
        max_retries: int = 2,
        retry_backoff: float = 0.2,
    ) -> None:
        self.webhook_url = webhook_url
        self.secret = secret or ""
        self._http_client = http_client
        self._owns_http_client = http_client is None
        self._send_hook = send_hook
        self.message_limit_chars = message_limit_chars
        self.max_retries = max(0, max_retries)
        self.retry_backoff = max(0.0, retry_backoff)

    def _client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(timeout=10.0)
        return self._http_client

    def render_outbound(self, outbound: OutboundMessage) -> dict[str, Any]:
        """Build the platform payload; subclasses override as needed."""
        return {"chat_id": outbound.chat_id, "msgtype": outbound.kind, "text": outbound.text}

    async def send_message(self, outbound: OutboundMessage) -> SendResult:
        body = self.render_outbound(outbound)
        if self._send_hook is not None:
            return await self._send_hook(body)
        if not self.webhook_url:
            return SendResult(ok=False, error="platform SDK client or webhook_url is not configured")
        last_error = "unknown transport failure"
        for attempt in range(self.max_retries + 1):
            try:
                response = await self._client().post(self.webhook_url, json=body)
                if response.status_code == 429 or response.status_code >= 500:
                    raise httpx.HTTPStatusError(f"retryable status {response.status_code}",
                                                request=response.request,
                                                response=response)
                response.raise_for_status()
                data = response.json() if response.content else {}
                error_code = data.get("errcode", data.get("code", 0))
                if error_code not in (0, "0", None):
                    return SendResult(ok=False, error=f"{self.channel} error={error_code}")
                message_id = data.get("message_id") or data.get("msgid") or data.get("processQueryKey")
                return SendResult(ok=True, message_id=str(message_id) if message_id else None)
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
                last_error = safe_error_message(exc)
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_backoff * (2**attempt))
                    continue
                break
            except Exception as exc:  # noqa: BLE001 - normalize permanent client errors
                return SendResult(ok=False, error=safe_error_message(exc))
        return SendResult(ok=False, error=last_error)

    async def send_stream(self, chat_id: str, stream: AsyncIterable[str]) -> SendResult:
        text = ""
        async for chunk in stream:
            text += chunk
        return await self.reply_text(
            InboundMessage(
                channel=self.channel,
                chat_id=chat_id,
                sender_id=chat_id,
                message_id="stream",
            ),
            text,
        )

    async def reply_text(self, inbound: InboundMessage, text: str) -> SendResult:
        last = SendResult(ok=True)
        for chunk in split_text(text, self.message_limit_chars):
            last = await self.send_message(OutboundMessage(chat_id=inbound.chat_id, text=chunk))
            if not last.ok:
                return last
        return last

    async def close(self) -> None:
        if self._owns_http_client and self._http_client is not None:
            await self._http_client.aclose()
            self._http_client = None
