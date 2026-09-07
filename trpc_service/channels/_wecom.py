# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""WeCom (企业微信) channel adapter."""

from __future__ import annotations

import asyncio
import time
import xml.etree.ElementTree as ET
from typing import Any
from typing import AsyncIterable
from typing import Awaitable
from typing import Callable
from typing import Optional
from uuid import uuid4

import httpx

from ._base import ChannelAdapter
from ._crypto import decrypt_message
from ._crypto import parse_decrypted
from ._crypto import verify_hmac_signature
from ._crypto import verify_sha1_signature
from ._models import CHAT_GROUP
from ._models import CHAT_PRIVATE
from ._models import InboundMessage
from ._models import OutboundMessage
from ._models import SendResult
from ._models import split_text_bytes
from trpc_service.log import safe_error_message

WECOM_MESSAGE_LIMIT_BYTES = 2048
WECOM_API_BASE = "https://qyapi.weixin.qq.com"


class WecomAdapter(ChannelAdapter):
    """企业微信 bot callback adapter.

    Supports the encrypted callback protocol: AES-CBC decryption plus legacy
    SHA1 / current HMAC-SHA256 signature verification. Outbound text is split
    to respect the 2048-byte message limit.
    """

    channel = "wecom"

    def __init__(
        self,
        *,
        token: str,
        encoding_aes_key: str,
        corp_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        access_token: Optional[str] = None,
        corp_secret: Optional[str] = None,
        http_client: Optional[httpx.AsyncClient] = None,
        api_base: str = WECOM_API_BASE,
        message_limit_bytes: int = WECOM_MESSAGE_LIMIT_BYTES,
        stream_edit_interval: float = 0.5,
        max_retries: int = 2,
        retry_backoff: float = 0.2,
        send_hook: Optional[Callable[[dict[str, Any]], Awaitable[SendResult]]] = None,
    ) -> None:
        self.token = token
        self.encoding_aes_key = encoding_aes_key
        self.corp_id = corp_id
        self.agent_id = agent_id
        self.access_token = access_token
        self.corp_secret = corp_secret
        self.api_base = api_base.rstrip("/")
        self.message_limit_bytes = message_limit_bytes
        self.stream_edit_interval = stream_edit_interval
        self.max_retries = max(0, max_retries)
        self.retry_backoff = max(0.0, retry_backoff)
        self._send_hook = send_hook
        self._http_client = http_client
        self._token_expires_at = float("inf") if access_token else 0.0
        self._token_lock = asyncio.Lock()

    def _client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(base_url=self.api_base, timeout=10.0)
        return self._http_client

    @staticmethod
    def _extract_encrypt(payload: Any) -> str:
        if isinstance(payload, (bytes, bytearray)):
            payload = payload.decode("utf-8")
        if isinstance(payload, str):
            root = ET.fromstring(payload)
            node = root.find("Encrypt")
            if node is not None and node.text:
                return node.text
            raise ValueError("missing <Encrypt> in WeCom payload")
        raise ValueError("WeCom payload must be an XML string or bytes")

    async def verify_signature(self, payload: Any, headers: dict[str, str], query: dict[str, str]) -> bool:
        signature = query.get("msg_signature", "")
        timestamp = query.get("timestamp", "")
        nonce = query.get("nonce", "")
        if not (signature and timestamp and nonce):
            return False
        try:
            encrypt = self._extract_encrypt(payload)
        except (ValueError, ET.ParseError):
            return False
        # Prefer the current HMAC-SHA256 scheme, fall back to legacy SHA1.
        if verify_hmac_signature(self.encoding_aes_key, timestamp, nonce, encrypt, signature):
            return True
        return verify_sha1_signature(self.token, timestamp, nonce, encrypt, signature)

    async def parse_message(self, payload: Any) -> InboundMessage:
        # The local validation console uses a plain platform-shaped fixture. It
        # calls ``parse_message`` directly; production webhook traffic still
        # has to pass encrypted callback verification in Gateway first.
        if isinstance(payload, dict):
            sender_id = str(payload.get("FromUserName") or payload.get("sender_id", ""))
            chat_id = str(payload.get("ChatId") or payload.get("chat_id") or sender_id)
            msg_type = str(payload.get("MsgType") or payload.get("msg_type", "text"))
            return InboundMessage(
                channel=self.channel,
                chat_id=chat_id,
                chat_type=CHAT_GROUP
                if payload.get("ChatId") or payload.get("chat_type") == CHAT_GROUP else CHAT_PRIVATE,
                sender_id=sender_id,
                message_id=str(payload.get("MsgId") or payload.get("message_id", "")),
                text=str(payload.get("Content") or payload.get("text", "")),
                images=[str(payload.get("PicUrl"))] if msg_type == "image" and payload.get("PicUrl") else [],
                files=[str(payload.get("MediaId"))] if msg_type == "file" and payload.get("MediaId") else [],
                metadata={
                    "msg_type": msg_type,
                    "local_fixture": True
                },
                raw=payload,
            )
        encrypt = self._extract_encrypt(payload)
        plain = decrypt_message(encrypt, self.encoding_aes_key)
        message_xml, _receive_id = parse_decrypted(plain)
        root = ET.fromstring(message_xml)

        def _text(tag: str) -> str:
            node = root.find(tag)
            return (node.text or "") if node is not None else ""

        sender_id = _text("FromUserName")
        chat_id_node = root.find("ChatId")
        if chat_id_node is not None and chat_id_node.text:
            chat_id = chat_id_node.text
            chat_type = CHAT_GROUP
        else:
            chat_id = sender_id
            chat_type = CHAT_PRIVATE

        msg_type = _text("MsgType")
        return InboundMessage(
            channel=self.channel,
            chat_id=chat_id,
            chat_type=chat_type,
            sender_id=sender_id,
            message_id=_text("MsgId") or _text("CreateTime"),
            text=_text("Content"),
            images=[_text("PicUrl")] if msg_type == "image" and _text("PicUrl") else [],
            metadata={"msg_type": msg_type},
            raw=message_xml,
        )

    async def _post(self, path: str, json_body: dict[str, Any]) -> SendResult:
        last_error = "unknown transport failure"
        for attempt in range(self.max_retries + 1):
            try:
                response = await self._client().post(f"{self.api_base}{path}", json=json_body)
                if response.status_code == 429 or response.status_code >= 500:
                    raise httpx.HTTPStatusError(f"retryable status {response.status_code}",
                                                request=response.request,
                                                response=response)
                response.raise_for_status()
                data = response.json()
                if data.get("errcode", 0) != 0:
                    return SendResult(ok=False, error=f"wecom errcode={data.get('errcode')} {data.get('errmsg')}")
                return SendResult(ok=True, message_id=str(data.get("msgid", "")) or None)
            except (httpx.TimeoutException, httpx.TransportError, httpx.HTTPStatusError) as exc:
                last_error = safe_error_message(exc)
                if attempt < self.max_retries:
                    await asyncio.sleep(self.retry_backoff * (2**attempt))
                    continue
                break
            except Exception as exc:  # noqa: BLE001 - report permanent client failures
                return SendResult(ok=False, error=safe_error_message(exc))
        return SendResult(ok=False, error=last_error)

    async def _ensure_access_token(self) -> Optional[str]:
        """Return a cached token or refresh it from corp_id/corp_secret."""
        if self.access_token and time.monotonic() < self._token_expires_at:
            return self.access_token
        if not self.corp_id or not self.corp_secret:
            return None
        async with self._token_lock:
            if self.access_token and time.monotonic() < self._token_expires_at:
                return self.access_token
            try:
                response = await self._client().get(
                    f"{self.api_base}/cgi-bin/gettoken",
                    params={
                        "corpid": self.corp_id,
                        "corpsecret": self.corp_secret
                    },
                )
                response.raise_for_status()
                data = response.json()
                if data.get("errcode", 0) != 0 or not data.get("access_token"):
                    return None
                self.access_token = str(data["access_token"])
                expires_in = max(60, int(data.get("expires_in", 7200)))
                self._token_expires_at = time.monotonic() + expires_in - 30
                return self.access_token
            except Exception:  # noqa: BLE001 - caller receives a normalized send failure
                return None

    async def send_message(self, outbound: OutboundMessage) -> SendResult:
        body = {
            "touser": outbound.chat_id,
            "msgtype": "text",
            "agentid": int(self.agent_id) if self.agent_id else None,
            "text": {
                "content": outbound.text
            },
        }
        if body["agentid"] is None:
            body.pop("agentid")
        if self._send_hook is not None:
            return await self._send_hook(body)
        access_token = await self._ensure_access_token()
        if not access_token:
            return SendResult(ok=False, error="access_token or platform SDK client is not configured")
        return await self._post(f"/cgi-bin/message/send?access_token={access_token}", body)

    async def send_stream(self, chat_id: str, stream: AsyncIterable[str]) -> SendResult:
        """Stream a reply using the native WeCom ``stream`` msgtype.

        WeCom's streaming contract: open a stream with ``msgtype=stream`` via
        ``message/send`` (``stream.finish=False``), append content via
        ``message/update`` (idempotent by ``stream.id``), and close with a final
        ``message/update`` where ``stream.finish=True``. Updates are throttled to
        ``stream_edit_interval`` seconds; the final update always flushes the
        complete text. If opening the stream fails, fall back to chunked plain
        messages (``reply_text`` behaviour).
        """
        stream_id = f"STREAMID_{uuid4().hex}"
        cumulative = ""
        opened = False
        fallback = False
        last_error: Optional[str] = None
        last_edit = 0.0

        async for chunk in stream:
            if not chunk:
                continue
            cumulative += chunk
            if fallback:
                result = await self.send_message(OutboundMessage(chat_id=chat_id, text=chunk))
                if not result.ok:
                    last_error = result.error
                continue
            if not opened:
                result = await self._send_stream_msg("send", chat_id, stream_id, cumulative, finish=False)
                if result.ok:
                    opened = True
                    last_edit = time.monotonic()
                else:
                    last_error = result.error
                    fallback = True
                    result = await self.send_message(OutboundMessage(chat_id=chat_id, text=chunk))
                    if not result.ok:
                        last_error = result.error
                continue

            now = time.monotonic()
            if now - last_edit >= self.stream_edit_interval:
                result = await self._send_stream_msg("update", chat_id, stream_id, cumulative, finish=False)
                if not result.ok:
                    last_error = result.error
                last_edit = now

        if opened:
            await self._send_stream_msg("update", chat_id, stream_id, cumulative, finish=True)
        return SendResult(ok=last_error is None, error=last_error)

    def _stream_body(self, chat_id: str, stream_id: str, content: str, finish: bool) -> dict[str, Any]:
        body: dict[str, Any] = {
            "touser": chat_id,
            "msgtype": "stream",
            "agentid": int(self.agent_id) if self.agent_id else None,
            "stream": {
                "id": stream_id,
                "content": content,
                "finish": finish
            },
        }
        if body["agentid"] is None:
            body.pop("agentid")
        return body

    async def _send_stream_msg(self, action: str, chat_id: str, stream_id: str, content: str,
                               finish: bool) -> SendResult:
        access_token = await self._ensure_access_token()
        if not access_token:
            return SendResult(ok=False, error="access_token is not configured")
        path = (f"/cgi-bin/message/send?access_token={access_token}"
                if action == "send" else f"/cgi-bin/message/update?access_token={access_token}")
        return await self._post(path, self._stream_body(chat_id, stream_id, content, finish))

    async def reply_text(self, inbound: InboundMessage, text: str) -> SendResult:
        last_error: Optional[str] = None
        for chunk in split_text_bytes(text, self.message_limit_bytes):
            result = await self.send_message(OutboundMessage(chat_id=inbound.chat_id, text=chunk))
            if not result.ok:
                last_error = result.error
        return SendResult(ok=last_error is None, error=last_error)
