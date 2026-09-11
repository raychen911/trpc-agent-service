"""Enterprise WeCom AES callback and application-message adapter."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import struct
import time
import urllib.parse
import xml.etree.ElementTree as ET
from collections.abc import Mapping
from typing import Any

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from ..config import resolve_secret
from ..tenant.models import ChannelBinding, InboundMessage, OutboundMessage
from .base import ChannelAdapter
from .feishu import AsyncHTTPClient, FeishuError, UrllibAsyncHTTPClient


class WeComVerificationError(ValueError):
    pass


class WeComAdapter(ChannelAdapter):
    name = "wecom"
    max_message_size = 2048
    byte_limit = True

    def __init__(
        self,
        http_client: AsyncHTTPClient | None = None,
        *,
        api_base_url: str = "https://qyapi.weixin.qq.com",
        dry_run: bool = False,
    ) -> None:
        self.http_client = http_client or UrllibAsyncHTTPClient()
        self.api_base_url = api_base_url.rstrip("/")
        self._tokens: dict[str, tuple[str, float]] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self.dry_run = dry_run
        self._bot_replies: dict[str, tuple[Any, Mapping[str, Any]]] = {}

    def register_bot_reply(self, message_id: str, client: Any, frame: Mapping[str, Any]) -> None:
        """Associate a callback message with its long-connection reply frame."""
        self._bot_replies[message_id] = (client, frame)

    def verify(self, binding: ChannelBinding, headers: dict[str, str], body: bytes) -> bool:
        signature = _header(headers, "x-wecom-signature", "msg_signature", "signature")
        timestamp = _header(headers, "x-wecom-timestamp", "timestamp")
        nonce = _header(headers, "x-wecom-nonce", "nonce")
        token = _secret(binding.verify_token) or ""
        encrypted = _encrypted_value(body)
        if not (signature and timestamp and nonce and token):
            return False
        values = (token, timestamp, nonce, encrypted) if encrypted else (token, timestamp, nonce)
        expected = hashlib.sha1("".join(sorted(values)).encode()).hexdigest()
        return hmac.compare_digest(expected, signature)

    def handle_callback(
        self, tenant_id: str, binding: ChannelBinding, headers: dict[str, str], body: bytes, trace_id: str
    ) -> InboundMessage:
        if not self.verify(binding, headers, body):
            raise WeComVerificationError("invalid WeCom callback signature")
        payload: bytes = body
        encrypted = _encrypted_value(body)
        if encrypted:
            payload = self.decrypt(binding, encrypted)
        return self.parse(tenant_id, binding, payload, trace_id)

    def verify_url(self, binding: ChannelBinding, headers: dict[str, str], echo: str) -> str:
        body = f"<xml><Encrypt><![CDATA[{echo}]]></Encrypt></xml>".encode()
        if not self.verify(binding, headers, body):
            raise WeComVerificationError("invalid WeCom URL verification signature")
        return self.decrypt(binding, echo).decode("utf-8")

    def decrypt(self, binding: ChannelBinding, encrypted: str) -> bytes:
        key = _aes_key(binding.encrypt_key_ref)
        try:
            ciphertext = base64.b64decode(encrypted)
            decryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).decryptor()
            padded = decryptor.update(ciphertext) + decryptor.finalize()
            unpadder = padding.PKCS7(256).unpadder()
            plaintext = unpadder.update(padded) + unpadder.finalize()
            size = struct.unpack("!I", plaintext[16:20])[0]
            message = plaintext[20 : 20 + size]
            receiver = plaintext[20 + size :].decode("utf-8")
        except (RuntimeError, ValueError, FeishuError, OSError) as exc:
            raise WeComVerificationError("invalid WeCom encrypted callback") from exc
        expected_receiver = binding.corp_id or binding.account_id
        if receiver and expected_receiver and not hmac.compare_digest(receiver, expected_receiver):
            raise WeComVerificationError("WeCom callback receiver mismatch")
        return message

    def parse(self, tenant_id: str, binding: ChannelBinding, payload: Any, trace_id: str) -> InboundMessage:
        if isinstance(payload, bytes):
            try:
                root = ET.fromstring(payload)
            except ET.ParseError as exc:
                raise ValueError("invalid WeCom XML") from exc
            data = {child.tag: (child.text or "") for child in root}
        else:
            data = dict(payload)
        user_id = str(data.get("FromUserName") or data.get("user_id") or data.get("from_user", ""))
        chat_id = str(data.get("ChatId") or data.get("chat_id") or user_id)
        chat_type = "group" if data.get("ChatId") or data.get("chat_type") == "group" else "direct"
        message_id = str(data.get("MsgId") or data.get("message_id") or data.get("id", ""))
        message_type = str(data.get("MsgType") or "text")
        media_id = str(data.get("MediaId") or data.get("media_id") or "")
        raw = dict(data)
        raw.pop("normalized_media", None)
        raw.pop("_artifact_materialized", None)
        if media_id:
            raw["normalized_media"] = {"type": message_type, "media_id": media_id}
        return InboundMessage(
            tenant_id=tenant_id,
            channel=self.name,
            account_id=binding.account_id,
            external_message_id=message_id,
            external_user_id=user_id,
            chat_id=chat_id,
            chat_type=chat_type,
            text=str(data.get("Content") or data.get("text") or (f"[{message_type}]" if media_id else "")),
            app_id=str(data.get("app_id", "default")),
            trace_id=trace_id,
            raw=raw,
        )

    async def send(self, message: OutboundMessage, binding: ChannelBinding | None = None) -> dict[str, Any]:
        if binding is None or binding.channel != self.name or binding.account_id != message.account_id:
            return {"ok": False, "code": "binding_mismatch"}
        if self.dry_run:
            return {"ok": True, "provider_message_id": f"dry-run-{message.in_reply_to}-{message.part}"}
        bot_reply = self._bot_replies.get(message.in_reply_to)
        if bot_reply is not None:
            client, frame = bot_reply
            try:
                receipt = await client.reply_stream(
                    frame,
                    f"stream-{message.in_reply_to}-{message.part}",
                    message.text,
                    True,
                )
                if message.part == message.total_parts:
                    self._bot_replies.pop(message.in_reply_to, None)
                errcode = int(receipt.get("errcode", 0)) if isinstance(receipt, Mapping) else 0
                return {"ok": errcode == 0, "provider_message_id": message.in_reply_to, "code": errcode}
            except Exception as exc:  # noqa: BLE001 - delivery layer turns this into a retryable failure
                return {"ok": False, "code": type(exc).__name__, "retryable": True}
        if not binding.corp_id or not binding.agent_id or not binding.secret_ref:
            return {"ok": False, "code": "credentials_missing"}
        try:
            token = await self._access_token(binding)
        except (RuntimeError, ValueError, FeishuError, OSError) as exc:
            return {"ok": False, "code": type(exc).__name__, "retryable": True}
        url = (
            f"{binding.api_base_url or self.api_base_url}/cgi-bin/message/send?access_token={urllib.parse.quote(token)}"
        )
        content_key = "media_id" if message.message_type != "text" else "content"
        content_value = message.media_id if content_key == "media_id" else message.text
        payload = {
            "touser": message.external_user_id,
            "msgtype": message.message_type,
            "agentid": int(binding.agent_id) if binding.agent_id.isdigit() else binding.agent_id,
            message.message_type: {content_key: content_value},
            "enable_duplicate_check": 1,
            "duplicate_check_interval": 1800,
        }
        return await self._post_retry(url, payload)

    async def _access_token(self, binding: ChannelBinding) -> str:
        cached = self._tokens.get(binding.account_id)
        if cached and cached[1] - 300 > time.monotonic():
            return cached[0]
        lock = self._locks.setdefault(binding.account_id, asyncio.Lock())
        async with lock:
            cached = self._tokens.get(binding.account_id)
            if cached and cached[1] - 300 > time.monotonic():
                return cached[0]
            secret = _secret(binding.secret_ref)
            query = urllib.parse.urlencode({"corpid": binding.corp_id, "corpsecret": secret})
            url = f"{binding.api_base_url or self.api_base_url}/cgi-bin/gettoken?{query}"
            if hasattr(self.http_client, "get"):
                response = await self.http_client.get(url, headers={})
            else:
                response = await self.http_client.post(url, headers={}, payload={})
            value = response.json()
            if response.status_code >= 400 or value.get("errcode") not in {0, None} or not value.get("access_token"):
                raise RuntimeError("WeCom token request failed")
            expires = int(value.get("expires_in", 7200))
            self._tokens[binding.account_id] = (str(value["access_token"]), time.monotonic() + expires)
            return str(value["access_token"])

    async def _post_retry(self, url: str, payload: Mapping[str, Any]) -> dict[str, Any]:
        for attempt in range(3):
            try:
                response = await self.http_client.post(url, headers={}, payload=payload)
                value = response.json()
            except (FeishuError, OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
                if attempt == 2:
                    return {"ok": False, "code": type(exc).__name__, "retryable": True}
                await asyncio.sleep(0.1 * (2**attempt))
                continue
            code = int(value.get("errcode", response.status_code))
            if response.status_code < 400 and code == 0:
                return {"ok": True, "provider_message_id": value.get("msgid")}
            retryable = response.status_code == 429 or response.status_code >= 500 or code in {45009, -1}
            if not retryable or attempt == 2:
                return {"ok": False, "code": code, "retryable": retryable}
            await asyncio.sleep(0.1 * (2**attempt))
        return {"ok": False, "code": "retry_exhausted", "retryable": True}

    async def download_media(
        self, binding: ChannelBinding, media_id: str, *, max_bytes: int = 30 * 1024 * 1024
    ) -> tuple[bytes, str]:
        token = await self._access_token(binding)
        query = urllib.parse.urlencode({"access_token": token, "media_id": media_id})
        get = getattr(self.http_client, "get", None)
        if not callable(get):
            raise TypeError("WeCom media download is unsupported")
        response = await get(f"{binding.api_base_url or self.api_base_url}/cgi-bin/media/get?{query}", headers={})
        content_type = next(
            (str(value) for key, value in response.headers.items() if str(key).lower() == "content-type"),
            "application/octet-stream",
        ).partition(";")[0]
        if response.status_code >= 400:
            raise ValueError("WeCom media download failed")
        if content_type == "application/json" or response.body.lstrip().startswith(b"{"):
            try:
                payload = response.json()
            except (ValueError, TypeError, json.JSONDecodeError) as exc:
                raise ValueError("WeCom media response is invalid") from exc
            if int(payload.get("errcode", 0)) != 0:
                raise ValueError("WeCom media download failed")
        if not response.body or len(response.body) > max_bytes:
            raise ValueError("WeCom media exceeds configured size")
        return response.body, content_type or "application/octet-stream"


def _header(headers: Mapping[str, str], *names: str) -> str:
    lowered = {key.lower(): str(value) for key, value in headers.items()}
    return next((lowered[name.lower()] for name in names if lowered.get(name.lower())), "")


def _encrypted_value(body: bytes) -> str:
    if not body or not body.lstrip().startswith(b"<"):
        return ""
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return ""
    return root.findtext("Encrypt") or ""


def _secret(reference: str | None) -> str | None:
    if reference and reference.startswith(("env://", "file://", "literal://")):
        return resolve_secret(reference)
    return reference


def _aes_key(reference: str | None) -> bytes:
    value = _secret(reference)
    if not value:
        raise WeComVerificationError("WeCom EncodingAESKey is not configured")
    try:
        key = base64.b64decode(value + "=")
    except ValueError as exc:
        raise WeComVerificationError("invalid WeCom EncodingAESKey") from exc
    if len(key) != 32:
        raise WeComVerificationError("invalid WeCom EncodingAESKey")
    return key
