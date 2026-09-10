"""WeCom internal-app callback crypto, normalization, and replies."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import struct
from datetime import UTC, datetime
from time import monotonic
from typing import Any

import httpx
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from defusedxml import ElementTree

from trpc_service.channels.models import ChannelMessage
from trpc_service.config.models import ChannelType


class WeComAdapter:
    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._client = client or httpx.AsyncClient(timeout=20)
        self._owns_client = client is None
        self._access_tokens: dict[str, tuple[str, float]] = {}
        self._token_lock = asyncio.Lock()

    @staticmethod
    def verify_signature(
        signature: str | None, token: str, timestamp: str, nonce: str, encrypted: str
    ) -> bool:
        if signature is None:
            return False
        signed = "".join(sorted((token, timestamp, nonce, encrypted))).encode()
        digest = hashlib.sha1(signed).hexdigest()
        return hmac.compare_digest(signature, digest)

    @staticmethod
    def extract_encrypted(xml_body: bytes) -> str:
        if len(xml_body) > 1_000_000 or b"<!DOCTYPE" in xml_body.upper():
            raise ValueError("invalid WeCom callback XML")
        root = ElementTree.fromstring(xml_body)
        encrypted = root.findtext("Encrypt")
        if not encrypted:
            raise ValueError("missing WeCom encrypted payload")
        return encrypted

    @staticmethod
    def decrypt(encrypted: str, encoding_aes_key: str, receive_id: str) -> bytes:
        try:
            key = base64.b64decode(f"{encoding_aes_key}=")
            ciphertext = base64.b64decode(encrypted)
        except ValueError as exc:
            raise ValueError("invalid WeCom encryption data") from exc
        if len(key) != 32:
            raise ValueError("WeCom EncodingAESKey must contain 43 characters")
        decryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).decryptor()
        padded = decryptor.update(ciphertext) + decryptor.finalize()
        pad = padded[-1]
        if pad < 1 or pad > 32 or padded[-pad:] != bytes([pad]) * pad:
            raise ValueError("invalid WeCom callback padding")
        plain = padded[:-pad]
        if len(plain) < 20:
            raise ValueError("invalid WeCom callback payload")
        message_length = struct.unpack(">I", plain[16:20])[0]
        message = plain[20 : 20 + message_length]
        actual_receive_id = plain[20 + message_length :].decode("utf-8")
        if not hmac.compare_digest(actual_receive_id, receive_id):
            raise ValueError("WeCom callback receiver mismatch")
        return message

    @staticmethod
    def parse(account_id: str, xml_body: bytes) -> ChannelMessage | None:
        if len(xml_body) > 1_000_000 or b"<!DOCTYPE" in xml_body.upper():
            raise ValueError("invalid WeCom message XML")
        root = ElementTree.fromstring(xml_body)
        if root.findtext("MsgType") != "text":
            return None
        sender = root.findtext("FromUserName")
        recipient = root.findtext("ToUserName")
        content = root.findtext("Content")
        message_id = root.findtext("MsgId")
        if not all((sender, recipient, content, message_id)) or recipient != account_id:
            raise ValueError("invalid WeCom text message")
        created = int(root.findtext("CreateTime") or "0")
        chat_id = root.findtext("ChatId") or sender
        return ChannelMessage(
            external_message_id=message_id,
            channel=ChannelType.WECOM,
            account_id=account_id,
            chat_type="group" if root.findtext("ChatId") else "private",
            chat_id=chat_id,
            sender_id=sender,
            text=content,
            timestamp=datetime.fromtimestamp(created, UTC),
            metadata={"agent_id": root.findtext("AgentID")},
        )

    async def send(self, corp_secret: str, message: ChannelMessage, text: str) -> None:
        access_token = await self._get_access_token(message.account_id, corp_secret)
        agent_id = message.metadata.get("agent_id")
        body: dict[str, Any] = {
            "touser": message.sender_id,
            "msgtype": "text",
            "agentid": int(agent_id) if agent_id else 0,
            "text": {"content": text},
            "safe": 0,
        }
        response = await self._client.post(
            "https://qyapi.weixin.qq.com/cgi-bin/message/send",
            params={"access_token": access_token},
            json=body,
        )
        response.raise_for_status()
        if response.json().get("errcode", 0) != 0:
            raise RuntimeError("WeCom rejected the message")

    async def _get_access_token(self, corp_id: str, corp_secret: str) -> str:
        cached = self._access_tokens.get(corp_id)
        if cached and cached[1] > monotonic():
            return cached[0]
        async with self._token_lock:
            cached = self._access_tokens.get(corp_id)
            if cached and cached[1] > monotonic():
                return cached[0]
            token_response = await self._client.get(
                "https://qyapi.weixin.qq.com/cgi-bin/gettoken",
                params={"corpid": corp_id, "corpsecret": corp_secret},
            )
            token_response.raise_for_status()
            token_data = token_response.json()
            if token_data.get("errcode", 0) != 0 or not token_data.get("access_token"):
                raise RuntimeError("WeCom rejected the access-token request")
            lifetime = max(60, int(token_data.get("expires_in", 7200)) - 60)
            token = str(token_data["access_token"])
            self._access_tokens[corp_id] = (token, monotonic() + lifetime)
            return token

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()


__all__ = ["WeComAdapter"]
