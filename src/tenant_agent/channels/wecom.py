"""WeCom encrypted callback and asynchronous application-message adapter."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import os
import re
import struct
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import httpx
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives.padding import PKCS7
from defusedxml import ElementTree

from tenant_agent.channels.base import (
    WECOM_TEXT_MAX_CHARS,
    WECOM_TEXT_MAX_UTF8_BYTES,
    DeliveryError,
    DeliveryResult,
    ParsedWebhook,
    PermanentDeliveryError,
    RateLimited,
    SignatureError,
    UnsupportedMessage,
    WebhookAck,
    WebhookRequest,
    split_text,
)
from tenant_agent.models import (
    Attachment,
    ChannelBindingConfig,
    ChannelType,
    ChatType,
    InboundEnvelope,
    OutboundMessage,
    TenantConfig,
)
from tenant_agent.security import CompositeSecretResolver


class WeComCrypto:
    def __init__(self, token: str, encoding_aes_key: str, receiver_id: str) -> None:
        self.token = token
        self.receiver_id = receiver_id
        if len(encoding_aes_key) != 43 or not re.fullmatch(r"[A-Za-z0-9]+", encoding_aes_key):
            raise SignatureError("invalid WeCom AES key format")
        try:
            self.key = base64.b64decode(encoding_aes_key + "=", validate=True)
        except (ValueError, binascii.Error) as exc:
            raise SignatureError("invalid WeCom AES key") from exc
        if len(self.key) != 32:
            raise SignatureError("invalid WeCom AES key length")

    def signature(self, timestamp: str, nonce: str, encrypted: str) -> str:
        joined = "".join(sorted((self.token, timestamp, nonce, encrypted)))
        # WeCom mandates this legacy SHA-1 construction. The flag preserves
        # protocol compatibility on FIPS-capable Python builds; it is not a claim
        # that SHA-1 is suitable for a new protocol design.
        return hashlib.sha1(joined.encode(), usedforsecurity=False).hexdigest()

    def verify(self, presented: str, timestamp: str, nonce: str, encrypted: str) -> None:
        expected = self.signature(timestamp, nonce, encrypted)
        if not hmac.compare_digest(presented.encode(), expected.encode()):
            raise SignatureError("invalid WeCom callback signature")

    def decrypt(self, encrypted: str) -> str:
        try:
            ciphertext = base64.b64decode(encrypted)
            decryptor = Cipher(algorithms.AES(self.key), modes.CBC(self.key[:16])).decryptor()
            padded = decryptor.update(ciphertext) + decryptor.finalize()
            unpadder = PKCS7(256).unpadder()
            plain = unpadder.update(padded) + unpadder.finalize()
            length = struct.unpack("!I", plain[16:20])[0]
            message = plain[20 : 20 + length]
            receiver = plain[20 + length :].decode()
        except Exception as exc:
            raise SignatureError("WeCom callback decryption failed") from exc
        if receiver != self.receiver_id:
            raise SignatureError("WeCom callback receiver mismatch")
        return message.decode("utf-8")

    def encrypt(self, plaintext: str, *, nonce: str, timestamp: str | None = None) -> tuple[str, str, str]:
        stamp = timestamp or str(int(time.time()))
        message = plaintext.encode()
        framed = os.urandom(16) + struct.pack("!I", len(message)) + message + self.receiver_id.encode()
        padder = PKCS7(256).padder()
        padded = padder.update(framed) + padder.finalize()
        encryptor = Cipher(algorithms.AES(self.key), modes.CBC(self.key[:16])).encryptor()
        encrypted = base64.b64encode(encryptor.update(padded) + encryptor.finalize()).decode()
        return encrypted, self.signature(stamp, nonce, encrypted), stamp


def validate_wecom_credentials(
    *,
    callback_token: str,
    encoding_aes_key: str,
    corp_id: str,
    corp_secret: str,
    agent_id: str,
) -> None:
    if not re.fullmatch(r"[A-Za-z0-9]{3,32}", callback_token):
        raise ValueError("WeCom callback token format is invalid")
    if not re.fullmatch(r"ww[A-Za-z0-9]{2,62}", corp_id):
        raise ValueError("WeCom CorpID format is invalid")
    if not re.fullmatch(r"[A-Za-z0-9_-]{8,128}", corp_secret):
        raise ValueError("WeCom CorpSecret format is invalid")
    if not agent_id.isdecimal() or str(int(agent_id)) != agent_id or int(agent_id) <= 0:
        raise ValueError("WeCom AgentID must be a canonical positive decimal")
    try:
        WeComCrypto(callback_token, encoding_aes_key, corp_id)
    except SignatureError as exc:
        raise ValueError("WeCom EncodingAESKey format is invalid") from exc


@dataclass(slots=True)
class _AccessToken:
    value: str
    expires_monotonic: float


class WeComAdapter:
    name = "wecom"

    def __init__(
        self,
        *,
        api_base: str = "https://qyapi.weixin.qq.com",
        timeout_seconds: float = 10.0,
    ) -> None:
        self.api_base = api_base.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._tokens: dict[str, _AccessToken] = {}

    async def _crypto(self, binding: ChannelBindingConfig, secrets: CompositeSecretResolver) -> WeComCrypto:
        required = ("callback_token", "encoding_aes_key", "corp_id")
        if any(name not in binding.credential_refs for name in required):
            raise SignatureError("WeCom callback credentials are incomplete")
        token, aes_key, corp_id = await asyncio_gather_strings(
            *(secrets.resolve(binding.credential_refs[name]) for name in required)
        )
        return WeComCrypto(token, aes_key, corp_id)

    async def parse(
        self,
        request: WebhookRequest,
        *,
        tenant: TenantConfig,
        binding: ChannelBindingConfig,
        secrets: CompositeSecretResolver,
    ) -> ParsedWebhook:
        crypto = await self._crypto(binding, secrets)
        signature = request.query.get("msg_signature", "")
        timestamp = request.query.get("timestamp", "")
        nonce = request.query.get("nonce", "")
        if request.method.upper() == "GET":
            encrypted_echo = request.query.get("echostr", "")
            crypto.verify(signature, timestamp, nonce, encrypted_echo)
            echo = crypto.decrypt(encrypted_echo)
            return ParsedWebhook((), WebhookAck(body=echo.encode()))
        try:
            outer = ElementTree.fromstring(request.body)
            encrypted = outer.findtext("Encrypt") or ""
        except Exception as exc:
            raise UnsupportedMessage("invalid WeCom XML") from exc
        crypto.verify(signature, timestamp, nonce, encrypted)
        plaintext = crypto.decrypt(encrypted)
        try:
            root = ElementTree.fromstring(plaintext)
        except Exception as exc:
            raise UnsupportedMessage("invalid decrypted WeCom XML") from exc
        values = {child.tag: child.text or "" for child in root}
        expected_agent_id = await secrets.resolve(binding.credential_refs["agent_id"])
        presented_agent_id = values.get("AgentID", "")
        if not presented_agent_id or not hmac.compare_digest(
            presented_agent_id.encode(),
            expected_agent_id.encode(),
        ):
            raise SignatureError("WeCom callback AgentID does not match the binding")
        sender = values.get("FromUserName", "")
        if not sender:
            raise UnsupportedMessage("WeCom sender is required")
        room = values.get("ChatId") or values.get("RoomId")
        chat_type = ChatType.GROUP if room else ChatType.DIRECT
        chat_id = room or sender
        msg_type = values.get("MsgType", "text")
        text = values.get("Content", "")
        attachments: list[Attachment] = []
        media_id = values.get("MediaId")
        if media_id and msg_type in {"image", "file", "voice", "video"}:
            kind = {"image": "image", "file": "file", "voice": "audio", "video": "video"}[msg_type]
            attachments.append(
                Attachment(
                    kind=kind,
                    external_id=media_id,
                    download_url=values.get("PicUrl") or None,
                )
            )
        if msg_type == "image" and not text:
            text = "[Image received]"
        elif msg_type == "file" and not text:
            text = "[File received]"
        create_time = int(values.get("CreateTime") or time.time())
        fallback_id = hashlib.sha256(
            json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        envelope = InboundEnvelope(
            message_id=values.get("MsgId") or fallback_id,
            tenant_id=tenant.tenant_id,
            app_id=binding.app_id,
            binding_id=binding.binding_id,
            channel=ChannelType.WECOM,
            external_account_id=binding.external_account_id,
            external_user_id=sender,
            external_chat_id=chat_id,
            chat_type=chat_type,
            text=text,
            attachments=tuple(attachments),
            occurred_at=datetime.fromtimestamp(create_time, UTC),
            metadata={"message_type": msg_type, "event": values.get("Event")},
        )
        return ParsedWebhook((envelope,), WebhookAck(body=b"success"))

    async def _access_token(
        self,
        binding: ChannelBindingConfig,
        secrets: CompositeSecretResolver,
        client: httpx.AsyncClient,
    ) -> tuple[str, str]:
        required = ("corp_id", "corp_secret")
        if any(name not in binding.credential_refs for name in required):
            raise DeliveryError("WeCom delivery credentials are incomplete")
        corp_id, corp_secret = await asyncio_gather_strings(
            *(secrets.resolve(binding.credential_refs[name]) for name in required)
        )
        cache_key = hashlib.sha256(f"{binding.binding_id}\0{corp_id}\0{corp_secret}".encode()).hexdigest()
        cached = self._tokens.get(cache_key)
        if cached and cached.expires_monotonic > time.monotonic() + 60:
            return cached.value, cache_key
        response = await client.get(
            f"{self.api_base}/cgi-bin/gettoken",
            params={"corpid": corp_id, "corpsecret": corp_secret},
        )
        try:
            payload = response.json()
        except ValueError as exc:
            raise DeliveryError("WeCom returned a non-JSON token response") from exc
        if not response.is_success or payload.get("errcode") != 0:
            raise DeliveryError("WeCom access-token request failed")
        token = str(payload["access_token"])
        self._tokens[cache_key] = _AccessToken(
            token, time.monotonic() + int(payload.get("expires_in", 7_200))
        )
        return token, cache_key

    async def deliver(
        self,
        message: OutboundMessage,
        *,
        tenant: TenantConfig,
        binding: ChannelBindingConfig,
        secrets: CompositeSecretResolver,
    ) -> DeliveryResult:
        del tenant
        agent_id_ref = binding.credential_refs.get("agent_id")
        if agent_id_ref is None:
            raise DeliveryError("WeCom agent_id is not configured")
        agent_id = await secrets.resolve(agent_id_ref)
        ids: list[str] = []
        async with httpx.AsyncClient(timeout=self.timeout_seconds) as client:
            token, token_cache_key = await self._access_token(binding, secrets, client)
            group_mode = bool(message.metadata.get("chat_type") == "group")
            chunks = (
                split_text(
                    message.text,
                    max_chars=WECOM_TEXT_MAX_CHARS,
                    max_utf8_bytes=WECOM_TEXT_MAX_UTF8_BYTES,
                )
                if message.text or message.cards
                else ()
            )
            for chunk in chunks:
                if message.cards and message.cards[0].get("template_card"):
                    msgtype = "template_card"
                    content = {"template_card": message.cards[0]["template_card"]}
                else:
                    msgtype = "text"
                    content = {"text": {"content": chunk or " "}}
                if group_mode:
                    url = f"{self.api_base}/cgi-bin/appchat/send"
                    payload: dict[str, Any] = {
                        "chatid": message.external_chat_id,
                        "msgtype": msgtype,
                        **content,
                    }
                else:
                    url = f"{self.api_base}/cgi-bin/message/send"
                    payload = {
                        "touser": message.external_chat_id,
                        "msgtype": msgtype,
                        "agentid": int(agent_id),
                        **content,
                        "enable_duplicate_check": 1,
                        "duplicate_check_interval": 1_800,
                    }
                response = await client.post(url, params={"access_token": token}, json=payload)
                try:
                    body = response.json()
                except ValueError as exc:
                    raise DeliveryError("WeCom returned a non-JSON delivery response") from exc
                if response.status_code == 429 or body.get("errcode") == 45009:
                    raise RateLimited(2.0)
                if response.status_code >= 500:
                    raise DeliveryError("WeCom is temporarily unavailable")
                if not response.is_success or body.get("errcode") != 0:
                    if body.get("errcode") in {40014, 42001}:
                        self._tokens.pop(token_cache_key, None)
                    raise DeliveryError(f"WeCom delivery failed with code {body.get('errcode', 'http')}")
                if not group_mode and (body.get("invaliduser") or body.get("unlicenseduser")):
                    raise PermanentDeliveryError("WeCom rejected the configured recipient")
                ids.append(str(body.get("msgid") or body.get("response_code") or "accepted"))
            media_types = {
                "image": "image",
                "file": "file",
                "audio": "voice",
                "video": "video",
            }
            for attachment in message.attachments:
                msgtype = media_types[attachment.kind]
                content = {msgtype: {"media_id": attachment.external_id}}
                if group_mode:
                    url = f"{self.api_base}/cgi-bin/appchat/send"
                    payload = {
                        "chatid": message.external_chat_id,
                        "msgtype": msgtype,
                        **content,
                    }
                else:
                    url = f"{self.api_base}/cgi-bin/message/send"
                    payload = {
                        "touser": message.external_chat_id,
                        "msgtype": msgtype,
                        "agentid": int(agent_id),
                        **content,
                        "enable_duplicate_check": 1,
                        "duplicate_check_interval": 1_800,
                    }
                response = await client.post(url, params={"access_token": token}, json=payload)
                try:
                    body = response.json()
                except ValueError as exc:
                    raise DeliveryError("WeCom returned a non-JSON delivery response") from exc
                if response.status_code == 429 or body.get("errcode") == 45009:
                    raise RateLimited(2.0)
                if response.status_code >= 500:
                    raise DeliveryError("WeCom is temporarily unavailable")
                if not response.is_success or body.get("errcode") != 0:
                    if body.get("errcode") in {40014, 42001}:
                        self._tokens.pop(token_cache_key, None)
                    raise DeliveryError(
                        f"WeCom media delivery failed with code {body.get('errcode', 'http')}"
                    )
                if not group_mode and (body.get("invaliduser") or body.get("unlicenseduser")):
                    raise PermanentDeliveryError("WeCom rejected the configured recipient")
                ids.append(str(body.get("msgid") or body.get("response_code") or "accepted"))
        return DeliveryResult(tuple(ids))


async def asyncio_gather_strings(*coroutines: Any) -> tuple[str, ...]:
    import asyncio

    values = await asyncio.gather(*coroutines)
    return tuple(str(value) for value in values)
