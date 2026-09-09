"""WeChat Customer Service protocol, crypto and injectable transport.

API references: https://kf.weixin.qq.com/api/doc/path/94745 (sync),
https://kf.weixin.qq.com/api/doc/path/94677 (send). No ordinary user account
automation: only the official enterprise customer-service API is used.
"""
import asyncio
import base64
import hashlib
import hmac
import json
import struct
import time
from typing import Protocol

import httpx
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from defusedxml.ElementTree import fromstring

from trpc_service.gateway.models import Attachment, MessageKind, NormalizedInboundMessage
from trpc_service.log.audit import register_secret
from .base import ChannelAuthenticationError, DeliveryResult, UnsupportedMessageError


class CustomerServiceError(RuntimeError):

    def __init__(self, code, retryable=False, uncertain=False, retry_after=0):
        super().__init__(code)
        self.code, self.retryable, self.uncertain = code, retryable, uncertain
        self.retry_after = retry_after


class CallbackCrypto:
    """WeCom SHA1 signature + AES-256-CBC + 32-byte PKCS#7 envelope."""

    def __init__(self, token, encoding_aes_key, corp_id):
        self.token, self.corp_id = token, corp_id
        self.key = base64.b64decode(encoding_aes_key + "=", validate=True)
        if not token or not corp_id or len(self.key) != 32:
            raise ValueError("invalid callback crypto configuration")
        register_secret(token)
        register_secret(encoding_aes_key)

    def decrypt(self, encrypted, signature, timestamp, nonce):
        expected = hashlib.sha1("".join(sorted([self.token, timestamp, nonce, encrypted])).encode()).hexdigest()
        if len(signature) != 40 or not signature.isascii() or not hmac.compare_digest(expected, signature):
            raise ChannelAuthenticationError("invalid customer-service callback signature")
        try:
            encrypted_bytes = base64.b64decode(encrypted, validate=True)
            decryptor = Cipher(algorithms.AES(self.key), modes.CBC(self.key[:16])).decryptor()
            plain = decryptor.update(encrypted_bytes) + decryptor.finalize()
            padding = plain[-1]
            if padding < 1 or padding > 32 or plain[-padding:] != bytes([padding]) * padding:
                raise ValueError("padding")
            plain = plain[:-padding]
            size = struct.unpack("!I", plain[16:20])[0]
            if size > len(plain) - 20 or plain[20 + size:].decode() != self.corp_id:
                raise ValueError("receiver")
            return plain[20:20 + size].decode("utf-8")
        except Exception:
            raise ChannelAuthenticationError("invalid customer-service encrypted envelope") from None

    def notification(self, body, signature, timestamp, nonce):
        if len(body) > 1024 * 1024:
            raise ChannelAuthenticationError("callback exceeds size limit")
        try:
            encrypted = fromstring(body).findtext("Encrypt") or ""
            plaintext = self.decrypt(encrypted, signature, timestamp, nonce)
            root = fromstring(plaintext)
            value = {item.tag: item.text or "" for item in root}
        except ChannelAuthenticationError:
            raise
        except Exception:
            raise ChannelAuthenticationError("invalid callback XML") from None
        if value.get("Event") != "kf_msg_or_event" or not value.get("Token"):
            raise UnsupportedMessageError("unsupported customer-service notification")
        return value


class CustomerServiceClient(Protocol):

    async def sync_messages(self, open_kfid, cursor, token):
        ...

    async def service_state(self, open_kfid, external_userid):
        ...

    async def send(self, payload):
        ...

    async def download_file(self, media_id):
        ...

    async def upload_file(self, name, mime_type, content, kind):
        ...

    async def close(self):
        ...


class HttpCustomerServiceClient:

    def __init__(self, corp_id, secret, http=None):
        self.corp_id, self.secret = corp_id, secret
        register_secret(secret)
        self.http = http or httpx.AsyncClient(
            base_url="https://qyapi.weixin.qq.com/cgi-bin/", timeout=20, follow_redirects=False)
        self._owns_http = http is None
        self._access_token, self._expires = "", 0
        self._token_lock = asyncio.Lock()

    async def access_token(self):
        async with self._token_lock:
            if time.monotonic() < self._expires:
                return self._access_token
            response = await self.http.get("gettoken", params={"corpid": self.corp_id, "corpsecret": self.secret})
            self._check_http(response)
            data = response.json()
            self._check_api(data)
            self._access_token = data["access_token"]
            register_secret(self._access_token)
            self._expires = time.monotonic() + max(1, int(data.get("expires_in", 7200)) - 120)
            return self._access_token

    @staticmethod
    def _check_http(response):
        if response.status_code == 429:
            try:
                delay = float(response.headers.get("Retry-After", "1"))
            except ValueError:
                delay = 1
            raise CustomerServiceError("kf_rate_limited", True, retry_after=delay)
        if response.status_code >= 400:
            raise CustomerServiceError(f"kf_http_{response.status_code}", response.status_code >= 500)

    @staticmethod
    def _check_api(data):
        code = int(data.get("errcode", 0))
        if code:
            raise CustomerServiceError(f"kf_api_{code}", code in {-1, 45009})

    async def call(self, path, payload, *, sending=False):
        for attempt in range(2):
            token = await self.access_token()
            try:
                response = await self.http.post(path, params={"access_token": token}, json=payload)
                self._check_http(response)
                data = response.json()
            except CustomerServiceError as error:
                if sending and error.code.startswith("kf_http_5"):
                    raise CustomerServiceError("kf_delivery_unknown", uncertain=True) from None
                raise
            except (httpx.ConnectError, httpx.ConnectTimeout):
                raise CustomerServiceError("kf_connect_failed", True) from None
            except (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError):
                raise CustomerServiceError("kf_delivery_unknown" if sending else "kf_network_failed", not sending,
                                           sending) from None
            except ValueError:
                raise CustomerServiceError("kf_invalid_response", not sending, sending) from None
            if data.get("errcode") in {40014, 42001} and attempt == 0:
                self._expires = 0
                continue
            self._check_api(data)
            return data
        raise CustomerServiceError("kf_token_rejected")

    async def sync_messages(self, open_kfid, cursor, token):
        return await self.call("kf/sync_msg", {"open_kfid": open_kfid, "cursor": cursor, "token": token, "limit": 100})

    async def service_state(self, open_kfid, external_userid):
        data = await self.call("kf/service_state/get", {"open_kfid": open_kfid, "external_userid": external_userid})
        return int(data["service_state"])

    async def send(self, payload):
        return (await self.call("kf/send_msg", payload, sending=True))["msgid"]

    async def download_file(self, media_id):
        for attempt in range(2):
            token = await self.access_token()
            async with self.http.stream("GET", "media/get", params={
                    "access_token": token,
                    "media_id": media_id
            }) as response:
                self._check_http(response)
                data = bytearray()
                async for chunk in response.aiter_bytes():
                    data.extend(chunk)
                    if len(data) > 10 * 1024 * 1024:
                        raise CustomerServiceError("kf_attachment_too_large")
                mime = response.headers.get("content-type", "application/octet-stream").split(";")[0]
                if mime == "application/json":
                    value = json.loads(data)
                    if value.get("errcode") in {40014, 42001} and attempt == 0:
                        self._expires = 0
                        continue
                    self._check_api(value)
                    raise CustomerServiceError("kf_invalid_media_response")
                return bytes(data), mime

    async def upload_file(self, name, mime_type, content, kind):
        for attempt in range(2):
            token = await self.access_token()
            response = await self.http.post("media/upload",
                                            params={
                                                "access_token": token,
                                                "type": kind
                                            },
                                            files={"media": (name, content, mime_type)})
            self._check_http(response)
            data = response.json()
            if data.get("errcode") in {40014, 42001} and attempt == 0:
                self._expires = 0
                continue
            self._check_api(data)
            return data["media_id"]

    async def close(self):
        if self._owns_http:
            await self.http.aclose()


class FakeCustomerServiceClient:

    def __init__(self, pages=None):
        self.pages = pages or {}
        self.states = {}
        self.sent = []
        self.calls = []
        self.files = {}
        self.send_error = None

    async def sync_messages(self, open_kfid, cursor, token):
        self.calls.append((open_kfid, cursor))
        return self.pages.get(cursor, {"msg_list": [], "next_cursor": cursor, "has_more": 0})

    async def service_state(self, open_kfid, external_userid):
        return self.states.get(external_userid, 1)

    async def send(self, payload):
        if self.send_error:
            error, self.send_error = self.send_error, None
            raise error
        self.sent.append(payload)
        return payload["msgid"]

    async def download_file(self, media_id):
        return self.files[media_id]

    async def upload_file(self, name, mime_type, content, kind):
        media_id = hashlib.sha256(content).hexdigest()
        self.files[media_id] = (content, mime_type)
        return media_id

    async def close(self):
        pass


class CustomerServiceAdapter:

    def __init__(self, binding, client, store, crypto=None, artifacts=None):
        self.binding, self.client, self.store = binding, client, store
        self.crypto, self.artifacts = crypto, artifacts

    async def normalize(self, binding_id, payload, headers):
        if binding_id != self.binding.binding_id or payload.get("open_kfid") != self.binding.open_kfid:
            raise ChannelAuthenticationError("customer-service account mismatch")
        if payload.get("origin") != 3:
            raise UnsupportedMessageError("only customer-origin messages may invoke the agent")
        kind = payload.get("msgtype")
        if kind not in {"text", "image", "file", "voice"}:
            raise UnsupportedMessageError("unsupported customer-service message type")
        user = payload.get("external_userid")
        if not user or not payload.get("msgid"):
            raise UnsupportedMessageError("missing customer-service identity")
        media = payload.get(kind) or {}
        attachments = []
        text = media.get("content", "") if kind in {"text", "voice"} else ""
        if kind != "text" and not text:
            if not media.get("media_id"):
                raise UnsupportedMessageError("missing media_id")
            attachments.append(
                Attachment(attachment_id=payload["msgid"],
                           kind=MessageKind.AUDIO if kind == "voice" else MessageKind(kind),
                           name=media.get("name", f"customer-{kind}"),
                           source_url=f"wecom-kf://media/{media['media_id']}"))
        return NormalizedInboundMessage(message_id=payload["msgid"],
                                        binding_id=binding_id,
                                        channel="wecom_kf",
                                        external_user_id=user,
                                        external_conversation_id=f"{self.binding.open_kfid}:{user}",
                                        text=text,
                                        attachments=attachments,
                                        kind=(MessageKind.TEXT if text else
                                              (MessageKind.AUDIO if kind == "voice" else MessageKind(kind))),
                                        metadata={
                                            "external_userid": user,
                                            "send_time": payload.get("send_time", 0)
                                        })

    async def download_file(self, media_id):
        return await self.client.download_file(media_id)

    async def download_attachment(self, attachment):
        prefix = "wecom-kf://media/"
        if not attachment.source_url.startswith(prefix):
            raise UnsupportedMessageError("invalid customer-service attachment reference")
        content, mime = await self.download_file(attachment.source_url.removeprefix(prefix))
        return content, mime, attachment.name

    async def deliver(self, message):
        user = message.external_conversation_id.removeprefix(self.binding.open_kfid + ":")
        if user == message.external_conversation_id or not user:
            return DeliveryResult(delivered=False, error_code="kf_conversation_mismatch")
        reserved = False
        try:
            if await self.client.service_state(self.binding.open_kfid, user) not in {0, 1}:
                return DeliveryResult(delivered=False, error_code="kf_human_or_closed")
            # A durable attempt marker prevents replay after a crash between the
            # external send and the local acknowledgement. Ambiguity needs review.
            decision = await self.store.reserve_send(self.binding.binding_id, user, message.outbound_id)
            if decision != "reserved":
                return DeliveryResult(delivered=decision == "delivered",
                                      uncertain=decision == "unknown",
                                      external_message_id=message.outbound_id if decision == "delivered" else "",
                                      error_code=f"kf_{decision}")
            reserved = True
            payload = {
                "touser": user,
                "open_kfid": self.binding.open_kfid,
                "msgid": message.outbound_id,
                "msgtype": "text",
                "text": {
                    "content": message.text
                }
            }
            if message.attachments:
                if len(message.attachments) != 1 or not self.artifacts:
                    raise CustomerServiceError("kf_one_attachment_per_outbound_required")
                attachment = message.attachments[0]
                metadata, content = await self.artifacts.get(message.tenant_id, attachment.attachment_id)
                kind = "image" if metadata.mime_type.startswith("image/") else "file"
                media_id = await self.client.upload_file(metadata.original_name, metadata.mime_type, content, kind)
                payload.pop("text")
                payload.update(msgtype=kind, **{kind: {"media_id": media_id}})
            elif len(message.text.encode("utf-8")) > 2048:
                raise CustomerServiceError("kf_text_too_long")
            external_id = await self.client.send(payload)
            await self.store.finish_send(message.outbound_id, "delivered", self.binding.binding_id)
            return DeliveryResult(delivered=True, external_message_id=external_id)
        except CustomerServiceError as error:
            if reserved:
                await self.store.finish_send(message.outbound_id, "unknown" if error.uncertain else "failed",
                                             self.binding.binding_id)
            return DeliveryResult(delivered=False,
                                  retryable=error.retryable,
                                  uncertain=error.uncertain,
                                  error_code=error.code,
                                  retry_after_seconds=error.retry_after)

    async def close(self):
        await self.client.close()
