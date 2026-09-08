import base64
import hashlib
import hmac
import json
import os
import struct
import time
import uuid
from xml.etree import ElementTree
from xml.etree.ElementTree import Element, SubElement, tostring

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from trpc_service.channels.bindings import ResolvedChannelBinding
from trpc_service.channels.telegram import (
    ChannelAuthenticationError,
    UnsupportedChannelMessageError,
)
from trpc_service.gateway.contracts import NormalizedMessage


class IgnoredWeComAIBotEvent(Exception):
    """A valid control callback that needs an empty acknowledgement, not an Agent turn."""


def _xml_value(root: Element, name: str, default: str = "") -> str:
    node = root.find(name)
    return node.text if node is not None and node.text is not None else default


def _parse_xml(value: bytes | str) -> Element:
    raw = value.encode() if isinstance(value, str) else value
    upper = raw.upper()
    if b"<!DOCTYPE" in upper or b"<!ENTITY" in upper:
        raise ValueError("DTD and XML entities are not allowed")
    return ElementTree.fromstring(raw)


class WeComCrypto:
    def __init__(self, token: str, encoding_aes_key: str, receive_id: str) -> None:
        self._token = token
        try:
            self._key = base64.b64decode(encoding_aes_key + "=")
        except Exception as error:
            raise ValueError("invalid WeCom EncodingAESKey") from error
        if len(self._key) != 32:
            raise ValueError("WeCom EncodingAESKey must decode to 32 bytes")
        self._receive_id = receive_id.encode()

    def signature(self, timestamp: str, nonce: str, encrypted: str) -> str:
        joined = "".join(sorted((self._token, timestamp, nonce, encrypted)))
        return hashlib.sha1(joined.encode()).hexdigest()  # noqa: S324 - protocol requires SHA-1

    def decrypt(self, encrypted: str, signature: str, timestamp: str, nonce: str) -> str:
        expected = self.signature(timestamp, nonce, encrypted)
        if not hmac.compare_digest(expected, signature):
            raise ChannelAuthenticationError("invalid WeCom message signature")
        ciphertext = base64.b64decode(encrypted)
        decryptor = Cipher(algorithms.AES(self._key), modes.CBC(self._key[:16])).decryptor()
        padded = decryptor.update(ciphertext) + decryptor.finalize()
        unpadder = padding.PKCS7(256).unpadder()
        plain = unpadder.update(padded) + unpadder.finalize()
        if len(plain) < 20:
            raise ChannelAuthenticationError("invalid WeCom encrypted payload")
        message_length = struct.unpack("!I", plain[16:20])[0]
        message = plain[20 : 20 + message_length]
        receive_id = plain[20 + message_length :]
        if self._receive_id and not hmac.compare_digest(receive_id, self._receive_id):
            raise ChannelAuthenticationError("WeCom receive_id mismatch")
        return message.decode()

    def encrypt(self, plaintext: str, timestamp: str, nonce: str) -> tuple[str, str]:
        encoded = plaintext.encode()
        plain = os.urandom(16) + struct.pack("!I", len(encoded)) + encoded + self._receive_id
        padder = padding.PKCS7(256).padder()
        padded = padder.update(plain) + padder.finalize()
        encryptor = Cipher(algorithms.AES(self._key), modes.CBC(self._key[:16])).encryptor()
        encrypted = base64.b64encode(encryptor.update(padded) + encryptor.finalize()).decode()
        return encrypted, self.signature(timestamp, nonce, encrypted)


class WeComAdapter:
    @staticmethod
    def verify_url(
        echostr: str,
        signature: str,
        timestamp: str,
        nonce: str,
        crypto: WeComCrypto,
    ) -> str:
        return crypto.decrypt(echostr, signature, timestamp, nonce)

    def normalize(
        self,
        body: bytes,
        binding: ResolvedChannelBinding,
        *,
        token: str | None,
        encoding_aes_key: str | None,
        signature: str | None,
        timestamp: str | None,
        nonce: str | None,
        trace_id: str | None = None,
    ) -> NormalizedMessage:
        if binding.options.get("mode", "app") == "aibot":
            return self._normalize_aibot(
                body,
                binding,
                token=token,
                encoding_aes_key=encoding_aes_key,
                signature=signature,
                timestamp=timestamp,
                nonce=nonce,
                trace_id=trace_id,
            )
        root = _parse_xml(body)
        encrypted = _xml_value(root, "Encrypt")
        if encrypted:
            if not all((token, encoding_aes_key, signature, timestamp, nonce)):
                raise ChannelAuthenticationError("incomplete WeCom encrypted callback parameters")
            crypto = WeComCrypto(
                token or "",
                encoding_aes_key or "",
                str(binding.options.get("receive_id", binding.account_id)),
            )
            root = _parse_xml(
                crypto.decrypt(encrypted, signature or "", timestamp or "", nonce or "")
            )
        elif not binding.options.get("allow_plaintext", False):
            raise ChannelAuthenticationError("plaintext WeCom callbacks are disabled")

        message_type = _xml_value(root, "MsgType")
        text = _xml_value(root, "Content")
        attachments: list[dict[str, str]] = []
        if message_type in {"image", "file", "voice", "video"}:
            media_id = _xml_value(root, "MediaId")
            if not media_id:
                raise UnsupportedChannelMessageError("WeCom media message has no MediaId")
            attachments.append(
                {
                    "type": "audio" if message_type == "voice" else message_type,
                    "provider_media_id": media_id,
                    "pic_url": _xml_value(root, "PicUrl"),
                    "format": _xml_value(root, "Format"),
                }
            )
            recognition = _xml_value(root, "Recognition")
            text = recognition or f"[WeCom {message_type} attachment]"
        elif message_type == "link":
            text = "\n".join(
                item
                for item in (
                    _xml_value(root, "Title"),
                    _xml_value(root, "Description"),
                    _xml_value(root, "Url"),
                )
                if item
            )
        elif message_type != "text" or not text:
            raise UnsupportedChannelMessageError("unsupported WeCom message type")
        sender_id = _xml_value(root, "FromUserName")
        chat_id = _xml_value(root, "ChatId")
        create_time = _xml_value(root, "CreateTime")
        message_id = _xml_value(root, "MsgId") or f"{sender_id}:{create_time}"
        return NormalizedMessage(
            tenant_id=binding.tenant_id,
            agent_app_id=binding.agent_app_id,
            channel="wecom",
            account_id=binding.account_id,
            external_message_id=message_id,
            sender_user_id=sender_id,
            conversation_id=chat_id or sender_id,
            conversation_type="group" if chat_id else "direct",
            text=text,
            trace_id=trace_id or str(uuid.uuid4()),
            metadata={
                "to_user": _xml_value(root, "ToUserName"),
                "create_time": create_time,
                "agent_id": _xml_value(root, "AgentID"),
                "message_type": message_type,
                "attachments": attachments,
            },
        )

    def _normalize_aibot(
        self,
        body: bytes,
        binding: ResolvedChannelBinding,
        *,
        token: str | None,
        encoding_aes_key: str | None,
        signature: str | None,
        timestamp: str | None,
        nonce: str | None,
        trace_id: str | None,
    ) -> NormalizedMessage:
        if not all((token, encoding_aes_key, signature, timestamp, nonce)):
            raise ChannelAuthenticationError("incomplete WeCom AIBot callback parameters")
        try:
            envelope = json.loads(body)
            encrypted = str(envelope["encrypt"])
        except (KeyError, TypeError, ValueError) as error:
            raise ChannelAuthenticationError(
                "invalid WeCom AIBot encrypted JSON envelope"
            ) from error
        crypto = WeComCrypto(token or "", encoding_aes_key or "", "")
        try:
            payload = json.loads(
                crypto.decrypt(encrypted, signature or "", timestamp or "", nonce or "")
            )
        except json.JSONDecodeError as error:
            raise ChannelAuthenticationError("invalid WeCom AIBot decrypted JSON") from error
        if not isinstance(payload, dict):
            raise ChannelAuthenticationError("invalid WeCom AIBot message payload")

        actual_bot_id = str(payload.get("aibotid", ""))
        expected_bot_id = str(binding.options.get("aibot_id", binding.account_id))
        if not actual_bot_id or not hmac.compare_digest(
            actual_bot_id.encode("utf-8"), expected_bot_id.encode("utf-8")
        ):
            raise ChannelAuthenticationError("WeCom AIBot id mismatch")
        sender = payload.get("from")
        sender_id = str(sender.get("userid", "")) if isinstance(sender, dict) else ""
        message_id = str(payload.get("msgid", ""))
        if not sender_id or not message_id:
            raise ChannelAuthenticationError("WeCom AIBot sender or msgid is missing")

        message_type = str(payload.get("msgtype", ""))
        if message_type in {"event", "stream"}:
            raise IgnoredWeComAIBotEvent(message_type)
        text = ""
        attachments: list[dict[str, str]] = []
        if message_type == "text":
            text_payload = payload.get("text")
            text = str(text_payload.get("content", "")) if isinstance(text_payload, dict) else ""
        elif message_type == "voice":
            voice = payload.get("voice")
            text = str(voice.get("content", "")) if isinstance(voice, dict) else ""
        elif message_type == "mixed":
            mixed = payload.get("mixed")
            items = mixed.get("msg_item", []) if isinstance(mixed, dict) else []
            text_parts: list[str] = []
            for item in items if isinstance(items, list) else []:
                if not isinstance(item, dict):
                    continue
                item_type = str(item.get("msgtype", ""))
                item_payload = item.get(item_type)
                if item_type == "text" and isinstance(item_payload, dict):
                    content = str(item_payload.get("content", ""))
                    if content:
                        text_parts.append(content)
                elif item_type == "image" and isinstance(item_payload, dict):
                    attachments.append(
                        {"type": "image", "provider_url": str(item_payload.get("url", ""))}
                    )
            text = "\n".join(text_parts) or "[WeCom AIBot mixed attachment]"
        elif message_type in {"image", "file", "video"}:
            media = payload.get(message_type)
            url = str(media.get("url", "")) if isinstance(media, dict) else ""
            if not url:
                raise UnsupportedChannelMessageError("WeCom AIBot media message has no URL")
            attachments.append({"type": message_type, "provider_url": url})
            text = f"[WeCom AIBot {message_type} attachment]"
        else:
            raise UnsupportedChannelMessageError("unsupported WeCom AIBot message type")
        if not text:
            raise UnsupportedChannelMessageError("WeCom AIBot message has no usable content")

        conversation_type = "group" if payload.get("chattype") == "group" else "direct"
        chat_id = str(payload.get("chatid", ""))
        conversation_id = chat_id if conversation_type == "group" and chat_id else sender_id
        return NormalizedMessage(
            tenant_id=binding.tenant_id,
            agent_app_id=binding.agent_app_id,
            channel="wecom",
            account_id=binding.account_id,
            external_message_id=message_id,
            sender_user_id=sender_id,
            conversation_id=conversation_id,
            conversation_type=conversation_type,
            text=text,
            trace_id=trace_id or str(uuid.uuid4()),
            metadata={
                "wecom_mode": "aibot",
                "aibot_id": actual_bot_id,
                "create_time": str(payload.get("create_time", "")),
                "message_type": message_type,
                "response_url": str(payload.get("response_url", "")),
                "attachments": attachments,
            },
        )

    @staticmethod
    def webhook_reply(
        message: NormalizedMessage,
        text: str,
        *,
        crypto: WeComCrypto | None = None,
        timestamp: str | None = None,
        nonce: str | None = None,
    ) -> str:
        root = Element("xml")
        SubElement(root, "ToUserName").text = message.sender_user_id
        SubElement(root, "FromUserName").text = str(message.metadata.get("to_user", ""))
        SubElement(root, "CreateTime").text = str(int(time.time()))
        SubElement(root, "MsgType").text = "text"
        SubElement(root, "Content").text = text
        plaintext = tostring(root, encoding="unicode")
        if crypto is None:
            return plaintext
        current_timestamp = timestamp or str(int(time.time()))
        current_nonce = nonce or uuid.uuid4().hex
        encrypted, signature = crypto.encrypt(plaintext, current_timestamp, current_nonce)
        envelope = Element("xml")
        SubElement(envelope, "Encrypt").text = encrypted
        SubElement(envelope, "MsgSignature").text = signature
        SubElement(envelope, "TimeStamp").text = current_timestamp
        SubElement(envelope, "Nonce").text = current_nonce
        return tostring(envelope, encoding="unicode")
