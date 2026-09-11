"""Enterprise WeChat callback signature, decryption and text decoding."""

from __future__ import annotations

import base64
import hashlib
import hmac
import struct
import xml.etree.ElementTree as etree

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from trpc_service.channels.models import UnboundChannelMessage

_ERROR = "Webhook verification failed."


class WebhookVerificationError(ValueError):
    """A callback is unauthenticated or malformed, without sensitive detail."""


def verify_wecom_signature(*, msg_signature: str, timestamp: str, nonce: str, encrypted: str, token: str) -> None:
    """Verify ``sha1(sort(token, timestamp, nonce, encrypted))``."""
    try:
        values = (token, timestamp, nonce, encrypted, msg_signature)
        if any(not isinstance(value, str) or not value for value in values):
            raise ValueError
        expected = hashlib.sha1("".join(sorted((token, timestamp, nonce, encrypted))).encode()).hexdigest()
        if not hmac.compare_digest(expected, msg_signature):
            raise ValueError
    except (TypeError, ValueError):
        raise WebhookVerificationError(_ERROR) from None


def decrypt_wecom_payload(encrypted: str, *, aes_key: str, receive_id: str) -> bytes:
    """Decrypt an Enterprise WeChat EncodingAESKey payload."""
    try:
        if not isinstance(encrypted, str) or not isinstance(aes_key, str) or not isinstance(receive_id, str):
            raise ValueError
        key = base64.b64decode(aes_key + "=", validate=True)
        if len(key) != 32 or not receive_id:
            raise ValueError
        ciphertext = base64.b64decode(encrypted, validate=True)
        if not ciphertext or len(ciphertext) % 16:
            raise ValueError
        decryptor = Cipher(algorithms.AES(key), modes.CBC(key[:16])).decryptor()
        padded = decryptor.update(ciphertext) + decryptor.finalize()
        padding = padded[-1]
        if not 1 <= padding <= 32 or padded[-padding:] != bytes([padding]) * padding:
            raise ValueError
        raw = padded[:-padding]
        if len(raw) < 20:
            raise ValueError
        length = struct.unpack("!I", raw[16:20])[0]
        content_end = 20 + length
        if content_end > len(raw):
            raise ValueError
        content, actual_receive_id = raw[20:content_end], raw[content_end:]
        if not hmac.compare_digest(actual_receive_id, receive_id.encode()):
            raise ValueError
        return content
    except Exception:
        raise WebhookVerificationError(_ERROR) from None


def decode_wecom_message(xml: bytes, *, external_account_id: str) -> UnboundChannelMessage:
    """Decode one decrypted text XML payload into the shared unbound contract."""
    try:
        root = etree.fromstring(xml)
        values = {child.tag: child.text or "" for child in root}
        if values.get("MsgType") != "text":
            raise ValueError
        user_id, message_id, text = values["FromUserName"], values["MsgId"], values["Content"]
        chat_id = values.get("ChatId", "").strip()
        conversation_id = chat_id or user_id
        return UnboundChannelMessage(
            channel="wecom",
            external_account_id=external_account_id,
            conversation_kind="group" if chat_id else "direct",
            external_user_id=user_id,
            external_conversation_id=conversation_id,
            external_message_id=message_id,
            kind="text",
            text=text,
            occurred_at_ms=int(values["CreateTime"]) * 1000,
        )
    except Exception:
        raise WebhookVerificationError(_ERROR) from None


__all__ = ["WebhookVerificationError", "decode_wecom_message", "decrypt_wecom_payload", "verify_wecom_signature"]
