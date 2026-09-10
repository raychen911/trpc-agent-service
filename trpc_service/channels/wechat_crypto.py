"""微信/企业微信 AES-CBC message decryption."""

from __future__ import annotations

import base64
import hashlib
import hmac
import struct
from xml.etree import ElementTree

from trpc_service.security.secrets import SecretManager


def _decrypt_aes_payload(encrypted: str, aes_key_ref: str) -> bytes:
    try:
        from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
    except ImportError as exc:
        raise RuntimeError("encrypted WeChat callbacks require cryptography") from exc
    key = base64.b64decode(SecretManager().resolve(aes_key_ref) + "=")
    cipher = Cipher(algorithms.AES(key), modes.CBC(key[:16]))
    decrypted = cipher.decryptor().update(base64.b64decode(encrypted))
    pad = decrypted[-1]
    if pad < 1 or pad > 32:
        raise ValueError("invalid WeChat AES padding")
    return decrypted[:-pad]


def decrypt_message(encrypted: str, aes_key_ref: str) -> dict[str, str]:
    decrypted = _decrypt_aes_payload(encrypted, aes_key_ref)
    message_length = struct.unpack("!I", decrypted[16:20])[0]
    xml_body = decrypted[20 : 20 + message_length].decode("utf-8")
    root = ElementTree.fromstring(xml_body)
    return {child.tag: child.text or "" for child in root}


def verify_handshake(payload: dict, token_ref: str, aes_key_ref: str | None = None) -> str:
    """Verify a WeChat/WeCom GET callback and return the echo response."""

    token = SecretManager().resolve(token_ref)
    timestamp = str(payload.get("timestamp", ""))
    nonce = str(payload.get("nonce", ""))
    echo = str(payload.get("echostr", ""))
    msg_signature = str(payload.get("msg_signature", ""))
    signature = str(payload.get("signature", ""))
    if not timestamp or not nonce or not echo:
        raise ValueError("incomplete WeChat callback handshake")

    if msg_signature:
        expected = hashlib.sha1("".join(sorted((token, timestamp, nonce, echo))).encode("utf-8")).hexdigest()
        if not hmac.compare_digest(expected, msg_signature):
            raise ValueError("invalid encrypted WeChat callback signature")
        if aes_key_ref:
            decrypted = _decrypt_aes_payload(echo, aes_key_ref)
            message_length = struct.unpack("!I", decrypted[16:20])[0]
            return decrypted[20 : 20 + message_length].decode("utf-8")
        return echo

    expected = hashlib.sha1("".join(sorted((token, timestamp, nonce))).encode("utf-8")).hexdigest()
    if not hmac.compare_digest(expected, signature):
        raise ValueError("invalid WeChat callback signature")
    return echo
