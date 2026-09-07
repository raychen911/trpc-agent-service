# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""WeCom (企业微信) callback crypto primitives.

Implements the AES-256-CBC message decryption and both the legacy SHA1 and the
current HMAC-SHA256 signature verification schemes used by the WeCom bot
callback protocol.
"""

from __future__ import annotations

import base64
import hashlib
import hmac

from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers import algorithms
from cryptography.hazmat.primitives.ciphers import modes


def decode_aes_key(encoding_aes_key: str) -> bytes:
    """Decode a 43-char base64 EncodingAESKey into 32 raw bytes."""
    return base64.b64decode(encoding_aes_key + "=")


def decrypt_message(encrypt: str, encoding_aes_key: str) -> bytes:
    """AES-256-CBC decrypt a WeCom ``Encrypt`` payload and strip PKCS7 padding."""
    key = decode_aes_key(encoding_aes_key)
    cipher = Cipher(algorithms.AES(key), modes.CBC(key[:16]))
    decryptor = cipher.decryptor()
    padded = decryptor.update(base64.b64decode(encrypt)) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    return unpadder.update(padded) + unpadder.finalize()


def parse_decrypted(plain: bytes) -> tuple[str, str]:
    """Split decrypted payload into ``(message, receive_id)``.

    Layout: 16 bytes random + 4 bytes network-order length + message + receive_id.
    """
    if len(plain) < 20:
        raise ValueError("decrypted payload too short")
    msg_len = int.from_bytes(plain[16:20], "big")
    message = plain[20:20 + msg_len].decode("utf-8")
    receive_id = plain[20 + msg_len:].decode("utf-8")
    return message, receive_id


def verify_sha1_signature(token: str, timestamp: str, nonce: str, encrypt: str, signature: str) -> bool:
    """Legacy signature: SHA1 of the sorted concatenation of the four parts."""
    parts = sorted([token, timestamp, nonce, encrypt])
    digest = hashlib.sha1("".join(parts).encode("utf-8")).hexdigest()
    return hmac.compare_digest(digest, signature)


def verify_hmac_signature(encoding_aes_key: str, timestamp: str, nonce: str, encrypt: str, signature: str) -> bool:
    """Current signature: base64(HMAC-SHA256(key=AESKey, msg=timestamp\\nnonce\\nencrypt\\n))."""
    key = decode_aes_key(encoding_aes_key)
    message = f"{timestamp}\n{nonce}\n{encrypt}\n".encode("utf-8")
    digest = base64.b64encode(hmac.new(key, message, hashlib.sha256).digest()).decode("ascii")
    return hmac.compare_digest(digest, signature)
