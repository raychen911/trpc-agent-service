"""企业微信回调加解密官方协议实现。"""
from __future__ import annotations

import base64
import hashlib
import os
import struct

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes


class WeComCryptoError(Exception):
    """加解密或密钥格式错误。"""


def _decode_key(encoding_aes_key: str) -> bytes:
    try:
        key = base64.b64decode(encoding_aes_key + "=")
    except Exception as exc:
        raise WeComCryptoError(f"invalid EncodingAESKey: {exc}") from exc
    if len(key) != 32:
        raise WeComCryptoError("EncodingAESKey must decode to 32 bytes")
    return key


def sha1_signature(token: str, timestamp: str, nonce: str, encrypt: str) -> str:
    """官方签名算法：sha1(sort(token, timestamp, nonce, encrypt))。"""
    items = sorted([token or "", timestamp or "", nonce or "", encrypt or ""])
    return hashlib.sha1("".join(items).encode()).hexdigest()


def verify_signature(token: str, timestamp: str, nonce: str, encrypt: str, signature: str) -> bool:
    return sha1_signature(token, timestamp, nonce, encrypt) == (signature or "")


def _pkcs7_pad(data: bytes) -> bytes:
    pad = 32 - len(data) % 32
    return data + bytes([pad]) * pad


def _pkcs7_unpad(data: bytes) -> bytes:
    pad = data[-1]
    if pad < 1 or pad > 32:
        raise WeComCryptoError("invalid padding")
    return data[:-pad]


def encrypt_message(encoding_aes_key: str, plain: str, receiveid: str) -> str:
    """明文 → AES-256-CBC → base64。"""
    key = _decode_key(encoding_aes_key)
    iv = key[:16]
    raw = (
        os.urandom(16)
        + struct.pack("!I", len(plain.encode("utf-8")))
        + plain.encode("utf-8")
        + receiveid.encode("utf-8")
    )
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    encryptor = cipher.encryptor()
    encrypted = encryptor.update(_pkcs7_pad(raw)) + encryptor.finalize()
    return base64.b64encode(encrypted).decode()


def decrypt_message(encoding_aes_key: str, encrypt_b64: str) -> tuple[str, str]:
    """base64 → AES-256-CBC → (plain_xml, receiveid)。"""
    key = _decode_key(encoding_aes_key)
    iv = key[:16]
    try:
        cipher_text = base64.b64decode(encrypt_b64)
    except Exception as exc:
        raise WeComCryptoError(f"invalid base64: {exc}") from exc
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv))
    decryptor = cipher.decryptor()
    plain = decryptor.update(cipher_text) + decryptor.finalize()
    data = _pkcs7_unpad(plain)
    if len(data) < 20:
        raise WeComCryptoError("decrypted content too short")
    msg_len = struct.unpack("!I", data[16:20])[0]
    plain_xml = data[20:20 + msg_len].decode("utf-8", errors="replace")
    receiveid = data[20 + msg_len:].decode("utf-8", errors="replace")
    return plain_xml, receiveid
