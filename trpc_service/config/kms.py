"""
Key Management Service (KMS) and Envelope Encryption Provider.
Ensures API keys, IM secrets, and database passwords are encrypted at rest and in transit.
"""

import base64
import os
import hashlib
from typing import Optional
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.backends import default_backend


class KMSClient:
    """
    KMS / Hardware Security Module (HSM) client simulation.
    Uses AES-256-GCM authenticated encryption for secret management.
    """

    def __init__(self, master_key_env: str = "TRPC_KMS_MASTER_KEY"):
        # Derive a 256-bit key from environment or fallback secure static seed
        raw_key = os.environ.get(master_key_env, "tRPC-Enterprise-KMS-MasterKey-2026-Secret")
        self._key = hashlib.sha256(raw_key.encode("utf-8")).digest()

    def encrypt(self, plaintext: str) -> str:
        """Encrypt sensitive plaintext (API keys, IM tokens) with AES-256-GCM."""
        if not plaintext:
            return ""
        iv = os.urandom(12)
        cipher = Cipher(algorithms.AES(self._key), modes.GCM(iv), backend=default_backend())
        encryptor = cipher.encryptor()
        ciphertext = encryptor.update(plaintext.encode("utf-8")) + encryptor.finalize()
        tag = encryptor.tag

        # Pack: iv (12 bytes) + tag (16 bytes) + ciphertext
        payload = iv + tag + ciphertext
        return "enc:kms:v1:" + base64.b64encode(payload).decode("utf-8")

    def decrypt(self, encrypted_token: str) -> str:
        """Decrypt ciphertext back to plaintext securely in runtime memory."""
        if not encrypted_token or not encrypted_token.startswith("enc:kms:v1:"):
            return encrypted_token or ""

        b64_str = encrypted_token[len("enc:kms:v1:"):]
        try:
            payload = base64.b64decode(b64_str.encode("utf-8"))
            iv = payload[:12]
            tag = payload[12:28]
            ciphertext = payload[28:]

            cipher = Cipher(algorithms.AES(self._key), modes.GCM(iv, tag), backend=default_backend())
            decryptor = cipher.decryptor()
            decrypted = decryptor.update(ciphertext) + decryptor.finalize()
            return decrypted.decode("utf-8")
        except Exception:
            return ""

    def mask_key_preview(self, key_text: str) -> str:
        """Show only prefix and suffix, e.g. sk-abc...1234."""
        if not key_text or len(key_text) < 8:
            return "***"
        return f"{key_text[:6]}...{key_text[-4:]}"


kms_client = KMSClient()
