"""Secret references and authenticated encryption for short-lived credentials."""

from __future__ import annotations

import base64
import binascii
import os
import re
import secrets
from collections.abc import Collection, Mapping
from typing import Protocol

from cryptography.exceptions import InvalidTag
from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from cryptography.hazmat.primitives.hashes import SHA256
from cryptography.hazmat.primitives.kdf.hkdf import HKDF
from pydantic import SecretStr

_ENV_SECRET_REFERENCE = re.compile(r"\Asecret://env/([A-Z][A-Z0-9_]*)\Z")
_ENVELOPE_PREFIX = "v1."
_NONCE_BYTES = 12


class SecretResolutionError(RuntimeError):
    """Raised without secret content when a reference cannot be resolved."""


class InvalidCiphertextError(ValueError):
    """Raised when an encrypted credential is malformed or fails authentication."""


class SecretResolver(Protocol):
    """Narrow adapter implemented by environment, Vault, or cloud secret stores."""

    def resolve(self, reference: str) -> SecretStr:
        """Resolve a trusted reference without returning a plain logging value."""

        ...


class EnvironmentSecretResolver:
    """Resolve an explicit allowlist of environment-backed secret references.

    The allowlist is deliberate: a tenant-authored reference must never become a
    general-purpose process-environment reader. Production deployments can replace
    this adapter with a cloud KMS/Vault implementation behind the same method.
    """

    def __init__(self, allowed_names: Collection[str]) -> None:
        self._allowed_names = frozenset(allowed_names)

    def resolve(self, reference: str) -> SecretStr:
        """Resolve one allowlisted ``secret://env/NAME`` reference."""

        match = _ENV_SECRET_REFERENCE.fullmatch(reference)
        if match is None:
            raise SecretResolutionError("unsupported secret reference")
        name = match.group(1)
        if name not in self._allowed_names:
            raise SecretResolutionError("secret reference is not allowlisted")
        value = os.environ.get(name)
        if value is None or not value:
            raise SecretResolutionError("secret is unavailable")
        return SecretStr(value)


class EnvelopeCipher:
    """Encrypt reply URLs and other short-lived credentials with AES-256-GCM."""

    def __init__(self, root_key: str | bytes) -> None:
        material = root_key.encode() if isinstance(root_key, str) else root_key
        if len(material) < 32:
            raise ValueError("root key must contain at least 32 bytes")
        self._key = HKDF(
            algorithm=SHA256(),
            length=32,
            salt=b"trpc-agent-service/envelope/v1",
            info=b"channel-reply-credential",
        ).derive(material)

    def encrypt(self, plaintext: str, *, context: Mapping[str, str]) -> str:
        """Return a versioned URL-safe envelope bound to canonical context."""

        if not plaintext:
            raise ValueError("plaintext must not be empty")
        nonce = secrets.token_bytes(_NONCE_BYTES)
        ciphertext = AESGCM(self._key).encrypt(nonce, plaintext.encode(), _aad(context))
        encoded = base64.urlsafe_b64encode(nonce + ciphertext).decode().rstrip("=")
        return _ENVELOPE_PREFIX + encoded

    def decrypt(self, envelope: str, *, context: Mapping[str, str]) -> SecretStr:
        """Authenticate and decrypt an envelope without exposing failures' content."""

        if not envelope.startswith(_ENVELOPE_PREFIX):
            raise InvalidCiphertextError("unsupported credential envelope")
        encoded = envelope.removeprefix(_ENVELOPE_PREFIX)
        padding = "=" * (-len(encoded) % 4)
        try:
            packed = base64.b64decode(encoded + padding, altchars=b"-_", validate=True)
        except (ValueError, binascii.Error) as exc:
            raise InvalidCiphertextError("malformed credential envelope") from exc
        if len(packed) <= _NONCE_BYTES + 16:
            raise InvalidCiphertextError("malformed credential envelope")
        nonce, ciphertext = packed[:_NONCE_BYTES], packed[_NONCE_BYTES:]
        try:
            plaintext = AESGCM(self._key).decrypt(nonce, ciphertext, _aad(context))
        except InvalidTag as exc:
            raise InvalidCiphertextError("credential authentication failed") from exc
        try:
            return SecretStr(plaintext.decode())
        except UnicodeDecodeError as exc:
            raise InvalidCiphertextError("credential encoding is invalid") from exc


def _aad(context: Mapping[str, str]) -> bytes:
    if not context or any(not key or not value for key, value in context.items()):
        raise ValueError("encryption context must contain non-empty keys and values")
    return "\x1f".join(f"{key}={context[key]}" for key in sorted(context)).encode()
