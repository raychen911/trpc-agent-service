"""Confidential locators for deferred WeCom media downloads.

Inbound media is persisted before a Worker downloads it.  This tiny wrapper
keeps the provider URL and AES key out of that persisted ``MediaReference``:
the opaque locator is carried in ``provider_media_id`` and is authenticated
to the binding and message that created it.
"""

from __future__ import annotations

import base64
import json
import os
import re
from dataclasses import dataclass, field
from typing import Final

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

from trpc_service.channels.envelopes import MediaReference

_VERSION: Final[str] = "v1"
_TOKEN_PREFIX: Final[str] = f"{_VERSION}."
_AAD_PREFIX: Final[bytes] = b"trpc-service:wecom-media-locator:" + _VERSION.encode()
_TOKEN_RE: Final[re.Pattern[str]] = re.compile(r"^v1\.[A-Za-z0-9_-]+$")
_ERROR_MESSAGE: Final[str] = "WeCom media locator is invalid"


class WeComMediaLocatorError(ValueError):
    """Stable, non-sensitive error for invalid or tampered locators."""

    provider_code: Final[str] = "invalid_locator"

    def __init__(self) -> None:
        super().__init__(_ERROR_MESSAGE)


@dataclass(frozen=True, slots=True, repr=False)
class WeComMediaLocatorCipher:
    """Seal and open one-use-in-context, versioned WeCom media locators."""

    _key: bytes = field(repr=False)

    def __init__(self, key: bytes) -> None:
        if not isinstance(key, (bytes, bytearray, memoryview)) or len(key) != 32:
            raise ValueError("WeCom media locator AES-GCM key must be 32 bytes")
        object.__setattr__(self, "_key", bytes(key))

    def __repr__(self) -> str:
        return "WeComMediaLocatorCipher()"

    def seal(
        self,
        reference: MediaReference,
        binding_id: str,
        message_id: str,
    ) -> MediaReference:
        """Return a persisted reference containing only an opaque locator."""

        self._validate_context(binding_id, message_id)
        if not isinstance(reference, MediaReference):
            raise WeComMediaLocatorError()
        url = reference.provider_url
        aes_key = reference.encryption_key_ref
        if not isinstance(url, str) or not url.strip():
            raise WeComMediaLocatorError()
        if not isinstance(aes_key, str) or not aes_key.strip():
            raise WeComMediaLocatorError()

        aad = self._aad(binding_id, message_id)
        payload = json.dumps(
            {"provider_url": url, "encryption_key_ref": aes_key},
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        nonce = os.urandom(12)
        encrypted = AESGCM(self._key).encrypt(nonce, payload, aad)
        token = _TOKEN_PREFIX + _b64url(nonce + encrypted)
        return reference.model_copy(
            update={
                "provider_url": None,
                "encryption_key_ref": None,
                "provider_media_id": token,
            }
        )

    def open(
        self,
        reference: MediaReference,
        binding_id: str,
        message_id: str,
    ) -> MediaReference:
        """Authenticate and recover a transient provider media reference."""

        self._validate_context(binding_id, message_id)
        if not isinstance(reference, MediaReference):
            raise WeComMediaLocatorError()
        token = reference.provider_media_id
        if (
            not isinstance(token, str)
            or not _TOKEN_RE.fullmatch(token)
            or reference.provider_url is not None
            or reference.encryption_key_ref is not None
        ):
            raise WeComMediaLocatorError()

        try:
            encoded = token[len(_TOKEN_PREFIX) :]
            encrypted = _b64url_decode(encoded)
            if len(encrypted) <= 12 + 16:
                raise ValueError
            nonce, ciphertext = encrypted[:12], encrypted[12:]
            payload = AESGCM(self._key).decrypt(
                nonce,
                ciphertext,
                self._aad(binding_id, message_id),
            )
            values = json.loads(payload.decode("utf-8"))
            if (
                not isinstance(values, dict)
                or set(values) != {"provider_url", "encryption_key_ref"}
                or not isinstance(values["provider_url"], str)
                or not values["provider_url"].strip()
                or not isinstance(values["encryption_key_ref"], str)
                or not values["encryption_key_ref"].strip()
            ):
                raise ValueError
        except Exception:
            raise WeComMediaLocatorError() from None

        return reference.model_copy(
            update={
                "provider_url": values["provider_url"],
                "encryption_key_ref": values["encryption_key_ref"],
            }
        )

    @staticmethod
    def _validate_context(binding_id: str, message_id: str) -> None:
        if (
            not isinstance(binding_id, str)
            or not binding_id
            or "\x00" in binding_id
            or not isinstance(message_id, str)
            or not message_id
            or "\x00" in message_id
        ):
            raise WeComMediaLocatorError()

    @staticmethod
    def _aad(binding_id: str, message_id: str) -> bytes:
        binding = binding_id.encode("utf-8")
        message = message_id.encode("utf-8")
        return (
            _AAD_PREFIX
            + len(binding).to_bytes(4, "big")
            + binding
            + len(message).to_bytes(4, "big")
            + message
        )


def _b64url(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    if not value or not re.fullmatch(r"[A-Za-z0-9_-]+", value):
        raise ValueError
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


__all__ = ["WeComMediaLocatorCipher", "WeComMediaLocatorError"]
