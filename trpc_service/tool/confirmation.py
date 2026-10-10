"""Short-lived, one-time confirmation tokens bound to exact tool arguments."""

from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import json
import time
from dataclasses import asdict, dataclass
from typing import Protocol
from uuid import uuid4


class ConfirmationError(RuntimeError):
    pass


class ConfirmationLedger(Protocol):
    async def issue(self, token_id: str, expires_at: int, scope: ConfirmationScope) -> None: ...

    async def consume(self, token_id: str, scope: ConfirmationScope) -> bool: ...


class InMemoryConfirmationLedger:
    def __init__(self) -> None:
        self._tokens: dict[str, tuple[int, bool]] = {}

    async def issue(self, token_id: str, expires_at: int, scope: ConfirmationScope) -> None:
        self._tokens[token_id] = (expires_at, False)

    async def consume(self, token_id: str, scope: ConfirmationScope) -> bool:
        current = self._tokens.get(token_id)
        if current is None or current[1] or current[0] < int(time.time()):
            return False
        self._tokens[token_id] = (current[0], True)
        return True


@dataclass(frozen=True, slots=True)
class ConfirmationScope:
    tenant_id: str
    principal_id: str
    session_id: str
    tool_name: str
    arguments_hash: str


class ConfirmationTokenService:
    def __init__(self, key: bytes, ledger: ConfirmationLedger, *, ttl_seconds: int = 300) -> None:
        if len(key) < 32:
            raise ValueError("confirmation signing key must contain at least 32 bytes")
        self._key = key
        self._ledger = ledger
        self._ttl_seconds = ttl_seconds

    async def issue(self, scope: ConfirmationScope) -> str:
        expires = int(time.time()) + self._ttl_seconds
        payload = {
            "v": 1,
            "jti": str(uuid4()),
            "exp": expires,
            **asdict(scope),
        }
        encoded = _encode(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
        signature = _encode(hmac.new(self._key, encoded.encode(), hashlib.sha256).digest())
        await self._ledger.issue(payload["jti"], expires, scope)
        return f"{encoded}.{signature}"

    async def consume(self, token: str, expected: ConfirmationScope) -> None:
        try:
            if not isinstance(token, str) or len(token) > 16_384:
                raise ConfirmationError("confirmation token is invalid")
            encoded, supplied = token.split(".", 1)
            if not encoded or not supplied or "." in supplied:
                raise ConfirmationError("confirmation token is invalid")
            calculated = _encode(hmac.new(self._key, encoded.encode(), hashlib.sha256).digest())
            if not hmac.compare_digest(calculated, supplied):
                raise ConfirmationError("confirmation token is invalid")
            payload = json.loads(_decode(encoded).decode("utf-8"))
            if not isinstance(payload, dict):
                raise ConfirmationError("confirmation token is invalid")
        except (ValueError, TypeError, OverflowError, UnicodeError, binascii.Error) as exc:
            raise ConfirmationError("confirmation token is invalid") from exc
        try:
            expires = payload.get("exp")
            if isinstance(expires, bool) or not isinstance(expires, int):
                raise ConfirmationError("confirmation token is invalid")
            if expires < int(time.time()):
                raise ConfirmationError("confirmation token expired")
            for field, value in asdict(expected).items():
                if payload.get(field) != value:
                    raise ConfirmationError("confirmation token scope mismatch")
            token_id = payload.get("jti")
            if not isinstance(token_id, str) or not token_id:
                raise ConfirmationError("confirmation token is invalid")
            if not await self._ledger.consume(token_id, expected):
                raise ConfirmationError("confirmation token was already used")
        except ConfirmationError:
            raise
        except (TypeError, ValueError, OverflowError, UnicodeError) as exc:
            raise ConfirmationError("confirmation token is invalid") from exc


def arguments_hash(arguments: dict[str, object]) -> str:
    canonical = json.dumps(arguments, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode()).hexdigest()


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _decode(value: str) -> bytes:
    alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
    if not isinstance(value, str) or not value or any(char not in alphabet for char in value):
        raise ValueError("invalid base64")
    return base64.b64decode(
        value.encode("ascii") + b"=" * (-len(value) % 4),
        altchars=b"-_",
        validate=True,
    )


__all__ = [
    "ConfirmationError",
    "ConfirmationScope",
    "ConfirmationTokenService",
    "InMemoryConfirmationLedger",
    "arguments_hash",
]
