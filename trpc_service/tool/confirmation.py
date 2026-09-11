"""Short-lived, scoped one-time confirmation tokens."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from dataclasses import asdict, dataclass
from uuid import uuid4


class ConfirmationError(ValueError):
    pass


@dataclass(frozen=True)
class ConfirmationScope:
    tenant_id: str
    user_id: str
    session_id: str
    tool_name: str
    arguments_hash: str


class ConfirmationStore:
    def __init__(self) -> None:
        self._records: dict[str, tuple[int, bool, ConfirmationScope]] = {}

    async def issue(self, token_id: str, expires_at: int, scope: ConfirmationScope) -> None:
        self._records[token_id] = (expires_at, False, scope)

    async def consume(self, token_id: str, scope: ConfirmationScope) -> bool:
        record = self._records.get(token_id)
        if record is None or record[1] or record[0] < int(time.time()) or record[2] != scope:
            return False
        self._records[token_id] = (record[0], True, record[2])
        return True


class ConfirmationService:
    def __init__(self, key: bytes, store: ConfirmationStore | None = None, ttl_seconds: int = 300) -> None:
        if len(key) < 32:
            raise ValueError("confirmation key must contain at least 32 bytes")
        self.key = key
        self.store = store or ConfirmationStore()
        self.ttl_seconds = ttl_seconds

    async def issue(self, scope: ConfirmationScope) -> str:
        payload = {"jti": uuid4().hex, "exp": int(time.time()) + self.ttl_seconds, **asdict(scope)}
        encoded = _encode(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode())
        signature = _encode(hmac.new(self.key, encoded.encode(), hashlib.sha256).digest())
        await self.store.issue(payload["jti"], payload["exp"], scope)
        return encoded + "." + signature

    async def consume(self, token: str, scope: ConfirmationScope) -> None:
        try:
            encoded, signature = token.split(".", 1)
            expected = _encode(hmac.new(self.key, encoded.encode(), hashlib.sha256).digest())
            if not hmac.compare_digest(expected, signature):
                raise ConfirmationError("invalid confirmation token")
            payload = json.loads(_decode(encoded).decode())
            if payload.get("exp", 0) < int(time.time()):
                raise ConfirmationError("confirmation token expired")
            for key, value in asdict(scope).items():
                if payload.get(key) != value:
                    raise ConfirmationError("confirmation token scope mismatch")
            if not await self.store.consume(payload["jti"], scope):
                raise ConfirmationError("confirmation token already used")
        except ConfirmationError:
            raise
        except (ValueError, TypeError, KeyError, UnicodeError, json.JSONDecodeError) as exc:
            raise ConfirmationError("invalid confirmation token") from exc


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def arguments_hash(arguments: dict[str, object]) -> str:
    return hashlib.sha256(json.dumps(arguments, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()
