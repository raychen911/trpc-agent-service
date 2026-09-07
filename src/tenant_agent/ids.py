"""Opaque, deterministic identity and routing key derivation."""

from __future__ import annotations

import base64
import hashlib
import hmac

from tenant_agent.models import ChatType, InboundEnvelope


class IdentityDeriver:
    """Derive tenant-scoped IDs without exposing external platform identifiers."""

    def __init__(self, key: str | bytes):
        raw = key.encode("utf-8") if isinstance(key, str) else key
        if len(raw) < 16:
            raise ValueError("the identity HMAC key must be at least 16 bytes")
        self._key = raw

    def _derive(self, prefix: str, *parts: str, length: int = 32) -> str:
        framed = b"".join(len(part.encode()).to_bytes(4, "big") + part.encode() for part in parts)
        digest = hmac.new(self._key, framed, hashlib.sha256).digest()
        encoded = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
        return f"{prefix}_{encoded[:length]}"

    def user_id(self, envelope: InboundEnvelope) -> str:
        return self._derive(
            "u",
            envelope.tenant_id,
            envelope.binding_id,
            envelope.external_user_id,
        )

    def session_id(self, envelope: InboundEnvelope, *, group_scope: str = "conversation") -> str:
        common = (
            envelope.tenant_id,
            envelope.app_id,
            envelope.binding_id,
            envelope.channel.value,
        )
        if envelope.chat_type is ChatType.DIRECT:
            scope: tuple[str, ...] = ("direct", envelope.external_user_id)
        elif group_scope == "per_user":
            scope = (
                "group-user",
                envelope.external_chat_id,
                envelope.thread_id or "root",
                envelope.external_user_id,
            )
        else:
            scope = (
                "group",
                envelope.external_chat_id,
                envelope.thread_id or "root",
            )
        return self._derive("s", *common, *scope)

    def dedupe_key(self, envelope: InboundEnvelope) -> str:
        return self._derive(
            "d",
            envelope.tenant_id,
            envelope.channel.value,
            envelope.external_account_id,
            envelope.message_id,
            length=43,
        )

    def tenant_namespace(self, tenant_id: str) -> str:
        return self._derive("t", tenant_id, length=20)

    def content_fingerprint(self, tenant_id: str, value: str) -> str:
        return self._derive("p", tenant_id, value, length=43)


def stable_checksum(*parts: str) -> str:
    framed = b"".join(len(part.encode()).to_bytes(4, "big") + part.encode() for part in parts)
    return hashlib.sha256(framed).hexdigest()
