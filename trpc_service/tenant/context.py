"""Trusted tenant execution context.

External payloads never construct this object directly. A verified ChannelBinding and
published TenantSpec are the only accepted sources.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from enum import StrEnum


class ConversationScope(StrEnum):
    """Conversation isolation scope."""

    PRIVATE = "private"
    GROUP = "group"
    GROUP_MEMBER = "group_member"


@dataclass(frozen=True, slots=True)
class TenantContext:
    """Immutable-by-construction identity and configuration snapshot for one turn."""

    tenant_id: str
    app_id: str
    app_revision: int
    binding_id: str
    binding_revision: int
    principal_id: str
    session_id: str
    scope: ConversationScope
    request_id: str
    trace_id: str

    def __post_init__(self) -> None:
        for field_name in (
            "tenant_id",
            "app_id",
            "binding_id",
            "principal_id",
            "session_id",
            "request_id",
            "trace_id",
        ):
            if not getattr(self, field_name):
                raise ValueError(f"{field_name} must not be empty")
        if self.app_revision < 1 or self.binding_revision < 1:
            raise ValueError("configuration revisions must be positive")


class SessionKeyDeriver:
    """Derive opaque, tenant-bound identifiers with a versioned HMAC key."""

    def __init__(self, secret: str | bytes, *, key_version: int = 1) -> None:
        self._key = secret.encode("utf-8") if isinstance(secret, str) else secret
        if len(self._key) < 32:
            raise ValueError("session derivation key must be at least 32 bytes")
        if key_version < 1:
            raise ValueError("key_version must be positive")
        self._key_version = key_version

    def session_id(
        self,
        *,
        tenant_id: str,
        app_id: str,
        binding_id: str,
        scope: ConversationScope,
        conversation_id: str,
        principal_id: str,
        thread_id: str | None = None,
    ) -> str:
        """Derive one stable session ID without exposing external identifiers."""

        if not all((tenant_id, app_id, binding_id, conversation_id, principal_id)):
            raise ValueError("session derivation inputs must not be empty")
        principal_component = (
            principal_id
            if scope in {ConversationScope.PRIVATE, ConversationScope.GROUP_MEMBER}
            else ""
        )
        canonical = json.dumps(
            [
                self._key_version,
                tenant_id,
                app_id,
                binding_id,
                scope.value,
                conversation_id,
                thread_id or "",
                principal_component,
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        digest = hmac.new(self._key, canonical, hashlib.sha256).digest()
        encoded = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        return f"s{self._key_version}_{encoded}"

    def pseudonym(self, *, tenant_id: str, channel: str, external_user_id: str) -> str:
        """Derive a log-safe user pseudonym within a tenant/channel domain."""

        canonical = "\x1f".join(
            (str(self._key_version), tenant_id, channel, external_user_id)
        ).encode()
        digest = hmac.new(self._key, canonical, hashlib.sha256).digest()
        encoded = base64.urlsafe_b64encode(digest[:18]).rstrip(b"=").decode("ascii")
        return f"u{self._key_version}_{encoded}"
