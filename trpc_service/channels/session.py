"""Versioned HMAC identities for channel principals, conversations, and sessions."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass

from trpc_service.channels.contracts import Channel, ConversationKind


@dataclass(frozen=True, slots=True)
class DerivedChannelIdentity:
    """Log-safe identity values derived from external channel identifiers."""

    principal_id: str
    conversation_id: str
    session_id: str
    thread_id: str | None


class ChannelIdentityDeriver:
    """Derive unlinkable identifiers with explicit domain separation."""

    def __init__(self, secret: str | bytes, *, key_version: int = 1) -> None:
        key = secret.encode("utf-8") if isinstance(secret, str) else secret
        if len(key) < 32:
            raise ValueError("channel identity key must be at least 32 bytes")
        if key_version < 1:
            raise ValueError("key_version must be positive")
        self._key = key
        self._key_version = key_version

    def derive(
        self,
        *,
        tenant_id: str,
        app_id: str,
        app_revision: int,
        binding_id: str,
        channel: Channel,
        conversation_kind: ConversationKind,
        external_user_id: str,
        external_conversation_id: str,
        external_thread_id: str | None = None,
    ) -> DerivedChannelIdentity:
        """Derive identities while keeping group sessions shared by default."""

        required = (
            tenant_id,
            app_id,
            binding_id,
            external_user_id,
            external_conversation_id,
        )
        if any(not value or not value.strip() for value in required):
            raise ValueError("channel identity inputs must not be empty")
        if external_thread_id is not None and not external_thread_id.strip():
            raise ValueError("external_thread_id must not be blank")

        if app_revision < 1:
            raise ValueError("app_revision must be positive")
        # Agent revisions intentionally occupy different Session namespaces. This
        # prevents a rollout from mixing prompts/tool schemas with the old active
        # context, while a rollback deterministically recovers the former namespace.
        common = (tenant_id, app_id, str(app_revision), binding_id, channel.value)
        principal_id = self._opaque("principal", (*common, external_user_id), "usr")
        conversation_id = self._opaque(
            "conversation",
            (*common, conversation_kind.value, external_conversation_id),
            "conv",
        )
        thread_id = (
            self._opaque(
                "thread",
                (*common, external_conversation_id, external_thread_id),
                "thr",
            )
            if external_thread_id is not None
            else None
        )
        principal_component = principal_id if conversation_kind is ConversationKind.PRIVATE else ""
        session_id = self._opaque(
            "session",
            (
                *common,
                conversation_kind.value,
                conversation_id,
                thread_id or "",
                principal_component,
            ),
            "sess",
        )
        return DerivedChannelIdentity(
            principal_id=principal_id,
            conversation_id=conversation_id,
            session_id=session_id,
            thread_id=thread_id,
        )

    def derive_message_id(
        self,
        *,
        tenant_id: str,
        app_id: str,
        app_revision: int,
        binding_id: str,
        channel: Channel,
        external_message_id: str,
    ) -> str:
        """Derive a log-safe reference for a channel-native message identifier."""

        values = (tenant_id, app_id, binding_id, external_message_id)
        if any(not value or not value.strip() for value in values):
            raise ValueError("message identity inputs must not be empty")
        if app_revision < 1:
            raise ValueError("app_revision must be positive")
        return self._opaque(
            "message",
            (
                tenant_id,
                app_id,
                str(app_revision),
                binding_id,
                channel.value,
                external_message_id,
            ),
            "msg",
        )

    def _opaque(self, domain: str, values: tuple[str, ...], prefix: str) -> str:
        canonical = json.dumps(
            [self._key_version, domain, *values],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        digest = hmac.new(self._key, canonical, hashlib.sha256).digest()
        encoded = base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")
        return f"{prefix}_v{self._key_version}_{encoded}"
