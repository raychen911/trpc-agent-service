"""Tenant-scoped opaque Redis keys."""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256

from trpc_service.channels.contracts import Channel
from trpc_service.storage.models import IdempotencyKey


def _digest(*parts: str) -> str:
    value = sha256()
    for part in parts:
        encoded = part.encode("utf-8")
        value.update(len(encoded).to_bytes(8, "big"))
        value.update(encoded)
    return value.hexdigest()


@dataclass(frozen=True, slots=True)
class RedisKeyCodec:
    namespace: str = "trpc:v1"

    def idempotency(self, key: IdempotencyKey) -> str:
        # Phase 003 callers and contract doubles predate the channel field.
        # Preserve their LOCAL_HTTP identity while all new channel adapters
        # receive an explicitly channel-scoped key.
        channel = getattr(key, "channel", Channel.LOCAL_HTTP)
        channel_value = channel.value if isinstance(channel, Channel) else str(channel)
        return (
            f"{self.namespace}:msg:"
            f"{_digest(key.tenant_id, channel_value, key.binding_id, key.external_message_id)}"
        )

    def message_lease(self, key: IdempotencyKey) -> str:
        return f"{self.idempotency(key)}:lease"

    def session(self, tenant_id: str, agent_id: str, platform_session_id: str) -> str:
        return f"{self.namespace}:session:{_digest(tenant_id, agent_id, platform_session_id)}"

    def session_lease(
        self, tenant_id: str, agent_id: str, platform_session_id: str
    ) -> str:
        return f"{self.session(tenant_id, agent_id, platform_session_id)}:lease"

    def session_events(
        self, tenant_id: str, agent_id: str, platform_session_id: str
    ) -> str:
        return f"{self.session(tenant_id, agent_id, platform_session_id)}:events"

    def adapter_ownership(self, identity_digest: str) -> str:
        return f"{self.namespace}:adapter:{_digest(identity_digest)}"
