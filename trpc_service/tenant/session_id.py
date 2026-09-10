"""Stable, tenant-scoped session identifiers."""

from __future__ import annotations

import hashlib
import hmac

from trpc_service.config.models import ChannelType


class SessionIdFactory:
    def __init__(self, secret: str) -> None:
        if len(secret) < 16:
            raise ValueError("session HMAC secret must contain at least 16 characters")
        self._secret = secret.encode("utf-8")

    def create(
        self,
        *,
        tenant_id: str,
        app_id: str,
        channel: ChannelType,
        account_id: str,
        principal_id: str,
        conversation_id: str | None = None,
    ) -> str:
        components = [tenant_id, app_id, channel.value, account_id, principal_id]
        if conversation_id:
            components.append(conversation_id)
        payload = "\x1f".join(components).encode("utf-8")
        digest = hmac.new(self._secret, payload, hashlib.sha256).hexdigest()
        return f"ses_{digest}"


__all__ = ["SessionIdFactory"]
