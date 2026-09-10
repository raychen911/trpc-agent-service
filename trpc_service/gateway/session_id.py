"""Stable tenant-scoped session and idempotency key generation."""

from __future__ import annotations

import hashlib

from trpc_service.channels.base import InboundMessage


def _digest(*parts: str) -> str:
    value = "\x1f".join(parts).encode("utf-8")
    return hashlib.sha256(value).hexdigest()


def build_session_id(
    tenant_id: str,
    agent_app_id: str,
    channel: str,
    account_id: str,
    external_user_id: str,
    group_id: str | None = None,
) -> str:
    conversation = group_id or external_user_id
    scope = "group" if group_id else "user"
    return f"sess_{scope}_{_digest(tenant_id, channel, account_id, conversation, agent_app_id)[:32]}"


def build_idempotency_key(tenant_id: str, channel: str, account_id: str, external_message_id: str) -> str:
    return _digest(tenant_id, channel, account_id, external_message_id)


def session_id_for_message(tenant_id: str, agent_app_id: str, message: InboundMessage) -> str:
    return build_session_id(
        tenant_id=tenant_id,
        agent_app_id=agent_app_id,
        channel=message.channel,
        account_id=message.account_id,
        external_user_id=message.effective_user_id,
        group_id=message.group_id,
    )
