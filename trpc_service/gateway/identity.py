# Tencent is pleased to support the open source community by making trpc-agent-service available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# trpc-agent-service is licensed under the Apache License Version 2.0.
"""Deterministic namespacing for tenant, user and session identities."""

import hashlib

from trpc_service.gateway.models import NormalizedInboundMessage


def _digest(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def sdk_app_name(tenant_id: str, app_id: str) -> str:
    """Build the SDK app namespace used by Session and Memory services."""
    return f"tenant:{tenant_id}:app:{app_id}"


def internal_user_id(tenant_id: str, channel: str, external_user_id: str, binding_id: str = "") -> str:
    """Prevent raw IM identifiers and cross-tenant collisions in SDK storage."""
    subject = f"{binding_id}:{external_user_id}" if binding_id else external_user_id
    return f"t:{tenant_id}:c:{channel}:u:{_digest(subject)}"


def session_owner_id(tenant_id: str, message: NormalizedInboundMessage, mode: str) -> str:
    if message.is_group and mode == "shared":
        subject = f"{message.binding_id}:{message.external_conversation_id}"
        return f"t:{tenant_id}:c:{message.channel}:g:{_digest(subject)}"
    if message.is_group:
        subject = f"{message.binding_id}:{message.external_conversation_id}:{message.external_user_id}"
        # SDK Memory is keyed by app + user, not just session. Use a distinct
        # owner per group/member to keep private chats and other groups isolated.
        return f"t:{tenant_id}:c:{message.channel}:gu:{_digest(subject)}"
    return internal_user_id(tenant_id, message.channel, message.external_user_id, message.binding_id)


def internal_session_id(tenant_id: str,
                        app_id: str,
                        message: NormalizedInboundMessage,
                        group_session_mode: str = "per_user") -> str:
    """Derive stable session identity for direct and group conversations."""
    if message.is_group and group_session_mode == "shared":
        subject = message.external_conversation_id
    else:
        subject = f"{message.external_conversation_id}:{message.external_user_id}"
    subject = f"v2:{message.channel}:{message.binding_id}:{subject}"
    return f"t:{tenant_id}:a:{app_id}:s:{_digest(subject)}"
