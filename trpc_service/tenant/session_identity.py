"""Deterministic, tenant- and agent-scoped session identifiers."""

from __future__ import annotations

from hashlib import sha256

from pydantic import BaseModel, ConfigDict, Field

from trpc_service.channels.contracts import Channel, ConversationType
from trpc_service.tenant.models import VerifiedTenantContext


class SessionIdentity(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    tenant_id: str
    agent_id: str
    binding_id: str
    channel: Channel
    platform_session_id: str = Field(pattern=r"^sess_[0-9a-f]{64}$")
    sdk_app_name: str
    sdk_user_id: str
    external_user_digest: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    conversation_type: ConversationType


def _length_prefixed_digest(*parts: str) -> str:
    digest = sha256()
    for part in parts:
        encoded = part.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def derive_session_identity(
    context: VerifiedTenantContext,
    conversation_type: ConversationType | str,
    external_conversation_id: str,
    group_sender_id: str | None = None,
) -> SessionIdentity:
    kind = ConversationType(conversation_type)
    if kind == ConversationType.GROUP:
        sender_scope = group_sender_id or context.external_user_id
        scope_parts = (
            context.tenant_id, context.agent_id, context.binding_id,
            context.channel.value, kind.value, external_conversation_id, sender_scope,
        )
    else:
        scope_parts = (
            context.tenant_id, context.agent_id, context.binding_id,
            context.channel.value, kind.value, context.external_user_id,
            external_conversation_id,
        )
    scope_digest = _length_prefixed_digest(*scope_parts)
    user_digest = _length_prefixed_digest(context.tenant_id, context.external_user_id)
    return SessionIdentity(
        tenant_id=context.tenant_id,
        agent_id=context.agent_id,
        binding_id=context.binding_id,
        channel=context.channel,
        platform_session_id=f"sess_{scope_digest}",
        sdk_app_name=f"app_{_length_prefixed_digest(context.tenant_id, context.agent_id)[:32]}",
        sdk_user_id=f"user_{user_digest[:32]}",
        external_user_digest=f"sha256:{user_digest}",
        conversation_type=kind,
    )


def assert_session_ownership(
    context: VerifiedTenantContext,
    identity: SessionIdentity,
) -> None:
    expected_app = f"app_{_length_prefixed_digest(context.tenant_id, context.agent_id)[:32]}"
    expected_user_digest = _length_prefixed_digest(context.tenant_id, context.external_user_id)
    expected = (
        context.tenant_id, context.agent_id, context.binding_id,
        context.channel,
        expected_app, f"user_{expected_user_digest[:32]}", f"sha256:{expected_user_digest}",
    )
    actual = (
        identity.tenant_id, identity.agent_id, identity.binding_id,
        identity.channel,
        identity.sdk_app_name, identity.sdk_user_id, identity.external_user_digest,
    )
    if expected != actual:
        raise ValueError("session ownership mismatch")
