"""Safe identity projection from external channel IDs to internal IDs."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Literal
from uuid import UUID

from trpc_service.channels.models import validate_channel

_FIXED_IDENTITY_ERROR: str = "Invalid identity input."


@dataclass(frozen=True, slots=True)
class ChannelIdentity:
    user_id: str
    session_id: str


def _hash_dimension(channel: str, external_id: str, binding_id: UUID | None) -> str:
    canonical = json.dumps(
        [channel, str(binding_id) if binding_id is not None else None, external_id],
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()[:48]


def project_identity(
    channel: str,
    external_user_id: str,
    external_conversation_id: str,
    *,
    binding_id: UUID | None = None,
    conversation_kind: Literal["direct", "group"] | None = None,
) -> ChannelIdentity:
    if not isinstance(channel, str) or not channel.strip():
        raise ValueError(_FIXED_IDENTITY_ERROR)
    if not isinstance(external_user_id, str) or not external_user_id.strip():
        raise ValueError(_FIXED_IDENTITY_ERROR)
    if not isinstance(external_conversation_id, str) or not external_conversation_id.strip():
        raise ValueError(_FIXED_IDENTITY_ERROR)
    if binding_id is not None and not isinstance(binding_id, UUID):
        raise ValueError(_FIXED_IDENTITY_ERROR)
    if conversation_kind is not None and conversation_kind not in {"direct", "group"}:
        raise ValueError(_FIXED_IDENTITY_ERROR)

    ch = validate_channel(channel)
    user_hash = _hash_dimension(ch, external_user_id.strip(), binding_id)
    session_external_id = external_user_id if conversation_kind == "direct" else external_conversation_id
    session_hash = _hash_dimension(ch, session_external_id.strip(), binding_id)
    return ChannelIdentity(
        user_id=f"usr_v1_{user_hash}",
        session_id=f"ses_v1_{session_hash}",
    )


__all__ = ["ChannelIdentity", "project_identity"]
