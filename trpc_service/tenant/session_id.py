"""Server-authenticated session identifiers and rollout buckets."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json

from trpc_service.tenant.models import ConversationKind


def _mac(key: bytes, purpose: str, parts: tuple[str, ...]) -> bytes:
    canonical = json.dumps(
        {"v": 1, "purpose": purpose, "parts": parts},
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    return hmac.new(key, canonical, hashlib.sha256).digest()


def make_session_id(
    key: bytes,
    *,
    tenant_id: str,
    binding_id: str,
    kind: ConversationKind,
    external_user_id: str,
    external_conversation_id: str | None = None,
) -> str:
    """Create a non-enumerable tenant- and binding-scoped session id."""

    if kind == ConversationKind.GROUP:
        if not external_conversation_id:
            raise ValueError("group conversations require an external conversation id")
        subject = external_conversation_id
    else:
        subject = external_user_id
    digest = _mac(key, "session", (tenant_id, binding_id, kind.value, subject))
    return "s1_" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def make_principal_id(key: bytes, *, tenant_id: str, binding_id: str, external_user_id: str) -> str:
    """Create an opaque stable identity without leaking the provider user id."""

    digest = _mac(key, "principal", (tenant_id, binding_id, external_user_id))
    return "p1_" + base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")


def rollout_bucket(
    key: bytes,
    *,
    tenant_id: str,
    app_id: str,
    session_id: str,
) -> int:
    """Return a stable bucket in [0, 9999] for retry-safe canary selection."""

    digest = _mac(key, "rollout", (tenant_id, app_id, session_id))
    return int.from_bytes(digest[:8], "big") % 10_000


def select_config_version(
    *,
    active_version: int,
    candidate_version: int | None,
    candidate_percent: float,
    bucket: int,
) -> int:
    if not 0 <= candidate_percent <= 100:
        raise ValueError("candidate percentage must be between 0 and 100")
    if candidate_version is None or candidate_percent == 0:
        return active_version
    return candidate_version if bucket < round(candidate_percent * 100) else active_version


__all__ = [
    "make_principal_id",
    "make_session_id",
    "rollout_bucket",
    "select_config_version",
]
