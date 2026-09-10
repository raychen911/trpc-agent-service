"""Versioned HMAC-SHA256 authentication for exact HTTP request bytes."""

from __future__ import annotations

import hashlib
import hmac
import re
from datetime import datetime, timezone

from trpc_service.channels.contracts import Channel, VerifiedBindingScope
from trpc_service.storage.contracts import Unauthorized


_SIGNATURE = re.compile(r"^v1=([0-9a-f]{64})$")


def _validate_public_fields(timestamp: str, signature: str, now: datetime) -> re.Match[str]:
    try:
        request_second = int(timestamp)
    except (TypeError, ValueError):
        raise Unauthorized("Request authentication failed.") from None
    if abs(int(now.timestamp()) - request_second) > 300:
        raise Unauthorized("Request authentication failed.")
    match = _SIGNATURE.fullmatch(signature or "")
    if match is None:
        raise Unauthorized("Request authentication failed.")
    return match


def body_sha256(raw_body: bytes) -> str:
    return hashlib.sha256(raw_body).hexdigest()


def canonical_string(timestamp: str, binding_id: str, external_message_id: str, digest: str) -> str:
    return "\n".join(("v1", timestamp, binding_id, external_message_id, digest))


def sign_request(secret: bytes, canonical: str) -> str:
    return "v1=" + hmac.new(secret, canonical.encode("utf-8"), hashlib.sha256).hexdigest()


def verify_signature(
    *,
    binding_id: str,
    timestamp: str,
    signature: str,
    external_message_id: str,
    raw_body: bytes,
    secret: bytes,
    now: datetime | None = None,
) -> VerifiedBindingScope:
    current = now or datetime.now(timezone.utc)
    match = _validate_public_fields(timestamp, signature, current)
    canonical = canonical_string(timestamp, binding_id, external_message_id, body_sha256(raw_body))
    expected = hmac.new(secret, canonical.encode("utf-8"), hashlib.sha256).digest()
    supplied = bytes.fromhex(match.group(1))
    if not hmac.compare_digest(expected, supplied):
        raise Unauthorized("Request authentication failed.")
    return VerifiedBindingScope._issue(binding_id=binding_id, channel=Channel.LOCAL_HTTP)


async def verify_request(
    *,
    binding_id: str,
    timestamp: str,
    signature: str,
    external_message_id: str,
    raw_body: bytes,
    registry: object,
    resolver: object,
    now: datetime | None = None,
) -> VerifiedBindingScope:
    _validate_public_fields(timestamp, signature, now or datetime.now(timezone.utc))
    try:
        material = await registry.get_auth_material(binding_id, Channel.LOCAL_HTTP)
        secret = resolver.resolve(material.secret_ref)
        value = secret.reveal()
    except Exception:
        raise Unauthorized("Request authentication failed.") from None
    return verify_signature(
        binding_id=binding_id,
        timestamp=timestamp,
        signature=signature,
        external_message_id=external_message_id,
        raw_body=raw_body,
        secret=value,
        now=now,
    )
