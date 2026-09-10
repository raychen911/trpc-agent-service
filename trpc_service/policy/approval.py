"""Signed, tenant-scoped approval tokens for dangerous tools."""

from __future__ import annotations

import hashlib
import hmac
import json
import os


def approval_id(tenant_id: str, session_id: str, tool_name: str, argument_keys: list[str]) -> str:
    value = json.dumps(
        [tenant_id, session_id, tool_name, sorted(argument_keys)],
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(value.encode("utf-8")).hexdigest()[:32]


def approval_token(approval: str, tenant_id: str) -> str:
    secret = (
        os.getenv("APPROVAL_SIGNING_KEY", "").strip()
        or os.getenv("ADMIN_API_KEY", "").strip()
        or f"local-only:{tenant_id}"
    )
    return hmac.new(
        secret.encode("utf-8"),
        f"{tenant_id}:{approval}".encode(),
        hashlib.sha256,
    ).hexdigest()


def verify_approval_token(token: str, approval: str, tenant_id: str) -> bool:
    if not token:
        return False
    return hmac.compare_digest(token, approval_token(approval, tenant_id))
