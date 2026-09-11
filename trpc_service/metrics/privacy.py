"""Bounded, opaque labels for telemetry."""

from __future__ import annotations

import hashlib
import json

from ..log import redact


def tenant_label(tenant_id: str) -> str:
    return hashlib.blake2b(tenant_id.encode(), digest_size=10, person=b"trpc-label").hexdigest()


def sanitize_attributes(values: dict[str, object]) -> dict[str, object]:
    sanitized = redact(values)
    return {
        key: json.dumps(value, ensure_ascii=True) if isinstance(value, dict) else value
        for key, value in sanitized.items()
    }
