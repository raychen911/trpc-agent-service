"""审计入库前脱敏。"""
from __future__ import annotations

from typing import Any

from trpc_service.tenant.governance.pii_mask import mask_payload

SENSITIVE_KEY_HINTS = ("api_key", "apikey", "secret", "token", "password", "authorization")


def redact(payload: Any) -> Any:
    """递归脱敏：先按敏感键名整值遮蔽，再对文本做 PII 打码。"""
    if isinstance(payload, dict):
        cleaned = {}
        for key, value in payload.items():
            if any(hint in str(key).lower() for hint in SENSITIVE_KEY_HINTS):
                cleaned[key] = "***"
            else:
                cleaned[key] = redact(value)
        return cleaned
    if isinstance(payload, list):
        return [redact(v) for v in payload]
    if isinstance(payload, str):
        return mask_payload(payload)
    return payload
