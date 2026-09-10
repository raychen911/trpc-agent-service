"""敏感信息脱敏（TOOL 层 + 输出层复用）。"""
from __future__ import annotations

import re
from typing import Any

from trpc_agent_sdk.filter import BaseFilter
from trpc_agent_sdk.filter import register_tool_filter

from trpc_service.tenant.governance.context import resolve_tenant

# 常见敏感字段模式
_PATTERNS = [
    ("phone", re.compile(r"(?<!\d)(1[3-9]\d{9})(?!\d)")),
    ("id_card", re.compile(r"(?<!\d)(\d{17}[\dXx])(?!\d)")),
    ("email", re.compile(r"([A-Za-z0-9._%+-]+)@([A-Za-z0-9.-]+)")),
    ("secret", re.compile(r"(sk-[A-Za-z0-9-]{8,})")),
]


def mask_text(text: str) -> str:
    """对文本中的手机号/身份证/邮箱/密钥做打码，返回原样副本。"""
    masked = text
    for _, pattern in _PATTERNS:
        masked = pattern.sub(lambda m: m.group(1)[:3] + "****" + m.group(1)[-3:], masked)
    return masked


@register_tool_filter("pii_mask")
class PiiMaskFilter(BaseFilter):
    """工具入参脱敏。"""

    async def _before(self, ctx, req, rsp):
        tenant = resolve_tenant(ctx)
        if tenant is None or not tenant.audit.mask_pii:
            return
        if isinstance(req, dict):
            for key, value in list(req.items()):
                if isinstance(value, str):
                    req[key] = mask_text(value)


def mask_payload(payload: Any) -> Any:
    """对任意 JSON 结构做脱敏（输出侧使用）。"""
    if isinstance(payload, str):
        return mask_text(payload)
    if isinstance(payload, dict):
        return {k: mask_payload(v) for k, v in payload.items()}
    if isinstance(payload, list):
        return [mask_payload(v) for v in payload]
    return payload
