# ===================================================================
# channels.web - Web UI IM 通道（本地自测）
# ===================================================================
# 说明: 网页版 IM（PRD 3.3/REQUIREMENTS 六），零门槛本地自测 IM 流程。
#   非正式通道：无验签（signature 放行），回复直接返回给前端。
# 规范: msg_id 由前端生成（幂等仍生效）；平台限制宽松。
# ===================================================================

from __future__ import annotations

import time
import uuid
from typing import Optional

from ..events import AgentEvent, AgentResponse, AgentResponseChunk, MessageType
from ..tenant.models import ImChannelConfig
from .base import IMAdapter, ParsedWebhook, PlatformLimits


class WebImAdapter(IMAdapter):
    """Web UI IM（自测通道）。"""

    channel_type = "web"

    def __init__(self, config: Optional[ImChannelConfig] = None) -> None:
        super().__init__(config)

    def parse_webhook(self, body: bytes, headers: dict[str, str]) -> ParsedWebhook:
        """Web 前端 JSON: {msg_id, user_id, content, session_id?}"""
        import json

        try:
            data = json.loads(body.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(f"web webhook 解析失败: {exc}") from exc
        event = AgentEvent(
            tenant_id=data.get("tenant_id", ""),
            channel_type="web",
            channel_id="web_ui",
            user_id=data.get("user_id", "web_user"),
            session_id=data.get("session_id", ""),
            msg_id=data.get("msg_id", f"web_{int(time.time() * 1000)}_{uuid.uuid4().hex[:8]}"),
            content=data.get("content", ""),
            msg_type=MessageType.TEXT,
        )
        # 签名放行（自测通道无回调签名）
        return ParsedWebhook(event=event, raw_body=body, headers=headers)

    async def send_message(self, tenant_id: str, msg: AgentResponse) -> None:
        """Web 通道：回复直接由 API 响应返回（此处无需投递）。"""
        return None

    async def send_streaming(self, tenant_id: str, chunk: AgentResponseChunk) -> None:
        """Web 通道：流式由前端 SSE 承载，此处无操作。"""
        return None

    def verify_signature(self, body: bytes, signature: str) -> bool:
        return True  # 自测通道不验签

    def platform_limits(self) -> PlatformLimits:
        return PlatformLimits(
            max_message_len=4096,
            rate_limit_per_sec=60.0,
            supports_streaming=True,
            supports_card=True,
        )
