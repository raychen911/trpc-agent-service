"""通道适配器基类。"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class InboundMessage:
    """标准化后的入站消息。"""

    channel: str
    tenant_id: str
    external_user_id: str
    chat_id: str = ""  # 群聊 id；单聊为空
    text: str = ""
    msg_id: str = ""
    raw: dict = field(default_factory=dict)


@dataclass
class WebhookRequest:
    """webhook 原始请求片段。"""

    method: str = "POST"
    query: dict = field(default_factory=dict)
    body: str = ""
    headers: dict = field(default_factory=dict)


@dataclass
class WebhookResponse:
    """webhook 响应（文本/XML/JSON 由适配器决定）。"""

    status_code: int = 200
    content_type: str = "text/plain"
    body: str = "success"
    delivered_reply: Optional[str] = None


class ChannelAdapter(ABC):
    """IM 通道适配器接口。"""

    channel_type: str = "base"

    @abstractmethod
    async def handle_webhook(self, tenant_id: str, request: WebhookRequest) -> WebhookResponse:
        """处理一次 webhook 调用（验签、解密、去重、执行与回复）。"""
        raise NotImplementedError
