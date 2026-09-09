# ===================================================================
# events - 平台内部事件模型（AgentEvent / AgentResponse）
# ===================================================================
# 说明: 外部 IM 消息经 Channel Adapter 解析为 AgentEvent（PRD 3.2），
#   经 Filter 链治理后由 Runtime 消费；回复由 AgentResponse 承载。
# 规范: 字段对齐审计需求（tenant/channel/user/session/trace），
#   content 直接作为 Runner.run_async 的 new_message（PRD 3.2）。
# ===================================================================

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class MessageType(str, Enum):
    """IM 消息类型。"""

    TEXT = "text"
    IMAGE = "image"
    FILE = "file"
    CARD = "card"
    MARKDOWN = "markdown"


class ResponseType(str, Enum):
    """Agent 回复类型（PRD 3.2 事件流 -> IM 回复）。"""

    TEXT = "text"
    STREAM = "stream"
    CARD = "card"
    MARKDOWN = "markdown"
    ERROR = "error"


@dataclass
class AgentEvent:
    """一次从 IM 进入平台的消息（Gateway -> Filter -> Runtime 流转载体）。"""

    tenant_id: str = ""
    channel_type: str = "web"
    channel_id: str = ""
    """IM 平台侧账号标识（bot_id / corp_id）。"""
    user_id: str = ""
    """内部用户标识（经 user_id_mapping 映射，PRD 3.4）。"""
    session_id: str = ""
    """由 generate_session_id 生成（PRD 1.3-3）。"""
    msg_id: str = ""
    """IM 平台消息 ID，用于幂等去重（PRD 2.3-E）。"""
    content: str = ""
    """消息文本，直接作为 Runner 输入。"""
    msg_type: MessageType = MessageType.TEXT
    is_group: bool = False
    trace_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    """trace_id 在此注入，贯穿后续全部调用（PRD 0.3/4.3）。"""
    metadata: dict[str, Any] = field(default_factory=dict)
    """扩展字段（图片 URL、文件、@列表等）。"""

    def with_tenant(self, tenant_id: str) -> "AgentEvent":
        self.tenant_id = tenant_id
        return self

    def with_session(self, session_id: str) -> "AgentEvent":
        self.session_id = session_id
        return self


@dataclass
class AgentResponse:
    """Agent 对一次消息的回复（非流式）。"""

    response_type: ResponseType = ResponseType.TEXT
    content: str = ""
    """文本 / markdown / 卡片 JSON 字符串。"""
    session_id: str = ""
    tenant_id: str = ""
    channel_type: str = ""
    trace_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    """卡片标题、图片 URL 等平台特有字段。"""

    @classmethod
    def text(cls, content: str, **kwargs: Any) -> "AgentResponse":
        return cls(response_type=ResponseType.TEXT, content=content, **kwargs)


@dataclass
class AgentResponseChunk:
    """流式回复分片（PRD 3.6 打字机效果）。"""

    delta: str = ""
    done: bool = False
    session_id: str = ""
    tenant_id: str = ""
    channel_type: str = ""
    trace_id: str = ""
    error: Optional[str] = None
