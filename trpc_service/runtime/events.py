# ===================================================================
# runtime.events - 归一化 Runner 事件（Runtime 内部流转）
# ===================================================================
# 说明: 框架 Runner 产出的是 Event 流（user_message / model_output /
#   tool_call / tool_result / assistant_message），Runtime 需要把它
#   翻译成 IM 侧消息（PRD 0.3-6 / 3.2）。此处定义平台侧归一化事件，
#   由 Runner 适配层把框架事件转换为该模型，隔离框架细节。
# 规范: type 取值: content / tool_call / tool_result / done / error。
# ===================================================================

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class RunnerEvent:
    """归一化后的 Runner 事件（Runtime 消费）。"""

    type: str = "content"
    """content / tool_call / tool_result / done / error。"""
    content: str = ""
    """文本增量或工具结果内容。"""
    tool_name: str = ""
    tool_input: Optional[dict[str, Any]] = None
    tool_output: Optional[str] = None
    is_final: bool = False
    """是否为最终回复（assistant_message 的收尾事件）。"""
    error: Optional[str] = None
    input_tokens: int = 0
    """本事件对应的模型输入 token 数（usage 透传，PRD 4.2；无 usage 为 0）。"""
    output_tokens: int = 0
    """本事件对应的模型输出 token 数（usage 透传，PRD 4.2；无 usage 为 0）。"""
    extra: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def text(cls, text: str, *, is_final: bool = False, input_tokens: int = 0, output_tokens: int = 0) -> "RunnerEvent":
        return cls(type="content",
                   content=text,
                   is_final=is_final,
                   input_tokens=input_tokens,
                   output_tokens=output_tokens)

    @classmethod
    def tool_call(cls, name: str, tool_input: dict[str, Any]) -> "RunnerEvent":
        return cls(type="tool_call", tool_name=name, tool_input=tool_input)

    @classmethod
    def tool_result(cls, name: str, output: str) -> "RunnerEvent":
        return cls(type="tool_result", tool_name=name, tool_output=output)

    @classmethod
    def done(cls) -> "RunnerEvent":
        return cls(type="done", is_final=True)

    @classmethod
    def failure(cls, message: str) -> "RunnerEvent":
        return cls(type="error", error=message, is_final=True)
