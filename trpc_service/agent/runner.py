"""AgentRunner：封装 trpc_agent_sdk 的 Runner，提供文本化聊天接口。"""
from __future__ import annotations

import re

from dataclasses import dataclass, field
from typing import AsyncGenerator, List, Optional

from trpc_agent_sdk.agents import BaseAgent
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.memory import BaseMemoryService
from trpc_agent_sdk.runners import Runner
from trpc_agent_sdk.sessions import BaseSessionService
from trpc_agent_sdk.types import Content, Part

THINK_TAG = re.compile(r"<tool_call>(.*?)<tool_call>", re.S)


@dataclass
class RunResult:
    """一次 Agent 运行的结构化结果。"""

    text: str = ""
    reasoning: str = ""  # 思考过程（与正文分离，展示层可折叠）
    tool_calls: list = field(default_factory=list)
    tool_results: list = field(default_factory=list)
    error_type: str = ""
    error_message: str = ""


def split_think_tags(text: str) -> tuple[str, str]:
    """兜底剥离正文里的 <tool_call>...<tool_call> 标签（部分模型内联输出思考时命中）。"""
    match = THINK_TAG.search(text)
    if not match:
        return text, ""
    reasoning = match.group(1).strip()
    body = (text[:match.start()] + text[match.end():]).strip()
    return body, reasoning


def _decode_file_data(data: str) -> bytes:
    """文件数据可能是 base64 字符串或裸文本，尝试 base64 解码。"""
    import base64

    try:
        return base64.b64decode(data, validate=True)
    except Exception:  # noqa: BLE001
        return data.encode("utf-8")


class AgentRunner:
    """面向业务层的 Runner 封装。"""

    def __init__(
        self,
        app_name: str,
        agent: BaseAgent,
        session_service: BaseSessionService,
        memory_service: Optional[BaseMemoryService] = None,
    ):
        self.runner = Runner(
            app_name=app_name,
            agent=agent,
            session_service=session_service,
            memory_service=memory_service,
        )
        self.app_name = app_name

    @staticmethod
    def _build_parts(message: str, files: Optional[List[dict]] = None) -> list:
        parts = [Part(text=message)]
        for file in files or []:
            mime_type = file.get("mime_type", "image/jpeg")
            data = file.get("data", "")
            if data.startswith("http"):
                parts.append(Part.from_uri(file_uri=data, mime_type=mime_type))
            else:
                parts.append(Part.from_bytes(data=_decode_file_data(data), mime_type=mime_type))
        return parts

    async def run_stream(
        self,
        user_id: str,
        session_id: str,
        message: str,
        files: Optional[List[dict]] = None,
        agent_context: Optional[AgentContext] = None,
        result: Optional[RunResult] = None,
    ) -> AsyncGenerator[str, None]:
        """运行 Agent，按事件流增量产出回复文本。"""
        content = Content(parts=self._build_parts(message, files))
        outcome = result if result is not None else RunResult()
        async for event in self.runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=content,
            agent_context=agent_context,
        ):
            if event.get_function_calls():
                for call in event.get_function_calls():
                    outcome.tool_calls.append({"name": call.name, "args": dict(call.args or {})})
            if event.get_function_responses():
                for resp in event.get_function_responses():
                    outcome.tool_results.append({"name": resp.name, "response": resp.response})
            if event.is_error() and not outcome.error_type:
                outcome.error_type = event.error_code or ""
                outcome.error_message = event.error_message or ""
            # 跳过 partial 增量事件，避免与最终事件文本重复
            if event.partial:
                continue
            if event.content and event.content.parts:
                for part in event.content.parts:
                    if not part.text:
                        continue
                    if getattr(part, "thought", False):
                        # 思考内容：收敛进 reasoning，不进正文
                        outcome.reasoning += part.text
                        continue
                    yield part.text

    async def run(
        self,
        user_id: str,
        session_id: str,
        message: str,
        files: Optional[List[dict]] = None,
        agent_context: Optional[AgentContext] = None,
    ) -> RunResult:
        """运行 Agent 并返回完整结构化结果。"""
        outcome = RunResult()
        chunks: list[str] = []
        async for text in self.run_stream(
            user_id, session_id, message, files=files, agent_context=agent_context, result=outcome
        ):
            chunks.append(text)
        outcome.text = "".join(chunks).strip()
        # 兜底：部分模型把思考以 <tool_call>...<tool_call> 内联在正文里
        outcome.text, inline_reasoning = split_think_tags(outcome.text)
        if inline_reasoning:
            outcome.reasoning = inline_reasoning + ("\n" + outcome.reasoning if outcome.reasoning else "")
        outcome.reasoning = outcome.reasoning.strip()
        return outcome

    async def close(self) -> None:
        await self.runner.close()
