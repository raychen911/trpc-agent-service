"""Offline engine used for repeatable smoke tests and degraded demo mode."""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

from tenant_agent.models import AgentEvent, AgentEventType, RoutedEnvelope, TenantConfig
from tenant_agent.storage.base import TenantDataPlane


class DeterministicEngine:
    @staticmethod
    def _response(agent_name: str, text: str) -> str:
        """Return a useful offline response without parroting user input."""

        normalized = text.casefold().strip()
        if "你叫什么" in normalized or "what is your name" in normalized:
            return f"我是 {agent_name}。"
        if normalized in {"hi", "hello", "hey", "你好", "您好", "hi 你好"}:
            return f"你好! 我是 {agent_name}, 很高兴和你聊天。"
        if any("\u4e00" <= character <= "\u9fff" for character in normalized):
            return f"你好! 我是 {agent_name} 的离线演示助手。我已经收到你的消息。"
        return (
            f"Hello! I am {agent_name}, running in offline demo mode. "
            "Configure a model profile for generated answers."
        )

    async def stream(
        self,
        *,
        tenant: TenantConfig,
        routed: RoutedEnvelope,
        effective_text: str,
        plane: TenantDataPlane,
    ) -> AsyncIterator[AgentEvent]:
        del plane
        app = tenant.apps[routed.inbound.app_id]
        response = self._response(app.agent_name, effective_text)
        yield AgentEvent(event_id=uuid.uuid4().hex, event_type=AgentEventType.START)
        words = response.split(" ")
        for index, word in enumerate(words):
            await asyncio.sleep(0)
            suffix = " " if index < len(words) - 1 else ""
            yield AgentEvent(
                event_id=uuid.uuid4().hex,
                event_type=AgentEventType.TEXT_DELTA,
                text=word + suffix,
                partial=True,
            )
        input_tokens = max(1, len(effective_text) // 4)
        output_tokens = max(1, len(response) // 4)
        yield AgentEvent(
            event_id=uuid.uuid4().hex,
            event_type=AgentEventType.TEXT_FINAL,
            text=response,
            token_input=input_tokens,
            token_output=output_tokens,
        )

    async def close(self) -> None:
        return None
