# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Demo agent factory for the 3-tenant SaaS customer-service example.

When ``TRPC_AGENT_API_KEY`` is set, a real :class:`LlmAgent` is built from the
tenant's model configuration; otherwise a deterministic :class:`MockCustomerServiceAgent`
is used so the demo runs without any LLM credentials.
"""

from __future__ import annotations

import os

from trpc_agent_sdk.agents import BaseAgent
from trpc_agent_sdk.agents import LlmAgent
from trpc_agent_sdk.events import Event
from trpc_service import to_agent_name
from trpc_agent_sdk.models import OpenAIModel
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import Part

from tools import build_tools


class MockCustomerServiceAgent(BaseAgent):
    """A deterministic agent that echoes the user message with tenant context.

    Used when no LLM API key is configured so the multi-tenant routing and
    isolation behaviour can be demonstrated offline.
    """

    def __init__(self, name: str, instruction: str = "") -> None:
        super().__init__(name=name)
        self._instruction = instruction

    async def _run_async_impl(self, ctx):
        user_text = "".join(p.text or "" for p in (ctx.user_content.parts if ctx.user_content else []))
        reply = f"[{self.name} 客服] 收到你的问题：「{user_text or '(空)'}」"
        if self._instruction:
            reply += f"\n（角色指令：{self._instruction.strip()[:60]}）"
        yield Event(author=self.name, content=Content(parts=[Part.from_text(text=reply)]), partial=False)


def create_agent(tenant):
    """Build the tenant's agent: LLM-backed if configured, otherwise mock."""
    api_key = os.environ.get("TRPC_AGENT_API_KEY")
    instruction = tenant.app_config.default_instruction or "你是客服助手，请友好专业地回复用户。"
    if api_key:
        model = OpenAIModel(
            model_name=tenant.model.model_name,
            api_key=api_key,
            base_url=tenant.model.api_endpoint,
        )
        return LlmAgent(name=to_agent_name(tenant.tenant_id), model=model, instruction=instruction,
                        tools=build_tools(tenant))
    return MockCustomerServiceAgent(name=to_agent_name(tenant.tenant_id), instruction=instruction)
