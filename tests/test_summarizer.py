# ===================================================================
# LLM 摘要器与 Runtime 异步摘要集成测试
# ===================================================================
# 说明: HTTP 层由 tests/fakes.py 的 FakeLLMModel 替换（同阶段二策略），
#   验证的是摘要器自身的 prompt 组装、错误处理与 Runtime 的
#   「后台任务不阻塞回复 + 失败回落确定性摘要」逻辑。
# ===================================================================

import asyncio

import pytest

from tests.fakes import FakeLLMModel
from trpc_service.agent.summarizer import LlmSummarizer
from trpc_service.events import AgentEvent
from trpc_service.runtime import MockAgentRunner, Runtime
from trpc_service.storage import InMemoryStorage
from trpc_service.tenant import ModelConfig, TenantConfig, TenantRegistry


def _summarizer(fake: FakeLLMModel) -> LlmSummarizer:
    return LlmSummarizer(lambda _cfg: fake, timeout_s=5.0)


TENANT = TenantConfig(tenant_id="t1", name="x", model=ModelConfig(provider="fake", model_name="fake-model"))
MESSAGES = [
    {
        "role": "user",
        "content": "介绍一下你自己"
    },
    {
        "role": "assistant",
        "content": "我是 Teneuris 演示助手"
    },
]


@pytest.mark.asyncio
async def test_summarize_returns_model_text():
    fake = FakeLLMModel(reply_prefix="[S]")
    summary = await _summarizer(fake).summarize(TENANT, MESSAGES)

    assert summary.startswith("[S]")
    # prompt 应包含按角色拼装的对话内容
    sent = fake.calls[0]["user_text"]
    assert "用户: 介绍一下你自己" in sent
    assert "助手: 我是 Teneuris 演示助手" in sent


@pytest.mark.asyncio
async def test_summarize_raises_on_model_error():
    fake = FakeLLMModel(fail_with="boom")
    with pytest.raises(RuntimeError, match="摘要模型调用失败"):
        await _summarizer(fake).summarize(TENANT, MESSAGES)


@pytest.mark.asyncio
async def test_summarize_raises_on_empty_output():
    from trpc_agent_sdk.models import LlmResponse
    from trpc_agent_sdk.types import Content

    class EmptyModel(FakeLLMModel):

        async def _generate_async_impl(self, request, stream=False, ctx=None):
            yield LlmResponse(content=Content(role="model", parts=[]), turn_complete=True)

    with pytest.raises(RuntimeError, match="输出为空"):
        await _summarizer(EmptyModel()).summarize(TENANT, MESSAGES)


def _runtime_with_summary(summarizer, storage):
    registry = TenantRegistry()

    async def load_fn(tid: str):
        return TENANT.model_dump() if tid == TENANT.tenant_id else None

    registry._load_fn = load_fn
    rt = Runtime(registry=registry, storage=storage, runner=MockAgentRunner(), summarizer=summarizer)
    return rt


async def _await_bg(rt: Runtime) -> None:
    """等全部后台摘要任务结束（任务引用挂在 Runtime 上防 GC）。"""
    for task in list(rt._bg_tasks):
        await task


@pytest.mark.asyncio
async def test_runtime_llm_summary_persisted_async():
    """有摘要器: 摘要经后台任务落库，内容为 LLM 输出而非确定性拼接。"""
    storage = InMemoryStorage()
    rt = _runtime_with_summary(_summarizer(FakeLLMModel(reply_prefix="[S]")), storage)

    resp = await rt.handle(AgentEvent(tenant_id="t1", session_id="s1", user_id="u1", content="第一轮消息"))
    assert "第一轮消息" in resp.content  # 回复不依赖摘要，立即返回

    await _await_bg(rt)
    summary = await storage.summary.get_summary("t1", "s1")
    assert summary is not None and summary.startswith("[S]")


@pytest.mark.asyncio
async def test_runtime_llm_summary_falls_back_on_failure():
    """模型失败: 回落确定性摘要（含最新用户消息），不留空。"""
    storage = InMemoryStorage()
    rt = _runtime_with_summary(_summarizer(FakeLLMModel(fail_with="boom")), storage)

    await rt.handle(AgentEvent(tenant_id="t1", session_id="s1", user_id="u1", content="回落验证"))
    await _await_bg(rt)

    summary = await storage.summary.get_summary("t1", "s1")
    assert summary is not None
    assert "会话共" in summary and "回落验证" in summary


@pytest.mark.asyncio
async def test_runtime_no_summarizer_keeps_deterministic():
    """无摘要器（mock 路径）: 维持原确定性摘要行为。"""
    storage = InMemoryStorage()
    rt = _runtime_with_summary(None, storage)

    await rt.handle(AgentEvent(tenant_id="t1", session_id="s1", user_id="u1", content="确定性摘要"))
    await asyncio.sleep(0)  # 无后台任务，直接可断言

    summary = await storage.summary.get_summary("t1", "s1")
    assert summary is not None
    assert "会话共 2 条消息" in summary and "确定性摘要" in summary
