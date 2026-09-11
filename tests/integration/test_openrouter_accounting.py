"""Real official SDK HTTP conversion, injected local provider, durable cost ledger."""

import asyncio
from decimal import Decimal
import json
from types import SimpleNamespace

import httpx
import pytest
from sqlalchemy import select
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.models import LlmRequest
from trpc_agent_sdk.types import Content, Part

from trpc_service.agent.openrouter import BudgetedOpenRouter, MODEL, MODEL_ID, PRICE_ID
from trpc_service.governance.model_filter import ModelCallFailed
from trpc_service.persistence.models import ModelAttemptRow
from tests.reliability.test_budget import budget as budget_fixture, TENANT

budget = budget_fixture


def test_openrouter_accounting_accepts_a_tenant_selected_model(budget):
    budget.register_price(TENANT, "model_alt", "model_alt", "USD", "1", "2", actor="test")
    model = BudgetedOpenRouter("synthetic-secret", budget, TENANT, "app_test", 32768,
                               model_name="qwen/qwen3-32b",
                               model_id="model_alt",
                               price_id="model_alt",
                               base_url="https://openrouter.ai/api/v1",
                               max_output_tokens=64)
    assert model.accounting.model_name == "qwen/qwen3-32b"
    assert model.accounting.endpoint.endswith("/api/v1")
    assert model.model.name == "qwen/qwen3-32b"


def provider_response(cost="0.00001234"):
    usage = {
        "prompt_tokens": 10,
        "completion_tokens": 5,
        "total_tokens": 15,
        "prompt_tokens_details": {
            "cached_tokens": 7
        }
    }
    if cost is not None:
        usage["cost"] = float(cost)
    return {
        "id": "synthetic-response",
        "object": "chat.completion",
        "created": 1,
        "model": MODEL,
        "choices": [{
            "index": 0,
            "finish_reason": "stop",
            "message": {
                "role": "assistant",
                "content": "已通过完整测试"
            }
        }],
        "usage": usage
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("cost", ["0.00001234", None])
@pytest.mark.parametrize("finish_reason", ["stop", "length"])
@pytest.mark.parametrize("summary_adapter", [False, True])
async def test_official_sdk_uses_provider_total_and_holds_missing_cost(budget, cost, finish_reason, summary_adapter):
    budget.register_price(TENANT, PRICE_ID, MODEL_ID, "USD", "1", "2", actor="test")
    budget.configure(TENANT, "USD", "1", "1", "UTC", 0, actor="test")
    requests = []

    def handle(request):
        requests.append(json.loads(request.content))
        response = provider_response(cost)
        response["choices"][0]["finish_reason"] = finish_reason
        return httpx.Response(200, json=response)

    model = BudgetedOpenRouter("synthetic-secret",
                               budget,
                               TENANT,
                               "app_test",
                               163840,
                               transport=httpx.MockTransport(handle))
    ctx = AgentContext()
    for key, value in {"tenant_id": TENANT, "agent_app_id": "app_test", "execution_id": TENANT + "-execution"}.items():
        ctx.with_metadata(key, value)
    context = SimpleNamespace(agent_context=ctx, agent=SimpleNamespace(), run_config=None)
    request = LlmRequest(model=MODEL,
                         config=model.accounting.generation_config(),
                         contents=[Content(role="user", parts=[Part.from_text(text="测试")])])
    from trpc_service.agent.summary_model import ConfiguredSummaryModel
    runtime_model = ConfiguredSummaryModel(model) if summary_adapter else model.model
    try:
        if cost is None or finish_reason != "stop":
            with pytest.raises(ModelCallFailed):
                _ = [item async for item in runtime_model.generate_async(request, ctx=context)]
        else:
            _ = [item async for item in runtime_model.generate_async(request, ctx=context)]
        assert len(requests) == 1
        with budget.database.sessions() as session:
            row = session.scalar(select(ModelAttemptRow))
            assert row.status == ("settled" if cost else "pending_reconciliation")
            assert row.actual_amount == (Decimal(cost) if cost else None)
            assert row.reserved_amount == Decimal("0.164096")
    finally:
        await model.close()


@pytest.mark.asyncio
async def test_network_hook_without_filter_cannot_send(budget):
    requests = []
    model = BudgetedOpenRouter("synthetic-secret",
                               budget,
                               TENANT,
                               "app_test",
                               163840,
                               transport=httpx.MockTransport(lambda req: requests.append(req)))
    try:
        with pytest.raises(PermissionError):
            await model.client.post("https://openrouter.ai/api/v1/chat/completions", json={})
        assert not requests
    finally:
        await model.close()


@pytest.mark.asyncio
async def test_parallel_accounting_contexts_are_isolated():
    from trpc_service.agent.openrouter import OpenRouterAccounting
    accounting = OpenRouterAccounting(163840)

    async def run(value):
        with accounting.capture() as state:
            state.usage = {"id": value}
            await asyncio.sleep(0)
            assert accounting.current.get().usage == {"id": value}

    await asyncio.gather(run("first"), run("second"))
    assert accounting.current.get() is None


@pytest.mark.parametrize("body", [
    {
        "tools": [{
            "type": "function",
            "function": {
                "name": "execute_unregistered"
            }
        }]
    },
    {
        "tools": [{
            "type": "web_search",
            "function": {
                "name": "read_artifact"
            }
        }]
    },
    {
        "messages": [{
            "role": "user",
            "content": [{
                "type": "image_url",
                "image_url": "synthetic"
            }]
        }]
    },
    {
        "messages":
        [{
            "role": "assistant",
            "content": None,
            "tool_calls": [{
                "type": "function",
                "function": {
                    "name": "execute_unregistered",
                    "arguments": "{}"
                }
            }]
        }]
    },
])
def test_tool_accounting_rejects_unregistered_tools_and_media(body):
    from trpc_service.agent.openrouter import OpenRouterAccounting
    from trpc_service.governance.budget import BudgetDenied
    with pytest.raises(BudgetDenied):
        OpenRouterAccounting(163840, allowed_tools=("read_artifact", )).validate_tools_and_messages(body)


def test_text_contract_still_rejects_tools():
    from trpc_service.agent.openrouter import OpenRouterAccounting
    from trpc_service.governance.budget import BudgetDenied
    with pytest.raises(BudgetDenied):
        OpenRouterAccounting(163840).validate_tools_and_messages({"tools": []})


@pytest.mark.asyncio
async def test_multiple_model_calls_each_settle_and_stop_at_shared_cap(budget):
    from trpc_service.governance.budget import BudgetDenied
    budget.register_price(TENANT, PRICE_ID, MODEL_ID, "USD", "1", "2", actor="test")
    budget.configure(TENANT, "USD", "1", "1", "UTC", 0, actor="test")
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, json=provider_response())

    model = BudgetedOpenRouter("synthetic-secret",
                               budget,
                               TENANT,
                               "app_test",
                               163840,
                               transport=httpx.MockTransport(handle),
                               allowed_tools=("read_artifact", ),
                               max_calls=2)
    ctx = AgentContext()
    for key, value in {"tenant_id": TENANT, "agent_app_id": "app_test", "execution_id": TENANT + "-execution"}.items():
        ctx.with_metadata(key, value)
    context = SimpleNamespace(agent_context=ctx, agent=SimpleNamespace(), run_config=None)
    request = LlmRequest(model=MODEL,
                         config=model.accounting.generation_config(),
                         contents=[Content(role="user", parts=[Part.from_text(text="测试")])])
    try:
        for _ in range(2):
            _ = [item async for item in model.model.generate_async(request, ctx=context)]
        with pytest.raises(BudgetDenied):
            _ = [item async for item in model.model.generate_async(request, ctx=context)]
        assert len(requests) == 2
        with budget.database.sessions() as session:
            attempts = list(session.scalars(select(ModelAttemptRow)))
            assert len(attempts) == 2 and all(row.status == "settled" for row in attempts)
            assert sum(row.actual_amount for row in attempts) == Decimal("0.00002468")
    finally:
        await model.close()
