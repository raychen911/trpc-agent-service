"""Public LLMModel generation invokes the Filter around exactly one provider attempt."""

from types import SimpleNamespace
from decimal import Decimal
import asyncio
import threading

import pytest
from sqlalchemy import select
from trpc_agent_sdk.context import AgentContext
from trpc_agent_sdk.configs import ModelRetryConfig
from trpc_agent_sdk.models import LLMModel, LlmRequest, LlmResponse
from trpc_agent_sdk.types import Content, Part, GenerateContentResponseUsageMetadata

from trpc_service.governance.model_filter import (ModelBudgetFilter, RequestEstimate, attach_budget_filter,
                                                  ModelCallFailed)
from trpc_service.persistence.models import ModelAttemptRow
from tests.reliability.test_budget import budget as budget_fixture, TENANT, accounts

budget = budget_fixture


class ContractModel(LLMModel):

    def __init__(self, mode="normal", retries=0):
        super().__init__(model_name="contract-model", model_retry_config=ModelRetryConfig(num_retries=retries))
        self.mode, self.calls = mode, 0

    @classmethod
    def supported_models(cls):
        return ["contract-model"]

    async def _generate_async_impl(self, request, stream=False, ctx=None):
        self.calls += 1
        if self.mode == "exception":
            raise RuntimeError("synthetic-secret-in-provider-error")
        yield LlmResponse(content=Content(role="model", parts=[Part.from_text(text="hello")]), partial=True)
        if self.mode == "cancel":
            raise asyncio.CancelledError()
        if self.mode == "missing":
            yield LlmResponse(turn_complete=True)
        else:
            yield LlmResponse(turn_complete=True,
                              usage_metadata=GenerateContentResponseUsageMetadata(
                                  prompt_token_count=100_000,
                                  candidates_token_count=50_000,
                                  total_token_count=150_000,
                                  thoughts_token_count=10 if self.mode == "unsupported" else None))


def setup(ledger, mode="normal", retries=0):
    # Deliberately a contract-test estimate, not a tokenizer for real providers.
    guard = ModelBudgetFilter(TENANT, "app_test", ledger, "model-test", "price-v1",
                              lambda _: RequestEstimate("a" * 64, 200_000, 200_000))
    model = attach_budget_filter(ContractModel(mode, retries), guard)
    agent_context = AgentContext()
    for key, value in {"tenant_id": TENANT, "agent_app_id": "app_test", "execution_id": TENANT + "-execution"}.items():
        agent_context.with_metadata(key, value)
    ctx = SimpleNamespace(agent_context=agent_context, agent=SimpleNamespace())
    return model, ctx


@pytest.mark.asyncio
async def test_real_sdk_model_path_settles_provider_usage_once(budget):
    model, ctx = setup(budget)
    result = [item async for item in model.generate_async(LlmRequest(), stream=True, ctx=ctx)]
    assert len(result) == 2 and model.calls == 1
    assert all(row.settled == Decimal("0.2") and row.reserved == 0 for row in accounts(budget))


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["missing", "unsupported", "cancel", "exception"])
async def test_unknown_usage_and_cancellation_hold_reservation(budget, mode, caplog):
    model, ctx = setup(budget, mode)
    error = asyncio.CancelledError if mode == "cancel" else ModelCallFailed
    with pytest.raises(error) as captured:
        _ = [item async for item in model.generate_async(LlmRequest(), stream=True, ctx=ctx)]
    assert "synthetic-secret" not in str(captured.value)
    assert "synthetic-secret" not in caplog.text
    assert model.calls == 1
    assert all(row.reserved == Decimal("0.6") and row.settled == 0 for row in accounts(budget))
    with budget.database.sessions() as session:
        assert session.scalar(select(ModelAttemptRow)).status == "pending_reconciliation"


def test_sdk_implicit_retry_cannot_be_attached(budget):
    with pytest.raises(ValueError, match="zero SDK retries"):
        setup(budget, retries=2)


@pytest.mark.asyncio
async def test_wrong_tenant_never_calls_provider(budget):
    model, ctx = setup(budget)
    ctx.agent_context.with_metadata("tenant_id", "tenant_other")
    with pytest.raises(PermissionError):
        _ = [item async for item in model.generate_async(LlmRequest(), ctx=ctx)]
    assert model.calls == 0 and accounts(budget) == []


@pytest.mark.asyncio
async def test_cancel_during_sql_reservation_drains_and_releases_without_network(budget, monkeypatch):
    entered, release = threading.Event(), threading.Event()
    original = budget.reserve

    def slow_reserve(*args, **kwargs):
        result = original(*args, **kwargs)
        entered.set()
        assert release.wait(5)
        return result

    monkeypatch.setattr(budget, "reserve", slow_reserve)
    model, ctx = setup(budget)

    async def invoke():
        return [item async for item in model.generate_async(LlmRequest(), ctx=ctx)]

    task = asyncio.create_task(invoke())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0.01)
        assert not task.done() and model.calls == 0
        task.cancel()  # Repeated cancellation must not detach an in-flight SQL thread.
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert model.calls == 0
        with budget.database.sessions() as session:
            assert session.scalar(select(ModelAttemptRow)).status == "cancelled"
        assert all(row.reserved == 0 and row.settled == 0 for row in accounts(budget))
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
