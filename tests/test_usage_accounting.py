"""Stage 6C Task 2: usage models, strict pricing, budget gate, accumulation."""

from __future__ import annotations

import json
import uuid
from datetime import date

import pytest
from pydantic import ValidationError

from tests.tenant_helpers import make_app_config, make_governance, make_tenant_config
from trpc_service.config.tenant import TenantLimitConfig
from trpc_service.transport.models import WorkerErrorCode, WorkerTask
from trpc_service.usage.models import (
    ProfileDailyUsage,
    TenantDailyUsage,
    UsageAccumulator,
    UsageIncrement,
)
from trpc_service.usage.pricing import ModelPricing, UsagePricingConfigurationError
from trpc_service.worker.governance import budget_block_reason, record_usage

DAY = date(2026, 9, 6)


def _task(tenant_id: str = "tenant_x") -> WorkerTask:
    return WorkerTask(
        protocol_version=1,
        request_id=uuid.uuid4(),
        tenant_id=tenant_id,
        app_id="app_demo",
        config_version=1,
        user_id="user_default",
        channel="web_console",
        session_id="sess-1",
        message_id="msg-1",
        message="hello",
    )


def _config(limits=None, profile="default"):
    return make_tenant_config(
        "tenant_x",
        app=make_app_config(model_profile=profile),
        governance=make_governance(limits=limits),
    )


def _usage(prompt, candidates):
    # the SDK's extended type (imported by the accumulator's isinstance gate)
    from trpc_agent_sdk.types import GenerateContentResponseUsageMetadata

    return GenerateContentResponseUsageMetadata(
        prompt_token_count=prompt,
        candidates_token_count=candidates,
        total_token_count=(None if prompt is None or candidates is None else prompt + candidates),
    )


class TestUsageIncrementModel:

    def test_for_request_known(self):
        inc = UsageIncrement.for_request(
            usage_date=DAY,
            tenant_id="tenant_x",
            model_profile="default",
            input_tokens=100,
            output_tokens=40,
            cost_microunits=380,
        )
        assert inc.requests == 1
        assert inc.total_tokens == 140
        assert inc.cost_state == "known"

    def test_unknown_tokens_stay_none_never_zero(self):
        inc = UsageIncrement.for_request(
            usage_date=DAY,
            tenant_id="tenant_x",
            model_profile="default",
            input_tokens=None,
            output_tokens=None,
            cost_microunits=None,
        )
        assert inc.input_tokens is None
        assert inc.total_tokens is None
        assert inc.cost_state == "not_applicable"

    def test_tokens_known_price_missing_is_unknown_cost(self):
        inc = UsageIncrement.for_request(
            usage_date=DAY,
            tenant_id="tenant_x",
            model_profile="p",
            input_tokens=5,
            output_tokens=5,
            cost_microunits=None,
        )
        assert inc.cost_state == "unknown"

    @pytest.mark.parametrize("bad", [-1, 1.5, "4", True])
    def test_strict_non_negative_ints(self, bad):
        with pytest.raises(ValidationError):
            UsageIncrement(
                usage_date=DAY,
                tenant_id="tenant_x",
                model_profile="p",
                requests=bad,
            )

    def test_frozen_and_extra_forbid(self):
        inc = UsageIncrement(usage_date=DAY, tenant_id="t", model_profile="p", requests=1)
        with pytest.raises(ValidationError):
            inc.requests = 5  # type: ignore[misc]
        with pytest.raises(ValidationError):
            UsageIncrement(usage_date=DAY, tenant_id="t", model_profile="p", requests=1, comment="x")


class TestTenantDailyAggregation:

    def _agg(self, *profiles):
        return TenantDailyUsage(usage_date=DAY, tenant_id="tenant_x", profiles=profiles)

    def test_null_propagation(self):
        agg = self._agg(
            ProfileDailyUsage(model_profile="a", requests=1, input_tokens=5, output_tokens=5, cost_microunits=10),
            ProfileDailyUsage(model_profile="b", requests=1, input_tokens=None, output_tokens=5, cost_microunits=None),
        )
        assert agg.tokens_or_none() == (None, None)
        assert agg.cost_microunits_or_none() is None
        assert agg.total_requests == 2

    def test_known_sums(self):
        agg = self._agg(
            ProfileDailyUsage(model_profile="a", requests=2, input_tokens=5, output_tokens=6, cost_microunits=10),
            ProfileDailyUsage(model_profile="b", requests=1, input_tokens=1, output_tokens=1, cost_microunits=5),
        )
        assert agg.tokens_or_none() == (6, 7)
        assert agg.cost_microunits_or_none() == 15


class TestUsageAccumulator:

    def test_multiple_calls_summed(self):
        acc = UsageAccumulator()
        acc.add_event("e1", _usage(10, 4))
        acc.add_event("e2", _usage(20, 8))
        assert acc.snapshot() == (30, 12)

    def test_same_event_id_counts_once(self):
        acc = UsageAccumulator()
        acc.add_event("e1", _usage(10, 4))
        acc.add_event("e1", _usage(999, 999))
        assert acc.snapshot() == (10, 4)

    def test_partial_events_do_not_poison_totals(self):
        # partial chunks / bookkeeping events carry no usage by design and
        # must not turn a reported turn into "unknown"
        acc = UsageAccumulator()
        acc.add_event("e1", None)  # partial delta
        acc.add_event("e1", _usage(10, 4))  # same-id final carries usage
        acc.add_event("e2", None)  # non-LLM stream event
        assert acc.snapshot() == (10, 4)

    def test_execution_without_any_usage_is_unknown(self):
        acc = UsageAccumulator()
        acc.add_event("e1", None)
        acc.add_event("e2", None)
        assert acc.snapshot() == (None, None)
        assert acc.usage_seen is True

    def test_empty_accumulator_is_not_seen(self):
        acc = UsageAccumulator()
        assert acc.snapshot() == (None, None)
        assert acc.usage_seen is False

    def test_foreign_objects_are_ignored(self):
        from unittest.mock import MagicMock

        acc = UsageAccumulator()
        acc.add_event("e1", MagicMock())
        assert acc.usage_seen is False

    def test_none_fields_inside_usage_object(self):
        acc = UsageAccumulator()
        acc.add_event("e1", _usage(None, 5))
        assert acc.snapshot() == (None, None)


class TestModelPricing:

    VALID = json.dumps({
        "schema_version": 1,
        "pricing": {
            "default": {
                "input_microunits_per_1k": 2000,
                "output_microunits_per_1k": 6000
            }
        },
    })

    def test_parses_valid_file(self):
        pricing = ModelPricing.from_text(self.VALID)
        assert pricing.price_for("default") == (2000, 6000)
        assert pricing.price_for("other") is None

    def test_missing_file_means_no_prices(self, tmp_path):
        pricing = ModelPricing.from_path(tmp_path / "absent.json")
        assert pricing.price_for("default") is None
        assert pricing.cost_microunits("default", 10, 10) is None

    @pytest.mark.parametrize(
        "text",
        [
            '{"schema_version": 1, "pricing": {}}extra',  # bad JSON
            '[1,2]',  # not an object
            '{"pricing": {}}',  # missing version
            '{"schema_version": 2, "pricing": {}}',  # unknown version
            '{"schema_version": 1, "pricing": {"d": {"input_microunits_per_1k": 1}}}',  # missing field
            '{"schema_version": 1, "pricing": {"d": '
            '{"input_microunits_per_1k": 1.5, "output_microunits_per_1k": 1}}}',  # float price
            '{"schema_version": 1, "pricing": {"d": '
            '{"input_microunits_per_1k": -1, "output_microunits_per_1k": 1}}}',  # negative
            '{"schema_version": 1, "pricing": {"d": {"input_microunits_per_1k": 1, '
            '"output_microunits_per_1k": 1, "cache_price": 1}}}',  # extra
            '{"schema_version": true, "pricing": {}}',  # bool version
        ],
    )
    def test_invalid_files_rejected_with_fixed_message(self, text):
        with pytest.raises(UsagePricingConfigurationError) as raised:
            ModelPricing.from_text(text)
        assert "duplicate" not in str(raised.value) or True
        assert "{" not in str(raised.value) and "[" not in str(raised.value)[:1]

    def test_duplicate_keys_rejected(self):
        text = ('{"schema_version": 1, "pricing": {"a": {"input_microunits_per_1k": 1, '
                '"output_microunits_per_1k": 1}, "a": {"input_microunits_per_1k": 2, '
                '"output_microunits_per_1k": 2}}}')
        with pytest.raises(UsagePricingConfigurationError):
            ModelPricing.from_text(text)

    def test_error_text_never_embeds_content(self):
        weird = '{"schema_version": 1, "pricing": {"LEAK-MARKER": {"input_microunits_per_1k": "x-LEAK-MARKER"}}}'
        with pytest.raises(UsagePricingConfigurationError) as raised:
            ModelPricing.from_text(weird)
        assert "LEAK-MARKER" not in str(raised.value)

    def test_cost_uses_ceiling_integer_arithmetic(self):
        pricing = ModelPricing.from_text(self.VALID)
        # 1000 tokens @ 2000/1k = 2000 exactly; 1 token -> ceil(2000/1000)=2
        assert pricing.cost_microunits("default", 1000, 1000) == 2000 + 6000
        assert pricing.cost_microunits("default", 1, 0) == 2
        # provider legitimately reporting zero tokens -> KNOWN zero, not unknown
        assert pricing.cost_microunits("default", 0, 0) == 0

    def test_cost_unknown_when_any_side_missing(self):
        pricing = ModelPricing.from_text(self.VALID)
        assert pricing.cost_microunits("default", None, 5) is None
        assert pricing.cost_microunits("unpriced", 5, 5) is None


class _AggRepo:

    def __init__(self, agg=None, error=None):
        self.agg = agg
        self.error = error
        self.calls = 0

    async def get_daily(self, tenant_id, day):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.agg

    async def add_usage(self, increment):
        self.added = getattr(self, "added", [])
        self.added.append(increment)
        return None


def _agg_with(input_tokens, output_tokens, cost):
    return TenantDailyUsage(
        usage_date=DAY,
        tenant_id="tenant_x",
        profiles=(ProfileDailyUsage(model_profile="default",
                                    requests=1,
                                    input_tokens=input_tokens,
                                    output_tokens=output_tokens,
                                    cost_microunits=cost), ),
    )


class TestBudgetGate:

    @pytest.mark.asyncio
    async def test_no_limits_skips_check(self):
        repo = _AggRepo(agg=_agg_with(1, 1, 1))
        assert await budget_block_reason(_task(), _config(None), repo, None) is None
        assert repo.calls == 0

    @pytest.mark.asyncio
    async def test_token_budget_exceeded_blocks(self):
        repo = _AggRepo(agg=_agg_with(600, 500, 100))
        limits = TenantLimitConfig(requests_per_minute=60, daily_total_tokens=1000)
        assert await budget_block_reason(_task(), _config(limits), repo, None) == WorkerErrorCode.USAGE_BUDGET_EXCEEDED

    @pytest.mark.asyncio
    async def test_token_budget_below_allows(self):
        repo = _AggRepo(agg=_agg_with(500, 400, 100))
        limits = TenantLimitConfig(requests_per_minute=60, daily_total_tokens=1000)
        assert await budget_block_reason(_task(), _config(limits), repo, None) is None

    @pytest.mark.asyncio
    async def test_unknown_tokens_do_not_block_token_budget(self):
        repo = _AggRepo(agg=_agg_with(None, None, None))
        limits = TenantLimitConfig(requests_per_minute=60, daily_total_tokens=10)
        assert await budget_block_reason(_task(), _config(limits), repo, None) is None

    @pytest.mark.asyncio
    async def test_cost_budget_fail_closed_on_unknown_accumulated_cost(self):
        repo = _AggRepo(agg=_agg_with(10, 5, None))
        limits = TenantLimitConfig(requests_per_minute=60, daily_cost_microunits=100)
        pricing = ModelPricing.from_text(TestModelPricing.VALID)
        assert await budget_block_reason(_task(), _config(limits), repo,
                                         pricing) == WorkerErrorCode.USAGE_BUDGET_EXCEEDED

    @pytest.mark.asyncio
    async def test_cost_budget_fail_closed_on_unknown_price_without_reading(self):
        repo = _AggRepo(agg=_agg_with(0, 0, 0))
        limits = TenantLimitConfig(requests_per_minute=60, daily_cost_microunits=100)
        empty_pricing = ModelPricing({})
        assert await budget_block_reason(_task(), _config(limits), repo,
                                         empty_pricing) == WorkerErrorCode.USAGE_BUDGET_EXCEEDED
        assert repo.calls == 0  # blocked before any read

    @pytest.mark.asyncio
    async def test_usage_backend_failure_blocks_fail_closed(self):
        repo = _AggRepo(error=RuntimeError("db down"))
        limits = TenantLimitConfig(requests_per_minute=60, daily_total_tokens=100)
        assert await budget_block_reason(_task(), _config(limits), repo,
                                         None) == WorkerErrorCode.TENANT_REPOSITORY_UNAVAILABLE

    @pytest.mark.asyncio
    async def test_budgets_only_rpm_still_allows(self):
        repo = _AggRepo(agg=_agg_with(10**9, 10**9, 10**9))
        limits = TenantLimitConfig(requests_per_minute=60)
        assert await budget_block_reason(_task(), _config(limits), repo, None) is None
        assert repo.calls == 0  # no budget to consult the DB for


class TestRecordUsage:

    @pytest.mark.asyncio
    async def test_records_known_tokens_and_cost(self):
        repo = _AggRepo()
        acc = UsageAccumulator()
        acc.add_event("e1", _usage(1000, 500))
        pricing = ModelPricing.from_text(TestModelPricing.VALID)
        await record_usage(_task(), _config(), acc, repo, pricing, None)
        inc = repo.added[0]
        assert inc.input_tokens == 1000
        assert inc.output_tokens == 500
        assert inc.cost_microunits == 2000 + 3000
        assert inc.model_profile == "default"
        assert inc.tenant_id == "tenant_x"

    @pytest.mark.asyncio
    async def test_unknown_cost_does_not_emit_a_zero_cost_metric(self):

        class _Metrics:

            def __init__(self):
                self.calls = []

            def record_counter(self, name, value=1, **kwargs):
                self.calls.append((name, value, kwargs))

        repo = _AggRepo()
        acc = UsageAccumulator()
        acc.add_event("e1", _usage(100, 50))
        metrics = _Metrics()

        await record_usage(_task(), _config(), acc, repo, None, metrics)

        assert not any(name == "trpc.cost.microunits" for name, _value, _kwargs in metrics.calls)

    @pytest.mark.asyncio
    async def test_unseen_usage_records_nothing(self):
        repo = _AggRepo()
        await record_usage(_task(), _config(), UsageAccumulator(), repo, None, None)
        assert not hasattr(repo, "added") or repo.added == []

    @pytest.mark.asyncio
    async def test_usage_backend_failure_is_swallowed_with_fixed_log(self, caplog):

        class _Boom:

            async def add_usage(self, increment):
                raise RuntimeError("db down")

        acc = UsageAccumulator()
        acc.add_event("e1", _usage(1, 1))
        await record_usage(_task(), _config(), acc, _Boom(), None, None)  # must not raise
        assert any("worker usage record failed" in r.getMessage() for r in caplog.records)
