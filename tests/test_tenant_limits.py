"""Stage 6C Task 2: tenant limit config, Redis limiter contract, ingress gate."""

from __future__ import annotations

import uuid

import pytest
from pydantic import ValidationError

from tests.tenant_helpers import (
    FakeTenantConfigRepository,
    make_app_config,
    make_governance,
    make_tenant_config,
)
from trpc_service.channels.models import InboundMessage
from trpc_service.config.tenant import TenantGovernanceConfig, TenantLimitConfig
from trpc_service.gateway.channel_service import ChannelIngressService
from trpc_service.governance.content_policy import ContentPolicyConfig
from trpc_service.governance.limits import (
    RateLimiterUnavailableError,
    RedisTenantRateLimiter,
    TTL_SECONDS,
    WINDOW_SECONDS,
    rate_limit_key,
)
from trpc_service.transport.models import WorkerChatResult
from trpc_service.gateway.errors import RATE_LIMITED_TEXT, TENANT_SERVICE_UNAVAILABLE_TEXT

TENANT = "tenant_default"

SENTINEL_URL = "redis://user:fake-redis-pw-sentinel@h:6379"


def _limit(**overrides):
    defaults = {"requests_per_minute": 60}
    defaults.update(overrides)
    return TenantLimitConfig(**defaults)


class TestTenantLimitConfig:

    def test_required_fields_and_defaults(self):
        limit = _limit()
        assert limit.requests_per_minute == 60
        assert limit.daily_total_tokens is None
        assert limit.daily_cost_microunits is None

    def test_is_frozen_and_extra_forbidden(self):
        limit = _limit()
        with pytest.raises(ValidationError):
            limit.requests_per_minute = 1  # type: ignore[misc]
        with pytest.raises(ValidationError):
            _limit(custom_rule=True)

    @pytest.mark.parametrize("bad", [0, -1, True, "60", 60.0])
    def test_requests_per_minute_strict_positive(self, bad):
        with pytest.raises(ValidationError):
            _limit(requests_per_minute=bad)

    @pytest.mark.parametrize("bad", [0, -5, 1.5, "10", True])
    def test_budgets_strict(self, bad):
        with pytest.raises(ValidationError):
            _limit(daily_total_tokens=bad)
        with pytest.raises(ValidationError):
            _limit(daily_cost_microunits=bad)

    def test_bool_never_substitutes_int(self):
        with pytest.raises(ValidationError):
            _limit(requests_per_minute=True)
        with pytest.raises(ValidationError):
            _limit(daily_total_tokens=True)
        assert _limit(daily_total_tokens=1).daily_total_tokens == 1


class TestGovernanceLimitsRequirement:

    def test_legacy_governance_without_limits_key_is_rejected(self):
        with pytest.raises(ValidationError):
            TenantGovernanceConfig(
                allowed_channels=("web_console", ),
                tool_decisions={},
                content_policy=ContentPolicyConfig(),
            )

    def test_explicit_null_limits_preserves_unlimited_behavior(self):
        gov = make_governance()
        assert gov.limits is None

    def test_limits_round_trip(self):
        gov = make_governance(limits=_limit(requests_per_minute=5, daily_total_tokens=1000))
        dumped = gov.model_dump(mode="json")
        assert dumped["limits"] == {
            "requests_per_minute": 5,
            "daily_total_tokens": 1000,
            "daily_cost_microunits": None,
        }
        assert TenantGovernanceConfig(**dumped).limits == gov.limits


class TestRateLimitKey:

    def test_key_shape_is_tenant_plus_utc_minute_bucket(self):
        key = rate_limit_key("tenant_x")
        parts = key.split(":")
        assert parts[:3] == ["trpc", "rl", "tenant_x"]
        assert int(parts[3]) * WINDOW_SECONDS > 0

    def test_key_contains_no_high_cardinality_identity(self):
        key = rate_limit_key("tenant_x")
        for banned in ("user", "session", "message", "http"):
            assert banned not in key


class _FakeScript:

    def __init__(self, result=None, error=None):
        self.result = result
        self.error = error
        self.calls: list = []

    async def __call__(self, keys, args):
        self.calls.append((list(keys), list(args)))
        if self.error is not None:
            raise self.error
        return self.result


class _FakeRedis:

    def __init__(self, script: _FakeScript, ping_error=None):
        self._script = script
        self.ping_error = ping_error
        self.register_calls = 0
        self.closed = False

    def register_script(self, lua):
        self.register_calls += 1
        assert "INCR" in lua and "EXPIRE" in lua and "KEYS[1]" in lua
        return self._script

    async def ping(self):
        if self.ping_error is not None:
            raise self.ping_error
        return True

    async def aclose(self):
        self.closed = True


class TestRedisTenantRateLimiter:

    @pytest.mark.asyncio
    async def test_acquire_passes_limit_and_ttl_args_once(self):
        script = _FakeScript(result=0)
        limiter = RedisTenantRateLimiter(_FakeRedis(script))
        allowed = await limiter.acquire("tenant_x", 5)
        assert allowed is False
        keys, args = script.calls[0]
        assert keys[0].startswith("trpc:rl:tenant_x:")
        assert [str(a) for a in args] == ["5", str(TTL_SECONDS)]

    @pytest.mark.asyncio
    async def test_backend_error_maps_to_unavailable_without_cause_or_url(self):

        class _Exploding:

            def register_script(self, lua):

                async def _script(keys, args):
                    raise ConnectionError(f"cannot connect to {SENTINEL_URL}")

                return _script

            async def ping(self):
                return True

        limiter = RedisTenantRateLimiter(_Exploding())
        with pytest.raises(RateLimiterUnavailableError) as raised:
            await limiter.acquire("tenant_x", 3)
        assert raised.value.__cause__ is None
        assert SENTINEL_URL not in str(raised.value)
        assert SENTINEL_URL not in repr(raised.value)

    @pytest.mark.asyncio
    async def test_invalid_limit_rejected_before_backend(self):
        script = _FakeScript(result=1)
        limiter = RedisTenantRateLimiter(_FakeRedis(script))
        for bad in (0, -1, True, "5"):
            with pytest.raises(ValueError):
                await limiter.acquire("tenant_x", bad)
        assert script.calls == []

    @pytest.mark.asyncio
    async def test_check_ready_and_close(self):
        redis = _FakeRedis(_FakeScript(result=1))
        limiter = RedisTenantRateLimiter(redis)
        await limiter.check_ready()
        await limiter.close()
        assert redis.closed
        await limiter.close()  # idempotent

    @pytest.mark.asyncio
    async def test_check_ready_failure_maps_unavailable(self):
        redis = _FakeRedis(_FakeScript(result=1), ping_error=ConnectionError(SENTINEL_URL))
        limiter = RedisTenantRateLimiter(redis)
        with pytest.raises(RateLimiterUnavailableError):
            await limiter.check_ready()

    def test_from_env_missing_url_fails_closed(self):
        with pytest.raises(RateLimiterUnavailableError):
            RedisTenantRateLimiter.from_env({})


# ---------------------------------------------------------------------------
# Gateway ingress rate gate
# ---------------------------------------------------------------------------


class _NoopWorker:

    def __init__(self):
        self.chat_calls = 0
        self.stream_calls = 0

    async def chat(self, task):
        self.chat_calls += 1
        return WorkerChatResult(protocol_version=1, request_id=task.request_id, response="hi")

    async def stream(self, task):
        self.stream_calls += 1
        raise AssertionError("unused")

    async def decide(self, task):
        raise AssertionError("unused")


class _StubLimiter:

    def __init__(self, allowed=True, error=None):
        self.allowed = allowed
        self.error = error
        self.calls: list = []

    async def acquire(self, tenant_id, limit):
        self.calls.append((tenant_id, limit))
        if self.error is not None:
            raise self.error
        return self.allowed


def _inbound() -> InboundMessage:
    return InboundMessage(
        tenant_id=TENANT,
        channel="web_console",
        external_user_id="user_abc",
        external_conversation_id="conv_1",
        external_message_id=f"msg-{uuid.uuid4().hex[:8]}",
        text="hello",
    )


def _config_with(limits):
    cfg = make_tenant_config(TENANT, app=make_app_config(), governance=make_governance(limits=limits))
    return {TENANT: cfg}


def _ingress(limits, limiter):
    return ChannelIngressService(
        FakeTenantConfigRepository(_config_with(limits)),
        _NoopWorker(),
        rate_limiter=limiter,
    )


@pytest.mark.asyncio
async def test_gate_passes_when_within_budget():
    worker_client = _NoopWorker()
    limiter = _StubLimiter(allowed=True)
    service = ChannelIngressService(FakeTenantConfigRepository(_config_with(_limit(requests_per_minute=3))),
                                    worker_client,
                                    rate_limiter=limiter)
    reply = await service.chat(_inbound())
    assert reply.response == "hi"
    assert limiter.calls == [(TENANT, 3)]


@pytest.mark.asyncio
async def test_gate_rejects_with_fixed_text_and_zero_worker_calls():
    worker_client = _NoopWorker()
    service = _ingress(_limit(requests_per_minute=3), _StubLimiter(allowed=False))
    # override with our counting client
    service._worker_client = worker_client
    reply = await service.chat(_inbound())
    assert reply.response == RATE_LIMITED_TEXT
    assert worker_client.chat_calls == 0


@pytest.mark.asyncio
async def test_gate_fail_closed_without_limiter():
    worker_client = _NoopWorker()
    service = ChannelIngressService(
        FakeTenantConfigRepository(_config_with(_limit(requests_per_minute=3))),
        worker_client,
        rate_limiter=None,
    )
    reply = await service.chat(_inbound())
    assert reply.response == TENANT_SERVICE_UNAVAILABLE_TEXT
    assert worker_client.chat_calls == 0


@pytest.mark.asyncio
async def test_gate_fail_closed_on_limiter_error():
    worker_client = _NoopWorker()
    service = ChannelIngressService(
        FakeTenantConfigRepository(_config_with(_limit(requests_per_minute=3))),
        worker_client,
        rate_limiter=_StubLimiter(error=RateLimiterUnavailableError("backend failed")),
    )
    reply = await service.chat(_inbound())
    assert reply.response == TENANT_SERVICE_UNAVAILABLE_TEXT
    assert worker_client.chat_calls == 0


@pytest.mark.asyncio
async def test_no_limits_never_touches_limiter():
    limiter = _StubLimiter(allowed=False)
    worker_client = _NoopWorker()
    service = ChannelIngressService(FakeTenantConfigRepository(_config_with(None)), worker_client, rate_limiter=limiter)
    reply = await service.chat(_inbound())
    assert reply.response == "hi"
    assert limiter.calls == []


@pytest.mark.asyncio
async def test_stream_gate_emits_error_event():
    limiter = _StubLimiter(allowed=False)
    service = ChannelIngressService(
        FakeTenantConfigRepository(_config_with(_limit(requests_per_minute=3))),
        _NoopWorker(),
        rate_limiter=limiter,
    )
    events = [e async for e in service.stream(_inbound())]
    assert [e.type for e in events] == ["error"]
    assert events[0].data == RATE_LIMITED_TEXT
