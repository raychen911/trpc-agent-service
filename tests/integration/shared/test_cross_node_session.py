from __future__ import annotations

from uuid import uuid4
from random import Random
from statistics import quantiles
from time import perf_counter

import pytest
from redis.asyncio import Redis

from tests.support import inbound_message_data
from trpc_service.config.settings import build_demo_settings
from trpc_service.storage.inmemory import InMemoryPlatformAdapters
from trpc_service.storage.redis_session import SharedSessionBackendFactory
from trpc_service.tenant.session_identity import derive_session_identity
from trpc_service.worker.service import AgentExecutor


@pytest.mark.shared_backend
async def test_two_independent_workers_alternate_twenty_turns_without_cross_tenant_leak(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    local = InMemoryPlatformAdapters(build_demo_settings())
    alpha = local.context_for_test("binding-alpha", "same-user", uuid4())
    beta = local.context_for_test("binding-beta", "same-user", uuid4())
    alpha_id = derive_session_identity(alpha, "direct", "same-conversation")
    beta_id = derive_session_identity(beta, "direct", "same-conversation")
    workers = [
        AgentExecutor(SharedSessionBackendFactory(redis, namespace=shared_namespace)),
        AgentExecutor(SharedSessionBackendFactory(redis, namespace=shared_namespace)),
    ]

    assert alpha_id.platform_session_id != beta_id.platform_session_id
    for turn in range(20):
        context, identity, token = (alpha, alpha_id, "ALPHA") if turn % 2 == 0 else (beta, beta_id, "BRAVO")
        text = f"Remember validation token {token}." if turn < 2 else "Recall the validation token."
        result = await (await workers[turn % 2].prepare(context, identity, text)).execute()
        assert result.final_text == (f"stored:{token}" if turn < 2 else f"recalled:{token}")

    assert sum(worker.call_count for worker in workers) == 20
    for worker in workers:
        await worker.close()
    await redis.aclose()


@pytest.mark.shared_backend
async def test_two_hundred_random_routes_preserve_context_and_report_p95(shared_redis_url: str, shared_namespace: str) -> None:
    redis = Redis.from_url(shared_redis_url, decode_responses=True)
    context = InMemoryPlatformAdapters(build_demo_settings()).context_for_test("binding-alpha", "stress-user", uuid4())
    identity = derive_session_identity(context, "direct", "stress-conversation")
    workers = [AgentExecutor(SharedSessionBackendFactory(redis, namespace=shared_namespace)) for _ in range(2)]
    random = Random(20260907)
    latencies: list[float] = []
    errors = 0
    for turn in range(200):
        text = "Remember validation token ALPHA." if turn == 0 else "Recall the validation token."
        started = perf_counter()
        result = await (await workers[random.randrange(2)].prepare(context, identity, text)).execute()
        latencies.append((perf_counter() - started) * 1000)
        expected = "stored:ALPHA" if turn == 0 else "recalled:ALPHA"
        errors += result.final_text != expected
    p95 = quantiles(latencies, n=20)[18]
    print(f"stress p95_ms={p95:.2f} errors={errors} executions=200")
    assert errors == 0 and sum(worker.call_count for worker in workers) == 200
    for worker in workers:
        await worker.close()
    await redis.aclose()
