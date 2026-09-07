# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Adversarial tests: concurrency, isolation and idempotency under contention."""

from __future__ import annotations

import asyncio
import threading

from trpc_service import InboundMessage
from trpc_service import TenantConfigManager
from trpc_service import TenantWorker
from trpc_service import RedisSessionLockManager
from trpc_service import generate_session_id
from trpc_service.channels import CHAT_PRIVATE
from trpc_service.web.gateway import LocalIdempotencyStore
from trpc_service.tool import BudgetTracker
from trpc_service.workspace import TenantSessionService
from trpc_service.tenant import BudgetConfig
from trpc_service.tenant import ModelEndpoint
from trpc_service.tenant import Tenant
from trpc_agent_sdk.sessions import InMemorySessionService


def _tenant(tenant_id: str, token_budget=None) -> Tenant:
    tenant = Tenant(tenant_id=tenant_id, name=tenant_id, model=ModelEndpoint(model_name="m"))
    if token_budget is not None:
        tenant.budget = BudgetConfig(daily_token_budget=token_budget)
    return tenant


# ------------------------------------------------ concurrent cross-tenant isolation


async def test_concurrent_cross_tenant_isolation_same_ids():
    shared = InMemorySessionService()
    svc_a = TenantSessionService(shared, "tenant_a")
    svc_b = TenantSessionService(shared, "tenant_b")

    async def write_a(i: int):
        await svc_a.create_session(app_name="myapp", user_id="u1", session_id=f"s{i}", state={"owner": "a"})

    async def write_b(i: int):
        await svc_b.create_session(app_name="myapp", user_id="u1", session_id=f"s{i}", state={"owner": "b"})

    # Same user_id + session_id, same shared backend, different tenants, concurrent.
    await asyncio.gather(*[write_a(i) for i in range(5)], *[write_b(i) for i in range(5)])

    for i in range(5):
        a = await svc_a.get_session(app_name="myapp", user_id="u1", session_id=f"s{i}")
        b = await svc_b.get_session(app_name="myapp", user_id="u1", session_id=f"s{i}")
        assert a is not None and a.state["owner"] == "a"
        assert b is not None and b.state["owner"] == "b"


# ------------------------------------------------------- idempotency under contention


async def test_idempotency_store_dedups_concurrent_duplicates():
    store = LocalIdempotencyStore()
    results = await asyncio.gather(*[store.check_and_set("msg1") for _ in range(10)])
    # Exactly one "new" and nine "duplicate" under concurrent delivery.
    assert results.count(False) == 1
    assert results.count(True) == 9


# ------------------------------------------------------- budget reserve thread safety


def test_budget_reserve_is_thread_safe():
    tracker = BudgetTracker()
    tenant = _tenant("t_a", token_budget=100)
    results: list[bool] = []
    lock = threading.Lock()

    def worker():
        ok = tracker.reserve(tenant, 60)
        with lock:
            results.append(ok)

    threads = [threading.Thread(target=worker) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Only one 60-token reservation fits under a 100-token budget.
    assert results.count(True) == 1
    assert tracker.reserved_tokens("t_a") == 60


# ------------------------------------------------------- concurrent turns (no crash)


async def test_concurrent_turns_same_session_complete():
    manager = TenantConfigManager()
    manager.register(_tenant("t_a"))
    shared = InMemorySessionService()

    from trpc_agent_sdk.agents import BaseAgent
    from trpc_agent_sdk.events import Event
    from trpc_agent_sdk.types import Content, Part

    class EchoAgent(BaseAgent):

        def __init__(self, name):
            super().__init__(name=name)

        async def _run_async_impl(self, ctx):
            text = "".join(p.text or "" for p in (ctx.user_content.parts if ctx.user_content else []))
            yield Event(author=self.name, content=Content(parts=[Part.from_text(text=f"echo:{text}")]), partial=False)

    worker = TenantWorker(
        manager=manager,
        agent_factory=lambda t: EchoAgent(name="t_a"),
        session_service_factory=lambda t: shared,
    )

    def inbound(msg_id: str, text: str) -> InboundMessage:
        return InboundMessage(channel="wecom",
                              chat_id="u1",
                              chat_type=CHAT_PRIVATE,
                              sender_id="u1",
                              message_id=msg_id,
                              text=text)

    r1, r2 = await asyncio.gather(
        worker.handle("t_a", "wecom", inbound("m1", "one")),
        worker.handle("t_a", "wecom", inbound("m2", "two")),
    )
    assert "echo:one" in r1
    assert "echo:two" in r2

    session_id = generate_session_id("t_a", "wecom", CHAT_PRIVATE, "u1", "u1")
    session = await shared.get_session(app_name="t_a:default", user_id="u1", session_id=session_id)
    assert session is not None
    event_text = " ".join(part.text or "" for event in session.events if event.content for part in event.content.parts)
    assert "one" in event_text
    assert "two" in event_text


async def test_redis_session_lock_serializes_different_workers():
    import fakeredis.aioredis as faioredis

    client = faioredis.FakeRedis(decode_responses=True)
    manager_a = RedisSessionLockManager(client=client, acquire_timeout=1, retry_interval=0.001)
    manager_b = RedisSessionLockManager(client=client, acquire_timeout=1, retry_interval=0.001)
    active = 0
    max_active = 0

    async def critical(manager):
        nonlocal active, max_active
        async with manager.acquire("tenant:session"):
            active += 1
            max_active = max(max_active, active)
            await asyncio.sleep(0.01)
            active -= 1

    await asyncio.gather(critical(manager_a), critical(manager_b))
    assert max_active == 1
