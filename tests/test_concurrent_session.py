"""并发写 session 实测（sync-and-idempotency.md §2 的经验验证）。"""
import asyncio
from types import SimpleNamespace

from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.types import Content, Part


def _user_event(text: str):
    return Event(author="user", content=Content(parts=[Part(text=text)]))


async def test_concurrent_append_no_loss():
    service = InMemorySessionService()
    session = await service.create_session(app_name="app1", user_id="u1", session_id="s1")
    before = len(session.events)

    async def worker(n: int):
        for i in range(10):
            await service.append_event(session, _user_event(f"w{n}-{i}"))

    await asyncio.gather(*[worker(n) for n in range(5)])
    after = len(session.events)
    # 5 节点 × 10 事件全部落库：append 语义不丢不重
    assert after - before == 50


async def test_concurrent_state_update_merge():
    service = InMemorySessionService()
    session = await service.create_session(app_name="app1", user_id="u2", session_id="s2")

    async def worker(key: str, value: str):
        session.state[key] = value
        await service.update_session(session)

    await asyncio.gather(*[worker(f"k{i}", f"v{i}") for i in range(10)])
    fresh = await service.get_session(app_name="app1", user_id="u2", session_id="s2")
    # state_delta 键值合并天然可交换：10 个不同键全部存在
    assert all(fresh.state.get(f"k{i}") == f"v{i}" for i in range(10))


def test_timed_session_service_reports_latency():
    """热路径方法计时上报（进程内聚合可见）。"""
    import asyncio

    from trpc_service.metrics.collector import metrics_collector
    from trpc_service.tenant.storage.factory import TimedSessionService

    class Inner:
        async def get_session(self, **kwargs):
            return SimpleNamespace(events=[])

    metrics_collector.reset()
    svc = TimedSessionService(Inner(), "tenant_t", "in_memory")
    asyncio.run(svc.get_session(app_name="a", user_id="u", session_id="s"))
    snap = metrics_collector.backend_snapshot("tenant_t")
    assert snap["in_memory"]["calls"] == 1
    assert snap["in_memory"]["avg_ms"] >= 0
    metrics_collector.reset()
