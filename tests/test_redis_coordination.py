from uuid import uuid4

import pytest

from trpc_service.agent.coordination import RedisExecutionLeaseCoordinator, RedisOutboxNotifier
from trpc_service.tenant import TenantContext


class FakeRedis:

    def __init__(self) -> None:
        self.counter = 0
        self.values: dict[str, str] = {}
        self.messages: list[tuple[str, dict[str, str]]] = []

    async def incr(self, key: str) -> int:
        self.counter += 1
        return self.counter

    async def set(self, key: str, value: str, *, nx: bool, px: int) -> bool:
        del px
        if nx and key in self.values:
            return False
        self.values[key] = value
        return True

    async def eval(self, script: str, numkeys: int, *values: object) -> int:
        del script, numkeys
        key, token = str(values[0]), str(values[1])
        if self.values.get(key) != token:
            return 0
        if len(values) == 2:
            del self.values[key]
        return 1

    async def xadd(self, stream: str, fields: dict[str, str]) -> str:
        self.messages.append((stream, fields))
        return "1-0"


def _context() -> TenantContext:
    return TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-1",
        trace_id="trace-1",
    )


@pytest.mark.anyio
async def test_redis_coordinates_fenced_leases_and_outbox_wakeups() -> None:
    redis = FakeRedis()
    context = _context()
    coordinator = RedisExecutionLeaseCoordinator(redis, prefix="trpc", ttl_ms=30000)
    notifier = RedisOutboxNotifier(redis, prefix="trpc")

    lease = await coordinator.acquire(context, "session-1")

    assert lease is not None
    assert lease.fencing_token == 1
    assert await coordinator.renew(lease)
    assert await coordinator.release(lease)
    await notifier.notify(context, ["outbox-1", "outbox-2"])
    assert redis.messages[0][0] == "trpc:outbox:wake"
