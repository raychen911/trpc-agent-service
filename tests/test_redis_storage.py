import asyncio

import fakeredis.aioredis
import pytest

from trpc_service.storage.contracts import SessionIdentity
from trpc_service.storage.exceptions import LockNotAcquiredError, VersionConflictError
from trpc_service.storage.redis_backend import RedisCoordinationStore, RedisSessionStore


def test_redis_coordination_and_session_cas() -> None:
    async def scenario() -> None:
        redis = fakeredis.aioredis.FakeRedis()
        coordination = RedisCoordinationStore(redis, key_prefix="test")

        assert await coordination.claim("tenant:wecom:message", ttl_seconds=60)
        assert not await coordination.claim("tenant:wecom:message", ttl_seconds=60)
        await coordination.complete("tenant:wecom:message", {"reply": "ok"})
        completed = await coordination.get("tenant:wecom:message")
        assert completed is not None
        assert completed.result == {"reply": "ok"}
        await coordination.claim("temporary", ttl_seconds=60)
        await coordination.abandon("temporary")
        assert await coordination.claim("temporary", ttl_seconds=60)

        await coordination.set_state("typing", {"active": True}, ttl_seconds=60)
        assert await coordination.get_state("typing") == {"active": True}
        await coordination.delete_state("typing")
        assert await coordination.get_state("typing") is None

        assert (await coordination.check("tenant", limit=1, window_seconds=60)).allowed
        assert not (await coordination.check("tenant", limit=1, window_seconds=60)).allowed

        async with coordination.acquire("session-lock", ttl_seconds=10):
            with pytest.raises(LockNotAcquiredError):
                async with coordination.acquire(
                    "session-lock", ttl_seconds=10, wait_timeout_seconds=0.01
                ):
                    pass

        sessions = RedisSessionStore(redis, key_prefix="test")
        identity = SessionIdentity("tenant", "app", "user", "session")
        created = await sessions.create_session(identity)
        assert created.version == 0
        updated = await sessions.compare_and_swap_state(identity, 0, {"turn": 1})
        assert updated.version == 1
        assert (await sessions.get_session(identity)).state == {"turn": 1}  # type: ignore[union-attr]
        with pytest.raises(VersionConflictError):
            await sessions.compare_and_swap_state(identity, 0, {"turn": 2})

        await redis.aclose()

    asyncio.run(scenario())
