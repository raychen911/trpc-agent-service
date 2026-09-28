"""Redis Session cache contract tests without requiring a live Redis server."""

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from trpc_service.storage.session_cache import RedisSessionSnapshotCache
from trpc_service.storage.types import SessionEvent, SessionSnapshot
from trpc_service.tenant.context import TenantContext


class FakeRedisClient:
    """Minimal async Redis double used to verify cache semantics."""

    def __init__(self) -> None:
        self.values: dict[str, bytes | str] = {}
        self.deleted: list[str] = []
        self.expiry: int | None = None
        self.closed = False

    async def get(self, name: str) -> bytes | str | None:
        return self.values.get(name)

    async def set(self, name: str, value: str, *, ex: int) -> object:
        self.values[name] = value
        self.expiry = ex
        return True

    async def delete(self, *names: str) -> int:
        self.deleted.extend(names)
        for name in names:
            self.values.pop(name, None)
        return len(names)

    async def aclose(self) -> None:
        self.closed = True


def _context() -> TenantContext:
    return TenantContext(
        tenant_id=uuid4(),
        agent_app_id=uuid4(),
        config_version=1,
        request_id="request-1",
        trace_id="trace-1",
    )


@pytest.mark.anyio
async def test_redis_session_cache_round_trips_recent_versioned_events() -> None:
    client = FakeRedisClient()
    cache = RedisSessionSnapshotCache(client, ttl_seconds=900, max_events=2)
    context = _context()
    events = tuple(
        SessionEvent(
            event_id=f"event-{index}",
            event_type="message.received",
            occurred_at=datetime.now(timezone.utc),
            payload={"text": f"message-{index}"},
        ) for index in range(3))
    snapshot = SessionSnapshot(
        session_id="session-1",
        version=3,
        events=events,
        state={"turn": 3},
    )

    await cache.put(context, snapshot)
    loaded = await cache.get(context, "session-1", expected_version=3)

    assert loaded is not None
    assert loaded.version == 3
    assert [event.event_id for event in loaded.events] == ["event-1", "event-2"]
    assert loaded.state == {"turn": 3}
    assert client.expiry == 900


@pytest.mark.anyio
async def test_redis_session_cache_rejects_stale_versions_and_is_tenant_scoped() -> None:
    client = FakeRedisClient()
    cache = RedisSessionSnapshotCache(client, ttl_seconds=900, max_events=10)
    first = _context()
    second = _context()
    snapshot = SessionSnapshot(session_id="shared-name", version=1)

    await cache.put(first, snapshot)

    assert await cache.get(first, "shared-name", expected_version=2) is None
    await cache.put(first, snapshot)
    assert await cache.get(second, "shared-name", expected_version=1) is None
    assert client.deleted


@pytest.mark.anyio
async def test_redis_session_cache_fails_open_when_redis_is_unavailable() -> None:

    class FailingRedisClient(FakeRedisClient):

        async def get(self, name: str) -> bytes | str | None:
            del name
            raise ConnectionError("redis unavailable")

        async def set(self, name: str, value: str, *, ex: int) -> object:
            del name, value, ex
            raise ConnectionError("redis unavailable")

    cache = RedisSessionSnapshotCache(FailingRedisClient(), ttl_seconds=900, max_events=10)
    context = _context()

    assert await cache.get(context, "session-1", expected_version=0) is None
    await cache.put(context, SessionSnapshot(session_id="session-1", version=0))


@pytest.mark.anyio
async def test_redis_session_cache_discards_oversized_or_invalid_payloads() -> None:
    client = FakeRedisClient()
    cache = RedisSessionSnapshotCache(client, ttl_seconds=900, max_events=10)
    context = _context()
    await cache.put(context, SessionSnapshot(session_id="session-1", version=0))
    key = next(iter(client.values))

    client.values[key] = b"x" * (1024 * 1024 + 1)
    assert await cache.get(context, "session-1", expected_version=0) is None
    assert key in client.deleted

    client.values[key] = "not-json"
    assert await cache.get(context, "session-1", expected_version=0) is None


@pytest.mark.anyio
async def test_redis_session_cache_validates_options_and_closes_clients() -> None:
    client = FakeRedisClient()
    with pytest.raises(ValueError, match="positive"):
        RedisSessionSnapshotCache(client, ttl_seconds=0, max_events=10)

    cache = RedisSessionSnapshotCache(client, ttl_seconds=900, max_events=10)
    await cache.close()

    assert client.closed

    configured = RedisSessionSnapshotCache.from_url(
        "redis://127.0.0.1:6379/0",
        ttl_seconds=900,
        max_events=10,
    )
    await configured.close()
