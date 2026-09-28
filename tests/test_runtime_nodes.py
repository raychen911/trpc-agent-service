from datetime import timedelta
from pathlib import Path

import anyio
import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from trpc_service.agent.nodes import (
    PostgreSQLRuntimeNodeRegistry,
    RuntimeNodeHeartbeatService,
    RuntimeNodeHealth,
)
from trpc_service.storage.orm import Base, utc_now
from trpc_service.storage.runtime_orm import RuntimeNodeRow


class TransientHeartbeatRegistry:
    """Fail one heartbeat to model a short database interruption."""

    def __init__(self) -> None:
        self.heartbeat_calls = 0
        self.stopped = False

    async def register(self, node_id: str, role: str, worker_concurrency: int) -> None:
        del node_id, role, worker_concurrency

    async def heartbeat(self, node_id: str) -> bool:
        del node_id
        self.heartbeat_calls += 1
        if self.heartbeat_calls == 1:
            raise ConnectionError("database failover")
        return True

    async def stop(self, node_id: str) -> None:
        del node_id
        self.stopped = True


@pytest.mark.anyio
async def test_runtime_node_registry_retains_graceful_shutdown_state(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'nodes.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    registry = PostgreSQLRuntimeNodeRegistry(
        sessions,
        stale_after_seconds=20,
    )

    await registry.register("worker-a", "worker", 4)
    assert await registry.active_worker_count() == 1
    assert await registry.heartbeat("worker-a")
    await registry.register("worker-a", "api_worker", 2)
    assert await registry.active_worker_count() == 1
    assert await registry.drain("worker-a")
    draining = await registry.list()
    assert draining.items[0].health is RuntimeNodeHealth.DRAINING
    async with sessions.begin() as database:
        row = await database.get(RuntimeNodeRow, "worker-a")
        assert row is not None
        row.heartbeat_at = utc_now() - timedelta(seconds=30)
    stale_drain = await registry.list()
    assert stale_drain.items[0].health is RuntimeNodeHealth.STALE
    assert await registry.heartbeat("worker-a")
    refreshed_drain = await registry.list()
    assert refreshed_drain.items[0].health is RuntimeNodeHealth.DRAINING
    assert await registry.active_worker_count() == 0
    await registry.stop("worker-a")
    assert not await registry.heartbeat("worker-a")
    nodes = await registry.list()

    assert nodes.total == 1
    assert nodes.items[0].health is RuntimeNodeHealth.STOPPED
    assert nodes.items[0].worker_concurrency == 2

    lifecycle = RuntimeNodeHeartbeatService(
        registry,
        node_id="worker-b",
        role="worker",
        worker_concurrency=1,
        heartbeat_interval_seconds=0.01,
    )
    await lifecycle.start()
    await lifecycle.start()
    await lifecycle.close()
    lifecycle_nodes = await registry.list()
    assert lifecycle_nodes.total == 2
    assert lifecycle_nodes.items[1].health is RuntimeNodeHealth.STOPPED

    await engine.dispose()


@pytest.mark.anyio
async def test_runtime_heartbeat_recovers_after_transient_database_failure() -> None:
    registry = TransientHeartbeatRegistry()
    lifecycle = RuntimeNodeHeartbeatService(
        registry,  # type: ignore[arg-type]
        node_id="worker-a",
        role="worker",
        worker_concurrency=1,
        heartbeat_interval_seconds=0.01,
    )

    await lifecycle.start()
    for _ in range(20):
        if registry.heartbeat_calls >= 2:
            break
        await anyio.sleep(0.01)
    await lifecycle.close()

    assert registry.heartbeat_calls >= 2
    assert registry.stopped
