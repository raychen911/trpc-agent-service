"""Runtime node registration and heartbeat for horizontal operations."""

import asyncio
from datetime import datetime, timedelta
from enum import StrEnum
import logging

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from trpc_service.storage.orm import as_utc, utc_now
from trpc_service.storage.runtime_orm import RuntimeNodeRow

logger = logging.getLogger(__name__)


class RuntimeNodeHealth(StrEnum):
    """Operator-facing health derived from status and heartbeat freshness."""

    ACTIVE = "active"
    DRAINING = "draining"
    STALE = "stale"
    STOPPED = "stopped"


class RuntimeNodeRead(BaseModel):
    """Safe runtime node state exposed through the management API."""

    model_config = ConfigDict(from_attributes=True)

    node_id: str
    role: str
    health: RuntimeNodeHealth
    worker_concurrency: int
    started_at: datetime
    heartbeat_at: datetime
    stopped_at: datetime | None


class RuntimeNodeList(BaseModel):
    """Bounded collection returned to platform operators."""

    items: list[RuntimeNodeRead]
    total: int


class PostgreSQLRuntimeNodeRegistry:
    """Persist node liveness in the primary database shared by every process."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        *,
        stale_after_seconds: int,
    ) -> None:
        if stale_after_seconds < 2:
            raise ValueError("runtime node stale timeout must be at least two seconds")
        self._sessions = sessions
        self._stale_after_seconds = stale_after_seconds

    async def register(self, node_id: str, role: str, worker_concurrency: int) -> None:
        """Create or reactivate one operator-defined process identity."""

        if not node_id.strip() or role not in {
                "api", "worker", "api_worker", "channel", "supervisor"
        }:
            raise ValueError("runtime node identity or role is invalid")
        if worker_concurrency < 0:
            raise ValueError("runtime node concurrency cannot be negative")
        now = utc_now()
        async with self._sessions.begin() as database:
            row = await database.get(RuntimeNodeRow, node_id, with_for_update=True)
            if row is None:
                database.add(
                    RuntimeNodeRow(
                        node_id=node_id,
                        role=role,
                        status="active",
                        worker_concurrency=worker_concurrency,
                        started_at=now,
                        heartbeat_at=now,
                    ))
            else:
                row.role = role
                row.status = "active"
                row.worker_concurrency = worker_concurrency
                row.started_at = now
                row.heartbeat_at = now
                row.stopped_at = None

    async def heartbeat(self, node_id: str) -> bool:
        """Refresh a serving or draining node; stopped IDs fail closed."""

        async with self._sessions.begin() as database:
            row = await database.get(RuntimeNodeRow, node_id, with_for_update=True)
            if row is None or row.status not in {"active", "draining"}:
                return False
            row.heartbeat_at = utc_now()
            return True

    async def drain(self, node_id: str) -> bool:
        """Mark a Worker as no longer eligible for new work before shutdown."""

        async with self._sessions.begin() as database:
            row = await database.get(RuntimeNodeRow, node_id, with_for_update=True)
            if row is None or row.role not in {"worker", "api_worker"}:
                return False
            if row.status == "stopped":
                return False
            row.status = "draining"
            row.heartbeat_at = utc_now()
            return True

    async def stop(self, node_id: str) -> None:
        """Mark graceful shutdown without deleting operational history."""

        now = utc_now()
        async with self._sessions.begin() as database:
            row = await database.get(RuntimeNodeRow, node_id, with_for_update=True)
            if row is not None:
                row.status = "stopped"
                row.heartbeat_at = now
                row.stopped_at = now

    async def list(self) -> RuntimeNodeList:
        """Return all known nodes with stale state computed at read time."""

        async with self._sessions() as database:
            rows = (await database.scalars(select(RuntimeNodeRow).order_by(RuntimeNodeRow.node_id)
                                           )).all()
        stale_before = utc_now() - timedelta(seconds=self._stale_after_seconds)
        items = []
        for row in rows:
            health = RuntimeNodeHealth.STOPPED
            if row.status == "active":
                health = (RuntimeNodeHealth.ACTIVE
                          if as_utc(row.heartbeat_at) >= stale_before else RuntimeNodeHealth.STALE)
            elif row.status == "draining":
                # A process killed during drain cannot publish its terminal
                # state. Freshness must still win so it does not appear to be
                # draining forever in the administrator console.
                health = (RuntimeNodeHealth.DRAINING
                          if as_utc(row.heartbeat_at) >= stale_before else RuntimeNodeHealth.STALE)
            items.append(
                RuntimeNodeRead(
                    node_id=row.node_id,
                    role=row.role,
                    health=health,
                    worker_concurrency=row.worker_concurrency,
                    started_at=row.started_at,
                    heartbeat_at=row.heartbeat_at,
                    stopped_at=row.stopped_at,
                ))
        return RuntimeNodeList(items=items, total=len(items))

    async def active_worker_count(self) -> int:
        """Count fresh Worker-capable nodes for readiness checks."""

        cutoff = utc_now() - timedelta(seconds=self._stale_after_seconds)
        async with self._sessions() as database:
            count = await database.scalar(
                select(func.count()).select_from(RuntimeNodeRow).where(
                    RuntimeNodeRow.status == "active",
                    RuntimeNodeRow.role.in_(("worker", "api_worker")),
                    RuntimeNodeRow.heartbeat_at >= cutoff,
                    RuntimeNodeRow.worker_concurrency > 0,
                ))
        return int(count or 0)


class RuntimeNodeHeartbeatService:
    """Register one process and refresh its liveness until graceful shutdown."""

    def __init__(
        self,
        registry: PostgreSQLRuntimeNodeRegistry,
        *,
        node_id: str,
        role: str,
        worker_concurrency: int,
        heartbeat_interval_seconds: float,
    ) -> None:
        if heartbeat_interval_seconds <= 0:
            raise ValueError("runtime node heartbeat interval must be positive")
        self._registry = registry
        self._node_id = node_id
        self._role = role
        self._worker_concurrency = worker_concurrency
        self._heartbeat_interval_seconds = heartbeat_interval_seconds
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        """Register before the process starts accepting or claiming work."""

        if self._task is not None:
            return
        await self._registry.register(
            self._node_id,
            self._role,
            self._worker_concurrency,
        )
        self._stop.clear()
        self._task = asyncio.create_task(
            self._run(),
            name=f"runtime-node-heartbeat:{self._node_id}",
        )

    async def close(self) -> None:
        """Stop heartbeat and retain an explicit stopped record."""

        self._stop.set()
        task, self._task = self._task, None
        if task is not None:
            await task
        await self._registry.stop(self._node_id)

    async def begin_drain(self) -> bool:
        """Publish graceful Worker scale-in before waiting for active work."""

        if self._role not in {"worker", "api_worker"}:
            return False
        return await self._registry.drain(self._node_id)

    async def _run(self) -> None:
        while True:
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self._heartbeat_interval_seconds,
                )
                return
            except TimeoutError:
                try:
                    active = await self._registry.heartbeat(self._node_id)
                except asyncio.CancelledError:
                    raise
                except Exception as error:
                    # Database failover must not permanently kill liveness
                    # reporting. Readiness still fails while SQL is unavailable.
                    logger.warning(
                        "Runtime node %s heartbeat failed with %s; retrying",
                        self._node_id,
                        type(error).__name__,
                    )
                    continue
                if not active:
                    logger.error("Runtime node %s registration is no longer active", self._node_id)
                    return
