"""PostgreSQL-backed recovery loops for the session mailbox.

The recovery role deliberately contains no SQL.  It coordinates three
repository operations which are safe to run concurrently on multiple service
instances because the repository owns row locking, fencing, and tenant
scoping:

* :class:`LeaseSweeper` releases expired session leases;
* :class:`RetryScheduler` makes due retry work visible again; and
* :class:`SessionReconciler` repairs mailbox state that is durable in
  PostgreSQL but is not reflected in a derived queue or projection.

The repository methods are intentionally small and explicit.  The current
PostgreSQL repository is the integration point for these methods; keeping the
contract here lets the recovery service and its deterministic tests land
without embedding a second persistence implementation in the role.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Protocol
from uuid import uuid4

from prometheus_client import Counter, Gauge, Histogram

from trpc_service.metrics.prometheus import SESSION_READY_RECOVERY_HEALTH

logger = logging.getLogger(__name__)


class MailboxRecoveryRepository(Protocol):
    """Minimal PG repository contract used by the recovery role.

    Implementations must apply tenant/RLS context and use bounded,
    row-locked transactions internally.  For mailbox recovery, the lock
    order is part of this contract: lock the mailbox row first, then lock the
    matching ``sessions`` row and treat its unexpired owner/epoch/expiry as
    authoritative.  An active session lease must make recovery a no-op.  Any
    ready transition and its ``session.ready.v2`` outbox repair must commit in
    the same transaction and the returned count must not exceed ``limit``.
    ``owner_id`` makes concurrent recovery instances distinguishable in
    audit/claim records; it is not a tenant identifier.
    """

    async def sweep_expired_leases(self, *, owner_id: str, limit: int) -> int: ...

    async def schedule_retries(self, *, owner_id: str, limit: int) -> int: ...

    async def reconcile_sessions(self, *, owner_id: str, limit: int) -> int: ...


RECOVERY_COMPONENTS = ("lease_sweeper", "retry_scheduler", "session_reconciler")

RECOVERY_RUNS = Counter(
    "trpc_session_recovery_runs_total",
    "Session mailbox recovery loop runs by component and outcome.",
    ("component", "outcome"),
)
RECOVERY_ITEMS = Counter(
    "trpc_session_recovery_items_total",
    "Session mailbox records handled by recovery components.",
    ("component",),
)
RECOVERY_ERRORS = Counter(
    "trpc_session_recovery_errors_total",
    "Session mailbox recovery errors by component and exception type.",
    ("component", "error_type"),
)
RECOVERY_DURATION = Histogram(
    "trpc_session_recovery_seconds",
    "Duration of one session mailbox recovery operation.",
    ("component",),
    buckets=(0.001, 0.01, 0.05, 0.1, 0.5, 1, 5, 15, 60),
)
RECOVERY_LAST_RUN = Gauge(
    "trpc_session_recovery_last_run_timestamp_seconds",
    "Unix timestamp of the most recent recovery operation.",
    ("component",),
)


@dataclass(frozen=True, slots=True)
class RecoveryCycleResult:
    """Machine-readable result of one bounded recovery cycle."""

    counts: Mapping[str, int]
    failures: Mapping[str, str]

    @property
    def total(self) -> int:
        return sum(self.counts.values())

    @property
    def status(self) -> str:
        return "fail" if self.failures else "pass"


class _RecoveryLoop:
    component: str

    def __init__(
        self, repository: MailboxRecoveryRepository, *, owner_id: str, batch_size: int
    ) -> None:
        if batch_size < 1:
            raise ValueError("recovery batch_size must be positive")
        self._repository = repository
        self._owner_id = owner_id
        self._batch_size = batch_size

    async def run_once(self) -> int:
        raise NotImplementedError

    async def _operation(self, operation: Callable[..., Awaitable[int]]) -> int:
        started = time.perf_counter()
        try:
            count = await operation(owner_id=self._owner_id, limit=self._batch_size)
            if isinstance(count, bool) or not isinstance(count, int) or count < 0:
                raise TypeError("recovery repository operation must return a non-negative int")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            RECOVERY_RUNS.labels(component=self.component, outcome="error").inc()
            SESSION_READY_RECOVERY_HEALTH.labels(component=self.component).set(0)
            RECOVERY_ERRORS.labels(component=self.component, error_type=type(error).__name__).inc()
            raise
        else:
            RECOVERY_RUNS.labels(component=self.component, outcome="success").inc()
            SESSION_READY_RECOVERY_HEALTH.labels(component=self.component).set(1)
            if count:
                RECOVERY_ITEMS.labels(component=self.component).inc(count)
            return count
        finally:
            RECOVERY_DURATION.labels(component=self.component).observe(
                time.perf_counter() - started
            )
            RECOVERY_LAST_RUN.labels(component=self.component).set(time.time())


class LeaseSweeper(_RecoveryLoop):
    """Release expired session leases and make their turns recoverable."""

    component = "lease_sweeper"

    async def run_once(self) -> int:
        return await self._operation(self._repository.sweep_expired_leases)


class RetryScheduler(_RecoveryLoop):
    """Make due retry records visible to the normal mailbox pipeline."""

    component = "retry_scheduler"

    async def run_once(self) -> int:
        return await self._operation(self._repository.schedule_retries)


class SessionReconciler(_RecoveryLoop):
    """Repair durable session/mailbox state missed by an earlier worker."""

    component = "session_reconciler"

    async def run_once(self) -> int:
        return await self._operation(self._repository.reconcile_sessions)


class SessionRecoveryService:
    """Run the three bounded recovery loops with cooperative shutdown."""

    def __init__(
        self,
        repository: MailboxRecoveryRepository,
        *,
        owner_id: str | None = None,
        batch_size: int = 25,
        poll_seconds: float = 5.0,
    ) -> None:
        if poll_seconds <= 0:
            raise ValueError("recovery poll_seconds must be positive")
        self._owner_id = owner_id or f"session-recovery-{uuid4()}"
        self._poll_seconds = poll_seconds
        self._stop_event = asyncio.Event()
        self._loops = (
            LeaseSweeper(repository, owner_id=self._owner_id, batch_size=batch_size),
            RetryScheduler(repository, owner_id=self._owner_id, batch_size=batch_size),
            SessionReconciler(repository, owner_id=self._owner_id, batch_size=batch_size),
        )
        self._tasks: tuple[asyncio.Task[None], ...] = ()

    @property
    def owner_id(self) -> str:
        return self._owner_id

    @property
    def loops(self) -> tuple[_RecoveryLoop, ...]:
        """Expose loop objects for health checks and deterministic tests."""

        return self._loops

    async def run_once(self) -> RecoveryCycleResult:
        """Run each component exactly once, retaining partial failures."""

        counts: dict[str, int] = {}
        failures: dict[str, str] = {}
        for loop in self._loops:
            try:
                counts[loop.component] = await loop.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                counts[loop.component] = 0
                failures[loop.component] = type(error).__name__
                logger.warning(
                    "session recovery component failed",
                    extra={"component": loop.component, "error_type": type(error).__name__},
                )
        return RecoveryCycleResult(counts=counts, failures=failures)

    async def _run_loop(self, loop: _RecoveryLoop) -> None:
        while not self._stop_event.is_set():
            try:
                await loop.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                logger.warning(
                    "session recovery loop will retry",
                    extra={"component": loop.component, "error_type": type(error).__name__},
                )
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._poll_seconds)
            except TimeoutError:
                continue

    async def run(self) -> None:
        """Run all components until :meth:`stop` is called or cancelled."""

        if self._tasks:
            raise RuntimeError("session recovery service is already running")
        self._stop_event.clear()
        tasks = tuple(
            asyncio.create_task(self._run_loop(loop), name=f"recovery:{loop.component}")
            for loop in self._loops
        )
        self._tasks = tasks
        try:
            await self._stop_event.wait()
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._tasks = ()

    def stop(self) -> None:
        """Request cooperative shutdown; safe to call repeatedly."""

        self._stop_event.set()


__all__ = [
    "RECOVERY_COMPONENTS",
    "RECOVERY_DURATION",
    "RECOVERY_ERRORS",
    "RECOVERY_ITEMS",
    "RECOVERY_LAST_RUN",
    "RECOVERY_RUNS",
    "LeaseSweeper",
    "MailboxRecoveryRepository",
    "RecoveryCycleResult",
    "RetryScheduler",
    "SessionReconciler",
    "SessionRecoveryService",
]
