"""Fair, stop-aware supervision loops for Worker and Outbox processes."""

from __future__ import annotations

import asyncio
import contextlib
import functools
import logging
import time
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from typing import cast

from trpc_service.log import bind_log_context
from trpc_service.runtime.catalog import TenantCatalog

LOGGER = logging.getLogger(__name__)
PollTenant = Callable[[str], Awaitable[bool]]


class FairTenantRing:
    """A mutable round-robin ring that preserves the current scheduling cursor."""

    def __init__(self) -> None:
        self._tenants: deque[str] = deque()

    def update(self, tenant_ids: Iterable[str]) -> None:
        """Remove stale tenants and append newly discovered tenants deterministically."""

        snapshot = frozenset(tenant_ids)
        retained = deque(tenant_id for tenant_id in self._tenants if tenant_id in snapshot)
        known = set(retained)
        retained.extend(sorted(snapshot - known))
        self._tenants = retained

    def take(self) -> str | None:
        """Return one tenant and rotate it to the back of the queue."""

        if not self._tenants:
            return None
        tenant_id = self._tenants.popleft()
        self._tenants.append(tenant_id)
        return tenant_id

    def __len__(self) -> int:
        return len(self._tenants)

    def snapshot(self) -> tuple[str, ...]:
        """Expose ordering for deterministic tests and diagnostics."""

        return tuple(self._tenants)


class ExponentialBackoff:
    """Bounded exponential backoff with an explicit reset point."""

    def __init__(self, initial: float, maximum: float) -> None:
        if initial <= 0 or maximum <= 0 or initial > maximum:
            raise ValueError("backoff bounds must satisfy 0 < initial <= maximum")
        self._initial = initial
        self._maximum = maximum
        self._current = initial

    def take(self) -> float:
        """Return the current delay and advance to the next bounded value."""

        delay = self._current
        self._current = min(self._current * 2, self._maximum)
        return delay

    def reset(self) -> None:
        """Reset after observed useful work."""

        self._current = self._initial


class PollingSupervisor:
    """Continuously poll every active tenant with fair rotation and idle backoff.

    The catalog's last successful snapshot remains usable during a transient catalog
    failure.  A shutdown signal races the current poll operation; if shutdown wins,
    the operation is cancelled and awaited so Worker claim cleanup can run before
    process resources are closed.
    """

    def __init__(
        self,
        *,
        role: str,
        catalog: TenantCatalog,
        poll_tenant: PollTenant,
        catalog_refresh_seconds: float,
        idle_backoff_initial_seconds: float,
        idle_backoff_max_seconds: float,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if not role or len(role) > 32:
            raise ValueError("role must contain 1..32 characters")
        if catalog_refresh_seconds <= 0:
            raise ValueError("catalog_refresh_seconds must be positive")
        self._role = role
        self._catalog = catalog
        self._poll_tenant = poll_tenant
        self._catalog_refresh_seconds = catalog_refresh_seconds
        self._monotonic = monotonic
        self._ring = FairTenantRing()
        self._backoff = ExponentialBackoff(
            idle_backoff_initial_seconds,
            idle_backoff_max_seconds,
        )

    async def run(self, stop: asyncio.Event) -> None:
        """Run until ``stop`` is set, without leaking an in-flight poll task."""

        next_catalog_refresh = 0.0
        idle_visits = 0
        LOGGER.info("runtime_role_started", extra={"runtime_role": self._role})
        try:
            while not stop.is_set():
                now = self._monotonic()
                if now >= next_catalog_refresh:
                    try:
                        tenant_ids = await _interruptible(
                            self._catalog.list_active_tenant_ids,
                            stop,
                        )
                    except _StopRequestedError:
                        return
                    except Exception as error:
                        LOGGER.warning(
                            "tenant_catalog_refresh_failed",
                            extra={
                                "runtime_role": self._role,
                                "error_type": type(error).__name__,
                            },
                        )
                    else:
                        self._ring.update(tenant_ids)
                        idle_visits = min(idle_visits, len(self._ring))
                    next_catalog_refresh = self._monotonic() + self._catalog_refresh_seconds

                tenant_id = self._ring.take()
                if tenant_id is None:
                    await _wait_or_stop(stop, self._backoff.take())
                    continue

                try:
                    with bind_log_context(tenant_id=tenant_id):
                        activity = await _interruptible(
                            functools.partial(self._poll_tenant, tenant_id),
                            stop,
                        )
                except _StopRequestedError:
                    return
                except Exception as error:
                    activity = False
                    LOGGER.warning(
                        "tenant_poll_failed",
                        extra={
                            "runtime_role": self._role,
                            "error_type": type(error).__name__,
                        },
                    )

                if activity:
                    idle_visits = 0
                    self._backoff.reset()
                    continue

                idle_visits += 1
                if idle_visits >= len(self._ring):
                    idle_visits = 0
                    await _wait_or_stop(stop, self._backoff.take())
        finally:
            LOGGER.info("runtime_role_stopped", extra={"runtime_role": self._role})


class _StopRequestedError(Exception):
    """Internal control-flow marker raised after an awaited operation is cancelled."""


async def _interruptible[T](operation: Callable[[], Awaitable[T]], stop: asyncio.Event) -> T:
    if stop.is_set():
        raise _StopRequestedError
    operation_task: asyncio.Future[object] = asyncio.ensure_future(
        cast(Awaitable[object], operation())
    )
    stop_task: asyncio.Future[object] = asyncio.ensure_future(stop.wait())
    try:
        done, _ = await asyncio.wait(
            {operation_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if stop_task in done:
            operation_task.cancel()
            await asyncio.gather(operation_task, return_exceptions=True)
            raise _StopRequestedError
        stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)
        return cast(T, await operation_task)
    except BaseException:
        for task in (operation_task, stop_task):
            if not task.done():
                task.cancel()
        await asyncio.gather(operation_task, stop_task, return_exceptions=True)
        raise


async def _wait_or_stop(stop: asyncio.Event, delay: float) -> None:
    if stop.is_set():
        return
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), timeout=delay)
