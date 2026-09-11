"""Worker endpoint health management: active probing and passive circuit breaking."""

from __future__ import annotations

import asyncio
import enum
import logging
from collections.abc import Awaitable, Callable

from trpc_service.gateway.routing import WorkerEndpoint
from trpc_service.transport.models import WorkerErrorCode

logger = logging.getLogger(__name__)

_CONNECTIVITY_CODES = frozenset({
    WorkerErrorCode.WORKER_UNAVAILABLE,
    WorkerErrorCode.WORKER_TIMEOUT,
    WorkerErrorCode.INVALID_WORKER_RESPONSE,
})


class _HealthState(enum.Enum):
    UNKNOWN = "unknown"
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"


class _EndpointStatus:

    def __init__(self, endpoint: WorkerEndpoint) -> None:
        self.endpoint = endpoint
        self.state: _HealthState = _HealthState.UNKNOWN
        self.consecutive_failures: int = 0
        self.consecutive_successes: int = 0


ProbeFn = Callable[[WorkerEndpoint, float], Awaitable[bool]]


class WorkerHealthManager:
    """Maintains health state for Worker endpoints via active probing and passive signals."""

    def __init__(
        self,
        endpoints: list[WorkerEndpoint],
        probe_fn: ProbeFn,
        interval_seconds: float,
        timeout_seconds: float,
        failure_threshold: int,
        recovery_threshold: int,
    ) -> None:
        self._statuses: dict[str, _EndpointStatus] = {ep.endpoint_id: _EndpointStatus(ep) for ep in endpoints}
        self._probe_fn = probe_fn
        self._interval = interval_seconds
        self._timeout = timeout_seconds
        self._failure_threshold = failure_threshold
        self._recovery_threshold = recovery_threshold
        self._probe_task: asyncio.Task[None] | None = None
        self._closed = False

    async def start(self) -> None:
        """Run initial probes for all endpoints, then start background probing."""
        await asyncio.gather(*(self._probe_once(status) for status in self._statuses.values()))
        self._probe_task = asyncio.create_task(self._background_loop())

    def healthy_endpoints(self) -> list[WorkerEndpoint]:
        """Return endpoints currently in HEALTHY state."""
        return [s.endpoint for s in self._statuses.values() if s.state == _HealthState.HEALTHY]

    def record_passive_failure(self, endpoint: WorkerEndpoint, code: WorkerErrorCode) -> None:
        """Record a passive failure signal from a request-level error."""
        if code not in _CONNECTIVITY_CODES:
            return
        status = self._statuses.get(endpoint.endpoint_id)
        if status is None:
            return
        status.consecutive_successes = 0
        status.consecutive_failures += 1
        if status.state != _HealthState.UNHEALTHY:
            status.state = _HealthState.UNHEALTHY
            logger.warning("endpoint %s marked unhealthy (passive)", endpoint.endpoint_id)

    async def close(self) -> None:
        """Stop background probing. Idempotent."""
        if self._closed:
            return
        self._closed = True
        if self._probe_task is not None:
            self._probe_task.cancel()
            try:
                await self._probe_task
            except (asyncio.CancelledError, Exception):
                pass
            self._probe_task = None

    async def _probe_endpoint(self, endpoint: WorkerEndpoint) -> None:
        """Probe a single endpoint and update its status (exposed for testing)."""
        status = self._statuses.get(endpoint.endpoint_id)
        if status is None:
            return
        await self._probe_once(status)

    async def _probe_once(self, status: _EndpointStatus) -> None:
        try:
            ok = await self._probe_fn(status.endpoint, self._timeout)
        except Exception:
            ok = False
        if ok:
            status.consecutive_failures = 0
            status.consecutive_successes += 1
            if status.state == _HealthState.UNKNOWN:
                status.state = _HealthState.HEALTHY
                logger.info("endpoint %s healthy", status.endpoint.endpoint_id)
            elif status.state == _HealthState.UNHEALTHY:
                if status.consecutive_successes >= self._recovery_threshold:
                    status.state = _HealthState.HEALTHY
                    # Recovery is a topology state transition that must remain
                    # visible with the service's default WARNING log level.
                    logger.warning("endpoint %s recovered", status.endpoint.endpoint_id)
        else:
            status.consecutive_successes = 0
            status.consecutive_failures += 1
            if status.state == _HealthState.UNKNOWN:
                status.state = _HealthState.UNHEALTHY
                logger.warning("endpoint %s unhealthy (initial probe)", status.endpoint.endpoint_id)
            elif status.state == _HealthState.HEALTHY:
                if status.consecutive_failures >= self._failure_threshold:
                    status.state = _HealthState.UNHEALTHY
                    logger.warning("endpoint %s unhealthy (threshold)", status.endpoint.endpoint_id)

    async def _background_loop(self) -> None:
        try:
            while not self._closed:
                await asyncio.sleep(self._interval)
                if self._closed:
                    break
                for status in self._statuses.values():
                    await self._probe_once(status)
        except asyncio.CancelledError:
            return


__all__ = ["WorkerHealthManager"]
