"""RoutedWorkerClient: composes multiple HttpWorkerClients with health-aware routing."""

from __future__ import annotations

from collections.abc import AsyncIterator, Mapping

from trpc_service.gateway.client import WorkerClient, WorkerClientError
from trpc_service.gateway.health import WorkerHealthManager
from trpc_service.gateway.routing import (
    RendezvousRouter,
    WorkerEndpoint,
    WorkerRouteKey,
)
from trpc_service.transport.models import (
    WorkerApprovalResult,
    WorkerApprovalTask,
    WorkerChatResult,
    WorkerErrorCode,
    WorkerEvent,
    WorkerTask,
)

_CONNECTIVITY_CODES = frozenset({
    WorkerErrorCode.WORKER_UNAVAILABLE,
    WorkerErrorCode.WORKER_TIMEOUT,
    WorkerErrorCode.INVALID_WORKER_RESPONSE,
})


class RoutedWorkerClient:
    """Implements WorkerClient by routing to a single healthy endpoint per request.

    Manages the full lifecycle of underlying clients and the health manager.
    """

    def __init__(
        self,
        clients: Mapping[str, WorkerClient],
        router: RendezvousRouter,
        health_manager: WorkerHealthManager,
    ) -> None:
        self._clients = clients
        self._router = router
        self._health = health_manager
        self._started = False
        self._closed = False

    async def start(self) -> None:
        """Start all underlying clients, then health manager. Idempotent.

        If any client fails to start, already-started clients are closed (rollback).
        """
        if self._started:
            return
        self._started = True
        started_clients: list[WorkerClient] = []
        try:
            for client in self._clients.values():
                if hasattr(client, "start"):
                    await client.start()
                    started_clients.append(client)
            await self._health.start()
        except Exception:
            for client in started_clients:
                if hasattr(client, "close"):
                    try:
                        await client.close()
                    except Exception:
                        pass
            self._started = False
            raise

    async def chat(self, task: WorkerTask) -> WorkerChatResult:
        if self._closed:
            raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE)
        endpoint, client = self._select(task)
        try:
            return await client.chat(task)
        except WorkerClientError as exc:
            if exc.code in _CONNECTIVITY_CODES:
                self._health.record_passive_failure(endpoint, exc.code)
            raise

    async def stream(self, task: WorkerTask) -> AsyncIterator[WorkerEvent]:
        if self._closed:
            raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE)
        endpoint, client = self._select(task)
        try:
            async for event in client.stream(task):
                yield event
        except WorkerClientError as exc:
            if exc.code in _CONNECTIVITY_CODES:
                self._health.record_passive_failure(endpoint, exc.code)
            raise

    async def decide(self, task: WorkerApprovalTask) -> WorkerApprovalResult:
        """Route one decision to exactly one endpoint; never retry (6A2)."""
        if self._closed:
            raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE)
        endpoint, client = self._select(task)
        try:
            return await client.decide(task)
        except WorkerClientError as exc:
            if exc.code in _CONNECTIVITY_CODES:
                self._health.record_passive_failure(endpoint, exc.code)
            raise

    async def close(self) -> None:
        """Close health manager, then all underlying clients. Idempotent.

        Best-effort: continues closing remaining clients even if one fails.
        """
        if self._closed:
            return
        self._closed = True
        try:
            await self._health.close()
        except Exception:
            pass
        for client in self._clients.values():
            if hasattr(client, "close"):
                try:
                    await client.close()
                except Exception:
                    pass

    def _select(self, task: WorkerTask | WorkerApprovalTask) -> tuple[WorkerEndpoint, WorkerClient]:
        healthy = self._health.healthy_endpoints()
        if not healthy:
            raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE)
        route_key = WorkerRouteKey.from_task(task)
        ranked = self._router.rank(route_key, healthy)
        chosen = ranked[0]
        client = self._clients.get(chosen.endpoint_id)
        if client is None:
            raise WorkerClientError(WorkerErrorCode.WORKER_UNAVAILABLE)
        return chosen, client


__all__ = ["RoutedWorkerClient"]
