"""Repair non-terminal requests left behind by process crashes."""

from __future__ import annotations

from trpc_service.gateway.models import RequestState
from trpc_service.gateway.queue import AgentTaskEnvelope
from trpc_service.gateway.queue import AgentTaskQueue
from trpc_service.gateway.requests import RequestStore


class RequestRepairService:
    """Requeue admissions that were persisted but never confirmed queued.

    Redis Streams owns recovery after a request reaches queued/running state.
    Keeping the routine PostgreSQL scan limited to reserved records prevents a
    second Worker from enqueueing a duplicate while another Worker reclaims the
    original pending Stream delivery. Rebuilding after total Redis data loss is
    an explicit disaster-recovery operation, not this continuous repair loop.
    """

    def __init__(self, requests: RequestStore, queue: AgentTaskQueue) -> None:
        self._requests = requests
        self._queue = queue

    async def repair_stale(self, *, older_than_seconds: int = 300, limit: int = 100) -> int:
        repaired = 0
        records = await self._requests.claim_stale(older_than_seconds=older_than_seconds, limit=limit)
        for record in records:
            if record.request is None or not record.request.metadata.get("admission_complete"):
                await self._requests.transition(record.tenant_id,
                                                record.request_id,
                                                RequestState.FAILED,
                                                error_code="admission_incomplete",
                                                retryable=False)
                continue
            await self._queue.enqueue(
                AgentTaskEnvelope(
                    request=record.request,
                    idempotency_key=record.idempotency_key,
                    attempts=record.attempts,
                ))
            await self._requests.increment_recovery_count(record.tenant_id, record.request_id)
            await self._requests.transition(record.tenant_id,
                                            record.request_id,
                                            RequestState.QUEUED,
                                            error_code="requeued_by_repair",
                                            retryable=True)
            repaired += 1
        return repaired
