from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from trpc_service.agent.session_recovery import SessionRecoveryService
from trpc_service.metrics.prometheus import (
    SESSION_READY_ACK_LATENCY,
    SESSION_READY_ACKS,
    SESSION_READY_CLAIM_LATENCY,
    SESSION_READY_CLAIMS,
    SESSION_READY_LEASE_RENEWS,
    SESSION_READY_RECEIVE_LATENCY,
    SESSION_READY_RECEIVES,
    SESSION_READY_RECOVERY_HEALTH,
    observe_session_ready_claim,
    observe_session_ready_lease_renewal,
)
from trpc_service.queue.session_ready import (
    SessionReady,
    SessionReadyDelivery,
    SessionReadyQueue,
)
from trpc_service.storage.models import MailboxClaimStatus
from trpc_service.storage.protocols import FencingConflict


def _notice() -> SessionReady:
    return SessionReady(
        event_id="event-metrics",
        tenant_id="tenant-metrics",
        session_id="session-metrics",
        generation=1,
        priority=0,
        trace_id="trace-metrics",
        created_at=datetime(2026, 8, 23, 12, 0, tzinfo=UTC),
    )


class _Redis:
    def __init__(self) -> None:
        self.read_error: Exception | None = None
        self.ack_result = 0
        self.ack_error: Exception | None = None

    async def xreadgroup(self, *args: object, **kwargs: object) -> list[object]:
        if self.read_error is not None:
            raise self.read_error
        return []

    async def xack(self, *args: object, **kwargs: object) -> int:
        if self.ack_error is not None:
            raise self.ack_error
        return self.ack_result


def _counter(metric, **labels: str) -> float:
    return metric.labels(**labels)._value.get()


def _histogram_count(metric) -> float:
    sample_name = f"{metric._name}_count"
    return sum(sample.value for sample in metric.collect()[0].samples if sample.name == sample_name)


@pytest.mark.asyncio
async def test_session_ready_transport_metrics_distinguish_empty_and_error() -> None:
    redis = _Redis()
    queue = SessionReadyQueue(redis)
    delivery = SessionReadyDelivery("1-0", _notice())

    receive_latency_before = _histogram_count(SESSION_READY_RECEIVE_LATENCY)
    receive_before = _counter(SESSION_READY_RECEIVES, outcome="empty")
    await queue.receive_new(consumer="worker", count=1, block_ms=0)
    assert _counter(SESSION_READY_RECEIVES, outcome="empty") == receive_before + 1
    assert _histogram_count(SESSION_READY_RECEIVE_LATENCY) == receive_latency_before + 1

    redis.read_error = RuntimeError("redis unavailable")
    receive_error_before = _counter(SESSION_READY_RECEIVES, outcome="error")
    with pytest.raises(RuntimeError, match="redis unavailable"):
        await queue.receive_new(consumer="worker", count=1, block_ms=0)
    assert _counter(SESSION_READY_RECEIVES, outcome="error") == receive_error_before + 1

    ack_latency_before = _histogram_count(SESSION_READY_ACK_LATENCY)
    ack_before = _counter(SESSION_READY_ACKS, outcome="missing")
    assert await queue.ack(delivery) is False
    assert _counter(SESSION_READY_ACKS, outcome="missing") == ack_before + 1
    assert _histogram_count(SESSION_READY_ACK_LATENCY) == ack_latency_before + 1

    redis.ack_error = RuntimeError("redis unavailable")
    ack_error_before = _counter(SESSION_READY_ACKS, outcome="error")
    with pytest.raises(RuntimeError, match="redis unavailable"):
        await queue.ack(delivery)
    assert _counter(SESSION_READY_ACKS, outcome="error") == ack_error_before + 1


@pytest.mark.asyncio
async def test_session_ready_repository_decorators_record_status_and_fencing() -> None:
    claim_before = _counter(SESSION_READY_CLAIMS, status="claimed")
    fencing_before = _counter(SESSION_READY_CLAIMS, status="fencing_conflict")

    @observe_session_ready_claim
    async def claim() -> SimpleNamespace:
        return SimpleNamespace(status=MailboxClaimStatus.CLAIMED)

    @observe_session_ready_claim
    async def stale_claim() -> SimpleNamespace:
        raise FencingConflict("stale")

    assert (await claim()).status == MailboxClaimStatus.CLAIMED
    with pytest.raises(FencingConflict, match="stale"):
        await stale_claim()
    assert _counter(SESSION_READY_CLAIMS, status="claimed") == claim_before + 1
    assert _counter(SESSION_READY_CLAIMS, status="fencing_conflict") == fencing_before + 1
    assert _histogram_count(SESSION_READY_CLAIM_LATENCY) >= 2

    renew_before = _counter(SESSION_READY_LEASE_RENEWS, outcome="success")

    @observe_session_ready_lease_renewal
    async def renew() -> None:
        return None

    await renew()
    assert _counter(SESSION_READY_LEASE_RENEWS, outcome="success") == renew_before + 1


@pytest.mark.asyncio
async def test_recovery_health_gauge_tracks_each_component_failure() -> None:
    class Repository:
        fail = False

        async def sweep_expired_leases(self, *, owner_id: str, limit: int) -> int:
            if self.fail:
                raise RuntimeError("database unavailable")
            return 1

        async def schedule_retries(self, *, owner_id: str, limit: int) -> int:
            return 0

        async def reconcile_sessions(self, *, owner_id: str, limit: int) -> int:
            return 0

    repository = Repository()
    service = SessionRecoveryService(repository, owner_id="metrics-test", batch_size=1)
    await service.run_once()
    assert SESSION_READY_RECOVERY_HEALTH.labels(component="lease_sweeper")._value.get() == 1

    repository.fail = True
    await service.run_once()
    assert SESSION_READY_RECOVERY_HEALTH.labels(component="lease_sweeper")._value.get() == 0


@pytest.mark.asyncio
async def test_session_ready_metrics_do_not_include_unbounded_identifiers() -> None:
    # The transport API accepts identifiers, but metric label names are fixed
    # and never include tenant/session/event values.
    redis = _Redis()
    queue = SessionReadyQueue(redis)
    await queue.receive_new(consumer="tenant-secret-session-secret", count=1, block_ms=0)
    names = {label for label in SESSION_READY_RECEIVES._labelnames}
    assert names == {"outcome"}
    assert "tenant-secret-session-secret" not in names
