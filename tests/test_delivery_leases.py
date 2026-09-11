from __future__ import annotations

from datetime import UTC, datetime, timedelta

from trpc_service.runtime import InMemoryDeliveryLedger


class Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 1, 1, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value

    def advance(self, seconds: int) -> None:
        self.value += timedelta(seconds=seconds)


def _begin(ledger: InMemoryDeliveryLedger, capability: str, owner: str = "dispatcher-a"):
    return ledger.begin(
        tenant_id="tenant-a",
        outbox_id="outbox-a",
        session_id="session-a",
        channel_binding_id="binding-a",
        capability=capability,  # type: ignore[arg-type]
        request_hash="reply-hash",
        trace_id="trace-a",
        owner=owner,
        lease_seconds=5,
    )


def test_active_delivery_lease_prevents_a_reclaimed_entry_from_resending() -> None:
    clock = Clock()
    ledger = InMemoryDeliveryLedger(now=clock.now)

    first = _begin(ledger, "non_retriable")
    reclaimed = _begin(ledger, "non_retriable", owner="dispatcher-b")

    assert first.status == "sending"
    assert first.lease_acquired is True
    assert reclaimed.attempt_no == first.attempt_no
    assert reclaimed.lease_acquired is False
    assert reclaimed.lease_owner == "dispatcher-a"


def test_expired_delivery_attempts_follow_their_capability_contract() -> None:
    clock = Clock()

    idempotent = InMemoryDeliveryLedger(now=clock.now)
    original = _begin(idempotent, "idempotent")
    clock.advance(6)
    retry = _begin(idempotent, "idempotent", owner="dispatcher-b")
    assert retry.status == "sending"
    assert retry.lease_acquired is True
    assert retry.attempt_no == original.attempt_no + 1
    assert retry.provider_idempotency_key == original.provider_idempotency_key
    # The expired owner cannot overwrite the new attempt's recovery decision.
    assert idempotent.finish(original, status="accepted").status == "failed"

    queryable = InMemoryDeliveryLedger(now=clock.now)
    query = _begin(queryable, "queryable")
    clock.advance(6)
    reconciling = _begin(queryable, "queryable", owner="dispatcher-b")
    assert reconciling.status == "reconciling"
    assert reconciling.lease_acquired is True
    assert reconciling.attempt_no == query.attempt_no
    assert queryable.finish(query, status="accepted").status == "reconciling"

    non_retriable = InMemoryDeliveryLedger(now=clock.now)
    non_idempotent = _begin(non_retriable, "non_retriable")
    clock.advance(6)
    review = _begin(non_retriable, "non_retriable", owner="dispatcher-b")
    assert review.status == "manual_review"
    assert review.lease_acquired is False
    assert review.attempt_no == non_idempotent.attempt_no


def test_unknown_idempotent_delivery_stays_pending_for_a_safe_retry() -> None:
    clock = Clock()
    ledger = InMemoryDeliveryLedger(now=clock.now)

    first = _begin(ledger, "idempotent")
    unknown = ledger.finish(first, status="unknown", error_code="lost_ack")
    retry = _begin(ledger, "idempotent", owner="dispatcher-b")

    assert unknown.status == "unknown"
    assert retry.status == "sending"
    assert retry.attempt_no == first.attempt_no + 1
    assert retry.provider_idempotency_key == first.provider_idempotency_key
