"""Decision-table tests for the in-memory v2 runtime repository.

These tests deliberately exercise the same outcomes that the PostgreSQL
repository exposes to the worker: an authenticated wake-up may claim exactly
one head item, while stale, running, duplicate, and malformed states never
produce an executable lease.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from tests.conftest import envelope, repository
from trpc_service.config.settings import SchedulerVersion
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import (
    MailboxClaimStatus,
    MailboxStatus,
    OutboxRecord,
    TurnCommit,
)
from trpc_service.storage.protocols import FencingConflict


async def _accepted(repo, message_id: str = "memory-branch"):
    runtime = TenantRuntime(
        repo,
        routing_key=b"memory-v2-branch-coverage" * 2,
        scheduler_version=SchedulerVersion.V2,
    )
    return await runtime.accept("binding-unpredictable-a", envelope(message_id))


async def _claimed(repo, message_id: str = "memory-branch-claimed"):
    accepted = await _accepted(repo, message_id)
    mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert mailbox is not None and repo.mailbox.outbox
    claim = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert claim.execution_lease is not None
    return accepted, claim.execution_lease


def _ready_event(
    *, tenant_id: str, session_id: str, event_type: str, payload: dict[str, object]
) -> OutboxRecord:
    return OutboxRecord(
        outbox_id=str(uuid4()),
        tenant_id=tenant_id,
        event_type=event_type,
        aggregate_id=session_id,
        payload=payload,
    )


@pytest.mark.asyncio
async def test_claim_rejects_invalid_ready_event_and_missing_mailbox() -> None:
    repo = repository()
    # No mailbox exists: the valid event still cannot manufacture a claim.
    event = _ready_event(
        tenant_id="tenant-a",
        session_id="session-without-mailbox",
        event_type="session.ready.v2",
        payload={"generation": 1},
    )
    repo.mailbox._outbox.append(event)
    claim = await repo.claim_session_ready(
        "tenant-a",
        "session-without-mailbox",
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
        expected_event_id=event.outbox_id,
    )
    # The generation in a durable wake-up is not enough to invent a missing
    # mailbox; the in-memory generation is zero, so this is stale.
    assert claim.status == MailboxClaimStatus.STALE
    assert claim.mailbox.status == MailboxStatus.IDLE

    # All of these are rejected before the mailbox can be claimed.  In
    # particular, bool is not an integer generation and generation zero is
    # never a valid durable wake-up.
    accepted = await _accepted(repo, "memory-invalid-ready")
    invalid = (
        _ready_event(
            tenant_id="tenant-other",
            session_id=accepted.context.session_id,
            event_type="session.ready.v2",
            payload={"generation": 1},
        ),
        _ready_event(
            tenant_id="tenant-a",
            session_id=accepted.context.session_id,
            event_type="other.event",
            payload={"generation": 1},
        ),
        _ready_event(
            tenant_id="tenant-a",
            session_id="different-session",
            event_type="session.ready.v2",
            payload={"generation": 1},
        ),
        _ready_event(
            tenant_id="tenant-a",
            session_id=accepted.context.session_id,
            event_type="session.ready.v2",
            payload={"generation": True},
        ),
        _ready_event(
            tenant_id="tenant-a",
            session_id=accepted.context.session_id,
            event_type="session.ready.v2",
            payload={"generation": 0},
        ),
    )
    for bad in invalid:
        repo.mailbox._outbox.append(bad)
        result = await repo.claim_session_ready(
            accepted.context.tenant_id,
            accepted.context.session_id,
            owner_id="worker-a",
            lease_for=timedelta(seconds=30),
            expected_event_id=bad.outbox_id,
        )
        assert result.status == MailboxClaimStatus.STALE


@pytest.mark.asyncio
async def test_claim_returns_stale_running_duplicate_and_corrupt_inbound_outcomes() -> None:
    repo = repository()
    accepted = await _accepted(repo, "memory-outcomes")
    mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert mailbox is not None
    event_id = repo.mailbox.outbox[-1].outbox_id

    stale = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation + 1,
        expected_event_id=event_id,
    )
    assert stale.status == MailboxClaimStatus.STALE

    first = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=event_id,
    )
    assert first.claimed
    running = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-b",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=event_id,
    )
    assert running.status == MailboxClaimStatus.RUNNING

    # A wake-up with no durable mailbox item is an explicit EMPTY result.
    empty = await repo.claim_session_ready(
        "tenant-a",
        "missing-session",
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
    )
    assert empty.status == MailboxClaimStatus.EMPTY

    # A mailbox item without its authoritative inbound is corruption, not a
    # reason to construct a fake turn.
    corrupt = repository()
    await corrupt.accept_mailbox("tenant-a", "corrupt-session", str(uuid4()))
    corrupt_claim = await corrupt.claim_session_ready(
        "tenant-a", "corrupt-session", owner_id="worker-a", lease_for=timedelta(seconds=30)
    )
    assert corrupt_claim.status == MailboxClaimStatus.EMPTY
    assert corrupt_claim.lease is None

    # Redelivery after the inbound was committed is self-healed and marked as
    # duplicate rather than executed a second time.
    committed_repo = repository()
    committed, lease = await _claimed(committed_repo, "memory-committed")
    committed_repo._inbound_status[(committed.context.tenant_id, committed.inbound_id)] = (
        "committed"
    )
    committed_key = (committed.context.tenant_id, committed.context.session_id)
    committed_repo._leases[committed_key] = lease.model_copy(
        update={"expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    )
    committed_mailbox = committed_repo.mailbox._mailboxes[committed_key]
    committed_repo.mailbox._mailboxes[committed_key] = committed_mailbox.model_copy(
        update={"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    )
    duplicate = await committed_repo.claim_session_ready(
        committed.context.tenant_id,
        committed.context.session_id,
        owner_id="worker-b",
        lease_for=timedelta(seconds=30),
    )
    assert duplicate.status == MailboxClaimStatus.EMPTY
    assert duplicate.acceptance is not None and duplicate.acceptance.duplicate
    assert lease.inbound_id == committed.inbound_id


@pytest.mark.asyncio
async def test_memory_renew_commit_retry_and_fail_fence_every_transition() -> None:
    repo = repository()
    accepted, lease = await _claimed(repo, "memory-fence")
    key = (accepted.context.tenant_id, accepted.context.session_id)

    with pytest.raises(FencingConflict):
        await repo.renew_session_ready(
            lease.model_copy(update={"worker_id": "other-worker"}),
            lease_for=timedelta(seconds=30),
        )
    repo._mailbox_store._mailboxes.pop(key)
    with pytest.raises(FencingConflict):
        await repo.renew_session_ready(lease, lease_for=timedelta(seconds=30))

    # Rebuild a clean claim for the remaining transitions.
    repo = repository()
    accepted, lease = await _claimed(repo, "memory-fence-commit")
    with pytest.raises(FencingConflict):
        await repo.commit_session_ready(
            TurnCommit(
                context=accepted.context,
                lease=lease.model_copy(update={"fencing_token": lease.fencing_token + 1}),
                state={},
                events=(),
            )
        )
    repo.mailbox._mailboxes[key] = repo.mailbox._mailboxes[key].model_copy(
        update={"processing_inbound_id": "different-inbound"}
    )
    with pytest.raises(FencingConflict):
        await repo.commit_session_ready(
            TurnCommit(context=accepted.context, lease=lease, state={}, events=())
        )

    repo = repository()
    accepted, lease = await _claimed(repo, "memory-fence-retry")
    with pytest.raises(ValueError):
        await repo.retry_session_ready(lease, error_type="bad", delay=timedelta(seconds=-1))
    with pytest.raises(FencingConflict):
        await repo.retry_session_ready(
            lease.model_copy(update={"worker_id": "other-worker"}),
            error_type="stale",
            delay=timedelta(seconds=1),
        )
    repo.mailbox._mailboxes.pop((accepted.context.tenant_id, accepted.context.session_id))
    with pytest.raises(FencingConflict):
        await repo.retry_session_ready(
            lease, error_type="missing-mailbox", delay=timedelta(seconds=1)
        )

    repo = repository()
    accepted, lease = await _claimed(repo, "memory-fence-fail")
    with pytest.raises(FencingConflict):
        await repo.fail_session_ready(
            lease.model_copy(update={"fencing_token": lease.fencing_token + 1}),
            error_type="stale",
        )
    repo.mailbox._mailboxes.pop((accepted.context.tenant_id, accepted.context.session_id))
    with pytest.raises(FencingConflict):
        await repo.fail_session_ready(lease, error_type="missing-mailbox")


@pytest.mark.asyncio
async def test_memory_v2_expired_takeover_reuses_turn_and_commit_without_events() -> None:
    repo = repository()
    accepted, first_lease = await _claimed(repo, "memory-takeover")
    key = (accepted.context.tenant_id, accepted.context.session_id)
    expired = first_lease.model_copy(
        update={"expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    )
    repo._leases[key] = expired
    mailbox = repo.mailbox._mailboxes[key]
    repo.mailbox._mailboxes[key] = mailbox.model_copy(
        update={"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    )
    takeover = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-b",
        lease_for=timedelta(seconds=30),
    )
    assert takeover.claimed and takeover.execution_lease is not None
    assert takeover.execution_lease.turn_id == first_lease.turn_id
    result = await repo.commit_session_ready(
        TurnCommit(context=accepted.context, lease=takeover.execution_lease, state={}, events=())
    )
    assert result.first_sequence is None and result.last_sequence is None
    assert key not in repo._leases


@pytest.mark.asyncio
async def test_memory_accept_and_read_paths_preserve_duplicate_and_tenant_isolation() -> None:
    repo = repository()
    accepted = await repo.accept_inbound(
        context=(await _accepted(repository(), "unused")).context,
        envelope=envelope("memory-v1-duplicate"),
        trace_headers={},
    )
    duplicate = await repo.accept_inbound(
        context=accepted.context,
        envelope=envelope("memory-v1-duplicate"),
        trace_headers={},
    )
    assert duplicate.duplicate
    assert await repo.get_acceptance("other-tenant", accepted.inbound_id) is None
    with pytest.raises(LookupError):
        await repo.get_config("tenant-a", "missing-app", 1)
    assert await repo.list_bindings(accepted.envelope.channel)
    assert await repo.resolve_binding("missing-binding") is None


@pytest.mark.asyncio
async def test_accept_v2_duplicate_and_retry_wait_preserve_ready_wakeup_rules() -> None:
    repo = repository()
    accepted = await _accepted(repo, "memory-accept-duplicate")
    duplicate = await repo.accept_inbound_v2(
        context=accepted.context,
        envelope=envelope("memory-accept-duplicate"),
        trace_headers={},
    )
    assert duplicate.duplicate

    mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert mailbox is not None and repo.mailbox.outbox
    claim = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert claim.execution_lease is not None
    await repo.retry_session_ready(
        claim.execution_lease, error_type="temporary", delay=timedelta(seconds=30)
    )
    waiting = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert waiting is not None and waiting.status == MailboxStatus.RETRY_WAIT
    retry_at = waiting.retry_at
    outbox_count = len(repo.mailbox.outbox)

    # A later message does not wake a future retry head or replace its timer.
    await repo.accept_inbound_v2(
        context=accepted.context,
        envelope=envelope("memory-accept-while-waiting"),
        trace_headers={},
    )
    still_waiting = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert still_waiting is not None
    assert still_waiting.status == MailboxStatus.RETRY_WAIT
    assert still_waiting.retry_at == retry_at
    assert len(repo.mailbox.outbox) == outbox_count

    # Once the head is due, a new acceptance makes the mailbox runnable and
    # emits exactly one fresh wake-up for the head.
    repo.mailbox._mailboxes[(accepted.context.tenant_id, accepted.context.session_id)] = (
        still_waiting.model_copy(
            update={
                "retry_at": datetime.now(UTC) - timedelta(seconds=1),
                "status": MailboxStatus.RETRY_WAIT,
            }
        )
    )
    before_due = len(repo.mailbox.outbox)
    await repo.accept_inbound_v2(
        context=accepted.context,
        envelope=envelope("memory-accept-after-due"),
        trace_headers={},
    )
    due = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert due is not None and due.status == MailboxStatus.QUEUED
    assert len(repo.mailbox.outbox) == before_due + 1


@pytest.mark.asyncio
async def test_accept_v2_while_running_defers_ready_until_commit() -> None:
    repo = repository()
    first = await _accepted(repo, "memory-running-head")
    mailbox = await repo.mailbox.get("tenant-a", first.context.session_id)
    assert mailbox is not None
    claim = await repo.claim_session_ready(
        "tenant-a",
        first.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert claim.execution_lease is not None
    before = len(repo.mailbox.outbox)
    await repo.accept_inbound_v2(
        context=first.context,
        envelope=envelope("memory-running-next"),
        trace_headers={},
    )
    running = await repo.mailbox.get("tenant-a", first.context.session_id)
    assert running is not None and running.status == MailboxStatus.RUNNING
    assert len(repo.mailbox.outbox) == before
    await repo.commit_session_ready(
        TurnCommit(context=first.context, lease=claim.execution_lease, state={}, events=())
    )
    committed = await repo.mailbox.get("tenant-a", first.context.session_id)
    assert committed is not None and committed.status == MailboxStatus.QUEUED
    assert len(repo.mailbox.outbox) == before + 1


@pytest.mark.asyncio
async def test_runtime_mailbox_sync_is_idempotent_and_recovery_republishes_ready() -> None:
    """The runtime projection follows mailbox recovery without duplicating wake-ups."""

    repo = repository()
    key = ("tenant-a", "memory-runtime-recover")
    inbound_id = str(uuid4())
    initial = await repo.accept_mailbox(*key, inbound_id, trace_id="runtime-recovery-trace")
    initial_event = repo.mailbox.outbox[-1]
    assert initial.status is MailboxStatus.QUEUED
    assert repo._outbox[initial_event.outbox_id] is initial_event

    # Reprojecting append-only mailbox history must not overwrite the current
    # runtime record or create a second dispatch candidate.
    repo._sync_new_mailbox_outbox(0)
    assert len(repo._outbox) == 1
    assert repo._outbox[initial_event.outbox_id] is initial_event

    duplicate = await repo.accept_mailbox(*key, inbound_id)
    assert duplicate == initial
    assert len(repo.mailbox.outbox) == 1
    assert len(repo._outbox) == 1

    lease = await repo.claim_mailbox(
        *key,
        owner_id="worker-recovery",
        lease_for=timedelta(seconds=30),
    )
    assert lease is not None
    # Simulate Redis losing the first wake-up before the worker starts.
    repo._outbox.pop(initial_event.outbox_id)
    current = repo.mailbox._mailboxes[key]
    repo.mailbox._mailboxes[key] = current.model_copy(
        update={"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    )

    recovered = await repo.recover_mailbox(*key)
    assert recovered is not None
    assert recovered.status is MailboxStatus.QUEUED
    assert recovered.queue_generation == initial.queue_generation + 1
    replay = [
        record
        for record in repo._outbox.values()
        if record.event_type == "session.ready.v2" and record.aggregate_id == key[1]
    ]
    assert len(replay) == 1
    assert replay[0].outbox_id != initial_event.outbox_id
    assert replay[0].payload["generation"] == initial.queue_generation + 1

    # Recovery is one-shot after the lease has been cleared; a second pass is
    # harmless and does not append another wake-up.
    mailbox_count = len(repo.mailbox.outbox)
    assert await repo.recover_mailbox(*key) is None
    assert len(repo.mailbox.outbox) == mailbox_count
    assert (
        len(
            [
                record
                for record in repo._outbox.values()
                if record.event_type == "session.ready.v2" and record.aggregate_id == key[1]
            ]
        )
        == 1
    )


@pytest.mark.asyncio
async def test_runtime_sweep_and_retry_scheduler_sync_recovered_generations() -> None:
    """Independent recovery roles publish exactly the generations they create."""

    sweep_repo = repository()
    sweep_key = ("tenant-a", "memory-runtime-sweep")
    await sweep_repo.accept_mailbox(*sweep_key, str(uuid4()))
    sweep_event = sweep_repo.mailbox.outbox[-1]
    sweep_repo._outbox.clear()
    sweep_lease = await sweep_repo.claim_mailbox(
        *sweep_key,
        owner_id="worker-sweep",
        lease_for=timedelta(seconds=30),
    )
    assert sweep_lease is not None
    current = sweep_repo.mailbox._mailboxes[sweep_key]
    sweep_repo.mailbox._mailboxes[sweep_key] = current.model_copy(
        update={"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)}
    )

    assert await sweep_repo.sweep_expired_leases(owner_id="sweeper", limit=1) == 1
    sweep_mailbox = sweep_repo.mailbox._mailboxes[sweep_key]
    assert sweep_mailbox.status is MailboxStatus.QUEUED
    sweep_ready = [
        record
        for record in sweep_repo._outbox.values()
        if record.event_type == "session.ready.v2" and record.aggregate_id == sweep_key[1]
    ]
    assert len(sweep_ready) == 1
    assert sweep_ready[0].payload["generation"] == sweep_event.payload["generation"] + 1

    retry_repo = repository()
    accepted, lease = await _claimed(retry_repo, "memory-runtime-retry")
    retry_key = (accepted.context.tenant_id, accepted.context.session_id)
    initial_event = retry_repo.mailbox.outbox[-1]
    retry_repo._outbox.pop(initial_event.outbox_id)
    await retry_repo.retry_session_ready(lease, error_type="temporary", delay=timedelta(0))
    waiting = await retry_repo.mailbox.get(*retry_key)
    assert waiting is not None and waiting.status is MailboxStatus.RETRY_WAIT

    assert await retry_repo.schedule_retries(owner_id="retry-scheduler", limit=1) == 1
    retry_mailbox = await retry_repo.mailbox.get(*retry_key)
    assert retry_mailbox is not None and retry_mailbox.status is MailboxStatus.QUEUED
    retry_ready = [
        record
        for record in retry_repo._outbox.values()
        if record.event_type == "session.ready.v2" and record.aggregate_id == retry_key[1]
    ]
    assert len(retry_ready) == 1
    assert retry_ready[0].payload["generation"] == initial_event.payload["generation"] + 1

    # The scheduler only handles RETRY_WAIT rows.  A repeated pass is a
    # no-op, leaving both the generation and runtime projection stable.
    assert await retry_repo.schedule_retries(owner_id="retry-scheduler", limit=1) == 0
    assert (
        len(
            [
                record
                for record in retry_repo._outbox.values()
                if record.event_type == "session.ready.v2" and record.aggregate_id == retry_key[1]
            ]
        )
        == 1
    )
