from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from tests.unit.test_mailbox_architecture_acceptance import envelope, repository
from tests.unit.test_mailbox_store_branch_coverage import (
    SESSION,
    TENANT,
    Connection,
    Pool,
    item_row,
    mailbox_row,
)
from trpc_service.config.settings import SchedulerVersion
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.mailbox import (
    InMemorySessionMailboxStore,
    PostgresSessionMailboxStore,
)
from trpc_service.storage.models import MailboxStatus

BASE = datetime(2026, 8, 23, 12, 0, tzinfo=UTC)


@pytest.mark.asyncio
async def test_queued_reconcile_reuses_generation_and_durable_wakeup() -> None:
    """Backlog age must not turn one queued session into new generations."""

    store = InMemorySessionMailboxStore()
    await store.accept(TENANT, SESSION, "inbound-1", now=BASE, trace_id="trace-1")
    original = store.outbox[0]

    for offset in (6, 12, 18):
        mailbox = await store.reconcile(TENANT, SESSION, now=BASE + timedelta(seconds=offset))
        assert mailbox is not None
        assert mailbox.status is MailboxStatus.QUEUED
        assert mailbox.queue_generation == 1
        assert len(store.outbox) == 1
        assert store.outbox[0].outbox_id == original.outbox_id


@pytest.mark.asyncio
async def test_queued_reconcile_rebuilds_missing_record_without_generation_churn() -> None:
    """If the local durable projection is missing, rebuild the same generation."""

    store = InMemorySessionMailboxStore()
    await store.accept(TENANT, SESSION, "inbound-1", now=BASE, trace_id="trace-1")
    store._outbox.clear()

    mailbox = await store.reconcile(TENANT, SESSION, now=BASE + timedelta(seconds=6))

    assert mailbox is not None
    assert mailbox.queue_generation == 1
    assert len(store.outbox) == 1
    assert store.outbox[0].payload["generation"] == 1


@pytest.mark.asyncio
async def test_postgres_queued_reconcile_replays_current_event_without_insert() -> None:
    """The PG path reopens the current event and never allocates generation N+1."""

    current = mailbox_row(updated_at=BASE - timedelta(minutes=1), generation=7)
    updated = mailbox_row(updated_at=BASE, generation=7)
    connection = Connection(
        fetchrows=[current, None, item_row(), updated],
        fetchvals=[BASE],
        executes=["UPDATE 1"],
    )

    result = await PostgresSessionMailboxStore(Pool(connection)).reconcile(TENANT, SESSION)

    assert result is not None
    assert result.queue_generation == 7
    assert result.status is MailboxStatus.QUEUED
    statements = [
        str(args[0])
        for kind, args in connection.calls
        if kind == "execute" and "outbox_events" in str(args[0])
    ]
    assert len(statements) == 1
    assert "published_at=NULL" in statements[0]
    assert "greatest(published_at,coalesce(ready_replayed_at,published_at))" in statements[0]
    assert "ready_replayed_at IS NULL" not in statements[0]
    assert "INSERT INTO outbox_events" not in statements[0]


@pytest.mark.asyncio
async def test_inmemory_reconcile_sessions_leaves_running_recovery_to_sweeper() -> None:
    store = InMemorySessionMailboxStore()
    await store.accept(TENANT, SESSION, "inbound-1", now=BASE)
    lease = await store.claim(
        TENANT,
        SESSION,
        owner_id="worker-a",
        lease_for=timedelta(seconds=1),
        now=BASE,
    )
    assert lease is not None

    # The lease is expired relative to the real clock, but the reconciler role
    # must still not touch a RUNNING mailbox.  The sweeper owns that transition.
    handled = await store.reconcile_sessions(owner_id="recovery", limit=1)

    assert handled == 0
    current = await store.get(TENANT, SESSION)
    assert current is not None and current.status is MailboxStatus.RUNNING


@pytest.mark.asyncio
async def test_inmemory_reconcile_due_retry_gets_a_new_generation() -> None:
    store = InMemorySessionMailboxStore()
    retry_at = BASE + timedelta(seconds=10)
    await store.accept(TENANT, SESSION, "inbound-1", retry_at=retry_at, now=BASE)
    assert (await store.get(TENANT, SESSION)).status is MailboxStatus.RETRY_WAIT  # type: ignore[union-attr]

    mailbox = await store.reconcile(TENANT, SESSION, now=BASE + timedelta(seconds=11))

    assert mailbox is not None
    assert mailbox.status is MailboxStatus.QUEUED
    assert mailbox.queue_generation == 1
    assert len(store.outbox) == 1
    assert store.outbox[0].payload["generation"] == 1


def test_reconciler_migration_reuses_queued_generation_and_outbox_row() -> None:
    migration = Path("migrations/versions/0008_session_mailboxes.py").read_text(encoding="utf-8")
    reconciler = migration.split("CREATE OR REPLACE FUNCTION reconcile_session_mailboxes(", 1)[
        1
    ].split("REVOKE ALL ON FUNCTION reconcile_session_mailboxes(integer)", 1)[0]

    # A real state transition still increments, while the old QUEUED state
    # only reopens its existing event (or inserts it once if it is absent).
    assert "rec.old_status <> 'QUEUED'" in reconciler
    assert "published_at=NULL" in reconciler
    assert "ELSIF next_status='QUEUED' THEN" in reconciler
    assert "ON CONFLICT DO NOTHING" in reconciler
    # Ordinary expired RUNNING leases are selected only by the lease sweeper;
    # committed self-heal remains a separate exception in this function.
    assert "m.status='RUNNING'" not in reconciler.split("LOOP", 1)[0]
    assert "inbound.status='committed' AND i.resolved_at IS NULL" in reconciler
    assert "m.lease_owner, m.lease_epoch, m.lease_expires_at" in reconciler
    assert reconciler.count("AND lease_owner=rec.lease_owner") >= 2


def test_reconciler_replay_guard_is_bounded_and_repeatable() -> None:
    migration = Path("migrations/versions/0009_session_ready_replay_guard.py").read_text(
        encoding="utf-8"
    )
    normalized = " ".join(migration.lower().split())

    assert "add column ready_replayed_at timestamptz" in normalized
    assert "reconcile_session_mailboxes_v2(" in normalized
    assert "p_limit integer" in normalized
    assert "p_replay_cooldown_seconds integer" in normalized
    assert "greatest(published_at,coalesce(ready_replayed_at,published_at))" in normalized
    assert "make_interval(secs => p_replay_cooldown_seconds)" in normalized
    assert "ready_replayed_at=clock_timestamp()" in normalized
    assert "ready_replayed_at is null" not in normalized
    # The rolling marker makes the same generation ineligible during the
    # cooldown and eligible again after the next cooldown window.
    assert normalized.count("published_at is not null") >= 1
    assert normalized.count("ready_replayed_at=clock_timestamp()") >= 1
    assert "set published_at=null" in normalized
    assert "create function reconcile_session_mailboxes(p_limit integer)" in normalized


def test_reconciler_replay_timestamp_allows_recovery_after_a_new_cooldown() -> None:
    """The last replay time blocks only the current cooldown window."""

    cooldown = timedelta(seconds=30)
    published_at = BASE - timedelta(seconds=31)
    old_replay = BASE - timedelta(seconds=31)
    recent_publication = BASE - timedelta(seconds=1)

    def replay_due(last_replay: datetime | None, published: datetime) -> bool:
        anchor = max(published, last_replay or published)
        return anchor <= BASE - cooldown

    # A stale replay marker must not reset an event that was just published.
    assert not replay_due(old_replay, recent_publication)
    # If both timestamps are old, a later publish-then-loss is eligible for
    # another repair.
    assert replay_due(old_replay, published_at)
    # On the first replay, a NULL marker falls back to published_at.
    assert replay_due(None, published_at)


@pytest.mark.asyncio
async def test_postgres_reconcile_sessions_passes_configured_replay_cooldown() -> None:
    from tests.unit.test_mailbox_store_branch_coverage import Connection, Pool

    connection = Connection(fetchvals=[3])
    store = PostgresSessionMailboxStore(Pool(connection), ready_replay_cooldown_seconds=45)

    assert await store.reconcile_sessions(owner_id="recovery", limit=10) == 3
    call = next(
        args
        for kind, args in connection.calls
        if kind == "fetchval" and "reconcile" in str(args[0])
    )
    assert "reconcile_session_mailboxes_v2($1,$2)" in str(call[0])
    assert call[1:] == (10, 45)


@pytest.mark.asyncio
async def test_runtime_rearms_published_generation_after_reconcile() -> None:
    repo = repository()
    runtime = TenantRuntime(
        repo,
        routing_key=b"reconcile-runtime-outbox" * 2,
        scheduler_version=SchedulerVersion.V2,
    )
    accepted = await runtime.accept("binding-unpredictable-a", envelope("redis-loss"))
    first = (
        await repo.claim_outbox(
            event_type="session.ready.v2",
            owner_id="publisher-a",
            limit=1,
            lease_for=timedelta(seconds=30),
        )
    )[0]
    await repo.mark_outbox_published(first.tenant_id, first.outbox_id, owner_id="publisher-a")

    key = (accepted.context.tenant_id, accepted.context.session_id)
    mailbox = repo.mailbox._mailboxes[key]
    repo.mailbox._mailboxes[key] = mailbox.model_copy(
        update={"updated_at": datetime.now(UTC) - timedelta(seconds=6)}
    )
    await repo.reconcile_mailbox(*key)

    replay = await repo.claim_outbox(
        event_type="session.ready.v2",
        owner_id="publisher-b",
        limit=1,
        lease_for=timedelta(seconds=30),
    )
    assert len(replay) == 1
    assert replay[0].outbox_id == first.outbox_id
    assert replay[0].payload["generation"] == 1


@pytest.mark.asyncio
async def test_runtime_retry_scheduler_exposes_new_generation_to_dispatcher() -> None:
    repo = repository()
    runtime = TenantRuntime(
        repo,
        routing_key=b"retry-runtime-outbox-key" * 2,
        scheduler_version=SchedulerVersion.V2,
    )
    accepted = await runtime.accept("binding-unpredictable-a", envelope("retry-due"))
    initial = repo.mailbox.outbox[-1]
    published = (
        await repo.claim_outbox(
            event_type="session.ready.v2",
            owner_id="publisher-a",
            limit=1,
            lease_for=timedelta(seconds=30),
        )
    )[0]
    await repo.mark_outbox_published(
        published.tenant_id, published.outbox_id, owner_id="publisher-a"
    )
    claim = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker-a",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
        expected_event_id=initial.outbox_id,
    )
    assert claim.execution_lease is not None
    await repo.retry_session_ready(
        claim.execution_lease,
        error_type="temporary",
        delay=timedelta(0),
    )
    assert await repo.schedule_retries(owner_id="retry-scheduler", limit=1) == 1

    ready = await repo.claim_outbox(
        event_type="session.ready.v2",
        owner_id="publisher-b",
        limit=1,
        lease_for=timedelta(seconds=30),
    )
    assert len(ready) == 1
    assert ready[0].payload["generation"] == 2


@pytest.mark.asyncio
async def test_runtime_reconcile_sessions_rearms_each_generation_once_with_limit() -> None:
    """Reconciliation repairs lost notifications without generation churn."""

    repo = repository()
    keys = (
        ("tenant-a", "reconcile-session-1"),
        ("tenant-a", "reconcile-session-2"),
    )
    events = {}
    for key in keys:
        await repo.accept_mailbox(*key, str(uuid4()))
        events[key] = repo.mailbox.outbox[-1]
        current = repo.mailbox._mailboxes[key]
        repo.mailbox._mailboxes[key] = current.model_copy(
            update={"updated_at": datetime.now(UTC) - timedelta(seconds=6)}
        )

    # Keep a stale dispatcher claim for the first event.  Reconciliation must
    # clear that claim before rearming the same durable event.
    claimed = await repo.claim_outbox(
        event_type="session.ready.v2",
        owner_id="publisher-stale",
        limit=1,
        lease_for=timedelta(seconds=30),
    )
    assert len(claimed) == 1 and claimed[0].outbox_id == events[keys[0]].outbox_id
    repo._outbox.pop(events[keys[0]].outbox_id)
    repo._outbox.pop(events[keys[1]].outbox_id)

    assert await repo.reconcile_sessions(owner_id="reconciler", limit=1) == 1
    assert events[keys[0]].outbox_id not in repo._outbox_claims
    assert events[keys[0]].outbox_id in repo._outbox
    assert events[keys[1]].outbox_id not in repo._outbox

    # A small limit may revisit the now-recent first row, but cannot starve the
    # second row forever: a later larger pass reaches and rearms it.
    assert await repo.reconcile_sessions(owner_id="reconciler", limit=1) == 1
    assert events[keys[1]].outbox_id not in repo._outbox
    assert await repo.reconcile_sessions(owner_id="reconciler", limit=2) == 2
    assert set(repo._outbox) == {events[key].outbox_id for key in keys}

    # Further passes are idempotent: the existing records are reused and no
    # third wake-up or generation is introduced.
    generations = {key: repo.mailbox._mailboxes[key].queue_generation for key in keys}
    outbox_ids = set(repo._outbox)
    assert await repo.reconcile_sessions(owner_id="reconciler", limit=2) == 2
    assert set(repo._outbox) == outbox_ids
    assert {key: repo.mailbox._mailboxes[key].queue_generation for key in keys} == generations


@pytest.mark.asyncio
async def test_runtime_reconcile_missing_mailbox_is_a_safe_noop() -> None:
    repo = repository()

    assert await repo.reconcile_mailbox("tenant-a", "missing-reconcile") is None
    assert repo._outbox == {}
