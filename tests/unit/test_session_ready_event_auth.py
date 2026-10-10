from __future__ import annotations

from datetime import timedelta
from uuid import uuid4

import pytest

from tests.conftest import envelope, repository
from tests.unit.test_postgres_repository import Connection, Pool
from trpc_service.config.settings import SchedulerVersion
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import MailboxClaimStatus
from trpc_service.storage.postgres import PostgresRuntimeRepository


async def _memory_ready():
    repo = repository()
    accepted = await TenantRuntime(
        repo,
        routing_key=b"event-auth" * 4,
        scheduler_version=SchedulerVersion.V2,
    ).accept("binding-unpredictable-a", envelope("event-auth"))
    mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert mailbox is not None
    assert repo.mailbox.outbox
    return repo, accepted, mailbox, repo.mailbox.outbox[-1]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tenant_id", "session_id", "generation", "event_id"),
    [
        ("tenant-a", "wrong-session", 1, None),
        ("tenant-b", "session-a", 1, None),
        ("tenant-a", "session-a", 2, None),
        ("tenant-a", "session-a", 1, "forged-event"),
    ],
)
async def test_memory_rejects_untrusted_ready_event_fields(
    tenant_id: str,
    session_id: str,
    generation: int,
    event_id: str | None,
) -> None:
    repo, accepted, mailbox, ready_event = await _memory_ready()
    claim = await repo.claim_session_ready(
        tenant_id,
        session_id if session_id != "session-a" else accepted.context.session_id,
        owner_id="worker",
        lease_for=timedelta(seconds=30),
        expected_generation=generation,
        expected_event_id=event_id or ready_event.outbox_id,
    )
    assert claim.status == MailboxClaimStatus.STALE
    current = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert current is not None
    assert current.lease_owner is None
    assert current.queue_generation == mailbox.queue_generation


@pytest.mark.asyncio
async def test_memory_accepts_the_authoritative_ready_event() -> None:
    repo, accepted, mailbox, ready_event = await _memory_ready()
    claim = await repo.claim_session_ready(
        accepted.context.tenant_id,
        accepted.context.session_id,
        owner_id="worker",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=ready_event.outbox_id,
    )
    assert claim.status == MailboxClaimStatus.CLAIMED
    assert claim.execution_lease is not None


@pytest.mark.asyncio
async def test_memory_v2_claim_does_not_overwrite_an_active_v1_lease() -> None:
    repo = repository()
    v1 = await TenantRuntime(
        repo, routing_key=b"shared-lease" * 4, scheduler_version=SchedulerVersion.V1
    ).accept("binding-unpredictable-a", envelope("v1-active"))
    v1_lease = await repo.acquire(
        acceptance=v1, worker_id="v1-worker", lease_for=timedelta(seconds=30)
    )
    assert v1_lease is not None

    v2 = await repo.accept_inbound_v2(
        context=v1.context,
        envelope=envelope("v2-wakeup"),
        trace_headers={},
    )
    mailbox = await repo.mailbox.get(v2.context.tenant_id, v2.context.session_id)
    assert mailbox is not None and repo.mailbox.outbox
    claim = await repo.claim_session_ready(
        v2.context.tenant_id,
        v2.context.session_id,
        owner_id="v2-worker",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert claim.status == MailboxClaimStatus.RUNNING
    assert claim.execution_lease is None
    current = repo._leases[(v1.context.tenant_id, v1.context.session_id)]
    assert current.worker_id == "v1-worker"
    assert current.fencing_token == v1_lease.fencing_token


@pytest.mark.asyncio
async def test_memory_v1_acquire_cannot_enter_while_v2_is_active() -> None:
    repo = repository()
    v2 = await TenantRuntime(
        repo, routing_key=b"shared-lease" * 4, scheduler_version=SchedulerVersion.V2
    ).accept("binding-unpredictable-a", envelope("v2-active"))
    mailbox = await repo.mailbox.get(v2.context.tenant_id, v2.context.session_id)
    assert mailbox is not None and repo.mailbox.outbox
    v2_claim = await repo.claim_session_ready(
        v2.context.tenant_id,
        v2.context.session_id,
        owner_id="v2-worker",
        lease_for=timedelta(seconds=30),
        expected_generation=mailbox.queue_generation,
        expected_event_id=repo.mailbox.outbox[-1].outbox_id,
    )
    assert v2_claim.execution_lease is not None

    v1 = await repo.accept_inbound(
        context=v2.context, envelope=envelope("v1-contention"), trace_headers={}
    )
    assert (
        await repo.acquire(acceptance=v1, worker_id="v1-worker", lease_for=timedelta(seconds=30))
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "event_row",
    [
        None,
        {
            "outbox_id": uuid4(),
            "tenant_id": "tenant-b",
            "aggregate_type": "session",
            "aggregate_id": "session-a",
            "event_type": "session.ready.v2",
            "payload_json": {"generation": 1},
        },
        {
            "outbox_id": uuid4(),
            "tenant_id": "tenant-a",
            "aggregate_type": "other",
            "aggregate_id": "session-a",
            "event_type": "session.ready.v2",
            "payload_json": {"generation": 1},
        },
        {
            "outbox_id": uuid4(),
            "tenant_id": "tenant-a",
            "aggregate_type": "session",
            "aggregate_id": "other-session",
            "event_type": "session.ready.v2",
            "payload_json": {"generation": 1},
        },
        {
            "outbox_id": uuid4(),
            "tenant_id": "tenant-a",
            "aggregate_type": "session",
            "aggregate_id": "session-a",
            "event_type": "other.event",
            "payload_json": {"generation": 1},
        },
        {
            "outbox_id": uuid4(),
            "tenant_id": "tenant-a",
            "aggregate_type": "session",
            "aggregate_id": "session-a",
            "event_type": "session.ready.v2",
            "payload_json": {"generation": 2},
        },
    ],
)
async def test_postgres_rejects_forged_or_mismatched_ready_event(event_row) -> None:
    event_id = str(event_row["outbox_id"]) if event_row is not None else str(uuid4())
    connection = Connection(fetchrows=[event_row])
    claim = await PostgresRuntimeRepository(Pool(connection)).claim_session_ready(
        "tenant-a",
        "session-a",
        owner_id="worker",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
        expected_event_id=event_id,
    )
    assert claim.status == MailboxClaimStatus.STALE
    assert claim.lease is None
    assert not [
        call for call in connection.calls if call[0] == "execute" and "set_config" not in call[1][0]
    ]
    # Rejection occurs before the mailbox row is read, so no lease transition
    # or session acknowledgement can be caused by a forged wake-up.
    assert len([call for call in connection.calls if call[0] == "fetchrow"]) <= 1


@pytest.mark.asyncio
async def test_postgres_invalid_event_uuid_is_stale_without_database_lookup() -> None:
    connection = Connection()
    claim = await PostgresRuntimeRepository(Pool(connection)).claim_session_ready(
        "tenant-a",
        "session-a",
        owner_id="worker",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
        expected_event_id="not-a-uuid",
    )
    assert claim.status == MailboxClaimStatus.STALE
    assert all(call[0] == "execute" and "set_config" in call[1][0] for call in connection.calls)


@pytest.mark.asyncio
async def test_postgres_valid_event_is_checked_before_mailbox_claim() -> None:
    event_id = uuid4()
    connection = Connection(
        fetchrows=[
            {
                "outbox_id": event_id,
                "tenant_id": "tenant-a",
                "aggregate_type": "session",
                "aggregate_id": "session-a",
                "event_type": "session.ready.v2",
                "payload_json": {"generation": 1},
            },
            None,
        ]
    )
    claim = await PostgresRuntimeRepository(Pool(connection)).claim_session_ready(
        "tenant-a",
        "session-a",
        owner_id="worker",
        lease_for=timedelta(seconds=30),
        expected_generation=1,
        expected_event_id=str(event_id),
    )
    # The event itself is authentic; the following missing mailbox makes the
    # wake-up stale without ever producing a lease.
    assert claim.status == MailboxClaimStatus.STALE
    assert claim.lease is None
    fetchrows = [call for call in connection.calls if call[0] == "fetchrow"]
    assert "outbox_events" in fetchrows[0][1][0]
    assert "session_mailboxes" in fetchrows[1][1][0]
