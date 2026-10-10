from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from trpc_service.storage.mailbox import InMemorySessionMailboxStore

BASE = datetime(2026, 8, 23, tzinfo=UTC)
LEASE_FOR = timedelta(minutes=5)


async def _accept_messages(
    store: InMemorySessionMailboxStore,
    session_id: str,
    count: int,
    *,
    prefix: str,
) -> tuple[str, ...]:
    inbound_ids = tuple(f"{prefix}-{index:03d}" for index in range(count))
    for inbound_id in inbound_ids:
        await store.accept(
            "tenant-a",
            session_id,
            inbound_id,
            now=BASE,
        )
    return inbound_ids


@pytest.mark.asyncio
async def test_four_workers_serialize_one_session_exactly_once() -> None:
    """Four concurrent claimers must preserve mailbox order and uniqueness."""

    store = InMemorySessionMailboxStore()
    inbound_ids = await _accept_messages(
        store,
        "hot-session",
        100,
        prefix="hot",
    )
    worker_ids = ("worker-1", "worker-2", "worker-3", "worker-4")
    processed: list[tuple[int, str, str]] = []
    finished = asyncio.Event()

    async def worker(worker_id: str) -> None:
        while not finished.is_set():
            lease = await store.claim(
                "tenant-a",
                "hot-session",
                owner_id=worker_id,
                lease_for=LEASE_FOR,
                now=BASE,
            )
            if lease is None:
                if len(processed) >= len(inbound_ids):
                    return
                # Give the other simulated workers a chance to claim after a
                # commit; this is a cooperative yield, not retry backoff.
                await asyncio.sleep(0)
                continue

            # Keep the lease in flight for one scheduling turn so competitors
            # exercise the RUNNING path before this worker commits it.
            await asyncio.sleep(0)
            await store.commit(lease, now=BASE)
            processed.append((lease.sequence, lease.inbound_id, worker_id))
            if len(processed) == len(inbound_ids):
                finished.set()
            await asyncio.sleep(0)

    tasks = [asyncio.create_task(worker(worker_id)) for worker_id in worker_ids]
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)

    assert [sequence for sequence, _, _ in processed] == list(range(1, 101))
    assert [inbound_id for _, inbound_id, _ in processed] == list(inbound_ids)
    assert len({inbound_id for _, inbound_id, _ in processed}) == 100
    assert len({worker_id for _, _, worker_id in processed}) == len(worker_ids)

    mailbox = await store.get("tenant-a", "hot-session")
    assert mailbox is not None
    assert mailbox.accepted_sequence == mailbox.resolved_sequence == 100
    assert mailbox.processing_sequence is None


@pytest.mark.asyncio
async def test_independent_sessions_can_hold_claims_at_the_same_time() -> None:
    """A lease on one session must not serialize a different session."""

    store = InMemorySessionMailboxStore()
    session_ids = tuple(f"parallel-session-{index}" for index in range(4))
    for index, session_id in enumerate(session_ids):
        await _accept_messages(store, session_id, 1, prefix=f"parallel-{index}")

    claimed: list[str] = []
    all_claimed = asyncio.Event()
    release = asyncio.Event()

    async def worker(session_id: str) -> None:
        lease = await store.claim(
            "tenant-a",
            session_id,
            owner_id=f"worker-{session_id}",
            lease_for=LEASE_FOR,
            now=BASE,
        )
        assert lease is not None
        claimed.append(session_id)
        if len(claimed) == len(session_ids):
            all_claimed.set()
        await release.wait()
        await store.commit(lease, now=BASE)

    tasks = [asyncio.create_task(worker(session_id)) for session_id in session_ids]
    await asyncio.wait_for(all_claimed.wait(), timeout=1)
    assert len(claimed) == len(session_ids)
    assert set(claimed) == set(session_ids)

    release.set()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1)
    for session_id in session_ids:
        mailbox = await store.get("tenant-a", session_id)
        assert mailbox is not None
        assert mailbox.resolved_sequence == mailbox.accepted_sequence == 1


@pytest.mark.asyncio
async def test_hot_session_lease_does_not_block_cold_session() -> None:
    """A long-running hot turn must leave another session immediately claimable."""

    store = InMemorySessionMailboxStore()
    await _accept_messages(store, "hot-session", 100, prefix="hot")
    await _accept_messages(store, "cold-session", 1, prefix="cold")

    hot_lease = await store.claim(
        "tenant-a",
        "hot-session",
        owner_id="hot-worker",
        lease_for=LEASE_FOR,
        now=BASE,
    )
    assert hot_lease is not None

    cold_claimed = asyncio.Event()

    async def cold_worker() -> None:
        lease = await store.claim(
            "tenant-a",
            "cold-session",
            owner_id="cold-worker",
            lease_for=LEASE_FOR,
            now=BASE,
        )
        assert lease is not None
        cold_claimed.set()
        await store.commit(lease, now=BASE)

    cold_task = asyncio.create_task(cold_worker())
    await asyncio.wait_for(cold_claimed.wait(), timeout=1)
    await asyncio.wait_for(cold_task, timeout=1)

    cold_mailbox = await store.get("tenant-a", "cold-session")
    assert cold_mailbox is not None
    assert cold_mailbox.resolved_sequence == cold_mailbox.accepted_sequence == 1
    assert cold_mailbox.status.value == "IDLE"

    await store.commit(hot_lease, now=BASE)
