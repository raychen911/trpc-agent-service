from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from tests.conftest import envelope, repository
from trpc_service.agent.fake import DeterministicAgent
from trpc_service.agent.worker import AgentWorker, ProcessStatus
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import StoredEvent, TurnCommit
from trpc_service.storage.protocols import FencingConflict

KEY = b"w" * 32


async def agent_loader(config):
    return DeterministicAgent(name="deterministic_agent", response="answer")


@pytest.mark.asyncio
async def test_four_workers_serialize_one_session_without_lost_events() -> None:
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=KEY)
    acceptances = [
        await runtime.accept("binding-unpredictable-a", envelope(f"message-{index}"))
        for index in range(4)
    ]
    workers = [
        AgentWorker(
            repo,
            worker_id=f"worker-{index}",
            agent_loader=agent_loader,
            lease_for=timedelta(seconds=2),
        )
        for index in range(4)
    ]
    pending = list(enumerate(acceptances))
    committed: list[int] = []
    for _ in range(20):
        if not pending:
            break
        results = await asyncio.gather(
            *(workers[index % len(workers)].process(item) for index, item in pending)
        )
        next_pending = []
        for pair, result in zip(pending, results, strict=True):
            if result.status == ProcessStatus.COMMITTED:
                committed.append(pair[0])
            else:
                next_pending.append(pair)
        pending = next_pending
        await asyncio.sleep(0)

    assert committed == [0, 1, 2, 3]
    snapshot = repo.snapshot("tenant-a", acceptances[0].context.session_id)
    assert snapshot is not None
    assert [event.sequence for event in snapshot.events] == list(range(1, 9))
    assert [event.author for event in snapshot.events].count("user") == 4
    assert len(repo.delivery_receipts) == 0


@pytest.mark.asyncio
async def test_expired_worker_fencing_token_cannot_commit() -> None:
    repo = repository()
    accepted = await TenantRuntime(repo, routing_key=KEY).accept(
        "binding-unpredictable-a", envelope()
    )
    old = await repo.acquire(
        acceptance=accepted,
        worker_id="old-worker",
        lease_for=timedelta(milliseconds=1),
    )
    assert old is not None
    await asyncio.sleep(0.01)
    replacement = await repo.acquire(
        acceptance=accepted,
        worker_id="new-worker",
        lease_for=timedelta(seconds=1),
    )
    assert replacement is not None
    assert replacement.fencing_token > old.fencing_token
    with pytest.raises(FencingConflict):
        await repo.commit(
            TurnCommit(
                context=accepted.context,
                lease=old,
                state={"stale": True},
                events=(
                    StoredEvent(
                        event_id="stale",
                        author="agent",
                        timestamp=1,
                        event={},
                    ),
                ),
            )
        )


@pytest.mark.asyncio
async def test_different_sessions_can_run_in_parallel() -> None:
    repo = repository()
    runtime = TenantRuntime(repo, routing_key=KEY)
    left = await runtime.accept("binding-unpredictable-a", envelope("left", user_id="user-left"))
    right = await runtime.accept("binding-unpredictable-a", envelope("right", user_id="user-right"))
    worker = AgentWorker(
        repo,
        worker_id="worker",
        agent_loader=agent_loader,
        lease_for=timedelta(seconds=1),
    )
    results = await asyncio.gather(worker.process(left), worker.process(right))
    assert all(result.status == ProcessStatus.COMMITTED for result in results)
    assert left.context.session_id != right.context.session_id
