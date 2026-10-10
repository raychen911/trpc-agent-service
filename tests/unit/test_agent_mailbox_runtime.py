from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from tests.conftest import envelope, repository
from trpc_service.agent.fake import DeterministicAgent
from trpc_service.agent.mailbox_runtime import MailboxClaimExecutor, MailboxReadyClaimer
from trpc_service.agent.worker import AgentWorker, ProcessStatus
from trpc_service.config.settings import SchedulerVersion
from trpc_service.queue.session_ready import SessionReady
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import MailboxStatus


async def agent_loader(config):
    del config
    return DeterministicAgent(name="mailbox-agent", response="mailbox answer")


@pytest.mark.asyncio
async def test_v2_claim_is_executed_without_legacy_acquire(monkeypatch) -> None:
    repo = repository()
    accepted = await TenantRuntime(
        repo,
        routing_key=b"v" * 32,
        scheduler_version=SchedulerVersion.V2,
    ).accept("binding-unpredictable-a", envelope("mailbox-message"))
    mailbox = await repo.mailbox.get(
        accepted.context.tenant_id,
        accepted.context.session_id,
    )
    assert mailbox is not None and mailbox.status == MailboxStatus.QUEUED
    ready_event = repo.mailbox.outbox[-1]

    notice = SessionReady(
        event_id=ready_event.outbox_id,
        tenant_id=accepted.context.tenant_id,
        session_id=accepted.context.session_id,
        generation=mailbox.queue_generation,
        priority=mailbox.priority,
        trace_id=accepted.context.trace_id,
        created_at=datetime.now(UTC),
    )
    claim = await MailboxReadyClaimer(
        repo,
        owner_id="worker-v2",
        lease_for=timedelta(seconds=5),
    ).claim(notice)
    assert claim.claimed
    assert claim.acceptance == accepted
    assert claim.execution_lease is not None
    assert claim.execution_lease.snapshot.session_id == accepted.context.session_id

    async def forbidden_legacy_acquire(**kwargs):
        raise AssertionError(f"legacy acquire called: {kwargs}")

    monkeypatch.setattr(repo, "acquire", forbidden_legacy_acquire)
    worker = AgentWorker(
        repo,
        worker_id="worker-v2",
        agent_loader=agent_loader,
        lease_for=timedelta(seconds=5),
    )
    result = await worker.process_claimed(claim)

    assert result.status == ProcessStatus.COMMITTED
    resolved = await repo.mailbox.get(
        accepted.context.tenant_id,
        accepted.context.session_id,
    )
    assert resolved is not None
    assert resolved.status == MailboxStatus.IDLE
    assert resolved.accepted_sequence == resolved.resolved_sequence == 1


@pytest.mark.asyncio
async def test_mailbox_executor_delegates_the_already_claimed_turn() -> None:
    calls = []

    class Worker:
        async def process_claimed(self, claim) -> None:
            calls.append(claim)

    sentinel = object()
    await MailboxClaimExecutor(Worker()).execute(sentinel)  # type: ignore[arg-type]
    assert calls == [sentinel]
