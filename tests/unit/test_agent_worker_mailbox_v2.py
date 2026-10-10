from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

import trpc_service.agent.worker as worker_module
from tests.conftest import envelope, repository
from trpc_service.agent.mailbox_runtime import MailboxReadyClaimer
from trpc_service.agent.worker import AgentWorker, ProcessStatus
from trpc_service.config.settings import SchedulerVersion
from trpc_service.queue.session_ready import SessionReady
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import MailboxStatus
from trpc_service.storage.protocols import FencingConflict


async def _claimed(repo, message_id: str, *, owner_id: str = "worker-v2"):
    accepted = await TenantRuntime(
        repo,
        routing_key=b"worker-mailbox-v2" * 2,
        scheduler_version=SchedulerVersion.V2,
    ).accept("binding-unpredictable-a", envelope(message_id))
    mailbox = await repo.mailbox.get(
        accepted.context.tenant_id,
        accepted.context.session_id,
    )
    assert mailbox is not None
    ready_event = repo.mailbox.outbox[-1]
    claim = await MailboxReadyClaimer(
        repo,
        owner_id=owner_id,
        lease_for=timedelta(seconds=2),
    ).claim(
        SessionReady(
            event_id=ready_event.outbox_id,
            tenant_id=accepted.context.tenant_id,
            session_id=accepted.context.session_id,
            generation=mailbox.queue_generation,
            priority=mailbox.priority,
            trace_id=accepted.context.trace_id,
            created_at=datetime.now(UTC),
        )
    )
    assert claim.claimed
    assert claim.acceptance == accepted
    assert claim.execution_lease is not None
    return accepted, claim


async def _successful_loader(_config):
    from trpc_service.agent.fake import DeterministicAgent

    return DeterministicAgent(name="worker-v2-agent", response="done")


async def _failing_loader(_config):
    raise RuntimeError("offline turn failure")


@pytest.mark.asyncio
async def test_process_claimed_never_calls_legacy_acquire(monkeypatch) -> None:
    repo = repository()
    accepted, claim = await _claimed(repo, "claimed-no-legacy-acquire")
    calls: list[str] = []

    async def forbidden_acquire(**_kwargs):
        calls.append("acquire")
        raise AssertionError("v2 process_claimed must not call acquire")

    monkeypatch.setattr(repo, "acquire", forbidden_acquire)
    result = await AgentWorker(
        repo,
        worker_id="worker-v2",
        agent_loader=_successful_loader,
    ).process_claimed(claim)

    assert result.status == ProcessStatus.COMMITTED
    assert calls == []
    mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert mailbox is not None and mailbox.status == MailboxStatus.IDLE


@pytest.mark.asyncio
async def test_v2_heartbeat_renews_session_ready_and_success_commits_once(monkeypatch) -> None:
    repo = repository()
    accepted, claim = await _claimed(repo, "claimed-heartbeat")
    calls: list[str] = []
    original_renew = repo.renew_session_ready
    original_commit = repo.commit_session_ready

    async def renew(lease, *, lease_for):
        calls.append("renew_session_ready")
        return await original_renew(lease, lease_for=lease_for)

    async def commit(turn_commit):
        calls.append("commit_session_ready")
        return await original_commit(turn_commit)

    async def forbidden_acquire(**_kwargs):
        raise AssertionError("v2 process_claimed must not call acquire")

    monkeypatch.setattr(repo, "renew_session_ready", renew)
    monkeypatch.setattr(repo, "commit_session_ready", commit)
    monkeypatch.setattr(repo, "acquire", forbidden_acquire)

    class SlowRunner:
        def __init__(self, **_kwargs) -> None:
            self.state = {"turn": "done"}
            self.buffered_events = ()

        async def run(self, *_args, **_kwargs):
            await asyncio.sleep(0.25)
            yield SimpleNamespace(
                usage_metadata=None,
                visible=False,
                is_final_response=lambda: False,
                get_text=lambda: "",
            )

    monkeypatch.setattr(worker_module, "TenantRunner", SlowRunner)
    worker = AgentWorker(
        repo,
        worker_id="worker-v2",
        agent_loader=_successful_loader,
        lease_for=timedelta(milliseconds=400),
    )
    result = await worker.process_claimed(claim)

    assert result.status == ProcessStatus.COMMITTED
    assert calls.count("renew_session_ready") >= 1
    assert calls.count("commit_session_ready") == 1
    assert "retry_session_ready" not in calls
    assert "fail_session_ready" not in calls
    mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert mailbox is not None and mailbox.status == MailboxStatus.IDLE


@pytest.mark.asyncio
async def test_v2_turn_failure_schedules_retry_without_terminal_fail(monkeypatch) -> None:
    repo = repository()
    accepted, claim = await _claimed(repo, "claimed-retry")
    calls: list[tuple[str, object]] = []
    original_retry = repo.retry_session_ready
    original_fail = repo.fail_session_ready

    async def retry(lease, *, error_type, delay):
        calls.append(("retry_session_ready", delay))
        return await original_retry(lease, error_type=error_type, delay=delay)

    async def fail(lease, *, error_type):
        calls.append(("fail_session_ready", error_type))
        return await original_fail(lease, error_type=error_type)

    monkeypatch.setattr(repo, "retry_session_ready", retry)
    monkeypatch.setattr(repo, "fail_session_ready", fail)
    worker = AgentWorker(
        repo,
        worker_id="worker-v2",
        agent_loader=_failing_loader,
        max_turn_attempts=3,
    )

    with pytest.raises(RuntimeError, match="offline turn failure"):
        await worker.process_claimed(claim)

    assert len(calls) == 1
    assert calls[0][0] == "retry_session_ready"
    assert calls[0][1] == timedelta(seconds=1)
    mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert mailbox is not None and mailbox.status == MailboxStatus.RETRY_WAIT


@pytest.mark.asyncio
async def test_v2_turn_failure_at_attempt_limit_marks_terminal_failure(monkeypatch) -> None:
    repo = repository()
    accepted, claim = await _claimed(repo, "claimed-terminal-failure")
    calls: list[str] = []
    original_fail = repo.fail_session_ready

    async def fail(lease, *, error_type):
        calls.append(f"fail_session_ready:{error_type}")
        return await original_fail(lease, error_type=error_type)

    async def forbidden_retry(*_args, **_kwargs):
        raise AssertionError("attempt limit must not schedule another retry")

    monkeypatch.setattr(repo, "fail_session_ready", fail)
    monkeypatch.setattr(repo, "retry_session_ready", forbidden_retry)
    worker = AgentWorker(
        repo,
        worker_id="worker-v2",
        agent_loader=_failing_loader,
        max_turn_attempts=1,
    )

    with pytest.raises(RuntimeError, match="offline turn failure"):
        await worker.process_claimed(claim)

    assert calls == ["fail_session_ready:RuntimeError"]
    mailbox = await repo.mailbox.get(accepted.context.tenant_id, accepted.context.session_id)
    assert mailbox is not None and mailbox.status == MailboxStatus.IDLE
    assert mailbox.resolved_sequence == mailbox.accepted_sequence == 1


@pytest.mark.asyncio
async def test_v2_fencing_conflict_during_recovery_is_safely_suppressed(monkeypatch) -> None:
    repo = repository()
    _accepted, claim = await _claimed(repo, "claimed-fencing")
    calls: list[str] = []

    async def commit_conflict(_turn_commit):
        calls.append("commit_session_ready")
        raise FencingConflict("new owner committed")

    async def retry_conflict(_lease, *, error_type, delay):
        del error_type, delay
        calls.append("retry_session_ready")
        raise FencingConflict("recovery already owns lease")

    async def forbidden_fail(*_args, **_kwargs):
        raise AssertionError("fencing recovery must not call terminal fail")

    monkeypatch.setattr(repo, "commit_session_ready", commit_conflict)
    monkeypatch.setattr(repo, "retry_session_ready", retry_conflict)
    monkeypatch.setattr(repo, "fail_session_ready", forbidden_fail)
    worker = AgentWorker(
        repo,
        worker_id="worker-v2",
        agent_loader=_successful_loader,
    )

    with pytest.raises(FencingConflict, match="new owner committed"):
        await worker.process_claimed(claim)

    assert calls == ["commit_session_ready", "retry_session_ready"]
