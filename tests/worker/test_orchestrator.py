"""Worker state-machine tests for concurrency, fencing, crash, and Outbox commit."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

import pytest

from trpc_service.reliability import SessionClaim
from trpc_service.tenant.models import ChannelSpec, ChannelType, TenantSpec
from trpc_service.worker import (
    ActiveTenantTurnResolver,
    WorkerOrchestrator,
    WorkerOutcome,
)

from .helpers import (
    FakeExecutorFactory,
    FakeWorkerPort,
    MemoryEncryptedCodec,
    StaticResolver,
    make_app,
    make_claim,
    make_claim_input,
    make_resolved,
)


def make_orchestrator(
    port: FakeWorkerPort,
    *,
    crash: bool = False,
    delay: float = 0,
    max_attempts: int = 3,
    clock: Callable[[], datetime] | None = None,
    jitter: Callable[[float, float], float] | None = None,
) -> WorkerOrchestrator:
    assert port.claim_input is not None
    return WorkerOrchestrator(
        port=port,
        event_codec=MemoryEncryptedCodec(),
        tenant_resolver=StaticResolver(make_resolved(port.claim_input)),
        executor_factory=FakeExecutorFactory(crash=crash, delay=delay),
        lease_ttl=timedelta(milliseconds=100),
        heartbeat_interval=timedelta(milliseconds=20),
        max_attempts=max_attempts,
        clock=clock,
        jitter=jitter,
    )


@pytest.mark.asyncio
async def test_config_resolver_loads_the_revision_pinned_at_ingress() -> None:
    class RecordingRevisionLoader:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int]] = []

        async def load_revision(self, tenant_id: str, revision: int) -> TenantSpec:
            self.calls.append((tenant_id, revision))
            return TenantSpec(
                tenant_id="tenant-a",
                revision=5,
                display_name="Tenant A",
                apps=(make_app(),),
                channels=(
                    ChannelSpec(
                        binding_id="binding-a",
                        app_id="support",
                        app_revision=3,
                        channel=ChannelType.TELEGRAM,
                        external_account_id="bot-a",
                        callback_path="/v1/channels/telegram/public-a/callback",
                        public_callback_id="public-a",
                        secret_refs={
                            "webhook_secret": "secret://env/TELEGRAM_WEBHOOK_SECRET",
                            "bot_token": "secret://env/TELEGRAM_BOT_TOKEN",
                        },
                    ),
                ),
            )

    claim_input = make_claim_input(make_claim())
    loader = RecordingRevisionLoader()
    resolved = await ActiveTenantTurnResolver(loader).resolve(claim_input)
    assert loader.calls == [("tenant-a", 5)]
    assert resolved.config_revision == 5
    assert resolved.tenant_context.binding_revision == 5


@pytest.mark.asyncio
async def test_success_commits_state_events_audit_and_frozen_text_outbox_schema() -> None:
    claim = make_claim()
    port = FakeWorkerPort(claim=claim)
    result = await make_orchestrator(port).run_once(
        tenant_id="tenant-a",
        worker_id="worker-a",
    )

    assert result.outcome is WorkerOutcome.SUCCEEDED
    assert len(port.events) == 2
    assert len(port.finalized) == 1
    finalized = port.finalized[0]
    assert finalized["state"] == {"counter": 1}
    assert finalized["final_event_id"] == port.events[-1].event_id
    part = finalized["reply_parts"][0]
    assert part.payload == {"schema_version": 1, "kind": "text", "text": "answer"}
    audit = finalized["audit"]
    assert audit.channel == "telegram"
    assert audit.user_id == "principal-a"
    assert port.renewals >= 1, "the fence is renewed immediately before finalization"


@pytest.mark.asyncio
async def test_two_workers_cannot_claim_the_same_inbox() -> None:
    claim = make_claim()
    port = FakeWorkerPort(claim=claim)
    orchestrator = make_orchestrator(port, delay=0.01)
    first, second = await asyncio.gather(
        orchestrator.run_once(tenant_id="tenant-a", worker_id="worker-a"),
        orchestrator.run_once(tenant_id="tenant-a", worker_id="worker-b"),
    )
    assert {first.outcome, second.outcome} == {
        WorkerOutcome.SUCCEEDED,
        WorkerOutcome.IDLE,
    }
    assert len(port.finalized) == 1


@pytest.mark.asyncio
async def test_stale_fence_stops_execution_without_finalizing() -> None:
    claim = make_claim()
    port = FakeWorkerPort(claim=claim)
    port.raise_stale_on_append = True
    result = await make_orchestrator(port).run_once(
        tenant_id="tenant-a",
        worker_id="worker-a",
    )
    assert result.outcome is WorkerOutcome.LOST_CLAIM
    assert not port.finalized


@pytest.mark.asyncio
async def test_crash_aborts_staged_events_and_leaves_item_for_bounded_retry() -> None:
    claim = make_claim()
    port = FakeWorkerPort(claim=claim)
    now = datetime(2026, 8, 29, tzinfo=UTC)
    result = await make_orchestrator(
        port,
        crash=True,
        clock=lambda: now,
        jitter=lambda lower, upper: upper,
    ).run_once(
        tenant_id="tenant-a",
        worker_id="worker-a",
    )
    assert result.outcome is WorkerOutcome.RETRY_WAIT
    assert port.aborted == 1
    assert port.deferred == [(now + timedelta(seconds=1), "ConnectionError")]
    assert not port.finalized
    assert "secret backend message" not in repr(result)


@pytest.mark.asyncio
async def test_stale_retry_transition_is_reported_as_lost_claim() -> None:
    claim = make_claim()
    port = FakeWorkerPort(claim=claim)
    port.raise_stale_on_defer = True
    result = await make_orchestrator(port, crash=True).run_once(
        tenant_id="tenant-a",
        worker_id="worker-a",
    )
    assert result.outcome is WorkerOutcome.LOST_CLAIM
    assert not port.deferred


@pytest.mark.asyncio
async def test_attempt_budget_turns_poison_item_into_safe_error_outbox() -> None:
    claim = make_claim(attempt_no=3)
    claim_input = make_claim_input(claim, payload={"delivery_id": "delivery-a"})
    port = FakeWorkerPort(claim=claim, claim_input=claim_input)
    result = await make_orchestrator(port, max_attempts=3).run_once(
        tenant_id="tenant-a",
        worker_id="worker-a",
    )
    assert result.outcome is WorkerOutcome.REJECTED
    assert len(port.finalized) == 1
    part = port.finalized[0]["reply_parts"][0]
    assert part.payload == {
        "schema_version": 1,
        "kind": "text",
        "text": "Agent 服务暂时不可用, 请稍后重试。",
    }
    assert port.finalized[0]["final_event_id"] is None


@pytest.mark.asyncio
async def test_heartbeat_loss_cancels_slow_turn_before_reply_commit() -> None:
    claim = make_claim()
    port = FakeWorkerPort(claim=claim)
    port.lease_valid = False
    result = await make_orchestrator(port, delay=0.08).run_once(
        tenant_id="tenant-a",
        worker_id="worker-a",
    )
    assert result.outcome is WorkerOutcome.LOST_CLAIM
    assert not port.finalized


@pytest.mark.asyncio
async def test_caller_cancellation_cancels_turn_then_aborts_staged_events() -> None:
    claim = make_claim()
    port = FakeWorkerPort(claim=claim)
    task = asyncio.create_task(
        make_orchestrator(port, delay=0.2).run_once(
            tenant_id="tenant-a",
            worker_id="worker-a",
        )
    )
    for _ in range(100):
        if port.events:
            break
        await asyncio.sleep(0.001)
    assert len(port.events) == 1
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await asyncio.sleep(0.01)
    assert len(port.events) == 1, "the detached Agent task must not append after abort"
    assert port.aborted == 1
    assert not port.finalized


@pytest.mark.asyncio
async def test_claim_backend_failure_returns_only_a_sanitized_retry_result() -> None:
    class FailingClaimPort(FakeWorkerPort):
        async def claim_next(
            self,
            tenant_id: str,
            worker_id: str,
            *,
            lease_ttl: timedelta,
        ) -> SessionClaim | None:
            del tenant_id, worker_id, lease_ttl
            raise ConnectionError("database password must-never-escape")

    claim = make_claim()
    port = FailingClaimPort(claim=claim)
    result = await make_orchestrator(port).run_once(
        tenant_id="tenant-a",
        worker_id="worker-a",
    )
    assert result.outcome is WorkerOutcome.RETRY_WAIT
    assert result.error_type == "ConnectionError"
    assert "password" not in repr(result)
