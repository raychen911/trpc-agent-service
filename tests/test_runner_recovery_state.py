from datetime import datetime, timezone
from dataclasses import replace

import pytest

from tests.test_agent_task_queue import _request
from trpc_service.agent.contracts import AgentTaskClaim, AgentTaskStatus
from trpc_service.agent.recovery_state import (
    InMemoryRunnerRecoveryStore,
    RunnerAttemptStatus,
    StaleRunnerAttempt,
)


def _claim(token: int = 1) -> AgentTaskClaim:
    return AgentTaskClaim(
        task_id="task-1",
        request=_request(),
        status=AgentTaskStatus.RUNNING,
        attempt_count=1,
        fencing_token=token,
    )


@pytest.mark.anyio
async def test_runner_attempt_checkpoints_and_completes_under_one_fence() -> None:
    store = InMemoryRunnerRecoveryStore()
    attempt = await store.start(_claim(), node_id="worker-1")

    checkpoint = await store.checkpoint(
        attempt,
        stage="MODEL_COMPLETED",
        state_ref="storage://checkpoint-1",
        occurred_at=datetime.now(timezone.utc),
    )
    completed = await store.complete(attempt)

    assert checkpoint.sequence_no == 1
    assert completed.status is RunnerAttemptStatus.SUCCEEDED


@pytest.mark.anyio
async def test_new_fence_prevents_stale_attempt_from_writing_checkpoint() -> None:
    store = InMemoryRunnerRecoveryStore()
    stale = await store.start(_claim(1), node_id="worker-old")
    await store.start(replace(_claim(2), attempt_count=2), node_id="worker-new")

    with pytest.raises(StaleRunnerAttempt):
        await store.checkpoint(stale, stage="TOOL_COMPLETED", state_ref=None)


@pytest.mark.anyio
async def test_ambiguous_runner_attempt_is_recoverable_but_not_marked_success() -> None:
    store = InMemoryRunnerRecoveryStore()
    attempt = await store.start(_claim(), node_id="worker-1")

    unknown = await store.mark_unknown(attempt, "worker exited after provider call")

    assert unknown.status is RunnerAttemptStatus.UNKNOWN
    assert unknown.error_summary == "worker exited after provider call"


@pytest.mark.anyio
async def test_runner_recovery_rejects_invalid_and_conflicting_transitions() -> None:
    """Idempotent retries succeed while stale or contradictory writes fail closed."""

    store = InMemoryRunnerRecoveryStore()
    with pytest.raises(ValueError, match="node ID cannot be empty"):
        await store.start(_claim(), node_id=" ")

    attempt = await store.start(_claim(), node_id="worker-1")
    assert await store.start(_claim(), node_id="worker-1") == attempt
    with pytest.raises(ValueError, match="stage cannot be empty"):
        await store.checkpoint(attempt, stage=" ", state_ref=None)
    with pytest.raises(StaleRunnerAttempt, match="already owned"):
        await store.start(replace(_claim(), attempt_count=2), node_id="worker-2")

    completed = await store.complete(attempt)
    assert await store.complete(attempt) == completed
    with pytest.raises(StaleRunnerAttempt, match="already terminal"):
        await store.fail(attempt, "late failure", retryable=False)
    with pytest.raises(StaleRunnerAttempt, match="no longer running"):
        await store.checkpoint(attempt, stage="LATE", state_ref=None)

    newer_store = InMemoryRunnerRecoveryStore()
    await newer_store.start(_claim(2), node_id="worker-new")
    with pytest.raises(StaleRunnerAttempt, match="fence is stale"):
        await newer_store.start(_claim(1), node_id="worker-old")

    retry_store = InMemoryRunnerRecoveryStore()
    retry_attempt = await retry_store.start(_claim(), node_id="worker-retry")
    retryable = await retry_store.fail(retry_attempt, "temporary", retryable=True)
    assert retryable.status is RunnerAttemptStatus.RETRYABLE_FAILED

    permanent_store = InMemoryRunnerRecoveryStore()
    permanent_attempt = await permanent_store.start(_claim(), node_id="worker-permanent")
    permanent = await permanent_store.fail(permanent_attempt, "invalid", retryable=False)
    assert permanent.status is RunnerAttemptStatus.PERMANENT_FAILED
