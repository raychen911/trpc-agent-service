"""Runner attempt and checkpoint state machine for crash-safe recovery."""

from abc import ABC, abstractmethod
import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from enum import StrEnum

from trpc_service.agent.contracts import AgentTaskClaim


class RunnerAttemptStatus(StrEnum):
    """Lifecycle of one fenced execution attempt."""

    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    RETRYABLE_FAILED = "RETRYABLE_FAILED"
    PERMANENT_FAILED = "PERMANENT_FAILED"
    UNKNOWN = "UNKNOWN"


class StaleRunnerAttempt(RuntimeError):
    """Raised when a superseded Worker attempts to mutate recovery state."""


@dataclass(frozen=True, slots=True)
class RunnerAttemptSnapshot:
    """Durable identity and current outcome of one Worker execution."""

    attempt_id: str
    task_id: str
    request_id: str
    attempt_no: int
    fencing_token: int
    node_id: str
    status: RunnerAttemptStatus
    error_summary: str | None = None


@dataclass(frozen=True, slots=True)
class RunnerCheckpointSnapshot:
    """Ordered recovery position written by the current fenced attempt."""

    attempt_id: str
    sequence_no: int
    stage: str
    state_ref: str | None
    occurred_at: datetime


class RunnerRecoveryStore(ABC):
    """Persistence port for attempt, checkpoint and fencing transitions."""

    @abstractmethod
    async def start(self, claim: AgentTaskClaim, *, node_id: str) -> RunnerAttemptSnapshot:
        """Start or recover the attempt identified by the queue fence."""

    @abstractmethod
    async def checkpoint(
        self,
        attempt: RunnerAttemptSnapshot,
        *,
        stage: str,
        state_ref: str | None,
        occurred_at: datetime | None = None,
    ) -> RunnerCheckpointSnapshot:
        """Append a checkpoint only while the attempt owns the current fence."""

    @abstractmethod
    async def complete(self, attempt: RunnerAttemptSnapshot) -> RunnerAttemptSnapshot:
        """Mark an owned attempt successful."""

    @abstractmethod
    async def fail(
        self,
        attempt: RunnerAttemptSnapshot,
        summary: str,
        *,
        retryable: bool,
    ) -> RunnerAttemptSnapshot:
        """Mark a known retryable or permanent failure."""

    @abstractmethod
    async def mark_unknown(
        self,
        attempt: RunnerAttemptSnapshot,
        summary: str,
    ) -> RunnerAttemptSnapshot:
        """Quarantine an attempt whose final side effects cannot be proven."""


class InMemoryRunnerRecoveryStore(RunnerRecoveryStore):
    """Reference state machine used by deterministic unit tests."""

    def __init__(self) -> None:
        self._current_fence: dict[str, int] = {}
        self._attempts: dict[str, RunnerAttemptSnapshot] = {}
        self._checkpoints: dict[str, list[RunnerCheckpointSnapshot]] = {}
        self._lock = asyncio.Lock()

    async def start(self, claim: AgentTaskClaim, *, node_id: str) -> RunnerAttemptSnapshot:
        if not node_id.strip():
            raise ValueError("Runner node ID cannot be empty")
        attempt_id = f"{claim.task_id}:{claim.attempt_count}:{claim.fencing_token}"
        async with self._lock:
            current = self._current_fence.get(claim.task_id, 0)
            if claim.fencing_token < current:
                raise StaleRunnerAttempt("Runner attempt fence is stale")
            existing = self._attempts.get(attempt_id)
            if existing is not None:
                return existing
            if claim.fencing_token == current and current != 0:
                raise StaleRunnerAttempt("Runner fence is already owned by another attempt")
            self._current_fence[claim.task_id] = claim.fencing_token
            attempt = RunnerAttemptSnapshot(
                attempt_id=attempt_id,
                task_id=claim.task_id,
                request_id=claim.request.tenant.request_id,
                attempt_no=claim.attempt_count,
                fencing_token=claim.fencing_token,
                node_id=node_id,
                status=RunnerAttemptStatus.RUNNING,
            )
            self._attempts[attempt_id] = attempt
            self._checkpoints[attempt_id] = []
            return attempt

    async def checkpoint(
        self,
        attempt: RunnerAttemptSnapshot,
        *,
        stage: str,
        state_ref: str | None,
        occurred_at: datetime | None = None,
    ) -> RunnerCheckpointSnapshot:
        if not stage.strip():
            raise ValueError("Runner checkpoint stage cannot be empty")
        async with self._lock:
            current = self._owned(attempt)
            if current.status is not RunnerAttemptStatus.RUNNING:
                raise StaleRunnerAttempt("Runner attempt is no longer running")
            records = self._checkpoints[attempt.attempt_id]
            checkpoint = RunnerCheckpointSnapshot(
                attempt_id=attempt.attempt_id,
                sequence_no=len(records) + 1,
                stage=stage,
                state_ref=state_ref,
                occurred_at=occurred_at or datetime.now(timezone.utc),
            )
            records.append(checkpoint)
            return checkpoint

    async def complete(self, attempt: RunnerAttemptSnapshot) -> RunnerAttemptSnapshot:
        return await self._finish(attempt, RunnerAttemptStatus.SUCCEEDED, None)

    async def fail(
        self,
        attempt: RunnerAttemptSnapshot,
        summary: str,
        *,
        retryable: bool,
    ) -> RunnerAttemptSnapshot:
        status = (RunnerAttemptStatus.RETRYABLE_FAILED
                  if retryable else RunnerAttemptStatus.PERMANENT_FAILED)
        return await self._finish(attempt, status, summary)

    async def mark_unknown(
        self,
        attempt: RunnerAttemptSnapshot,
        summary: str,
    ) -> RunnerAttemptSnapshot:
        return await self._finish(attempt, RunnerAttemptStatus.UNKNOWN, summary)

    def _owned(self, attempt: RunnerAttemptSnapshot) -> RunnerAttemptSnapshot:
        current = self._attempts.get(attempt.attempt_id)
        if (current is None or self._current_fence.get(attempt.task_id) != attempt.fencing_token):
            raise StaleRunnerAttempt("Runner attempt fence is stale")
        return current

    async def _finish(
        self,
        attempt: RunnerAttemptSnapshot,
        status: RunnerAttemptStatus,
        summary: str | None,
    ) -> RunnerAttemptSnapshot:
        async with self._lock:
            current = self._owned(attempt)
            if current.status is not RunnerAttemptStatus.RUNNING:
                if current.status is status and current.error_summary == summary:
                    return current
                raise StaleRunnerAttempt("Runner attempt is already terminal")
            updated = replace(current, status=status, error_summary=summary)
            self._attempts[attempt.attempt_id] = updated
            return updated
