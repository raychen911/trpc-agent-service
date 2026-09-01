"""Lease-aware orchestration for durable post-turn Summary/Memory projection."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import timedelta
from enum import StrEnum

from trpc_service.reliability import (
    IdempotencyConflictError,
    ProjectionClaim,
    ProjectionInput,
    ReliabilityInvariantError,
    StaleClaimError,
)
from trpc_service.storage import (
    MemoryProjection,
    ProjectionConflictError,
    SummaryProjection,
)

from .contracts import MemoryAlgorithm, MemoryCandidate, ProjectionPort, SummaryAlgorithm


class ProjectionOutputError(ValueError):
    """An injected algorithm returned output outside the durable input boundary."""


class ProjectionLeaseLostError(RuntimeError):
    """The current process no longer owns the projection fencing capability."""


class ProjectionOutcome(StrEnum):
    """Sanitized result of one projection polling iteration."""

    IDLE = "idle"
    SUCCEEDED = "succeeded"
    RETRY_WAIT = "retry_wait"
    DEAD_LETTER = "dead_letter"
    LOST_CLAIM = "lost_claim"


@dataclass(frozen=True, slots=True)
class ProjectionRunResult:
    """Operational result containing no summary or memory content."""

    outcome: ProjectionOutcome
    tenant_id: str
    worker_id: str
    job_id: str | None = None
    attempt_no: int | None = None
    error_type: str | None = None


class ProjectionWorker:
    """Claim, compute and atomically publish one post-turn projection job.

    Algorithms are injected and receive detached committed data. This class does
    not call a model or external service on its own. Production deployments can
    supply versioned algorithms while retaining the same lease/fence protocol.
    """

    def __init__(
        self,
        *,
        port: ProjectionPort,
        summarizer: SummaryAlgorithm,
        memory_extractor: MemoryAlgorithm,
        lease_ttl: timedelta = timedelta(seconds=30),
        heartbeat_interval: timedelta = timedelta(seconds=10),
        max_attempts: int = 5,
        retry_base_delay: timedelta = timedelta(seconds=2),
        retry_max_delay: timedelta = timedelta(minutes=2),
    ) -> None:
        if lease_ttl <= timedelta(0):
            raise ValueError("lease_ttl must be positive")
        if heartbeat_interval <= timedelta(0) or heartbeat_interval >= lease_ttl:
            raise ValueError("heartbeat_interval must be positive and shorter than lease_ttl")
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if retry_base_delay <= timedelta(0) or retry_max_delay < retry_base_delay:
            raise ValueError("retry delay bounds are invalid")
        for name, version in (
            ("summarizer", summarizer.version),
            ("memory_extractor", memory_extractor.version),
        ):
            if not version or len(version) > 64:
                raise ValueError(f"{name} version must contain 1..64 characters")
        self._port = port
        self._summarizer = summarizer
        self._memory_extractor = memory_extractor
        self._lease_ttl = lease_ttl
        self._heartbeat_interval = heartbeat_interval
        self._max_attempts = max_attempts
        self._retry_base_delay = retry_base_delay
        self._retry_max_delay = retry_max_delay

    async def process_once(self, tenant_id: str, worker_id: str) -> ProjectionRunResult:
        """Process at most one tenant-scoped durable job."""

        claim = await self._port.claim_projection(
            tenant_id,
            worker_id,
            lease_ttl=self._lease_ttl,
        )
        if claim is None:
            return ProjectionRunResult(ProjectionOutcome.IDLE, tenant_id, worker_id)

        try:
            projection_input = await self._port.load_projection_input(claim)
            summary_content, candidates = await self._compute_with_heartbeat(
                claim,
                projection_input,
            )
            if not await self._port.renew_projection_claim(
                claim,
                lease_ttl=self._lease_ttl,
            ):
                raise ProjectionLeaseLostError("projection lease was lost before commit")
            summary, memories = self._materialize_outputs(
                projection_input,
                summary_content,
                candidates,
            )
            await self._port.complete_projection(
                claim,
                summary=summary,
                memories=memories,
            )
            return ProjectionRunResult(
                ProjectionOutcome.SUCCEEDED,
                tenant_id,
                worker_id,
                job_id=claim.job_id,
                attempt_no=claim.attempt_no,
            )
        except (StaleClaimError, ProjectionLeaseLostError):
            return ProjectionRunResult(
                ProjectionOutcome.LOST_CLAIM,
                tenant_id,
                worker_id,
                job_id=claim.job_id,
                attempt_no=claim.attempt_no,
                error_type="projection_lease_lost",
            )
        except Exception as error:
            error_type = self._safe_error_type(error)
            permanent = isinstance(
                error,
                (
                    IdempotencyConflictError,
                    ProjectionConflictError,
                    ProjectionOutputError,
                    ReliabilityInvariantError,
                    ValueError,
                ),
            )
            try:
                if permanent or claim.attempt_no >= self._max_attempts:
                    await self._port.dead_letter_projection(claim, error_type=error_type)
                    outcome = ProjectionOutcome.DEAD_LETTER
                else:
                    await self._port.defer_projection_retry(
                        claim,
                        retry_delay=self._retry_delay(claim.attempt_no),
                        error_type=error_type,
                    )
                    outcome = ProjectionOutcome.RETRY_WAIT
            except StaleClaimError:
                outcome = ProjectionOutcome.LOST_CLAIM
                error_type = "projection_lease_lost"
            return ProjectionRunResult(
                outcome,
                tenant_id,
                worker_id,
                job_id=claim.job_id,
                attempt_no=claim.attempt_no,
                error_type=error_type,
            )

    async def _compute_with_heartbeat(
        self,
        claim: ProjectionClaim,
        projection_input: ProjectionInput,
    ) -> tuple[str | None, tuple[MemoryCandidate, ...]]:
        stop = asyncio.Event()
        compute_task = asyncio.create_task(self._compute(projection_input))
        heartbeat_task = asyncio.create_task(self._heartbeat(claim, stop))
        try:
            done, _ = await asyncio.wait(
                {compute_task, heartbeat_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat_task in done:
                error = heartbeat_task.exception()
                if not compute_task.done():
                    compute_task.cancel()
                    await asyncio.gather(compute_task, return_exceptions=True)
                if error is not None:
                    raise error
                raise ProjectionLeaseLostError("projection heartbeat stopped early")
            return await compute_task
        finally:
            stop.set()
            if not heartbeat_task.done():
                await heartbeat_task

    async def _compute(
        self,
        projection_input: ProjectionInput,
    ) -> tuple[str | None, tuple[MemoryCandidate, ...]]:
        summary_content, candidates = await asyncio.gather(
            self._summarizer.summarize(projection_input),
            self._memory_extractor.extract(projection_input),
        )
        return summary_content, tuple(candidates)

    async def _heartbeat(self, claim: ProjectionClaim, stop: asyncio.Event) -> None:
        interval = self._heartbeat_interval.total_seconds()
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except TimeoutError:
                if not await self._port.renew_projection_claim(
                    claim,
                    lease_ttl=self._lease_ttl,
                ):
                    raise ProjectionLeaseLostError("projection heartbeat lost its fence") from None

    def _materialize_outputs(
        self,
        projection_input: ProjectionInput,
        summary_content: str | None,
        candidates: tuple[MemoryCandidate, ...],
    ) -> tuple[SummaryProjection | None, tuple[MemoryProjection, ...]]:
        summary = None
        if summary_content is not None:
            if not isinstance(summary_content, str):
                raise ProjectionOutputError("summary algorithm returned a non-string value")
            summary = SummaryProjection(
                tenant_id=projection_input.tenant_id,
                session_id=projection_input.session_id,
                through_seq=projection_input.through_seq,
                content=summary_content,
                summarizer_version=self._summarizer.version,
            )

        event_versions = {event.event_id: event.seq for event in projection_input.events}
        memories: list[MemoryProjection] = []
        seen: set[str] = set()
        for candidate in candidates:
            if candidate.source_event_id in seen:
                raise ProjectionOutputError("extractor returned a duplicate source event")
            seen.add(candidate.source_event_id)
            record_version = event_versions.get(candidate.source_event_id)
            if record_version is None:
                raise ProjectionOutputError("extractor referenced an event outside the input")
            memories.append(
                MemoryProjection(
                    tenant_id=projection_input.tenant_id,
                    principal_id=projection_input.principal_id,
                    session_id=projection_input.session_id,
                    source_event_id=candidate.source_event_id,
                    extractor_version=self._memory_extractor.version,
                    record_version=record_version,
                    content=candidate.content,
                    metadata=candidate.metadata,
                )
            )
        return summary, tuple(memories)

    def _retry_delay(self, attempt_no: int) -> timedelta:
        multiplier = 2 ** max(0, attempt_no - 1)
        seconds = min(
            self._retry_base_delay.total_seconds() * multiplier,
            self._retry_max_delay.total_seconds(),
        )
        return timedelta(seconds=seconds)

    @staticmethod
    def _safe_error_type(error: Exception) -> str:
        name = type(error).__name__
        return name[:128] if name else "projection_error"
