"""Lifecycle for horizontally scalable, stateless Agent Worker processes."""

import asyncio
from dataclasses import replace
from datetime import datetime, timedelta, timezone
import hashlib
import logging

from trpc_service.agent.contracts import (
    AgentExecutionReceipt,
    AgentTaskClaim,
)
from trpc_service.agent.pipeline import AgentExecutionPipeline, AgentExecutionRejected
from trpc_service.agent.ports import AgentTaskQueue
from trpc_service.agent.recovery import FailureDisposition, RecoveryPolicy
from trpc_service.log import bind_log_context
from trpc_service.config.runtime import LeasedWorkerConfig
from trpc_service.metrics import PlatformTelemetry
from trpc_service.agent.recovery_state import RunnerAttemptSnapshot, RunnerRecoveryStore

logger = logging.getLogger(__name__)


class AgentTaskLeaseLost(RuntimeError):
    """Raised when another Worker may reclaim the durable queue task."""


class AgentWorkerService:
    """Compete for durable tasks without retaining Session state in the node."""

    def __init__(
        self,
        *,
        queue: AgentTaskQueue,
        pipeline: AgentExecutionPipeline,
        node_id: str,
        concurrency: int,
        runtime: LeasedWorkerConfig,
        telemetry: PlatformTelemetry | None = None,
        recovery_store: RunnerRecoveryStore | None = None,
    ) -> None:
        if node_id.strip() == "":
            raise ValueError("Agent Worker node ID cannot be empty")
        if concurrency < 1:
            raise ValueError("Agent Worker concurrency must be positive")
        self._queue = queue
        self._pipeline = pipeline
        self._node_id = node_id
        self._concurrency = concurrency
        self._runtime = runtime
        self._recovery = RecoveryPolicy()
        self._telemetry = telemetry
        self._recovery_store = recovery_store
        self._stop = asyncio.Event()
        self._tasks: tuple[asyncio.Task[None], ...] = ()

    async def start(self) -> None:
        """Start configured local slots; other processes use the same queue."""

        if self._tasks:
            return
        self._stop.clear()
        self._tasks = tuple(
            asyncio.create_task(
                self._run_slot(slot),
                name=f"agent-worker:{self._node_id}:{slot}",
            ) for slot in range(self._concurrency))

    async def close(self) -> None:
        """Stop claiming new tasks and wait for local slots to finish cleanly."""

        self.stop_claiming()
        tasks, self._tasks = self._tasks, ()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    def stop_claiming(self) -> None:
        """Synchronously close intake before publishing the draining state."""

        self._stop.set()

    async def _run_slot(self, slot: int) -> None:
        """Continuously claim due work until graceful shutdown is requested."""

        claim_failures = 0
        while not self._stop.is_set():
            try:
                processed = await self.run_once(slot)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                claim_failures += 1
                logger.error(
                    "Agent Worker slot %s failed while claiming work with %s",
                    slot,
                    type(error).__name__,
                )
                delay = self._recovery.retry_delay_seconds(
                    operation_key=f"{self._node_id}:claim:{slot}",
                    attempt_count=claim_failures,
                    base_seconds=self._runtime.retry_base_seconds,
                    maximum_seconds=self._runtime.retry_max_seconds,
                    jitter_ratio=self._runtime.retry_jitter_ratio,
                )
                await self._wait_for_stop(delay)
                continue
            claim_failures = 0
            if not processed:
                await self._wait_for_stop(self._runtime.poll_interval_seconds)

    async def _wait_for_stop(self, delay_seconds: float) -> None:
        """Sleep interruptibly so database recovery never delays shutdown."""

        try:
            await asyncio.wait_for(self._stop.wait(), timeout=delay_seconds)
        except TimeoutError:
            pass

    async def run_once(self, slot: int) -> bool:
        """Process at most one task; exposed for deterministic health tests."""

        if slot < 0 or slot >= self._concurrency:
            raise ValueError("Agent Worker slot is outside configured concurrency")
        worker_id = f"{self._node_id}:{slot}"
        claim = await self._queue.claim(
            worker_id,
            lease_until=self._new_lease_deadline(),
        )
        if claim is None:
            return False

        attempt: RunnerAttemptSnapshot | None = None
        try:
            if self._recovery_store is not None:
                attempt = await self._recovery_store.start(claim, node_id=worker_id)
                await self._recovery_store.checkpoint(
                    attempt,
                    stage="PIPELINE_STARTED",
                    state_ref=None,
                )
            await self._execute_while_owned(claim, worker_id)
        except asyncio.CancelledError:
            # Leave the lease intact; another node reclaims it after expiry.
            if attempt is not None and self._recovery_store is not None:
                await asyncio.shield(
                    self._recovery_store.mark_unknown(attempt, "Worker was cancelled"))
            raise
        except AgentTaskLeaseLost:
            # Never mutate a task after losing ownership. The active lease owner
            # or a later reclaimer is responsible for its terminal state.
            if attempt is not None and self._recovery_store is not None:
                await self._recovery_store.mark_unknown(attempt, "Agent task lease was lost")
            logger.error("Agent task %s lost its Worker lease", claim.task_id)
        except AgentExecutionRejected:
            # Governance rejection is a successfully processed terminal outcome.
            if attempt is not None and self._recovery_store is not None:
                await self._recovery_store.checkpoint(
                    attempt,
                    stage="POLICY_REJECTION_COMMITTED",
                    state_ref=None,
                )
                await self._recovery_store.complete(attempt)
            await self._queue.complete(
                claim.task_id,
                worker_id=worker_id,
                fencing_token=claim.fencing_token,
            )
        except Exception as error:
            decision = self._recovery.classify_agent(error)
            next_attempt_at = self._retry_at(claim, decision.retry_after_seconds)
            if decision.disposition is not FailureDisposition.RETRY:
                next_attempt_at = None
            if attempt is not None and self._recovery_store is not None:
                await self._recovery_store.fail(
                    attempt,
                    decision.safe_summary,
                    retryable=decision.disposition is FailureDisposition.RETRY,
                )
            await self._queue.fail(
                claim.task_id,
                worker_id=worker_id,
                fencing_token=claim.fencing_token,
                error_code=decision.error_code,
                error_summary=decision.safe_summary,
                next_attempt_at=next_attempt_at,
            )
            logger.warning(
                "Agent task %s failed on node %s with %s",
                claim.task_id,
                self._node_id,
                type(error).__name__,
            )
        else:
            if attempt is not None and self._recovery_store is not None:
                await self._recovery_store.checkpoint(
                    attempt,
                    stage="RESULT_COMMITTED",
                    state_ref=None,
                )
                await self._recovery_store.complete(attempt)
            await self._queue.complete(
                claim.task_id,
                worker_id=worker_id,
                fencing_token=claim.fencing_token,
            )
        return True

    async def _execute_while_owned(self, claim: AgentTaskClaim, worker_id: str) -> None:
        """Cancel pipeline execution immediately when queue ownership is lost."""

        renewal_stop = asyncio.Event()
        execution = asyncio.create_task(
            self._execute_pipeline(claim),
            name=f"agent-task-execute:{claim.task_id}",
        )
        renewal = asyncio.create_task(
            self._renew_while_running(claim, worker_id, renewal_stop),
            name=f"agent-task-renew:{claim.task_id}",
        )
        try:
            done, _ = await asyncio.wait(
                {execution, renewal},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if renewal in done:
                await renewal
                raise RuntimeError("Agent task renewal ended unexpectedly")
            await execution
        finally:
            renewal_stop.set()
            if not execution.done():
                execution.cancel()
            await asyncio.gather(execution, renewal, return_exceptions=True)

    async def _execute_pipeline(self, claim: AgentTaskClaim) -> AgentExecutionReceipt:
        """Restore the producer trace and expose active execution state."""

        # The durable queue owns the attempt number. Propagating it into the
        # immutable request lets inner recovery stages choose the same budget.
        request = replace(claim.request, attempt=claim.attempt_count)
        session_hash = hashlib.sha256(request.session_id.encode()).hexdigest()[:16]
        log_fields = {
            "node_id": self._node_id,
            "node_role": "worker",
            "tenant_id": request.tenant.tenant_id,
            "agent_app_id": request.tenant.agent_app_id,
            "request_id": request.tenant.request_id,
            "trace_id": request.tenant.trace_id,
            "session_id": session_hash,
        }
        if self._telemetry is None:
            with bind_log_context(**log_fields):
                return await self._pipeline.execute(request)
        parent = self._telemetry.extract_context(dict(request.trace_context))
        with bind_log_context(**log_fields), self._telemetry.start_span(
                "worker.execute",
                context=parent,
                attributes={
                    "tenant.id": str(request.tenant.tenant_id),
                    "agent.name": str(request.tenant.agent_app_id),
                    "channel.type": request.channel.channel_type,
                    "request.id": request.tenant.request_id,
                    "session.id_hash": session_hash,
                    "config.version": request.tenant.config_version,
                    "retry.count": claim.attempt_count - 1,
                },
        ):
            self._telemetry.execution_started()
            try:
                return await self._pipeline.execute(request)
            finally:
                self._telemetry.execution_finished()

    def _new_lease_deadline(self) -> datetime:
        return datetime.now(timezone.utc) + timedelta(seconds=self._runtime.lease_seconds)

    def _retry_at(
        self,
        claim: AgentTaskClaim,
        retry_after_seconds: float | None = None,
    ) -> datetime | None:
        """Return bounded exponential backoff or None after the final attempt."""

        if claim.attempt_count >= self._runtime.max_attempts:
            return None
        delay = self._recovery.retry_delay_seconds(
            operation_key=claim.task_id,
            attempt_count=claim.attempt_count,
            base_seconds=self._runtime.retry_base_seconds,
            maximum_seconds=self._runtime.retry_max_seconds,
            jitter_ratio=self._runtime.retry_jitter_ratio,
            retry_after_seconds=retry_after_seconds,
        )
        return datetime.now(timezone.utc) + timedelta(seconds=delay)

    async def _renew_while_running(
        self,
        claim: AgentTaskClaim,
        worker_id: str,
        stop: asyncio.Event,
    ) -> None:
        """Keep long model calls owned without allowing a stale node to mutate."""

        interval = self._runtime.lease_seconds / 3
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except TimeoutError:
                renewed = await self._queue.renew(
                    claim.task_id,
                    worker_id=worker_id,
                    fencing_token=claim.fencing_token,
                    lease_until=self._new_lease_deadline(),
                )
                if not renewed:
                    raise AgentTaskLeaseLost(f"Agent task lease was lost: {claim.task_id}")
