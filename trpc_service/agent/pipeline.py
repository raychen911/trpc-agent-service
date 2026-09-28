"""Fixed orchestration for one stateless Agent Worker execution."""

import asyncio
from dataclasses import replace
import logging

from trpc_service.agent.contracts import (
    AgentExecutionClaim,
    AgentExecutionOutcome,
    AgentExecutionReceipt,
    AgentExecutionRequest,
    AgentRuntimeConfig,
    PolicyAction,
    PolicyDecision,
)
from trpc_service.agent.ports import (
    AgentConfigProvider,
    AgentContextBuilder,
    AgentExecutionCoordinator,
    AgentOutputFilter,
    AgentPolicyEngine,
    AgentResultCommitter,
    AgentResultPublisher,
    AgentRunner,
    AgentToolInvoker,
)
from trpc_service.agent.recovery import PermanentOperationError
from trpc_service.agent.usage import UsageRecorder
from trpc_service.workspace import WorkspaceHandle, WorkspaceProvider

logger = logging.getLogger(__name__)


class AgentExecutionRejected(PermissionError):
    """Raised when governance denies or pauses one Agent execution."""

    def __init__(
        self,
        decision: PolicyDecision,
        receipt: AgentExecutionReceipt,
    ) -> None:
        super().__init__(decision.reason or f"Agent execution requires {decision.action.value}")
        self.decision = decision
        self.receipt = receipt


class AgentConfigVersionMismatch(PermanentOperationError):
    """Raised when a provider returns a version different from the queued request."""


class AgentExecutionLeaseLost(RuntimeError):
    """Raised when this Worker no longer owns the durable execution fence."""


class AgentExecutionPipeline:
    """Coordinate stable stages while every operational detail stays replaceable."""

    def __init__(
        self,
        *,
        config_provider: AgentConfigProvider,
        coordinator: AgentExecutionCoordinator,
        policy_engine: AgentPolicyEngine,
        context_builder: AgentContextBuilder,
        runner: AgentRunner,
        tool_invoker: AgentToolInvoker,
        output_filter: AgentOutputFilter,
        committer: AgentResultCommitter,
        publisher: AgentResultPublisher,
        usage_recorder: UsageRecorder | None = None,
        workspace_provider: WorkspaceProvider | None = None,
    ) -> None:
        self._config_provider = config_provider
        self._coordinator = coordinator
        self._policy_engine = policy_engine
        self._context_builder = context_builder
        self._runner = runner
        self._tool_invoker = tool_invoker
        self._output_filter = output_filter
        self._committer = committer
        self._publisher = publisher
        self._usage_recorder = usage_recorder
        self._workspace_provider = workspace_provider

    async def execute(self, request: AgentExecutionRequest) -> AgentExecutionReceipt:
        """Run policy and Agent stages, then publish only committed Outbox work."""

        config = await self._config_provider.load(request)
        if config.config_version != request.tenant.config_version:
            raise AgentConfigVersionMismatch(
                "loaded Agent configuration does not match the request version")

        claim = await self._coordinator.begin(request, config)
        if claim.completed is not None:
            # A previous commit may have succeeded before its queue notification.
            # Re-publishing durable Outbox identifiers is therefore idempotent.
            await self._publisher.publish(request, config, claim.completed)
            if claim.completed.outcome is not AgentExecutionOutcome.SUCCEEDED:
                if claim.completed.policy is None:
                    raise RuntimeError("rejected execution receipt has no policy decision")
                raise AgentExecutionRejected(claim.completed.policy, claim.completed)
            return claim.completed

        renewal_stop = asyncio.Event()
        operation = asyncio.create_task(
            self._execute_claim(request, config, claim),
            name=f"agent-execution:{claim.claim_id}",
        )
        renewal = self._start_lease_renewal(claim, renewal_stop)
        if renewal is None:
            return await operation
        try:
            done, _ = await asyncio.wait(
                {operation, renewal},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if renewal in done:
                # Lost fencing authority cancels the Runner immediately. Commit-side
                # fencing remains the final guard, while cancellation limits Tool
                # side effects made by a stale Worker.
                await renewal
                raise RuntimeError("execution lease renewal ended unexpectedly")
            return await operation
        finally:
            renewal_stop.set()
            if not operation.done():
                operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            await asyncio.gather(renewal, return_exceptions=True)

    async def _execute_claim(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
        claim: AgentExecutionClaim,
    ) -> AgentExecutionReceipt:
        """Execute one owned claim; lease supervision is handled by execute."""

        succeeded = False
        try:
            try:
                decision = await self._policy_engine.evaluate(request, config)
            except Exception as error:
                await self._coordinator.fail(claim, error)
                raise
            if decision.action is not PolicyAction.ALLOW:
                receipt = await self._coordinator.reject(claim, decision)
                await self._publisher.publish(request, config, receipt)
                raise AgentExecutionRejected(decision, receipt)

            try:
                context = await self._context_builder.build(request, config, decision, claim)
                workspace: WorkspaceHandle | None = None
                if self._workspace_provider is not None:
                    workspace = await self._workspace_provider.acquire(
                        request.tenant,
                        request.tenant.request_id,
                    )
                    context = replace(context, workspace=workspace)
                try:
                    result = await self._runner.run(context, self._tool_invoker)
                    result = await self._output_filter.apply(context, result)
                    # The committer may persist stable references produced in the
                    # workspace, so retain the handle until the transaction ends.
                    receipt = await self._committer.commit(context, result)
                finally:
                    if workspace is not None:
                        await self._release_workspace(workspace)
                # Publishing before the commit above would allow replies for
                # rolled-back Session, Inbox, checkpoint or Outbox state.
            except Exception as error:
                await self._coordinator.fail(claim, error)
                raise
            await self._publisher.publish(request, config, receipt)
            succeeded = True
            return receipt
        finally:
            if not succeeded and self._usage_recorder is not None:
                try:
                    # This finally path also runs for asyncio cancellation, so a
                    # lost Worker lease cannot strand the tenant reservation.
                    await self._usage_recorder.release_request(request)
                except Exception as release_error:
                    logger.error(
                        "Usage reservation release failed error_type=%s",
                        type(release_error).__name__,
                    )

    async def _release_workspace(self, handle: WorkspaceHandle) -> None:
        """Release runtime resources without retrying completed model or Tool work."""

        if self._workspace_provider is None:
            return
        try:
            await self._workspace_provider.release(handle)
        except Exception as error:
            # Workspace teardown is operational cleanup. Turning it into an Agent
            # retry could duplicate already executed Tools and model usage.
            logger.error(
                "Workspace release failed workspace_id=%s error_type=%s",
                handle.workspace_id,
                type(error).__name__,
            )

    def _start_lease_renewal(
        self,
        claim: AgentExecutionClaim,
        stop: asyncio.Event,
    ) -> asyncio.Task[None] | None:
        """Start a keepalive only for coordinators backed by expiring leases."""

        interval = self._coordinator.lease_renewal_interval_seconds
        if interval is None:
            return None
        return asyncio.create_task(self._renew_lease(claim, stop, interval))

    async def _renew_lease(
        self,
        claim: AgentExecutionClaim,
        stop: asyncio.Event,
        interval: float,
    ) -> None:
        """Renew until completion; commit-side fencing remains authoritative."""

        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval)
                return
            except TimeoutError:
                if not await self._coordinator.renew(claim):
                    raise AgentExecutionLeaseLost(
                        f"Agent execution lease was lost: {claim.claim_id}")
