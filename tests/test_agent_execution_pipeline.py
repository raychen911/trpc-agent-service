import asyncio
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from trpc_service.agent import (
    AgentConfigProvider,
    AgentContextBuilder,
    AgentConfigVersionMismatch,
    AgentExecutionContext,
    AgentExecutionClaim,
    AgentExecutionCoordinator,
    AgentExecutionOutcome,
    AgentExecutionPipeline,
    AgentExecutionReceipt,
    AgentExecutionRejected,
    AgentExecutionRequest,
    AgentPolicyEngine,
    AgentOutputFilter,
    AgentReply,
    AgentResultCommitter,
    AgentResultPublisher,
    AgentRunner,
    AgentRuntimeConfig,
    AgentRunResult,
    AgentToolCall,
    AgentToolInvoker,
    AgentToolKind,
    AgentToolResult,
    PolicyAction,
    PolicyDecision,
)
from trpc_service.channels import ChannelBindingConfig, IncomingMessage, MessageKind
from trpc_service.agent.pipeline import AgentExecutionLeaseLost
from trpc_service.agent.usage import UsageRecorder
from trpc_service.storage import SessionSnapshot
from trpc_service.tenant import TenantContext
from trpc_service.workspace import WorkspaceHandle, WorkspaceKind, WorkspaceProvider


class StubConfigProvider(AgentConfigProvider):

    def __init__(self, calls: list[str], config_version: int = 3) -> None:
        self._calls = calls
        self._config_version = config_version

    async def load(self, request: AgentExecutionRequest) -> AgentRuntimeConfig:
        self._calls.append("config")
        return AgentRuntimeConfig(config_version=self._config_version, runner_name="trpc_agent")


class StubExecutionCoordinator(AgentExecutionCoordinator):

    def __init__(
        self,
        calls: list[str],
        completed: AgentExecutionReceipt | None = None,
    ) -> None:
        self._calls = calls
        self._completed = completed

    async def begin(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
    ) -> AgentExecutionClaim:
        self._calls.append("claim")
        return AgentExecutionClaim(claim_id="claim-1", completed=self._completed)

    async def reject(
        self,
        claim: AgentExecutionClaim,
        decision: PolicyDecision,
    ) -> AgentExecutionReceipt:
        self._calls.append("reject")
        outcome = (AgentExecutionOutcome.DENIED if decision.action is PolicyAction.DENY else
                   AgentExecutionOutcome.REVIEW_REQUIRED)
        return AgentExecutionReceipt(
            session=SessionSnapshot(session_id="session-1", version=0),
            committed_outbox_ids=("audit-outbox-1", ),
            outcome=outcome,
            policy=decision,
        )

    async def fail(self, claim: AgentExecutionClaim, error: Exception) -> None:
        self._calls.append("fail")


class LostLeaseCoordinator(StubExecutionCoordinator):
    """Lose fencing authority on the first scheduled renewal."""

    @property
    def lease_renewal_interval_seconds(self) -> float:
        return 0.01

    async def renew(self, claim: AgentExecutionClaim) -> bool:
        del claim
        return False


class StubPolicyEngine(AgentPolicyEngine):

    def __init__(
        self,
        calls: list[str],
        decision: PolicyDecision | None = None,
    ) -> None:
        self._calls = calls
        self._decision = decision or PolicyDecision(action=PolicyAction.ALLOW)

    async def evaluate(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
    ) -> PolicyDecision:
        self._calls.append("policy")
        return self._decision


class StubContextBuilder(AgentContextBuilder):

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def build(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
        policy: PolicyDecision,
        claim: AgentExecutionClaim,
    ) -> AgentExecutionContext:
        self._calls.append("context")
        return AgentExecutionContext(
            request=request,
            config=config,
            policy=policy,
            claim=claim,
        )


class StubRunner(AgentRunner):

    def __init__(self, calls: list[str], error: Exception | None = None) -> None:
        self._calls = calls
        self._error = error

    async def run(
        self,
        context: AgentExecutionContext,
        tools: AgentToolInvoker,
    ) -> AgentRunResult:
        self._calls.append("runner")
        if self._error is not None:
            raise self._error
        await tools.invoke(
            context,
            AgentToolCall(
                call_id="tool-call-1",
                name="lookup",
                kind=AgentToolKind.TOOL,
                logical_call_index=0,
            ),
        )
        return AgentRunResult(replies=(AgentReply(kind=MessageKind.TEXT, text="reply"), ))


class BlockingRunner(AgentRunner):
    """Record cancellation of a model call that has not returned."""

    def __init__(self) -> None:
        self.started = asyncio.Event()
        self.cancelled = asyncio.Event()

    async def run(
        self,
        context: AgentExecutionContext,
        tools: AgentToolInvoker,
    ) -> AgentRunResult:
        del context, tools
        self.started.set()
        try:
            await asyncio.Event().wait()
        finally:
            self.cancelled.set()
        raise AssertionError("unreachable")


class StubToolInvoker(AgentToolInvoker):

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def invoke(
        self,
        context: AgentExecutionContext,
        call: AgentToolCall,
    ) -> AgentToolResult:
        self._calls.append("tool")
        return AgentToolResult(call_id=call.call_id, content="tool result")


class StubOutputFilter(AgentOutputFilter):

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def apply(
        self,
        context: AgentExecutionContext,
        result: AgentRunResult,
    ) -> AgentRunResult:
        self._calls.append("output")
        return result


class StubCommitter(AgentResultCommitter):

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def commit(
        self,
        context: AgentExecutionContext,
        result: AgentRunResult,
    ) -> AgentExecutionReceipt:
        self._calls.append("commit")
        return AgentExecutionReceipt(
            session=SessionSnapshot(session_id=context.request.session_id, version=1),
            committed_outbox_ids=("reply-outbox-1", ),
        )


class StubPublisher(AgentResultPublisher):

    def __init__(self, calls: list[str], error: Exception | None = None) -> None:
        self._calls = calls
        self._error = error

    async def publish(
        self,
        request: AgentExecutionRequest,
        config: AgentRuntimeConfig,
        receipt: AgentExecutionReceipt,
    ) -> None:
        self._calls.append("publish")
        if self._error is not None:
            raise self._error


class RecordingWorkspaceProvider(WorkspaceProvider):
    """Record request-scoped workspace lifecycle without touching the filesystem."""

    def __init__(self, calls: list[str]) -> None:
        self._calls = calls

    async def acquire(self, context: TenantContext, workspace_id: str) -> WorkspaceHandle:
        self._calls.append("workspace.acquire")
        return WorkspaceHandle(
            tenant_id=context.tenant_id,
            agent_app_id=context.agent_app_id,
            workspace_id=workspace_id,
            kind=WorkspaceKind.LOCAL,
            location=f"/tmp/{context.tenant_id}/{context.agent_app_id}/{workspace_id}",
        )

    async def release(self, handle: WorkspaceHandle) -> None:
        assert handle.workspace_id == "request-1"
        self._calls.append("workspace.release")

    async def list_files(
        self,
        handle: WorkspaceHandle,
        relative_path: str,
        *,
        max_results: int,
        max_entries: int,
    ) -> tuple[str, ...]:
        del handle, relative_path, max_results, max_entries
        raise NotImplementedError

    async def read_text(
        self,
        handle: WorkspaceHandle,
        relative_path: str,
        *,
        max_bytes: int,
    ) -> str:
        del handle, relative_path, max_bytes
        raise NotImplementedError


class RecordingUsageRecorder(UsageRecorder):
    """Record cleanup calls without coupling pipeline tests to SQL."""

    def __init__(self) -> None:
        self.released_request_ids: list[str] = []

    async def record(self, context, result):  # type: ignore[no-untyped-def]
        del context, result

    async def release_request(self, request: AgentExecutionRequest) -> None:
        self.released_request_ids.append(request.tenant.request_id)


def _request() -> AgentExecutionRequest:
    tenant_id = uuid4()
    agent_app_id = uuid4()
    return AgentExecutionRequest(
        tenant=TenantContext(
            tenant_id=tenant_id,
            agent_app_id=agent_app_id,
            config_version=3,
            request_id="request-1",
            trace_id="trace-1",
        ),
        session_id="session-1",
        incoming=IncomingMessage(
            external_message_id="message-1",
            principal_id="user-1",
            conversation_id="conversation-1",
            kind=MessageKind.TEXT,
            occurred_at=datetime.now(timezone.utc),
            text="hello",
        ),
        channel=ChannelBindingConfig(
            binding_id=uuid4(),
            tenant_id=tenant_id,
            agent_app_id=agent_app_id,
            channel_type="web",
        ),
    )


@pytest.mark.anyio
async def test_agent_pipeline_executes_the_complete_chain_in_order() -> None:
    calls: list[str] = []
    pipeline = AgentExecutionPipeline(
        config_provider=StubConfigProvider(calls),
        coordinator=StubExecutionCoordinator(calls),
        policy_engine=StubPolicyEngine(calls),
        context_builder=StubContextBuilder(calls),
        runner=StubRunner(calls),
        tool_invoker=StubToolInvoker(calls),
        output_filter=StubOutputFilter(calls),
        committer=StubCommitter(calls),
        publisher=StubPublisher(calls),
    )

    receipt = await pipeline.execute(_request())

    assert calls == [
        "config",
        "claim",
        "policy",
        "context",
        "runner",
        "tool",
        "output",
        "commit",
        "publish",
    ]
    assert receipt.session.version == 1
    assert receipt.committed_outbox_ids == ("reply-outbox-1", )


@pytest.mark.anyio
async def test_agent_pipeline_binds_workspace_to_the_worker_execution() -> None:
    """Any Worker claiming the request receives its deterministic Workspace."""

    calls: list[str] = []

    class WorkspaceAwareRunner(StubRunner):

        async def run(
            self,
            context: AgentExecutionContext,
            tools: AgentToolInvoker,
        ) -> AgentRunResult:
            assert context.workspace is not None
            assert context.workspace.workspace_id == context.request.tenant.request_id
            return await super().run(context, tools)

    pipeline = AgentExecutionPipeline(
        config_provider=StubConfigProvider(calls),
        coordinator=StubExecutionCoordinator(calls),
        policy_engine=StubPolicyEngine(calls),
        context_builder=StubContextBuilder(calls),
        runner=WorkspaceAwareRunner(calls),
        tool_invoker=StubToolInvoker(calls),
        output_filter=StubOutputFilter(calls),
        committer=StubCommitter(calls),
        publisher=StubPublisher(calls),
        workspace_provider=RecordingWorkspaceProvider(calls),
    )

    await pipeline.execute(_request())

    assert calls == [
        "config",
        "claim",
        "policy",
        "context",
        "workspace.acquire",
        "runner",
        "tool",
        "output",
        "commit",
        "workspace.release",
        "publish",
    ]


@pytest.mark.anyio
async def test_agent_pipeline_releases_workspace_after_a_failed_runner() -> None:
    """A failed WorkNode attempt retains no provider resource lease."""

    calls: list[str] = []
    pipeline = AgentExecutionPipeline(
        config_provider=StubConfigProvider(calls),
        coordinator=StubExecutionCoordinator(calls),
        policy_engine=StubPolicyEngine(calls),
        context_builder=StubContextBuilder(calls),
        runner=StubRunner(calls, error=RuntimeError("model failed")),
        tool_invoker=StubToolInvoker(calls),
        output_filter=StubOutputFilter(calls),
        committer=StubCommitter(calls),
        publisher=StubPublisher(calls),
        workspace_provider=RecordingWorkspaceProvider(calls),
    )

    with pytest.raises(RuntimeError, match="model failed"):
        await pipeline.execute(_request())

    assert calls == [
        "config",
        "claim",
        "policy",
        "context",
        "workspace.acquire",
        "runner",
        "workspace.release",
        "fail",
    ]


@pytest.mark.anyio
async def test_agent_pipeline_stops_when_policy_requires_review() -> None:
    calls: list[str] = []
    decision = PolicyDecision(action=PolicyAction.REVIEW, reason="dangerous tool")
    pipeline = AgentExecutionPipeline(
        config_provider=StubConfigProvider(calls),
        coordinator=StubExecutionCoordinator(calls),
        policy_engine=StubPolicyEngine(calls, decision),
        context_builder=StubContextBuilder(calls),
        runner=StubRunner(calls),
        tool_invoker=StubToolInvoker(calls),
        output_filter=StubOutputFilter(calls),
        committer=StubCommitter(calls),
        publisher=StubPublisher(calls),
    )

    with pytest.raises(AgentExecutionRejected) as captured:
        await pipeline.execute(_request())

    assert captured.value.decision is decision
    assert captured.value.receipt.outcome is AgentExecutionOutcome.REVIEW_REQUIRED
    assert calls == ["config", "claim", "policy", "reject", "publish"]


@pytest.mark.anyio
async def test_agent_pipeline_rejects_a_different_configuration_version() -> None:
    calls: list[str] = []
    pipeline = AgentExecutionPipeline(
        config_provider=StubConfigProvider(calls, config_version=4),
        coordinator=StubExecutionCoordinator(calls),
        policy_engine=StubPolicyEngine(calls),
        context_builder=StubContextBuilder(calls),
        runner=StubRunner(calls),
        tool_invoker=StubToolInvoker(calls),
        output_filter=StubOutputFilter(calls),
        committer=StubCommitter(calls),
        publisher=StubPublisher(calls),
    )

    with pytest.raises(AgentConfigVersionMismatch):
        await pipeline.execute(_request())

    assert calls == ["config"]


@pytest.mark.anyio
async def test_agent_pipeline_never_commits_or_publishes_a_failed_run() -> None:
    calls: list[str] = []
    pipeline = AgentExecutionPipeline(
        config_provider=StubConfigProvider(calls),
        coordinator=StubExecutionCoordinator(calls),
        policy_engine=StubPolicyEngine(calls),
        context_builder=StubContextBuilder(calls),
        runner=StubRunner(calls, error=RuntimeError("model failed")),
        tool_invoker=StubToolInvoker(calls),
        output_filter=StubOutputFilter(calls),
        committer=StubCommitter(calls),
        publisher=StubPublisher(calls),
    )

    with pytest.raises(RuntimeError, match="model failed"):
        await pipeline.execute(_request())

    assert calls == ["config", "claim", "policy", "context", "runner", "fail"]


@pytest.mark.anyio
async def test_agent_pipeline_cancels_runner_after_execution_lease_loss() -> None:
    """A stale Worker cannot keep making model or Tool side effects."""

    calls: list[str] = []
    runner = BlockingRunner()
    usage = RecordingUsageRecorder()
    pipeline = AgentExecutionPipeline(
        config_provider=StubConfigProvider(calls),
        coordinator=LostLeaseCoordinator(calls),
        policy_engine=StubPolicyEngine(calls),
        context_builder=StubContextBuilder(calls),
        runner=runner,
        tool_invoker=StubToolInvoker(calls),
        output_filter=StubOutputFilter(calls),
        committer=StubCommitter(calls),
        publisher=StubPublisher(calls),
        usage_recorder=usage,
    )

    with pytest.raises(AgentExecutionLeaseLost):
        await pipeline.execute(_request())

    assert runner.started.is_set()
    assert runner.cancelled.is_set()
    assert usage.released_request_ids == ["request-1"]
    assert "commit" not in calls


@pytest.mark.anyio
async def test_agent_pipeline_reuses_a_completed_execution_without_rerunning() -> None:
    calls: list[str] = []
    completed = AgentExecutionReceipt(
        session=SessionSnapshot(session_id="session-1", version=5),
        committed_outbox_ids=("reply-outbox-1", ),
        replayed=True,
    )
    pipeline = AgentExecutionPipeline(
        config_provider=StubConfigProvider(calls),
        coordinator=StubExecutionCoordinator(calls, completed=completed),
        policy_engine=StubPolicyEngine(calls),
        context_builder=StubContextBuilder(calls),
        runner=StubRunner(calls),
        tool_invoker=StubToolInvoker(calls),
        output_filter=StubOutputFilter(calls),
        committer=StubCommitter(calls),
        publisher=StubPublisher(calls),
    )

    receipt = await pipeline.execute(_request())

    assert receipt is completed
    assert calls == ["config", "claim", "publish"]


@pytest.mark.anyio
async def test_replayed_policy_rejection_republishes_then_preserves_the_outcome() -> None:
    calls: list[str] = []
    decision = PolicyDecision(action=PolicyAction.DENY, reason="not allowed")
    completed = AgentExecutionReceipt(
        session=SessionSnapshot(session_id="session-1", version=0),
        committed_outbox_ids=("audit-outbox-1", ),
        replayed=True,
        outcome=AgentExecutionOutcome.DENIED,
        policy=decision,
    )
    pipeline = AgentExecutionPipeline(
        config_provider=StubConfigProvider(calls),
        coordinator=StubExecutionCoordinator(calls, completed=completed),
        policy_engine=StubPolicyEngine(calls),
        context_builder=StubContextBuilder(calls),
        runner=StubRunner(calls),
        tool_invoker=StubToolInvoker(calls),
        output_filter=StubOutputFilter(calls),
        committer=StubCommitter(calls),
        publisher=StubPublisher(calls),
    )

    with pytest.raises(AgentExecutionRejected) as captured:
        await pipeline.execute(_request())

    assert captured.value.receipt is completed
    assert calls == ["config", "claim", "publish"]


@pytest.mark.anyio
async def test_publish_failure_never_marks_a_committed_execution_as_failed() -> None:
    calls: list[str] = []
    pipeline = AgentExecutionPipeline(
        config_provider=StubConfigProvider(calls),
        coordinator=StubExecutionCoordinator(calls),
        policy_engine=StubPolicyEngine(calls),
        context_builder=StubContextBuilder(calls),
        runner=StubRunner(calls),
        tool_invoker=StubToolInvoker(calls),
        output_filter=StubOutputFilter(calls),
        committer=StubCommitter(calls),
        publisher=StubPublisher(calls, error=RuntimeError("queue unavailable")),
    )

    with pytest.raises(RuntimeError, match="queue unavailable"):
        await pipeline.execute(_request())

    assert calls == [
        "config",
        "claim",
        "policy",
        "context",
        "runner",
        "tool",
        "output",
        "commit",
        "publish",
    ]


def test_agent_request_rejects_a_cross_tenant_channel() -> None:
    request = _request()
    mismatched_channel = ChannelBindingConfig(
        binding_id=uuid4(),
        tenant_id=uuid4(),
        agent_app_id=request.tenant.agent_app_id,
        channel_type="web",
    )

    with pytest.raises(ValueError, match="channel tenant"):
        AgentExecutionRequest(
            tenant=request.tenant,
            session_id=request.session_id,
            incoming=request.incoming,
            channel=mismatched_channel,
        )


@pytest.mark.parametrize(
    ("outcome", "action"),
    [
        (AgentExecutionOutcome.SUCCEEDED, PolicyAction.DENY),
        (AgentExecutionOutcome.DENIED, PolicyAction.REVIEW),
        (AgentExecutionOutcome.REVIEW_REQUIRED, PolicyAction.DENY),
    ],
)
def test_execution_receipt_rejects_inconsistent_policy_outcome(
    outcome: AgentExecutionOutcome,
    action: PolicyAction,
) -> None:
    with pytest.raises(ValueError, match="does not match"):
        AgentExecutionReceipt(
            session=SessionSnapshot(session_id="session-1", version=0),
            outcome=outcome,
            policy=PolicyDecision(action=action),
        )
