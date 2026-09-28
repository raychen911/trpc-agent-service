from dataclasses import replace
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import update
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from tests.test_trpc_agent_runner import _context
from trpc_service.agent import AgentToolCall, AgentToolInvoker, AgentToolKind, AgentToolResult
from trpc_service.agent.approval import (
    ApprovalDecision,
    ApprovalService,
    ApprovalStatus,
)
from trpc_service.agent.governance import (
    GovernanceContextBuilder,
    GovernedToolInvoker,
    ToolApprovalRequired,
)
from trpc_service.agent.ports import AgentContextBuilder
from trpc_service.channels.approval import ApprovalCommandProcessor
from trpc_service.log import SensitiveDataRedactor
from trpc_service.storage.adapters.postgresql_approval import PostgreSQLApprovalStore
from trpc_service.storage.orm import Base
from trpc_service.storage.runtime_orm import ApprovalRequestRow


@pytest.mark.anyio
async def test_approval_is_bound_to_original_principal_call_and_single_execution(
    tmp_path, ) -> None:
    """A copied short code cannot authorize another user or a changed operation."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'approval.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    service = ApprovalService(
        PostgreSQLApprovalStore(sessions),
        ttl_seconds=600,
    )
    context = _context()
    call = AgentToolCall(
        call_id="request-1:0:project.start",
        name="project.start",
        kind=AgentToolKind.WORKSPACE,
        logical_call_index=0,
        action="execute",
        resource="project:demo",
        arguments={"environment": "test"},
    )

    pending = await service.request(context, call, risk_level=3)

    assert pending.status is ApprovalStatus.PENDING
    assert len(pending.short_code) == 8
    assert pending.arguments_hash
    repeated = await service.request(context, call, risk_level=3)
    assert repeated.approval_id == pending.approval_id

    with pytest.raises(PermissionError, match="original requester"):
        await service.decide(
            short_code=pending.short_code,
            tenant_id=context.request.tenant.tenant_id,
            agent_app_id=context.request.tenant.agent_app_id,
            principal_id="another-principal",
            session_id=context.request.session_id,
            decision=ApprovalDecision.APPROVE,
        )

    approved = await service.decide(
        short_code=pending.short_code,
        tenant_id=context.request.tenant.tenant_id,
        agent_app_id=context.request.tenant.agent_app_id,
        principal_id=context.request.incoming.principal_id,
        session_id=context.request.session_id,
        decision=ApprovalDecision.APPROVE,
    )
    approved_context = replace(
        context,
        attributes={"approval_id": str(approved.approval_id)},
    )

    claimed = await service.claim_execution(approved_context, call)
    assert claimed.status is ApprovalStatus.EXECUTING
    with pytest.raises(PermissionError, match="not executable"):
        await service.claim_execution(approved_context, call)

    completed = await service.complete_execution(claimed.approval_id)
    assert completed.status is ApprovalStatus.EXECUTED

    changed_call = replace(call, arguments={"environment": "production"})
    with pytest.raises(PermissionError, match="does not match"):
        await service.claim_execution(approved_context, changed_call)

    await engine.dispose()


@pytest.mark.anyio
async def test_channel_confirmation_resolves_to_trusted_execution_evidence(tmp_path) -> None:
    """Only an exact confirmation command from the original conversation is trusted."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'channel.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    approvals = ApprovalService(
        PostgreSQLApprovalStore(async_sessionmaker(engine, expire_on_commit=False)),
        ttl_seconds=600,
    )
    context = _context()
    context = replace(
        context,
        request=replace(
            context.request,
            incoming=replace(
                context.request.incoming,
                artifact_refs=("tenant-artifact-1", ),
            ),
        ),
    )
    call = AgentToolCall(
        call_id="request-1:0:project.start",
        name="project.start",
        kind=AgentToolKind.WORKSPACE,
        logical_call_index=0,
        resource="project:demo",
    )
    pending = await approvals.request(context, call, risk_level=3)
    processor = ApprovalCommandProcessor(approvals)

    ignored = await processor.process(
        replace(context.request.incoming, text=f"普通消息 {pending.short_code}"),
        context.request.channel,
        context.request.session_id,
    )
    assert ignored == replace(context.request.incoming, text=f"普通消息 {pending.short_code}")

    approved = await processor.process(
        replace(context.request.incoming, text=f"确认 {pending.short_code}"),
        context.request.channel,
        context.request.session_id,
    )

    assert approved.text == "已确认 project.start，请继续执行原操作。"
    assert approved.artifact_refs == ("tenant-artifact-1", )
    assert approved.attributes["approval_verified"] is True
    assert approved.attributes["approval_id"] == str(pending.approval_id)
    assert pending.short_code not in str(approved.attributes)
    assert pending.operation_arguments == {}

    class StaticContextBuilder(AgentContextBuilder):

        async def build(self, request, config, policy, claim):  # type: ignore[no-untyped-def]
            del config, policy, claim
            return replace(context, request=request)

    runtime_context = await GovernanceContextBuilder(
        StaticContextBuilder(),
        SensitiveDataRedactor(),
    ).build(
        replace(context.request, incoming=approved),
        context.config,
        context.policy,
        context.claim,
    )
    assert runtime_context.attributes == {"approval_id": str(pending.approval_id)}

    await engine.dispose()


@pytest.mark.anyio
async def test_expired_approval_persists_terminal_state(tmp_path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'expired.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    approvals = ApprovalService(PostgreSQLApprovalStore(sessions), ttl_seconds=600)
    context = _context()
    call = AgentToolCall(
        call_id="request-1:0:project.stop",
        name="project.stop",
        kind=AgentToolKind.WORKSPACE,
        logical_call_index=0,
    )
    pending = await approvals.request(context, call, risk_level=3)
    async with sessions.begin() as session:
        await session.execute(
            update(ApprovalRequestRow).where(
                ApprovalRequestRow.approval_id == pending.approval_id, ).values(
                    expires_at=datetime.now(timezone.utc) - timedelta(seconds=1)))

    with pytest.raises(PermissionError, match="expired"):
        await approvals.decide(
            short_code=pending.short_code,
            tenant_id=context.request.tenant.tenant_id,
            agent_app_id=context.request.tenant.agent_app_id,
            principal_id=context.request.incoming.principal_id,
            session_id=context.request.session_id,
            decision=ApprovalDecision.APPROVE,
        )
    assert (await approvals.get(pending.approval_id)).status is ApprovalStatus.EXPIRED
    await engine.dispose()


@pytest.mark.anyio
async def test_governed_invoker_requests_approval_then_executes_exactly_once(tmp_path) -> None:
    """A high-risk adapter is unreachable until its durable decision is consumed."""

    class RecordingCapability(AgentToolInvoker):

        def __init__(self) -> None:
            self.calls: list[AgentToolCall] = []

        async def invoke(self, context, call):  # type: ignore[no-untyped-def]
            del context
            self.calls.append(call)
            return AgentToolResult(call_id=call.call_id, content="started")

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'governed.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    approvals = ApprovalService(
        PostgreSQLApprovalStore(async_sessionmaker(engine, expire_on_commit=False)),
        ttl_seconds=600,
    )
    context = _context()
    context = replace(
        context,
        config=replace(
            context.config,
            tools={
                "grants": [{
                    "kind": "workspace",
                    "name": "project.start",
                    "actions": ["execute"],
                    "resources": ["project:demo"],
                    "risk_level": 3,
                }]
            },
        ),
    )
    call = AgentToolCall(
        call_id="request-1:0:project.start",
        name="project.start",
        kind=AgentToolKind.WORKSPACE,
        logical_call_index=0,
        action="execute",
        resource="project:demo",
        arguments={"environment": "test"},
    )
    delegate = RecordingCapability()
    invoker = GovernedToolInvoker(delegate, approvals=approvals)

    with pytest.raises(ToolApprovalRequired) as requested:
        await invoker.invoke(context, call)
    assert delegate.calls == []

    approval = requested.value.approval
    approved = await approvals.decide(
        short_code=approval.short_code,
        tenant_id=context.request.tenant.tenant_id,
        agent_app_id=context.request.tenant.agent_app_id,
        principal_id=context.request.incoming.principal_id,
        session_id=context.request.session_id,
        decision=ApprovalDecision.APPROVE,
    )
    approved_context = replace(
        context,
        attributes={"approval_id": str(approved.approval_id)},
    )

    result = await invoker.invoke(approved_context, call)
    assert result.content == "started"
    assert delegate.calls == [call]
    with pytest.raises(PermissionError, match="not executable"):
        await invoker.invoke(approved_context, call)

    class UnknownOutcomeCapability(AgentToolInvoker):

        async def invoke(self, runtime_context, pending_call):  # type: ignore[no-untyped-def]
            del runtime_context, pending_call
            raise TimeoutError("provider result is unknown")

    unknown_call = replace(call, call_id="request-1:1:project.start", logical_call_index=1)
    unknown_invoker = GovernedToolInvoker(UnknownOutcomeCapability(), approvals=approvals)
    with pytest.raises(ToolApprovalRequired) as unknown_requested:
        await unknown_invoker.invoke(context, unknown_call)
    unknown_approval = await approvals.decide(
        short_code=unknown_requested.value.approval.short_code,
        tenant_id=context.request.tenant.tenant_id,
        agent_app_id=context.request.tenant.agent_app_id,
        principal_id=context.request.incoming.principal_id,
        session_id=context.request.session_id,
        decision=ApprovalDecision.APPROVE,
    )
    unknown_context = replace(
        context,
        attributes={"approval_id": str(unknown_approval.approval_id)},
    )

    with pytest.raises(TimeoutError, match="unknown"):
        await unknown_invoker.invoke(unknown_context, unknown_call)

    unknown = await approvals.get(unknown_approval.approval_id)
    assert unknown.status is ApprovalStatus.UNKNOWN
    with pytest.raises(PermissionError, match="not executable"):
        await unknown_invoker.invoke(unknown_context, unknown_call)

    await engine.dispose()
