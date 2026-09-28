from datetime import datetime, timezone
from uuid import uuid4

import pytest

from trpc_service.agent.contracts import (
    AgentExecutionClaim,
    AgentExecutionOutcome,
    AgentExecutionReceipt,
    AgentExecutionRequest,
    AgentRuntimeConfig,
    PolicyAction,
    PolicyDecision,
)
from trpc_service.channels import ChannelBindingConfig, IncomingMessage, MessageKind
from trpc_service.storage.types import SessionSnapshot
from trpc_service.tenant import TenantContext


def _request() -> AgentExecutionRequest:
    tenant_id = uuid4()
    agent_id = uuid4()
    tenant = TenantContext(
        tenant_id=tenant_id,
        agent_app_id=agent_id,
        config_version=1,
        request_id="request",
        trace_id="trace",
    )
    incoming = IncomingMessage(
        external_message_id="message",
        principal_id="principal",
        conversation_id="conversation",
        kind=MessageKind.TEXT,
        occurred_at=datetime.now(timezone.utc),
        text="hello",
    )
    channel = ChannelBindingConfig(
        binding_id=uuid4(),
        tenant_id=tenant_id,
        agent_app_id=agent_id,
        channel_type="wecom",
    )
    return AgentExecutionRequest(
        tenant=tenant,
        session_id="session",
        incoming=incoming,
        channel=channel,
    )


def test_agent_contracts_reject_inconsistent_runtime_values() -> None:
    request = _request()

    with pytest.raises(ValueError, match="channel tenant"):
        AgentExecutionRequest(
            tenant=request.tenant,
            session_id="session",
            incoming=request.incoming,
            channel=ChannelBindingConfig(
                binding_id=uuid4(),
                tenant_id=uuid4(),
                agent_app_id=request.tenant.agent_app_id,
                channel_type="wecom",
            ),
        )
    with pytest.raises(ValueError, match="channel Agent"):
        AgentExecutionRequest(
            tenant=request.tenant,
            session_id="session",
            incoming=request.incoming,
            channel=ChannelBindingConfig(
                binding_id=uuid4(),
                tenant_id=request.tenant.tenant_id,
                agent_app_id=uuid4(),
                channel_type="wecom",
            ),
        )
    with pytest.raises(ValueError, match="session id"):
        AgentExecutionRequest(
            tenant=request.tenant,
            session_id=" ",
            incoming=request.incoming,
            channel=request.channel,
        )
    with pytest.raises(ValueError, match="attempt"):
        AgentExecutionRequest(
            tenant=request.tenant,
            session_id="session",
            incoming=request.incoming,
            channel=request.channel,
            attempt=0,
        )
    with pytest.raises(ValueError, match="config version"):
        AgentRuntimeConfig(config_version=0, runner_name="runner")
    with pytest.raises(ValueError, match="runner name"):
        AgentRuntimeConfig(config_version=1, runner_name=" ")


def test_agent_receipt_and_claim_require_matching_terminal_state() -> None:
    session = SessionSnapshot(session_id="session", version=0)

    with pytest.raises(ValueError, match="requires its policy"):
        AgentExecutionReceipt(session=session, outcome=AgentExecutionOutcome.DENIED)
    with pytest.raises(ValueError, match="does not match"):
        AgentExecutionReceipt(
            session=session,
            outcome=AgentExecutionOutcome.SUCCEEDED,
            policy=PolicyDecision(action=PolicyAction.DENY),
        )
    with pytest.raises(ValueError, match="claim id"):
        AgentExecutionClaim(claim_id=" ")
    with pytest.raises(ValueError, match="fencing token"):
        AgentExecutionClaim(claim_id="claim", fencing_token=0)
