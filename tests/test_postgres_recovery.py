"""Optional PostgreSQL checks for the recovery paths that require row locks/RLS."""

from __future__ import annotations

import os
import time
from uuid import uuid4

import pytest

from trpc_service.db import PostgresControlPlane
from trpc_service.runtime import (
    CommitInput,
    InboundEnvelope,
    PlatformRuntime,
    PostgresDeliveryLedger,
    PostgresRuntimeStore,
    ReplyDraft,
    TenantContext,
    ToolCapability,
    ToolStatus,
)

pytestmark = pytest.mark.integration


def _urls() -> tuple[str, str, str]:
    migrator = os.environ.get("TRPC_TEST_MIGRATOR_DATABASE_URL")
    worker = os.environ.get("TRPC_TEST_WORKER_DATABASE_URL")
    dispatcher = os.environ.get("TRPC_TEST_DISPATCHER_DATABASE_URL")
    if not migrator or not worker or not dispatcher:
        pytest.skip("set isolated TRPC_TEST_*_DATABASE_URL values to run PostgreSQL recovery tests")
    pytest.importorskip("psycopg", exc_type=ImportError)
    return migrator, worker, dispatcher


def _seed(control: PostgresControlPlane, tenant_id: str) -> None:
    control.create_tenant(tenant_id, "PostgreSQL recovery tenant")
    control.create_agent(tenant_id, "agent", "Recovery agent")
    control.create_release(
        tenant_id,
        "agent",
        1,
        app_config={},
        model_config={"mode": "mock"},
        tool_policy={},
        knowledge_config={},
        created_by="test",
        change_reason="recovery test",
    )
    control.activate_release(tenant_id, "agent", 1)
    control.create_binding(
        tenant_id,
        binding_id="binding",
        agent_id="agent",
        provider="mock",
        external_account_id=f"recovery-{tenant_id}",
        webhook_key=f"recovery-{uuid4().hex}",
        secret_ref="test-secret",
        capabilities={},
    )


def test_postgres_delivery_leases_and_tool_takeover_are_fenced() -> None:
    migrator_url, worker_url, dispatcher_url = _urls()
    tenant_id = f"recover_{uuid4().hex}"
    control = PostgresControlPlane(migrator_url, "platform_schema_owner")
    _seed(control, tenant_id)
    runtime = PlatformRuntime(PostgresRuntimeStore(worker_url, "agent_worker"))
    context = TenantContext(tenant_id, request_id="recovery", trace_id="a" * 32)

    accepted = runtime.accept_inbound(
        context,
        InboundEnvelope(
            tenant_id=tenant_id,
            channel_binding_id="binding",
            agent_id="agent",
            session_id="delivery-session",
            idempotency_key="delivery-message",
            external_message_id="delivery-message",
            payload={"text": "hello"},
            config_version=1,
        ),
    )
    delivery_claim = runtime.claim_execution(
        context, accepted.inbox.inbox_id, "worker-delivery", lease_seconds=5
    )
    committed = runtime.commit_execution(
        context,
        delivery_claim,
        CommitInput(
            expected_session_version=delivery_claim.session_version,
            new_state={},
            reply=ReplyDraft(
                blocks=[{"type": "text", "text": "reply"}],
                channel_binding_id="binding",
                recipient_id="recipient",
            ),
        ),
    )
    assert committed.reply_outbox is not None

    ledger = PostgresDeliveryLedger(dispatcher_url, "agent_dispatcher")
    first = ledger.begin(
        tenant_id=tenant_id,
        outbox_id=committed.reply_outbox.outbox_id,
        session_id="delivery-session",
        channel_binding_id="binding",
        capability="idempotent",
        request_hash="reply-hash",
        trace_id="a" * 32,
        owner="dispatcher-a",
        lease_seconds=1,
    )
    active = ledger.begin(
        tenant_id=tenant_id,
        outbox_id=committed.reply_outbox.outbox_id,
        session_id="delivery-session",
        channel_binding_id="binding",
        capability="idempotent",
        request_hash="reply-hash",
        trace_id="a" * 32,
        owner="dispatcher-b",
        lease_seconds=1,
    )
    assert active.lease_acquired is False
    time.sleep(1.1)
    retry = ledger.begin(
        tenant_id=tenant_id,
        outbox_id=committed.reply_outbox.outbox_id,
        session_id="delivery-session",
        channel_binding_id="binding",
        capability="idempotent",
        request_hash="reply-hash",
        trace_id="a" * 32,
        owner="dispatcher-b",
        lease_seconds=5,
    )
    assert retry.attempt_no == first.attempt_no + 1
    assert retry.provider_idempotency_key == first.provider_idempotency_key
    assert ledger.finish(first, status="accepted").status == "failed"

    tool_inbox = runtime.accept_inbound(
        context,
        InboundEnvelope(
            tenant_id=tenant_id,
            channel_binding_id="binding",
            agent_id="agent",
            session_id="tool-session",
            idempotency_key="tool-message",
            external_message_id="tool-message",
            payload={"text": "tool"},
            config_version=1,
        ),
    ).inbox
    initial_claim = runtime.claim_execution(
        context, tool_inbox.inbox_id, "worker-a", lease_seconds=1
    )
    intent = runtime.prepare_tool(
        context,
        initial_claim,
        tool_step=0,
        tool_name="ticket.lookup",
        arguments={"ticket_id": "42"},
        capability=ToolCapability.IDEMPOTENT,
    )
    runtime.start_tool(context, initial_claim, intent.tool_call_id)
    time.sleep(1.1)
    recovered_claim = runtime.claim_execution(
        context, tool_inbox.inbox_id, "worker-b", lease_seconds=5
    )
    recovered = runtime.prepare_tool(
        context,
        recovered_claim,
        tool_step=0,
        tool_name="ticket.lookup",
        arguments={"ticket_id": "42"},
        capability=ToolCapability.IDEMPOTENT,
    )
    assert recovered.lease_fence == recovered_claim.lease_fence
    assert recovered.status == ToolStatus.PREPARED
    assert recovered.provider_idempotency_key == intent.provider_idempotency_key
    assert runtime.start_tool(context, recovered_claim, recovered.tool_call_id).status == ToolStatus.RUNNING
