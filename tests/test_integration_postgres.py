"""Real PostgreSQL control-plane and Outbox integration tests.

Prerequisite: TRPC_TEST_POSTGRES_URL and migrations already applied by
docker-compose.test.yml. No model/IM is called. Expected: immutable config,
SKIP LOCKED claim and delivery lease recovery are durable.
"""

import os
import json
import uuid

import pytest

from trpc_service.config import AgentAppConfig
from trpc_service.config import ChannelBindingConfig
from trpc_service.config import ChannelType
from trpc_service.config import TenantConfig
from trpc_service.gateway import OutboundMessage
from trpc_service.gateway import PostgresOutboxStore
from trpc_service.tenant import PostgresTenantRegistry

POSTGRES_URL = os.getenv("TRPC_TEST_POSTGRES_URL")
pytestmark = [pytest.mark.integration, pytest.mark.skipif(not POSTGRES_URL, reason="TRPC_TEST_POSTGRES_URL is not set")]


@pytest.mark.asyncio
async def test_postgres_registry_and_outbox_lease():
    asyncpg = pytest.importorskip("asyncpg")
    pool = await asyncpg.create_pool(POSTGRES_URL, min_size=1, max_size=3)
    suffix = uuid.uuid4().hex[:12]
    tenant_id = f"test-{suffix}"
    binding_id = f"binding-{suffix}"
    config = TenantConfig(
        tenant_id=tenant_id,
        apps={"assistant": AgentAppConfig(app_id="assistant", model={"model_name": "fake"})},
        channels=[
            ChannelBindingConfig(binding_id=binding_id,
                                 channel=ChannelType.TELEGRAM,
                                 app_id="assistant",
                                 external_account_id=suffix)
        ],
    )
    registry = PostgresTenantRegistry(pool)
    await registry.publish(config)
    assert (await registry.resolve_binding(binding_id))[0].tenant_id == tenant_id
    await registry.publish(config.model_copy(update={"version": 2}))
    assert (await registry.rollback(tenant_id, 1)).version == 1

    from trpc_service.gateway.requests import PostgresRequestStore
    from trpc_service.gateway.models import AgentRequest, RequestRecord, RequestState
    from trpc_service.tenant.budget import PostgresUsageLedger
    from trpc_service.tenant.approval import PostgresApprovalStore, ApprovalError
    request = AgentRequest(tenant_id=tenant_id,
                           request_id=f"request-{suffix}",
                           app_id="assistant",
                           config_version=1,
                           user_id="u",
                           session_id="s",
                           text="hello")
    requests = PostgresRequestStore(pool)
    await requests.create(
        RequestRecord(tenant_id=tenant_id, request_id=request.request_id, state=RequestState.RESERVED, request=request))
    request.metadata["admission_complete"] = True
    await requests.save_payload(request)
    assert (await requests.get(tenant_id, request.request_id)).request.metadata["admission_complete"]
    ledger = PostgresUsageLedger(pool)
    values = dict(tenant_id=tenant_id,
                  app_id="assistant",
                  request_id=request.request_id,
                  model_name="fake",
                  input_tokens=2,
                  output_tokens=3,
                  cost_usd=0)
    assert await ledger.record(**values)
    assert not await ledger.record(**values)
    from trpc_service.log import AuditEvent, PostgresAuditSink
    audit = PostgresAuditSink(pool)
    await audit.write(
        AuditEvent(tenant_id=tenant_id,
                   channel="web",
                   user_id="u",
                   session_id="s",
                   agent_name="assistant",
                   action="agent_run",
                   request_id=request.request_id,
                   input_tokens=2,
                   output_tokens=3,
                   cost_usd=0.5))
    audit_usage = await pool.fetchval(
        "SELECT details FROM audit_log WHERE tenant_id=$1 AND request_id=$2 ORDER BY audit_id DESC LIMIT 1",
        tenant_id,
        request.request_id,
    )
    if isinstance(audit_usage, str):
        audit_usage = json.loads(audit_usage)
    assert audit_usage == {"input_tokens": 2, "output_tokens": 3}
    approvals = PostgresApprovalStore(pool)
    digest = approvals.arguments_hash('{}')
    approval = await approvals.create(tenant_id, "u", "s", "write", digest)
    _, token = await approvals.decide(approval.approval_id, approve=True, actor="test")
    scope = dict(tenant_id=tenant_id, user_id="u", session_id="s", tool_name="write", arguments_sha256=digest)
    await approvals.consume(approval.approval_id, token, **scope)
    with pytest.raises(ApprovalError):
        await approvals.consume(approval.approval_id, token, **scope)

    outbox = PostgresOutboxStore(pool, worker_id="worker-a", lease_seconds=5)
    message = OutboundMessage(outbound_id=(suffix * 6)[:64],
                              request_id=f"request-{suffix}",
                              tenant_id=tenant_id,
                              binding_id=binding_id,
                              channel=ChannelType.TELEGRAM,
                              external_conversation_id="chat",
                              text="hello")
    assert await outbox.add(message) is True
    claimed = await outbox.claim(1, binding_ids=[binding_id])
    assert len(claimed) == 1
    await pool.execute("UPDATE outbound_message SET locked_at=now()-interval '10 seconds' WHERE outbound_id=$1",
                       message.outbound_id)
    assert await outbox.recover_expired() == 1
    await pool.close()
