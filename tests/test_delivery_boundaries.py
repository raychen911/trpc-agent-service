"""Delivery queue and Worker failure boundaries through their public contracts."""

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from tests.test_delivery_worker import RecordingAdapter, RecordingDeliveryQueue, _claim
from trpc_service.agent.models import AgentApp
from trpc_service.agent.recovery import FailureDisposition, RecoveryDecision
from trpc_service.channels import ChannelAdapterRegistry, ChannelBinding
from trpc_service.channels.delivery import (
    DeliveryTaskClaim,
    DeliveryWorkerService,
    PostgreSQLDeliveryTaskQueue,
    outgoing_from_outbox,
)
from trpc_service.config import LeasedWorkerConfig
from trpc_service.metrics import PlatformTelemetry
from trpc_service.storage import OutboxMessage
from trpc_service.storage.orm import Base
from trpc_service.storage.runtime_orm import AgentTaskRow, OutboxMessageRow
from trpc_service.tenant import Tenant


def _runtime(*, max_attempts: int = 3) -> LeasedWorkerConfig:
    return LeasedWorkerConfig(
        lease_seconds=30,
        poll_interval_seconds=0.001,
        retry_base_seconds=0,
        retry_max_seconds=1,
        retry_jitter_ratio=0,
        max_attempts=max_attempts,
    )


def test_delivery_claim_and_payload_validation() -> None:
    claim = _claim()
    with pytest.raises(ValueError, match="only claim IM reply"):
        DeliveryTaskClaim(
            claim.context,
            claim.binding,
            replace(claim.message, category="AUDIT"),
        )
    with pytest.raises(ValueError, match="does not match"):
        DeliveryTaskClaim(
            claim.context,
            claim.binding,
            replace(claim.message, binding_id=uuid4()),
        )
    with pytest.raises(ValueError, match="crosses"):
        DeliveryTaskClaim(
            claim.context.model_copy(update={"tenant_id": uuid4()}),
            claim.binding,
            claim.message,
        )

    for payload in [
        {
            "delivery_id": "d",
            "conversation_id": "c",
            "kind": "text",
            "artifact_refs": "not-a-list",
        },
        {
            "delivery_id": "d",
            "conversation_id": "c",
            "kind": "text",
            "attributes": "not-an-object",
        },
    ]:
        with pytest.raises(ValueError, match="collection fields"):
            outgoing_from_outbox(OutboxMessage("outbox", "IM_REPLY", "key", payload=payload))

    outgoing = outgoing_from_outbox(
        OutboxMessage(
            "outbox",
            "IM_REPLY",
            "key",
            payload={
                "delivery_id": 1,
                "conversation_id": 2,
                "kind": "text",
                "text": 3,
                "artifact_refs": [4],
                "attributes": {
                    5: "value"
                },
            },
        ))
    assert outgoing.delivery_id == "1"
    assert outgoing.conversation_id == "2"
    assert outgoing.text == "3"
    assert outgoing.artifact_refs == ("4", )
    assert outgoing.attributes == {"5": "value"}


@pytest.mark.anyio
async def test_delivery_worker_lifecycle_idle_and_terminal_retry_budget() -> None:
    claim = _claim()
    queue = RecordingDeliveryQueue(claim)
    registry = ChannelAdapterRegistry()
    registry.register(RecordingAdapter(RuntimeError("provider down")))
    telemetry = PlatformTelemetry(
        service_name="test",
        environment="test",
        node_role="delivery",
        otlp_endpoint=None,
    )

    for node_id, concurrency in [("", 1), ("node", 0)]:
        with pytest.raises(ValueError, match="identity or concurrency"):
            DeliveryWorkerService(
                queue=queue,
                channels=registry,
                node_id=node_id,
                concurrency=concurrency,
                runtime=_runtime(),
            )

    exhausted_claim = replace(claim, message=replace(claim.message, attempt_count=3))
    exhausted_queue = RecordingDeliveryQueue(exhausted_claim)
    worker = DeliveryWorkerService(
        queue=exhausted_queue,
        channels=registry,
        node_id="delivery-node",
        concurrency=1,
        runtime=_runtime(max_attempts=3),
        telemetry=telemetry,
    )
    with pytest.raises(ValueError, match="outside configured concurrency"):
        await worker.run_once(1)
    assert await worker.run_once(0)
    assert exhausted_queue.failure is not None
    assert exhausted_queue.failure["next_attempt_at"] is None
    assert "result=\"error\"" in telemetry.render_prometheus().decode()
    assert not await worker.run_once(0)

    await worker.start()
    await worker.start()
    await worker.close()
    await worker.close()


@pytest.mark.anyio
async def test_delivery_worker_rejects_destination_mismatch() -> None:
    claim = _claim()
    mismatch = replace(claim, message=replace(claim.message, destination="other_im"))
    queue = RecordingDeliveryQueue(mismatch)
    registry = ChannelAdapterRegistry()
    registry.register(RecordingAdapter())
    worker = DeliveryWorkerService(
        queue=queue,
        channels=registry,
        node_id="delivery-node",
        concurrency=1,
        runtime=_runtime(),
    )

    assert await worker.run_once(0)
    assert queue.failure is not None
    decision = queue.failure["decision"]
    assert isinstance(decision, RecoveryDecision)
    assert decision.disposition is FailureDisposition.PERMANENT


@pytest.mark.anyio
async def test_postgresql_delivery_queue_validates_claims_and_fallback_identity(
    tmp_path: Path, ) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'delivery-boundary.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    tenant_id, agent_id, binding_id = uuid4(), uuid4(), uuid4()
    async with sessions.begin() as database:
        database.add(Tenant(tenant_id=tenant_id, name="Delivery Boundary"))
        database.add(AgentApp(tenant_id=tenant_id, agent_app_id=agent_id, name="Agent"))
        database.add(
            ChannelBinding(
                binding_id=binding_id,
                tenant_id=tenant_id,
                agent_app_id=agent_id,
                channel_type="wecom",
                external_account_hash="delivery-boundary",
            ))
        database.add(
            OutboxMessageRow(
                tenant_id=tenant_id,
                agent_app_id=agent_id,
                outbox_id="fallback-outbox",
                request_id=None,
                category="IM_REPLY",
                destination="wecom",
                binding_id=binding_id,
                idempotency_key="fallback-key",
                payload={
                    "delivery_id": "delivery",
                    "conversation_id": "conversation",
                    "kind": "text",
                    "text": "reply",
                },
            ))

    empty = PostgreSQLDeliveryTaskQueue(sessions, allowed_channel_types=("", ))
    assert await empty.claim("worker",
                             lease_until=datetime.now(timezone.utc) + timedelta(seconds=30)) is None
    queue = PostgreSQLDeliveryTaskQueue(sessions)
    with pytest.raises(ValueError, match="worker and future lease"):
        await queue.claim("", lease_until=datetime.now(timezone.utc) + timedelta(seconds=30))
    with pytest.raises(ValueError, match="future"):
        await queue.claim("worker", lease_until=datetime.now(timezone.utc) - timedelta(seconds=1))

    claim = await queue.claim("worker",
                              lease_until=datetime.now(timezone.utc) + timedelta(seconds=30))
    assert claim is not None
    assert claim.context.request_id == "delivery:fallback-outbox"
    assert claim.context.trace_id == "delivery:fallback-outbox"
    assert claim.context.config_version == 1
    assert claim.trace_context == {}
    with pytest.raises(ValueError, match="future"):
        await queue.renew(
            claim,
            worker_id="worker",
            lease_until=datetime.now(timezone.utc) - timedelta(seconds=1),
        )
    assert not await queue.renew(
        claim,
        worker_id="other",
        lease_until=datetime.now(timezone.utc) + timedelta(seconds=30),
    )
    await queue.fail(
        claim,
        worker_id="worker",
        decision=RecoveryDecision(
            FailureDisposition.UNKNOWN,
            "ProviderOutcomeUnknown",
            "requires reconciliation",
        ),
        next_attempt_at=None,
    )
    assert await queue.replay(tenant_id, "fallback-outbox")
    await engine.dispose()


@pytest.mark.anyio
async def test_postgresql_delivery_rejects_invalid_persisted_trace_context(
    tmp_path: Path, ) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'delivery-trace.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    tenant_id, agent_id, binding_id = uuid4(), uuid4(), uuid4()
    async with sessions.begin() as database:
        database.add(Tenant(tenant_id=tenant_id, name="Delivery Trace"))
        database.add(AgentApp(tenant_id=tenant_id, agent_app_id=agent_id, name="Agent"))
        database.add(
            ChannelBinding(
                binding_id=binding_id,
                tenant_id=tenant_id,
                agent_app_id=agent_id,
                channel_type="wecom",
                external_account_hash="delivery-trace",
            ))
        database.add(
            AgentTaskRow(
                tenant_id=tenant_id,
                agent_app_id=agent_id,
                binding_id=binding_id,
                external_message_id="message",
                request_id="request",
                trace_id="trace",
                session_id="session",
                config_version=1,
                routing_key="a" * 64,
                payload_hash="b" * 64,
                request_payload={"trace_context": "invalid"},
                status="succeeded",
            ))
        database.add(
            OutboxMessageRow(
                tenant_id=tenant_id,
                agent_app_id=agent_id,
                outbox_id="invalid-trace",
                request_id="request",
                category="IM_REPLY",
                destination="wecom",
                binding_id=binding_id,
                idempotency_key="invalid-trace",
                payload={},
            ))

    with pytest.raises(RuntimeError, match="trace context"):
        await PostgreSQLDeliveryTaskQueue(sessions).claim("worker",
                                                          lease_until=datetime.now(timezone.utc) +
                                                          timedelta(seconds=30))
    await engine.dispose()
