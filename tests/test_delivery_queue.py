from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from trpc_service.agent import AgentApp
from trpc_service.agent.recovery import FailureDisposition, RecoveryDecision
from trpc_service.channels import ChannelBinding, DeliveryReceipt
from trpc_service.channels.delivery import PostgreSQLDeliveryTaskQueue
from trpc_service.storage.orm import Base
from trpc_service.storage.runtime_orm import AgentTaskRow, OutboxMessageRow
from trpc_service.tenant import Tenant


@pytest.mark.anyio
async def test_delivery_queue_leases_completes_and_manually_replays_dlq(tmp_path: Path) -> None:
    """Any node can deliver a due reply and an operator can replay terminal work."""

    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'delivery.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    tenant_id, agent_app_id, binding_id = uuid4(), uuid4(), uuid4()
    async with sessions.begin() as database:
        database.add(Tenant(tenant_id=tenant_id, name="Delivery Tenant"))
        database.add(
            AgentApp(
                tenant_id=tenant_id,
                agent_app_id=agent_app_id,
                name="Delivery Agent",
            ))
        database.add(
            ChannelBinding(
                binding_id=binding_id,
                tenant_id=tenant_id,
                agent_app_id=agent_app_id,
                channel_type="wecom",
                external_account_hash="delivery-account",
            ))
        database.add(
            AgentTaskRow(
                tenant_id=tenant_id,
                agent_app_id=agent_app_id,
                binding_id=binding_id,
                external_message_id="message-reply-success",
                request_id="request-reply-success",
                trace_id="origin-trace",
                session_id="session-1",
                config_version=3,
                routing_key="a" * 64,
                payload_hash="b" * 64,
                request_payload={
                    "trace_context": {
                        "traceparent": "00-11111111111111111111111111111111-2222222222222222-01"
                    }
                },
                status="succeeded",
            ))
        for outbox_id in ("reply-success", "reply-dlq"):
            database.add(
                OutboxMessageRow(
                    tenant_id=tenant_id,
                    agent_app_id=agent_app_id,
                    outbox_id=outbox_id,
                    request_id=f"request-{outbox_id}",
                    category="IM_REPLY",
                    destination="wecom",
                    binding_id=binding_id,
                    idempotency_key=outbox_id,
                    payload={
                        "delivery_id": outbox_id,
                        "conversation_id": "conversation-1",
                        "kind": "text",
                        "text": "hello",
                    },
                ))
    lease = datetime.now(timezone.utc) + timedelta(seconds=30)
    feishu_only_queue = PostgreSQLDeliveryTaskQueue(
        sessions,
        allowed_channel_types=("feishu", ),
    )
    assert await feishu_only_queue.claim("delivery-feishu", lease_until=lease) is None

    queue = PostgreSQLDeliveryTaskQueue(
        sessions,
        allowed_channel_types=("wecom", ),
    )

    first = await queue.claim("delivery-a", lease_until=lease)
    assert first is not None
    assert first.message.outbox_id == "reply-success"
    assert first.context.trace_id == "origin-trace"
    assert first.context.config_version == 3
    assert first.trace_context["traceparent"].startswith("00-11111111")
    renewed_lease = datetime.now(timezone.utc) + timedelta(seconds=60)
    assert await queue.renew(
        first,
        worker_id="delivery-a",
        lease_until=renewed_lease,
    )
    await queue.complete(
        first,
        worker_id="delivery-a",
        receipt=DeliveryReceipt(
            delivery_id="reply-success",
            external_delivery_id="provider-1",
            accepted_at=datetime.now(timezone.utc),
        ),
    )

    second = await queue.claim("delivery-b", lease_until=lease)
    assert second is not None
    assert second.message.outbox_id == "reply-dlq"
    await queue.fail(
        second,
        worker_id="delivery-b",
        decision=RecoveryDecision(
            disposition=FailureDisposition.PERMANENT,
            error_code="InvalidRecipient",
            safe_summary="Channel delivery cannot be retried",
        ),
        next_attempt_at=None,
    )
    assert await queue.claim("delivery-c", lease_until=lease) is None

    assert await queue.replay(tenant_id, "reply-dlq")
    replay = await queue.claim("delivery-c", lease_until=lease)
    assert replay is not None
    assert replay.message.outbox_id == "reply-dlq"
    assert replay.message.attempt_count == 2
    assert replay.message.retry_count == 1
    assert not await queue.replay(tenant_id, "missing")
    await engine.dispose()


@pytest.mark.anyio
async def test_delivery_order_survives_concurrency_retry_and_unknown(tmp_path: Path) -> None:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'order.db'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    tenant_id, agent_id, binding_id = uuid4(), uuid4(), uuid4()
    now = datetime.now(timezone.utc)
    async with sessions.begin() as db:
        db.add(Tenant(tenant_id=tenant_id, name="Order"))
        db.add(AgentApp(tenant_id=tenant_id, agent_app_id=agent_id, name="Order"))
        db.add(
            ChannelBinding(binding_id=binding_id,
                           tenant_id=tenant_id,
                           agent_app_id=agent_id,
                           channel_type="wecom",
                           external_account_hash="order-account"))
        for identifier, session_id, sequence in (("z-first", "ordered", 0),
                                                 ("a-second", "ordered", 1), ("other",
                                                                              "independent", 2)):
            db.add(
                OutboxMessageRow(
                    tenant_id=tenant_id,
                    agent_app_id=agent_id,
                    binding_id=binding_id,
                    outbox_id=identifier,
                    category="IM_REPLY",
                    destination="wecom",
                    session_id=session_id,
                    sequence_no=sequence,
                    idempotency_key=identifier,
                    created_at=now,
                    payload={
                        "delivery_id": identifier,
                        "conversation_id": session_id,
                        "kind": "text",
                        "text": identifier
                    },
                ))
    queue = PostgreSQLDeliveryTaskQueue(sessions)
    lease = now + timedelta(seconds=60)
    try:
        first = await queue.claim("worker-a", lease_until=lease)
        assert first is not None and first.message.outbox_id == "z-first"
        independent = await queue.claim("worker-b", lease_until=lease)
        assert independent is not None and independent.message.outbox_id == "other"
        assert await queue.claim("worker-c", lease_until=lease) is None
        await queue.fail(first,
                         worker_id="worker-a",
                         decision=RecoveryDecision(disposition=FailureDisposition.RETRY,
                                                   error_code="RateLimit",
                                                   safe_summary="retry"),
                         next_attempt_at=now + timedelta(seconds=30))
        assert await queue.claim("worker-c", lease_until=lease) is None
        async with sessions.begin() as db:
            row = await db.get(OutboxMessageRow, (tenant_id, agent_id, "z-first"))
            assert row is not None
            row.status = "UNKNOWN"
        assert await queue.claim("worker-c", lease_until=lease) is None
        assert await queue.replay(tenant_id, "z-first")
        replay = await queue.claim("worker-c", lease_until=lease)
        assert replay is not None and replay.message.outbox_id == "z-first"
        await queue.complete(replay,
                             worker_id="worker-c",
                             receipt=DeliveryReceipt(delivery_id="z-first",
                                                     external_delivery_id="external-1",
                                                     accepted_at=now))
        second = await queue.claim("worker-d", lease_until=lease)
        assert second is not None and second.message.outbox_id == "a-second"
    finally:
        await engine.dispose()
