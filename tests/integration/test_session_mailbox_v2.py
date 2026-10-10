"""Low-volume PostgreSQL + Redis contracts for the SessionReady v2 scheduler.

These tests are opt-in through the existing ``TRPC_TEST_POSTGRES_DSN`` and
``TRPC_TEST_REDIS_URL`` variables.  They deliberately use unique tenant,
stream, and consumer-group names so an explicitly supplied test environment
can be reused without sharing scheduler state with another run.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import asyncpg
import pytest
import redis.asyncio as redis_async

from trpc_service.channels.envelopes import InboundEnvelope, PayloadKind
from trpc_service.config.settings import SchedulerVersion
from trpc_service.queue.session_ready import (
    SessionReady,
    SessionReadyCodec,
    SessionReadyDelivery,
    SessionReadyQueue,
)
from trpc_service.queue.session_ready_outbox import SessionReadyOutboxQueue
from trpc_service.runtime import TenantRuntime
from trpc_service.storage.models import (
    Acceptance,
    MailboxClaimStatus,
    OutboxRecord,
    StoredEvent,
    TurnCommit,
)
from trpc_service.storage.postgres import PostgresRuntimeRepository
from trpc_service.storage.protocols import FencingConflict
from trpc_service.tenant.models import (
    Channel,
    ConversationKind,
    ModelPolicy,
    StorageSelection,
    TenantConfig,
)

pytestmark = pytest.mark.integration


@dataclass(frozen=True)
class _Scenario:
    pool: asyncpg.Pool
    redis: Any
    repository: PostgresRuntimeRepository
    global_worker_repository: PostgresRuntimeRepository
    runtime: TenantRuntime
    tenant_id: str
    binding_id: str
    account_id: str
    stream: str
    group: str


def _required_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        pytest.skip(f"{name} is not set")
    return value


def _postgres_dsn() -> str:
    return _required_env("TRPC_TEST_POSTGRES_DSN").replace("postgresql+asyncpg://", "postgresql://")


def _worker_postgres_dsn() -> str:
    return _required_env("TRPC_TEST_POSTGRES_WORKER_DSN").replace(
        "postgresql+asyncpg://", "postgresql://"
    )


def _decode_text(value: str | bytes) -> str:
    return value.decode() if isinstance(value, bytes) else value


def _decode_fields(fields: Mapping[str | bytes, str | bytes]) -> dict[str, str]:
    return {_decode_text(key): _decode_text(value) for key, value in fields.items()}


def _json_object(value: Any) -> dict[str, Any]:
    parsed = json.loads(value) if isinstance(value, str) else value
    assert isinstance(parsed, dict)
    return parsed


@asynccontextmanager
async def _scenario() -> AsyncIterator[_Scenario]:
    dsn = _postgres_dsn()
    worker_dsn = _worker_postgres_dsn()
    redis_url = _required_env("TRPC_TEST_REDIS_URL")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=8)
    global_worker_pool = await asyncpg.create_pool(worker_dsn, min_size=1, max_size=2)
    redis = redis_async.from_url(
        redis_url,
        decode_responses=False,
    )
    await redis.ping()

    suffix = uuid4().hex
    tenant_id = f"it-mailbox-{suffix}"
    binding_id = f"it-binding-{suffix}"
    account_id = f"it-account-{suffix}"
    stream = f"trpc:test:session-ready:{suffix}"
    group = f"trpc-test-session-ready-{suffix}"
    try:
        await _seed_tenant(pool, tenant_id, binding_id, account_id)
        repository = PostgresRuntimeRepository(pool)
        global_worker_repository = PostgresRuntimeRepository(global_worker_pool)
        runtime = TenantRuntime(
            repository,
            routing_key=b"integration-session-ready-routing-key-32-bytes",
            scheduler_version=SchedulerVersion.V2,
        )
        yield _Scenario(
            pool=pool,
            redis=redis,
            repository=repository,
            global_worker_repository=global_worker_repository,
            runtime=runtime,
            tenant_id=tenant_id,
            binding_id=binding_id,
            account_id=account_id,
            stream=stream,
            group=group,
        )
    finally:
        await _cleanup_tenant(pool, tenant_id)
        await redis.delete(stream)
        await redis.aclose()
        await global_worker_pool.close()
        await pool.close()


async def _seed_tenant(
    pool: asyncpg.Pool, tenant_id: str, binding_id: str, account_id: str
) -> None:
    config = TenantConfig(
        tenant_id=tenant_id,
        app_id="support",
        version=1,
        model=ModelPolicy(provider="offline", model="deterministic-fake"),
        storage=StorageSelection(profile_id="default"),
    )
    config_json = config.model_dump(mode="json")
    config_text = json.dumps(config_json, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    checksum = hashlib.sha256(config_text.encode()).hexdigest()
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
        await connection.execute(
            "INSERT INTO tenants (tenant_id,display_name) VALUES ($1,$2)",
            tenant_id,
            "SessionReady integration tenant",
        )
        await connection.execute(
            """
            INSERT INTO agent_apps (tenant_id,app_id,display_name)
            VALUES ($1,'support','SessionReady integration app')
            """,
            tenant_id,
        )
        await connection.execute(
            """
            INSERT INTO config_revisions (
                tenant_id,app_id,version,config_json,checksum,created_by
            ) VALUES ($1,'support',1,$2::jsonb,$3,'integration-test')
            """,
            tenant_id,
            config_text,
            checksum,
        )
        await connection.execute(
            """
            INSERT INTO storage_profiles (tenant_id,profile_id,profile_json)
            VALUES ($1,'default','{}'::jsonb)
            """,
            tenant_id,
        )
        await connection.execute(
            """
            INSERT INTO channel_bindings (
                tenant_id,binding_id,app_id,channel,account_id,capabilities
            ) VALUES ($1,$2,'support','feishu',$3,'[\"text\"]'::jsonb)
            """,
            tenant_id,
            binding_id,
            account_id,
        )


async def _cleanup_tenant(pool: asyncpg.Pool, tenant_id: str) -> None:
    async with pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT set_config('app.tenant_id', $1, true)", tenant_id)
        for table in (
            "session_events",
            "session_mailbox_items",
            "session_turns",
            "audit_logs",
            "outbound_messages",
            "inbound_messages",
            "session_mailboxes",
            "sessions",
            "outbox_events",
            "channel_identities",
            "channel_bindings",
            "config_revisions",
            "storage_profiles",
            "tenant_policies",
            "agent_apps",
            "tenants",
        ):
            statement = f"DELETE FROM {table} WHERE tenant_id=$1"  # noqa: S608 - static allowlist
            await connection.execute(statement, tenant_id)


def _envelope(scenario: _Scenario, message_id: str, *, user_id: str) -> InboundEnvelope:
    return InboundEnvelope(
        channel=Channel.FEISHU,
        account_id=scenario.account_id,
        external_message_id=message_id,
        external_user_id=user_id,
        conversation_kind=ConversationKind.DIRECT,
        payload_kind=PayloadKind.TEXT,
        text=f"integration message {message_id}",
        occurred_at=datetime.now(UTC),
    )


async def _accept(
    scenario: _Scenario,
    message_id: str,
    *,
    user_id: str = "integration-user",
) -> Acceptance:
    return await scenario.runtime.accept(
        scenario.binding_id,
        _envelope(scenario, message_id, user_id=user_id),
    )


async def _claim_ready_outbox(scenario: _Scenario, owner_id: str) -> OutboxRecord:
    """Claim only this test tenant's ready event under its RLS transaction."""

    async with scenario.pool.acquire() as connection, connection.transaction():
        await connection.execute("SELECT set_config('app.tenant_id', $1, true)", scenario.tenant_id)
        row = await connection.fetchrow(
            """
            SELECT * FROM outbox_events
             WHERE tenant_id=$1 AND event_type='session.ready.v2'
               AND published_at IS NULL
             ORDER BY created_at,outbox_id
             LIMIT 1
             FOR UPDATE
            """,
            scenario.tenant_id,
        )
        assert row is not None
        await connection.execute(
            """
            UPDATE outbox_events
               SET claimed_by=$2,
                   claim_expires_at=clock_timestamp()+interval '30 seconds',
                   attempts=attempts+1
             WHERE tenant_id=$1 AND outbox_id=$3 AND published_at IS NULL
            """,
            scenario.tenant_id,
            owner_id,
            row["outbox_id"],
        )
    return OutboxRecord(
        outbox_id=str(row["outbox_id"]),
        tenant_id=row["tenant_id"],
        event_type=row["event_type"],
        aggregate_id=row["aggregate_id"],
        payload=_json_object(row["payload_json"]),
        trace_headers=_json_object(row["trace_headers"]),
        attempts=row["attempts"] + 1,
    )


async def _publish_next_ready(
    scenario: _Scenario,
    queue: SessionReadyQueue,
    *,
    owner_id: str,
) -> tuple[OutboxRecord, SessionReady]:
    record = await _claim_ready_outbox(scenario, owner_id)
    await SessionReadyOutboxQueue(queue).publish(record)
    await scenario.repository.mark_outbox_published(
        record.tenant_id,
        record.outbox_id,
        owner_id=owner_id,
    )
    entries = await scenario.redis.xrange(scenario.stream)
    assert entries
    fields = _decode_fields(entries[-1][1])
    assert set(fields) == {
        "event_id",
        "tenant_id",
        "session_id",
        "generation",
        "priority",
        "trace_id",
        "created_at",
    }
    ready = SessionReadyCodec.decode(fields)
    assert ready.event_id == record.outbox_id
    assert ready.tenant_id == record.tenant_id
    assert ready.session_id == record.aggregate_id
    return record, ready


async def _receive_four(
    queue: SessionReadyQueue,
) -> tuple[SessionReadyDelivery, ...]:
    async def receive(consumer: str) -> tuple[SessionReadyDelivery, ...]:
        return await queue.receive_new(consumer=consumer, count=1, block_ms=1_000)

    batches = await asyncio.gather(*(receive(f"worker-{index}") for index in range(4)))
    deliveries = tuple(delivery for batch in batches for delivery in batch)
    assert len(deliveries) == 4
    return deliveries


async def _acknowledged_claim(
    scenario: _Scenario,
    queue: SessionReadyQueue,
    delivery: SessionReadyDelivery,
    *,
    owner_id: str,
    lease_for: timedelta = timedelta(seconds=30),
) -> Any:
    claim = await scenario.repository.claim_session_ready(
        delivery.message.tenant_id,
        delivery.message.session_id,
        owner_id=owner_id,
        lease_for=lease_for,
        expected_generation=delivery.message.generation,
        expected_event_id=delivery.message.event_id,
    )
    assert await queue.ack(delivery)
    return claim


@pytest.mark.asyncio
async def test_real_v2_outbox_ready_four_consumers_commit_next_generation() -> None:
    async with _scenario() as scenario:
        queue = SessionReadyQueue(
            scenario.redis,
            stream=scenario.stream,
            group=scenario.group,
        )
        await queue.ensure_group()
        first = await _accept(scenario, f"message-{uuid4().hex}")
        second = await _accept(scenario, f"message-{uuid4().hex}")
        assert first.context.session_id == second.context.session_id

        _, ready = await _publish_next_ready(
            scenario,
            queue,
            owner_id=f"publisher-{uuid4().hex}",
        )
        for _ in range(3):
            await queue.publish(ready)
        deliveries = await _receive_four(queue)
        assert {delivery.message.generation for delivery in deliveries} == {1}
        claims = await asyncio.gather(
            *(
                _acknowledged_claim(
                    scenario,
                    queue,
                    delivery,
                    owner_id=f"claim-worker-{index}",
                )
                for index, delivery in enumerate(deliveries)
            )
        )
        assert sum(claim.status == MailboxClaimStatus.CLAIMED for claim in claims) == 1
        assert sum(claim.status == MailboxClaimStatus.RUNNING for claim in claims) == 3
        claimed = next(claim for claim in claims if claim.status == MailboxClaimStatus.CLAIMED)
        assert claimed.execution_lease is not None
        pending = await scenario.redis.xpending(scenario.stream, scenario.group)
        pending_count = pending.get("pending", 0) if isinstance(pending, Mapping) else pending
        assert pending_count == 0

        await scenario.repository.commit_session_ready(
            TurnCommit(
                context=first.context,
                lease=claimed.execution_lease,
                state={"turn": 1},
                events=(
                    StoredEvent(
                        event_id=f"event-{uuid4().hex}",
                        author="integration-test",
                        timestamp=datetime.now(UTC).timestamp(),
                        event={"text": "first"},
                    ),
                ),
            )
        )
        queued = await scenario.repository.mailbox.get(scenario.tenant_id, first.context.session_id)
        assert queued is not None
        assert queued.status.value == "QUEUED"
        assert queued.resolved_sequence == 1
        assert queued.accepted_sequence == 2
        assert queued.queue_generation == 2

        _, next_ready = await _publish_next_ready(
            scenario,
            queue,
            owner_id=f"publisher-next-{uuid4().hex}",
        )
        next_delivery = (await queue.receive_new(consumer="worker-next", count=1, block_ms=1_000))[
            0
        ]
        assert next_delivery.message == next_ready
        next_claim = await _acknowledged_claim(
            scenario,
            queue,
            next_delivery,
            owner_id="worker-next",
        )
        assert next_claim.status == MailboxClaimStatus.CLAIMED
        assert next_claim.execution_lease is not None
        await scenario.repository.commit_session_ready(
            TurnCommit(
                context=second.context,
                lease=next_claim.execution_lease,
                state={"turn": 2},
                events=(
                    StoredEvent(
                        event_id=f"event-{uuid4().hex}",
                        author="integration-test",
                        timestamp=datetime.now(UTC).timestamp(),
                        event={"text": "second"},
                    ),
                ),
            )
        )
        final = await scenario.repository.mailbox.get(scenario.tenant_id, first.context.session_id)
        assert final is not None
        assert final.status.value == "IDLE"
        assert final.resolved_sequence == 2
        assert final.accepted_sequence == 2
        assert final.queue_generation == 2


@pytest.mark.asyncio
async def test_real_v2_different_sessions_claim_in_parallel() -> None:
    async with _scenario() as scenario:
        queue = SessionReadyQueue(
            scenario.redis,
            stream=scenario.stream,
            group=scenario.group,
        )
        await queue.ensure_group()
        accepted = (
            await _accept(scenario, f"message-a-{uuid4().hex}", user_id="user-a"),
            await _accept(scenario, f"message-b-{uuid4().hex}", user_id="user-b"),
        )
        assert accepted[0].context.session_id != accepted[1].context.session_id
        await _publish_next_ready(
            scenario,
            queue,
            owner_id=f"publisher-a-{uuid4().hex}",
        )
        await _publish_next_ready(
            scenario,
            queue,
            owner_id=f"publisher-b-{uuid4().hex}",
        )
        deliveries = await asyncio.gather(
            queue.receive_new(consumer="parallel-a", count=1, block_ms=1_000),
            queue.receive_new(consumer="parallel-b", count=1, block_ms=1_000),
        )
        flat = tuple(batch[0] for batch in deliveries)
        assert {delivery.message.session_id for delivery in flat} == {
            accepted[0].context.session_id,
            accepted[1].context.session_id,
        }
        claims = await asyncio.gather(
            *(
                _acknowledged_claim(
                    scenario,
                    queue,
                    delivery,
                    owner_id=f"parallel-worker-{index}",
                )
                for index, delivery in enumerate(flat)
            )
        )
        assert all(claim.status == MailboxClaimStatus.CLAIMED for claim in claims)
        assert all(claim.execution_lease is not None for claim in claims)
        for claim in claims:
            await scenario.repository.fail_session_ready(
                claim.execution_lease,
                error_type="integration_cleanup",
            )


@pytest.mark.asyncio
async def test_real_v2_expired_epoch_rejects_stale_commit() -> None:
    async with _scenario() as scenario:
        queue = SessionReadyQueue(
            scenario.redis,
            stream=scenario.stream,
            group=scenario.group,
        )
        await queue.ensure_group()
        accepted = await _accept(scenario, f"message-stale-{uuid4().hex}")
        await _publish_next_ready(
            scenario,
            queue,
            owner_id=f"publisher-stale-{uuid4().hex}",
        )
        first_delivery = (await queue.receive_new(consumer="stale-a", count=1, block_ms=1_000))[0]
        first_claim = await _acknowledged_claim(
            scenario,
            queue,
            first_delivery,
            owner_id="stale-worker-a",
            lease_for=timedelta(milliseconds=100),
        )
        assert first_claim.execution_lease is not None

        await asyncio.sleep(0.25)
        second_claim = await scenario.repository.claim_session_ready(
            accepted.context.tenant_id,
            accepted.context.session_id,
            owner_id="stale-worker-b",
            lease_for=timedelta(seconds=30),
            expected_generation=first_delivery.message.generation,
            expected_event_id=first_delivery.message.event_id,
        )
        assert second_claim.status == MailboxClaimStatus.CLAIMED
        assert second_claim.execution_lease is not None
        assert (
            second_claim.execution_lease.fencing_token > first_claim.execution_lease.fencing_token
        )

        with pytest.raises(FencingConflict, match="stale"):
            await scenario.repository.commit_session_ready(
                TurnCommit(
                    context=accepted.context,
                    lease=first_claim.execution_lease,
                    state={},
                    events=(),
                )
            )
        current = await scenario.repository.mailbox.get(
            accepted.context.tenant_id,
            accepted.context.session_id,
        )
        assert current is not None
        assert current.lease_owner == "stale-worker-b"
        await scenario.repository.fail_session_ready(
            second_claim.execution_lease,
            error_type="integration_cleanup",
        )


@pytest.mark.asyncio
async def test_real_v2_pg_lease_sweeper_requeues_after_redis_ack() -> None:
    async with _scenario() as scenario:
        queue = SessionReadyQueue(
            scenario.redis,
            stream=scenario.stream,
            group=scenario.group,
        )
        await queue.ensure_group()
        accepted = await _accept(scenario, f"message-sweeper-{uuid4().hex}")
        await _publish_next_ready(
            scenario,
            queue,
            owner_id=f"publisher-sweeper-{uuid4().hex}",
        )
        delivery = (await queue.receive_new(consumer="sweeper-a", count=1, block_ms=1_000))[0]
        claim = await _acknowledged_claim(
            scenario,
            queue,
            delivery,
            owner_id="sweeper-worker-a",
            lease_for=timedelta(milliseconds=100),
        )
        assert claim.execution_lease is not None

        await asyncio.sleep(0.25)
        assert (
            await scenario.global_worker_repository.sweep_expired_leases(
                owner_id="integration-sweeper",
                limit=10,
            )
            == 1
        )
        mailbox = await scenario.repository.mailbox.get(
            accepted.context.tenant_id,
            accepted.context.session_id,
        )
        assert mailbox is not None
        assert mailbox.status.value == "QUEUED"
        assert mailbox.queue_generation == 2
        assert mailbox.lease_owner is None
        with pytest.raises(FencingConflict, match="stale"):
            await scenario.repository.commit_session_ready(
                TurnCommit(
                    context=accepted.context,
                    lease=claim.execution_lease,
                    state={},
                    events=(),
                )
            )


@pytest.mark.asyncio
async def test_real_v2_retry_scheduler_requeues_without_holding_worker() -> None:
    async with _scenario() as scenario:
        queue = SessionReadyQueue(
            scenario.redis,
            stream=scenario.stream,
            group=scenario.group,
        )
        await queue.ensure_group()
        accepted = await _accept(scenario, f"message-retry-{uuid4().hex}")
        await _publish_next_ready(
            scenario,
            queue,
            owner_id=f"publisher-retry-{uuid4().hex}",
        )
        delivery = (await queue.receive_new(consumer="retry-a", count=1, block_ms=1_000))[0]
        claim = await _acknowledged_claim(
            scenario,
            queue,
            delivery,
            owner_id="retry-worker-a",
        )
        assert claim.execution_lease is not None
        await scenario.repository.retry_session_ready(
            claim.execution_lease,
            error_type="integration_transient",
            delay=timedelta(milliseconds=100),
        )
        waiting = await scenario.repository.mailbox.get(
            accepted.context.tenant_id,
            accepted.context.session_id,
        )
        assert waiting is not None
        assert waiting.status.value == "RETRY_WAIT"
        assert waiting.lease_owner is None

        await asyncio.sleep(0.25)
        assert (
            await scenario.global_worker_repository.schedule_retries(
                owner_id="integration-retry-scheduler",
                limit=10,
            )
            == 1
        )
        queued = await scenario.repository.mailbox.get(
            accepted.context.tenant_id,
            accepted.context.session_id,
        )
        assert queued is not None
        assert queued.status.value == "QUEUED"
        assert queued.queue_generation == waiting.queue_generation + 1
        assert queued.retry_at is None


@pytest.mark.asyncio
async def test_real_v2_reconciler_rebuilds_lost_ready_notification() -> None:
    async with _scenario() as scenario:
        queue = SessionReadyQueue(
            scenario.redis,
            stream=scenario.stream,
            group=scenario.group,
        )
        await queue.ensure_group()
        accepted = await _accept(scenario, f"message-reconcile-{uuid4().hex}")
        await _publish_next_ready(
            scenario,
            queue,
            owner_id=f"publisher-reconcile-{uuid4().hex}",
        )
        await scenario.redis.delete(scenario.stream)
        async with scenario.pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('app.tenant_id', $1, true)",
                scenario.tenant_id,
            )
            await connection.execute(
                """
                UPDATE session_mailboxes
                   SET updated_at=clock_timestamp()-interval '10 seconds'
                 WHERE tenant_id=$1 AND session_id=$2
                """,
                scenario.tenant_id,
                accepted.context.session_id,
            )
            # Reconciliation waits for a bounded observation window before
            # treating a successful publication as a lost Redis wake-up.
            await connection.execute(
                """
                UPDATE outbox_events
                   SET published_at=clock_timestamp()-interval '31 seconds'
                 WHERE tenant_id=$1 AND aggregate_id=$2
                   AND event_type='session.ready.v2'
                """,
                scenario.tenant_id,
                accepted.context.session_id,
            )

        assert (
            await scenario.global_worker_repository.reconcile_sessions(
                owner_id="integration-reconciler",
                limit=10,
            )
            == 1
        )
        mailbox = await scenario.repository.mailbox.get(
            accepted.context.tenant_id,
            accepted.context.session_id,
        )
        assert mailbox is not None
        assert mailbox.status.value == "QUEUED"
        assert mailbox.queue_generation == 1
        async with scenario.pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "SELECT set_config('app.tenant_id', $1, true)",
                scenario.tenant_id,
            )
            generations = await connection.fetch(
                """
                SELECT (payload_json->>'generation')::bigint AS generation
                  FROM outbox_events
                 WHERE tenant_id=$1 AND aggregate_id=$2
                   AND event_type='session.ready.v2'
                 ORDER BY generation
                """,
                scenario.tenant_id,
                accepted.context.session_id,
            )
        assert [int(row["generation"]) for row in generations] == [1]

        _, rebuilt = await _publish_next_ready(
            scenario,
            queue,
            owner_id=f"publisher-rebuilt-{uuid4().hex}",
        )
        delivery = (
            await queue.receive_new(
                consumer="reconciler-worker",
                count=1,
                block_ms=1_000,
            )
        )[0]
        assert delivery.message == rebuilt
        assert delivery.message.generation == 1
        assert await queue.ack(delivery)
