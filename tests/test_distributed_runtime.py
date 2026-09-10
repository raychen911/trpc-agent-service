"""Simplified PostgreSQL/Redis Stream multi-replica component tests."""

from __future__ import annotations

import asyncio
import json
import os
from contextlib import suppress
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from trpc_service.agent.execution import AgentReply, RunAgentCommand
from trpc_service.bus.redis_bus import RedisExecutionBus, encode_result, serialize_command
from trpc_service.config.models import ExecutionOutboxRecord, OutboxStatus, TenantRecord
from trpc_service.storage.database import Database
from trpc_service.storage.lock import RedisSessionLockManager, SessionLockTimeoutError
from trpc_service.storage.repositories import ExecutionOutboxRepository, TenantRepository
from trpc_service.worker.redis_runtime import RedisWorkerRuntime


class FakeRedis:
    def __init__(self, result: str | None = None) -> None:
        self.result = result
        self.added: list[tuple[str, dict[str, str]]] = []
        self.acked: list[tuple[str, str, str]] = []
        self.pushed: list[tuple[str, str]] = []

    async def xadd(self, stream: str, fields: dict[str, str]) -> str:
        self.added.append((stream, fields))
        return "1-0"

    async def blpop(self, key: str, timeout: float) -> tuple[str, str] | None:
        del timeout
        return (key, self.result) if self.result is not None else None

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.acked.append((stream, group, message_id))
        return 1

    async def rpush(self, key: str, value: str) -> int:
        self.pushed.append((key, value))
        return 1

    async def expire(self, key: str, seconds: int) -> bool:
        del key, seconds
        return True

    async def aclose(self) -> None:
        pass


class ReplyWorker:
    async def run(self, command: RunAgentCommand) -> AgentReply:
        return AgentReply(
            tenant_id=command.tenant_id,
            app_id=command.app_id,
            user_id=command.user_id,
            session_id=command.session_id,
            trace_id=command.trace_id,
            text="queued reply",
            tool_events=(),
        )


class FailingWorker:
    async def run(self, command: RunAgentCommand) -> AgentReply:
        del command
        raise RuntimeError("test failure")


class CancelledWorker:
    async def run(self, command: RunAgentCommand) -> AgentReply:
        del command
        raise asyncio.CancelledError


@pytest.fixture
async def database() -> Database:
    value = Database("sqlite+aiosqlite:///:memory:")
    await value.initialize()
    await TenantRepository(value).create(TenantRecord(tenant_id="tenant-a", name="A"))
    try:
        yield value
    finally:
        await value.dispose()


def command() -> RunAgentCommand:
    return RunAgentCommand(
        tenant_id="tenant-a",
        app_id="assistant",
        user_id="user-a",
        session_id="session-a",
        message="hello",
        trace_id="trace-a",
    )


@pytest.mark.asyncio
async def test_redis_bus_persists_outbox_and_returns_worker_reply(database: Database) -> None:
    payload = encode_result(
        {
            "ok": True,
            "reply": {
                "tenant_id": "tenant-a",
                "app_id": "assistant",
                "user_id": "user-a",
                "session_id": "session-a",
                "trace_id": "trace-a",
                "text": "queued reply",
                "tool_events": [],
            },
        }
    )
    bus = RedisExecutionBus(database, "redis://unused", stream="runs")
    fake = FakeRedis(payload)
    bus._redis = fake  # type: ignore[assignment]

    reply = await bus.submit(command())

    assert reply.text == "queued reply"
    outbox_id = fake.added[0][1]["outbox_id"]
    stored = await ExecutionOutboxRepository(database).get(outbox_id)
    assert stored is not None and stored.status == OutboxStatus.QUEUED


@pytest.mark.asyncio
async def test_worker_completes_outbox_and_publishes_result(database: Database) -> None:
    repository = ExecutionOutboxRepository(database)
    await repository.create(
        ExecutionOutboxRecord(
            outbox_id="outbox-a",
            trace_id="trace-a",
            tenant_id="tenant-a",
            payload=serialize_command(command()),
        )
    )
    runtime = RedisWorkerRuntime(
        database,
        "redis://unused",
        ReplyWorker(),  # type: ignore[arg-type]
        stream="runs",
        group="workers",
        consumer="worker-a",
    )
    fake = FakeRedis()
    runtime._redis = fake  # type: ignore[assignment]

    await runtime.process_message("1-0", {"outbox_id": "outbox-a"})

    stored = await repository.get("outbox-a")
    assert stored is not None
    assert stored.status == OutboxStatus.COMPLETED
    assert stored.attempts == 1
    assert json.loads(fake.pushed[0][1])["reply"]["text"] == "queued reply"
    assert fake.acked == [("runs", "workers", "1-0")]


@pytest.mark.asyncio
async def test_worker_retries_then_dead_letters(database: Database) -> None:
    repository = ExecutionOutboxRepository(database)
    await repository.create(
        ExecutionOutboxRecord(
            outbox_id="outbox-fail",
            trace_id="trace-fail",
            tenant_id="tenant-a",
            payload=serialize_command(command()),
        )
    )
    runtime = RedisWorkerRuntime(
        database,
        "redis://unused",
        FailingWorker(),  # type: ignore[arg-type]
        stream="runs",
        group="workers",
        consumer="worker-a",
        max_attempts=2,
    )
    fake = FakeRedis()
    runtime._redis = fake  # type: ignore[assignment]

    await runtime.process_message("1-0", {"outbox_id": "outbox-fail"})
    retry = await repository.get("outbox-fail")
    assert retry is not None and retry.status == OutboxStatus.RETRY
    assert fake.added == [("runs", {"outbox_id": "outbox-fail"})]

    await runtime.process_message("2-0", {"outbox_id": "outbox-fail"})
    failed = await repository.get("outbox-fail")
    assert failed is not None and failed.status == OutboxStatus.DEAD_LETTER
    assert failed.attempts == 2
    assert json.loads(fake.pushed[0][1])["ok"] is False


@pytest.mark.asyncio
async def test_cancelled_worker_leaves_message_pending(database: Database) -> None:
    repository = ExecutionOutboxRepository(database)
    await repository.create(
        ExecutionOutboxRecord(
            outbox_id="outbox-cancelled",
            trace_id="trace-cancelled",
            tenant_id="tenant-a",
            payload=serialize_command(command()),
        )
    )
    runtime = RedisWorkerRuntime(
        database,
        "redis://unused",
        CancelledWorker(),  # type: ignore[arg-type]
        stream="runs",
        group="workers",
        consumer="worker-a",
    )
    fake = FakeRedis()
    runtime._redis = fake  # type: ignore[assignment]

    with pytest.raises(asyncio.CancelledError):
        await runtime.process_message("1-0", {"outbox_id": "outbox-cancelled"})

    assert fake.acked == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_redis_stream_request_reply(database: Database) -> None:
    redis_url = os.getenv("TRPC_TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("TRPC_TEST_REDIS_URL is not configured")
    suffix = uuid4().hex
    stream = f"trpc-test:runs:{suffix}"
    group = f"workers-{suffix}"
    bus = RedisExecutionBus(database, redis_url, stream=stream, result_timeout_seconds=5)
    runtime = RedisWorkerRuntime(
        database,
        redis_url,
        ReplyWorker(),  # type: ignore[arg-type]
        stream=stream,
        group=group,
        consumer="worker-a",
        claim_idle_ms=1_000,
    )
    worker_task = asyncio.create_task(runtime.run())
    cleanup_client = Redis.from_url(redis_url, decode_responses=True)
    try:
        reply = await asyncio.wait_for(bus.submit(command()), timeout=10)
        assert reply.text == "queued reply"
    finally:
        worker_task.cancel()
        with suppress(asyncio.CancelledError):
            await worker_task
        await bus.close()
        await runtime.close()
        await cleanup_client.delete(stream)
        await cleanup_client.aclose()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_redis_session_lock_has_single_owner() -> None:
    redis_url = os.getenv("TRPC_TEST_REDIS_URL")
    if not redis_url:
        pytest.skip("TRPC_TEST_REDIS_URL is not configured")
    suffix = uuid4().hex
    first = RedisSessionLockManager(redis_url, ttl_seconds=5, acquire_timeout=0.1)
    second = RedisSessionLockManager(redis_url, ttl_seconds=5, acquire_timeout=0.1)
    try:
        async with first.lock(f"tenant-{suffix}", "session-a"):
            with pytest.raises(SessionLockTimeoutError):
                async with second.lock(f"tenant-{suffix}", "session-a"):
                    pytest.fail("a second owner acquired the same session lock")
        async with second.lock(f"tenant-{suffix}", "session-a"):
            pass
    finally:
        await first.close()
        await second.close()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_postgresql_fact_store_and_outbox() -> None:
    database_url = os.getenv("TRPC_TEST_POSTGRES_URL")
    if not database_url:
        pytest.skip("TRPC_TEST_POSTGRES_URL is not configured")
    database = Database(database_url)
    suffix = uuid4().hex
    try:
        await database.initialize()
        await TenantRepository(database).create(
            TenantRecord(tenant_id=f"postgres-{suffix}", name="PostgreSQL")
        )
        stored = await ExecutionOutboxRepository(database).create(
            ExecutionOutboxRecord(
                outbox_id=suffix,
                trace_id=f"trace-{suffix}",
                tenant_id=f"postgres-{suffix}",
                payload={"message": "persisted"},
            )
        )
        assert stored.status == OutboxStatus.QUEUED
        assert (await ExecutionOutboxRepository(database).get(suffix)) is not None
    finally:
        await database.dispose()
