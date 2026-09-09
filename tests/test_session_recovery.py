"""Session/Summary/Memory/group/admission regression tests (all offline).

Run: python -m pytest tests/test_session_recovery.py -vv
Real SDK Runner + OfflineModel, SDK InMemory Session/Memory; injected failures
must not produce success or repeat a model call. No Redis/SQL/IM/model charges.
Inspect agent/post_turn, gateway/requests or storage/fencing on assertion failure.
"""
import asyncio
from contextlib import nullcontext
from unittest.mock import AsyncMock

import pytest
from redis.exceptions import ResponseError
from trpc_agent_sdk.storage import RedisCommand

from trpc_service.agent import AgentWorker, TenantRuntimeManager
from trpc_service.offline import OfflineRuntimeFactory
from trpc_service.config import ChannelBindingConfig
from trpc_service.gateway.service import GatewayService, DuplicateRequestError
from trpc_service.gateway.idempotency import InMemoryIdempotencyStore, IdempotencyConflictError
from trpc_service.gateway.models import NormalizedInboundMessage, RequestRecord, RequestState
from trpc_service.gateway.requests import InMemoryRequestStore
from trpc_service.storage import InMemorySessionExecutionGuard, SessionLease, SessionLockLostError
from trpc_service.storage.fencing import FencedRedisStorage, FencedSqlStorage, storage_lease_scope
from trpc_service.tenant import InMemoryTenantRegistry

pytestmark = pytest.mark.component


def make_gateway(tenant):
    factory = OfflineRuntimeFactory()
    runtimes = TenantRuntimeManager(InMemoryTenantRegistry([tenant]), factory)
    gateway = GatewayService(runtimes._registry, AgentWorker(runtimes, InMemorySessionExecutionGuard()),
                             InMemoryIdempotencyStore())
    return gateway, runtimes, factory


async def request(gateway, text="hello", key=""):
    return await gateway.web_request(tenant_id="tenant-a",
                                     app_id="assistant",
                                     external_user_id="u",
                                     session_id="s",
                                     text=text,
                                     idempotency_key=key)


@pytest.mark.asyncio
@pytest.mark.fault
async def test_memory_failure_resumes_post_turn_without_model(tenant_config):
    gateway, runtimes, factory = make_gateway(tenant_config)
    original = factory.storage.memory_service.store_session
    factory.storage.memory_service.store_session = AsyncMock(side_effect=ConnectionError("injected memory failure"))
    req, key = await request(gateway)
    try:
        with pytest.raises(ConnectionError):
            await gateway.chat(req, key)
        assert (await gateway.request_status(req.tenant_id, req.request_id)).state == RequestState.RETRYABLE_FAILED
        assert factory.models[0].calls == 1
        factory.storage.memory_service.store_session = original
        result = await gateway.chat(req, key)
        assert "echo:hello" in result.text
        assert factory.models[0].calls == 1
        assert (await gateway.request_status(req.tenant_id, req.request_id)).state == RequestState.SUCCEEDED
    finally:
        await runtimes.close()


@pytest.mark.asyncio
async def test_summary_anchor_and_memory_visible(tenant_config):
    policy = tenant_config.apps["assistant"].runtime
    policy.summary_event_threshold, policy.summary_keep_recent = 4, 2
    gateway, runtimes, factory = make_gateway(tenant_config)
    try:
        runtime = await runtimes.get("tenant-a", "assistant", 1)
        summary_model = runtime.runner.session_service.summarizer_manager._summarizer.model
        for index in range(3):
            req, key = await request(gateway, f"fact-{index}")
            await gateway.chat(req, key)
            if index < 2:
                assert summary_model.calls == 0
        assert summary_model.calls == 1
        runtime = await runtimes.get("tenant-a", "assistant", 1)
        session = await runtime.runner.session_service.get_session(app_name=runtime.runner.app_name,
                                                                   user_id=req.user_id,
                                                                   session_id=req.session_id)
        assert any(e.is_summary_event() for e in session.events)
        assert len(session.events) < 6
        assert len(session.state["_platform_turns"]) == 3
        memories = await runtime.runner.memory_service.search_memory(session.save_key, "fact")
        assert memories.memories
        assert factory.models[0].calls == 3
    finally:
        await runtimes.close()


@pytest.mark.asyncio
@pytest.mark.fault
async def test_failed_summary_does_not_look_like_no_summary_needed(tenant_config):
    policy = tenant_config.apps["assistant"].runtime
    policy.summary_event_threshold, policy.summary_keep_recent = 2, 1
    gateway, runtimes, factory = make_gateway(tenant_config)
    try:
        first, key = await request(gateway, "first")
        await gateway.chat(first, key)
        runtime = await runtimes.get("tenant-a", "assistant", 1)
        manager = runtime.runner.session_service.summarizer_manager
        original = manager._summarizer.create_session_summary
        manager._summarizer.create_session_summary = AsyncMock(return_value=None)
        second, key = await request(gateway, "second")
        with pytest.raises(RuntimeError, match="summary_generation_failed"):
            await gateway.chat(second, key)
        manager._summarizer.create_session_summary = original
        await gateway.chat(second, key)
        assert factory.models[0].calls == 2
    finally:
        await runtimes.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mode,expected", [("shared", 2), ("per_user", 1)])
async def test_group_mode_and_binding_isolation(tenant_config, mode, expected):
    tenant_config.apps["assistant"].runtime.group_session_mode = mode
    tenant_config.channels = [
        ChannelBindingConfig(binding_id=name, app_id="assistant", channel="wecom") for name in ("b1", "b2")
    ]
    gateway, runtimes, _ = make_gateway(tenant_config)
    try:
        requests = []
        for index, (binding, user) in enumerate((("b1", "alice"), ("b1", "bob"), ("b2", "alice"))):
            message = NormalizedInboundMessage(binding_id=binding,
                                               channel="wecom",
                                               message_id=str(index),
                                               external_user_id=user,
                                               external_conversation_id="group",
                                               is_group=True,
                                               text=user)
            req, key = await gateway.inbound_request(message)
            result = await gateway.chat(req, key)
            if index == 1:
                assert result.text.endswith(f"user_turns={expected}")
            if index == 2:
                assert result.text.endswith("user_turns=1")
            requests.append(req)
        assert requests[0].metadata["actor_user_id"] != requests[1].metadata["actor_user_id"]
        assert requests[0].session_id != requests[2].session_id
    finally:
        await runtimes.close()


@pytest.mark.asyncio
async def test_atomic_admission_twenty_concurrent_and_hash_conflict():
    store = InMemoryRequestStore()
    records = [
        RequestRecord(tenant_id="t", request_id=str(i), state="reserved", payload_hash="a", idempotency_key="k")
        for i in range(20)
    ]
    results = await asyncio.gather(*(store.reserve_and_create_request(item) for item in records))
    assert sum(created for _, created in results) == 1
    assert len({record.request_id for record, _ in results}) == 1
    with pytest.raises(IdempotencyConflictError):
        await store.reserve_and_create_request(records[0].model_copy(update={"payload_hash": "different"}))


@pytest.mark.asyncio
@pytest.mark.fault
async def test_result_commit_failure_releases_lock_and_reuses_model(tenant_config):
    gateway, runtimes, factory = make_gateway(tenant_config)
    req, key = await request(gateway, key="same")
    try:

        async def fail(_):
            raise ConnectionError("commit failed")

        with pytest.raises(ConnectionError):
            await gateway.chat(req, key, commit_callback=fail)
        await gateway.chat(req, key)
        assert factory.models[0].calls == 1
        with pytest.raises(DuplicateRequestError):
            await request(gateway, key="same")
    finally:
        await runtimes.close()


@pytest.mark.asyncio
@pytest.mark.fault
async def test_redis_write_checks_owner_at_commit_not_earlier_verify():
    # The fake models one atomic eval; the live companion executes the real Lua.
    class AtomicRedis:
        owner = "new"
        writes = 0

        async def eval(self, script, count, *args):
            assert "redis.call('get', KEYS[1])" in script
            if args[count] != self.owner:
                raise ResponseError("STALE_FENCE")
            self.writes += 1
            return 1

    redis = AtomicRedis()
    storage = FencedRedisStorage(None, "redis")
    old = SessionLease("old", asyncio.Event(), epoch=1, key="lease")
    # Old Python check succeeds: storage authority has already changed.
    old.assert_owned()
    with storage_lease_scope("redis", old), pytest.raises(SessionLockLostError):
        await storage.execute_command(redis, RedisCommand(method="set", args=("data", "stale")))
    assert redis.writes == 0 and old.is_lost


@pytest.mark.asyncio
async def test_postgres_stale_fence_rolls_back_before_sdk_commit():

    class Result:

        def mappings(self):
            return self

        def first(self):
            return {"token": "new", "epoch": 2, "valid": True}

    class Connection:
        no_autoflush = nullcontext()
        rolled_back = False

        def execute(self, statement, values):
            assert "FOR UPDATE" in str(statement)
            return Result()

        def rollback(self):
            self.rolled_back = True

    delegate = AsyncMock()
    conn = Connection()
    with storage_lease_scope("sql", SessionLease("old", asyncio.Event(), epoch=1, key="lease")):
        with pytest.raises(SessionLockLostError):
            await FencedSqlStorage(delegate, "sql").commit(conn)
    assert conn.rolled_back
    delegate.commit.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.fault
async def test_next_turn_repairs_final_event_without_checkpoint(tenant_config):
    from trpc_service.agent.post_turn import TurnFinalizer
    from unittest.mock import patch
    gateway, runtimes, factory = make_gateway(tenant_config)
    first, key = await request(gateway)
    try:
        with patch.object(TurnFinalizer, "finish", AsyncMock(side_effect=ConnectionError("exit before checkpoint"))):
            with pytest.raises(ConnectionError):
                await gateway.chat(first, key)
        assert factory.models[0].calls == 1
        second, key = await request(gateway, "next")
        result = await gateway.chat(second, key)
        assert result.text.endswith("user_turns=2")
        runtime = await runtimes.get("tenant-a", "assistant", 1)
        session = await runtime.runner.session_service.get_session(app_name=runtime.runner.app_name,
                                                                   user_id=first.user_id,
                                                                   session_id=first.session_id)
        assert session.state["_platform_turns"][first.request_id]["stage"] == "memory_done"
        assert factory.models[0].calls == 2
        await gateway.chat(first, "")
        assert factory.models[0].calls == 2
    finally:
        await runtimes.close()


@pytest.mark.asyncio
@pytest.mark.fault
async def test_legacy_orphan_requires_review_not_reexecution(tenant_config):
    from trpc_service.gateway.idempotency import AdmissionInDoubtError, IdempotencyRecord, IdempotencyState
    gateway, runtimes, factory = make_gateway(tenant_config)
    gateway._idempotency.lookup = AsyncMock(return_value=IdempotencyRecord("lost-request", IdempotencyState.PROCESSING))
    try:
        with pytest.raises(AdmissionInDoubtError):
            await request(gateway, key="legacy-key")
        assert not gateway._requests._records and not factory.models
    finally:
        await runtimes.close()


@pytest.mark.asyncio
@pytest.mark.fault
async def test_enqueue_repair_and_lost_response_reuse_original_request(tenant_config):
    from trpc_service.gateway.queue import InMemoryAgentTaskQueue, AgentTaskEnvelope
    from trpc_service.gateway.repair import RequestRepairService
    from trpc_service.gateway.dispatcher import AgentTaskProcessor
    from trpc_service.gateway.outbox import InMemoryOutboxStore
    gateway, runtimes, factory = make_gateway(tenant_config)
    queue = InMemoryAgentTaskQueue()
    first, key = await request(gateway, key="enqueue-key")
    await gateway.prepare(first)
    original = queue.enqueue
    queue.enqueue = AsyncMock(side_effect=ConnectionError("queue unavailable"))
    try:
        with pytest.raises(ConnectionError):
            await queue.enqueue(AgentTaskEnvelope(request=first, idempotency_key=key))
        with pytest.raises(DuplicateRequestError) as duplicate:
            await request(gateway, key="enqueue-key")
        assert duplicate.value.request_id == first.request_id
        queue.enqueue = original
        assert await RequestRepairService(gateway._requests, queue).repair_stale(older_than_seconds=0) == 1
        processor = AgentTaskProcessor(queue, gateway, InMemoryOutboxStore(), consumer="test")
        assert await processor.process_one(.1)
        with pytest.raises(DuplicateRequestError) as duplicate:
            await request(gateway, key="enqueue-key")
        state = await gateway.request_status("tenant-a", duplicate.value.request_id)
        assert state.state == RequestState.SUCCEEDED and state.result.text.startswith("echo:hello")
        assert factory.models[0].calls == 1
    finally:
        await runtimes.close()


def test_group_memory_owner_separates_groups_tenants_bindings_and_private_chat():
    from trpc_service.gateway.identity import session_owner_id, internal_session_id
    base = NormalizedInboundMessage(binding_id="b",
                                    channel="wecom",
                                    message_id="1",
                                    external_user_id="alice",
                                    external_conversation_id="group-1",
                                    is_group=True,
                                    text="hi")
    for mode in ("per_user", "shared"):
        pairs = [("t", base), ("other", base), ("t", base.model_copy(update={"binding_id": "other"})),
                 ("t", base.model_copy(update={"external_conversation_id": "group-2"})),
                 ("t", base.model_copy(update={"is_group": False}))]
        assert len({session_owner_id(tenant, msg, mode) for tenant, msg in pairs}) == 5
        # A direct conversation normally has a distinct external conversation ID;
        # SDK user ownership additionally protects private Memory regardless.
        assert len({internal_session_id(tenant, "app", msg, mode) for tenant, msg in pairs[:4]}) == 4


def test_summary_policy_rejects_ineffective_or_deferred_configuration():
    from trpc_service.config.models import RuntimePolicy
    with pytest.raises(ValueError):
        RuntimePolicy(summary_event_threshold=5, summary_keep_recent=5)
    with pytest.raises(ValueError):
        RuntimePolicy(defer_post_turn_processing=True)
