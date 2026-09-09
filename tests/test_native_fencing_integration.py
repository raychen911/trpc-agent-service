"""Explicit real-backend tests; skipped without dedicated test URLs.

Run: python -m pytest tests/test_native_fencing_integration.py -vv
Needs test Compose/migrations. Calls no real model/IM. Writes unique test names;
does not flush databases. Includes separate Python processes for SDK continuity.
"""
import asyncio
import json
import os
import subprocess
import sys
import uuid

import pytest

pytestmark = pytest.mark.integration
REDIS = os.getenv("TRPC_TEST_REDIS_URL")
POSTGRES = os.getenv("TRPC_TEST_POSTGRES_URL")


@pytest.mark.skipif(not REDIS, reason="TRPC_TEST_REDIS_URL not set")
@pytest.mark.asyncio
@pytest.mark.fault
async def test_real_lua_rejects_write_after_check_then_takeover():
    from redis.asyncio import from_url
    from trpc_agent_sdk.storage import RedisCommand
    from trpc_service.storage import RedisSessionExecutionGuard, SessionLockLostError
    from trpc_service.storage.fencing import FencedRedisStorage, storage_lease_scope
    client = from_url(REDIS, decode_responses=True)
    prefix = "test:fence:" + uuid.uuid4().hex
    guard = RedisSessionExecutionGuard(REDIS, client=client, prefix=prefix)
    proxy = FencedRedisStorage(None, "test")
    try:
        with pytest.raises(SessionLockLostError):
            async with guard.hold("session", lease_seconds=30) as old:
                await old.verify()
                # Simulate the authoritative ownership change after the Python check.
                await client.set(old.key, "replacement", px=1000)
                await client.incr(old.key + ":epoch")
                with storage_lease_scope("test", old):
                    await proxy.execute_command(client, RedisCommand(method="set", args=(prefix + ":data", "old")))
        assert await client.get(prefix + ":data") is None
    finally:
        await client.aclose()


@pytest.mark.skipif(not POSTGRES, reason="TRPC_TEST_POSTGRES_URL not set")
@pytest.mark.asyncio
@pytest.mark.fault
async def test_real_postgres_fence_rolls_back_stale_transaction():
    import asyncpg
    from sqlalchemy import create_engine, text
    from sqlalchemy.orm import Session
    from trpc_service.storage import SessionLockLostError
    from trpc_service.storage.fencing import PostgresExecutionGuard, FencedSqlStorage, storage_lease_scope
    pool = await asyncpg.create_pool(POSTGRES, min_size=1, max_size=3)
    engine = create_engine(POSTGRES)
    guard = PostgresExecutionGuard(pool=pool)

    class Delegate:

        async def commit(self, conn):
            conn.commit()

    try:
        with Session(engine) as conn:
            conn.execute(text("CREATE TEMP TABLE fence_probe (value TEXT)"))
            conn.commit()
            with pytest.raises(SessionLockLostError):
                async with guard.hold("test:" + uuid.uuid4().hex) as old:
                    await old.verify()
                    await pool.execute(
                        "UPDATE platform_execution_lease SET token='new',epoch=epoch+1 WHERE lease_key=$1", old.key)
                    conn.execute(text("INSERT INTO fence_probe VALUES ('stale')"))
                    with storage_lease_scope("pg", old):
                        await FencedSqlStorage(Delegate(), "pg").commit(conn)
            assert conn.execute(text("SELECT count(*) FROM fence_probe")).scalar() == 0
    finally:
        engine.dispose()
        await pool.close()


_PROBE = """
import asyncio, json, os
from trpc_service.config import TenantConfig, AgentAppConfig, StoragePolicy
from trpc_service.offline import OfflineRuntimeFactory
from trpc_service.storage import StorageProviderFactory, InMemorySessionExecutionGuard
from trpc_service.agent import TenantRuntimeManager, AgentWorker
from trpc_service.tenant import InMemoryTenantRegistry
from trpc_service.gateway import InMemoryIdempotencyStore
from trpc_service.gateway.service import GatewayService
async def main():
    kind = os.environ['PROBE_KIND']
    policy = StoragePolicy(session=kind, memory=kind,
        redis_url=os.environ.get('TRPC_TEST_REDIS_URL','redis://localhost'),
        sql_url=os.environ.get('TRPC_TEST_POSTGRES_URL','sqlite://'))
    tenant = TenantConfig(tenant_id=os.environ['PROBE_TENANT'], storage=policy,
        apps={'a':AgentAppConfig(app_id='a',model={'model_name':'offline'},runtime={'summary_enabled':False})})
    factory = OfflineRuntimeFactory(StorageProviderFactory().create(policy))
    registry = InMemoryTenantRegistry([tenant])
    runtimes = TenantRuntimeManager(registry,factory)
    gateway = GatewayService(registry,AgentWorker(runtimes,InMemorySessionExecutionGuard()),InMemoryIdempotencyStore())
    try:
        req,key = await gateway.web_request(tenant_id=tenant.tenant_id,app_id='a',
            external_user_id='u',session_id='s',text='continuity')
        result = await gateway.chat(req,key)
        runtime = await runtimes.get(tenant.tenant_id,'a',1)
        # SQL Memory search refreshes timestamps; acquire the same native fence.
        async with runtime.execution_scope(f'{req.tenant_id}:{req.app_id}:{req.session_id}'):
            session = await runtime.runner.session_service.get_session(
                app_name=runtime.runner.app_name,user_id=req.user_id,session_id=req.session_id)
            memory = await runtime.runner.memory_service.search_memory(session.save_key,'continuity')
        print('PROBE_RESULT='+json.dumps({'text':result.text,'events':len(session.events),'memories':len(memory.memories)}))
    finally:
        await runtimes.close()
asyncio.run(main())
"""

_RECOVERY_PROBE = """
import asyncio, json, os
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, Part
from trpc_service.config import TenantConfig, AgentAppConfig, StoragePolicy
from trpc_service.offline import OfflineRuntimeFactory
from trpc_service.storage import StorageProviderFactory, InMemorySessionExecutionGuard
from trpc_service.agent import TenantRuntimeManager, AgentWorker
from trpc_service.tenant import InMemoryTenantRegistry
from trpc_service.gateway.models import AgentRequest
async def main():
    kind = os.environ['PROBE_KIND']
    tenant_id = os.environ['PROBE_TENANT']
    policy = StoragePolicy(session=kind, memory=kind,
        redis_url=os.environ.get('TRPC_TEST_REDIS_URL','redis://localhost'),
        sql_url=os.environ.get('TRPC_TEST_POSTGRES_URL','sqlite://'))
    tenant = TenantConfig(tenant_id=tenant_id, storage=policy,
        apps={'a':AgentAppConfig(app_id='a',model={'model_name':'offline'},runtime={'summary_enabled':False})})
    factory = OfflineRuntimeFactory(StorageProviderFactory().create(policy))
    runtimes = TenantRuntimeManager(InMemoryTenantRegistry([tenant]),factory)
    runtime = await runtimes.get(tenant_id,'a',1)
    lock_key = f'{tenant_id}:a:s'
    try:
        if os.environ['PROBE_MODE'] == 'crash':
            async with runtime.execution_scope(lock_key):
                session = await runtime.runner.session_service.create_session(
                    app_name=runtime.runner.app_name,user_id='u',session_id='s')
                session.conversation_count = 1
                await runtime.runner.session_service.append_event(session,Event(author='user',request_id='crashed',
                    content=Content(parts=[Part.from_text(text='recover-me')])))
            print('RECOVERY_RESULT='+json.dumps({'created':True}))
        else:
            request = AgentRequest(request_id='crashed',tenant_id=tenant_id,config_version=1,app_id='a',
                user_id='u',session_id='s',text='recover-me',channel='web')
            events = [event async for event in AgentWorker(runtimes,InMemorySessionExecutionGuard()).stream(request)]
            async with runtime.execution_scope(lock_key):
                session = await runtime.runner.session_service.get_session(
                    app_name=runtime.runner.app_name,user_id='u',session_id='s')
            matching = [event for event in session.events if event.request_id == 'crashed']
            payload = {'completed':events[-1].type.value,
                'authors':[event.author for event in matching],
                'conversation_count':session.conversation_count}
            print('RECOVERY_RESULT='+json.dumps(payload))
    finally:
        await runtimes.close()
asyncio.run(main())
"""


@pytest.mark.parametrize("kind,url", [("redis", REDIS), ("sql", POSTGRES)])
def test_two_separate_workers_share_sdk_session_and_memory(kind, url):
    if not url:
        pytest.skip(f"dedicated {kind} test URL not set")
    env = {**os.environ, "PROBE_KIND": kind, "PROBE_TENANT": "probe-" + uuid.uuid4().hex[:12]}
    results = []
    for _ in range(2):
        proc = subprocess.run([sys.executable, "-c", _PROBE], env=env, capture_output=True, text=True, timeout=60)
        assert proc.returncode == 0, "worker probe failed; run the marked test with local diagnostic logging"
        line = next(line for line in proc.stdout.splitlines() if line.startswith("PROBE_RESULT="))
        results.append(json.loads(line.split("=", 1)[1]))
    assert results[0]["text"].endswith("user_turns=1")
    assert results[1]["text"].endswith("user_turns=2")
    assert results[1]["events"] == 4 and results[1]["memories"] >= 2


@pytest.mark.parametrize("kind", ["redis", "sql"])
def test_second_process_recovers_plain_user_event_after_worker_crash(kind):
    url = REDIS if kind == "redis" else POSTGRES
    if not url:
        pytest.skip(f"dedicated {kind} test URL not set")
    env = {
        **os.environ,
        "PROBE_KIND": kind,
        "PROBE_TENANT": "recovery-" + uuid.uuid4().hex[:12],
    }
    outputs = []
    for mode in ("crash", "recover"):
        proc = subprocess.run([sys.executable, "-c", _RECOVERY_PROBE],
                              env={
                                  **env, "PROBE_MODE": mode
                              },
                              capture_output=True,
                              text=True,
                              timeout=60)
        assert proc.returncode == 0, "recovery probe failed; run this test with local diagnostic logging"
        line = next(line for line in proc.stdout.splitlines() if line.startswith("RECOVERY_RESULT="))
        outputs.append(json.loads(line.split("=", 1)[1]))
    assert outputs[0]["created"]
    assert outputs[1] == {
        "completed": "completed",
        "authors": ["user", "offline_assistant"],
        "conversation_count": 1,
    }


@pytest.mark.skipif(not POSTGRES, reason="TRPC_TEST_POSTGRES_URL not set")
@pytest.mark.asyncio
async def test_postgres_admission_and_outbox_commit_are_atomic():
    import asyncpg
    from trpc_service.config import TenantConfig, AgentAppConfig, ChannelBindingConfig
    from trpc_service.tenant import PostgresTenantRegistry
    from trpc_service.gateway.requests import PostgresRequestStore
    from trpc_service.gateway.models import AgentRequest, RequestRecord, ChatResult, OutboundMessage
    from trpc_service.storage.fencing import PostgresExecutionGuard
    pool = await asyncpg.create_pool(POSTGRES, min_size=1, max_size=5)
    tid = "atomic-" + uuid.uuid4().hex[:12]
    try:
        await PostgresTenantRegistry(pool).publish(
            TenantConfig(tenant_id=tid,
                         apps={"a": AgentAppConfig(app_id="a", model={"model_name": "offline"})},
                         channels=[
                             ChannelBindingConfig(binding_id=tid,
                                                  channel="wecom_kf",
                                                  app_id="a",
                                                  corp_id="corp",
                                                  open_kfid="kf")
                         ]))
        store = PostgresRequestStore(pool)

        # Idempotency insertion succeeds first, then the Request FK fails.
        # The surrounding transaction must remove the new reservation too.
        missing_tenant = "missing-" + uuid.uuid4().hex[:12]
        with pytest.raises(asyncpg.ForeignKeyViolationError):
            await store.reserve_and_create_request(
                RequestRecord(tenant_id=missing_tenant,
                              request_id="failure",
                              state="reserved",
                              idempotency_key="rollback",
                              payload_hash="b" * 64))
        assert await pool.fetchval("SELECT count(*) FROM idempotency_record WHERE tenant_id=$1", missing_tenant) == 0

        async def reserve(index):
            req = AgentRequest(tenant_id=tid,
                               request_id=f"{tid}-{index}",
                               app_id="a",
                               config_version=1,
                               user_id="u",
                               session_id="s",
                               text="test")
            return await store.reserve_and_create_request(
                RequestRecord(tenant_id=tid,
                              request_id=req.request_id,
                              state="reserved",
                              request=req,
                              idempotency_key="one",
                              payload_hash="a" * 64))

        records = await asyncio.gather(*(reserve(index) for index in range(20)))
        assert sum(created for _, created in records) == 1
        req = records[0][0].request
        result = ChatResult(request_id=req.request_id,
                            tenant_id=tid,
                            app_id="a",
                            user_id="u",
                            session_id="s",
                            text="reply")
        message = OutboundMessage(outbound_id=uuid.uuid4().hex,
                                  request_id=req.request_id,
                                  tenant_id=tid,
                                  binding_id="nonexistent-binding",
                                  channel="wecom_kf",
                                  external_conversation_id="kf:u",
                                  text="reply")
        async with PostgresExecutionGuard(pool=pool, control=True).hold(tid):
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await store.finalize(req, result, [message])
            assert (await store.get(tid, req.request_id)).state == "reserved"
            await store.finalize(req, result, [message.model_copy(update={"binding_id": tid})])
        assert (await store.get(tid, req.request_id)).state == "succeeded"
        assert await pool.fetchval("SELECT state FROM idempotency_record WHERE tenant_id=$1", tid) == "succeeded"
    finally:
        await pool.close()
