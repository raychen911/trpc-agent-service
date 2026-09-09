"""Unit coverage for the real Session/Memory migration data plane."""

import json
import os
import uuid
import asyncio
import time
from types import SimpleNamespace

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import InMemorySessionService, SessionServiceConfig
from trpc_agent_sdk.sessions import Session
from trpc_agent_sdk.types import Content, EventActions, GenerateContentResponseUsageMetadata, Part

from trpc_service.migration.control import StorageRouteMode
from trpc_service.migration.redis_reader import SdkRedisSnapshotReader, SdkStorageCompatibilityError
from trpc_service.migration.routing import MigrationAwareSessionService
from trpc_service.migration.snapshots import SessionSnapshot
from trpc_service.config import AgentAppConfig, BackendType, StoragePolicy, TenantConfig
from trpc_service.gateway.identity import sdk_app_name
from trpc_service.migration import InMemoryMigrationStore, MigrationCoordinator, MigrationPhase, PostgresMigrationStore
from trpc_service.migration.control import PostgresMigrationControlStore
from trpc_service.migration.provider import RedisPostgresMigrationProvider
from trpc_service.migration.postgres_writer import SdkPostgresSnapshotWriter
from trpc_service.metrics import MetricsRegistry
from trpc_service.tenant import PostgresTenantRegistry
from trpc_service.storage import StorageProviderFactory
from trpc_service.storage.fencing import hold_storage_guards
from trpc_service.storage.keys import session_execution_key


def _event(event_id: str, text: str, request_id: str = "request-1") -> Event:
    return Event(id=event_id,
                 author="assistant",
                 request_id=request_id,
                 content=Content(parts=[Part.from_text(text=text)]))


def _summary_event() -> Event:
    event = Event(id="summary-1",
                  invocation_id="summary",
                  author="system",
                  request_id="request-1",
                  content=Content(role="user", parts=[Part.from_text(text="Previous conversation summary: earlier")]))
    event.set_summary_event(True)
    return event


@pytest.mark.unit
def test_session_snapshot_hash_preserves_current_and_historical_events():
    snapshot = SessionSnapshot(app_name="tenant:school:app:assistant",
                               user_id="user",
                               session_id="session",
                               session_state={
                                   "turn": 2
                               },
                               app_state={
                                   "prompt": "v1"
                               },
                               user_state={
                                   "name": "student"
                               },
                               events=[_event("current", "answer")],
                               historical_events=[_event("summary", "earlier summary")],
                               conversation_count=2).seal()
    restored = SessionSnapshot.model_validate_json(snapshot.model_dump_json())
    assert restored.calculate_hash() == snapshot.content_hash
    assert restored.historical_events[0].id == "summary"
    restored.session_state["turn"] = 3
    assert restored.calculate_hash() != snapshot.content_hash


@pytest.mark.unit
def test_incompatible_sdk_version_is_rejected(monkeypatch):
    monkeypatch.setattr("trpc_service.migration.redis_reader.version", lambda _: "1.1.20")
    with pytest.raises(SdkStorageCompatibilityError, match="supports trpc-agent-py 1.1.19"):
        SdkRedisSnapshotReader.validate_sdk_version()


@pytest.mark.asyncio
@pytest.mark.unit
async def test_migration_step_records_metrics_and_redacted_audit_shape():

    class Audit:

        def __init__(self):
            self.events = []

        async def write(self, event):
            self.events.append(event)

    metrics = MetricsRegistry()
    audit = Audit()
    provider = RedisPostgresMigrationProvider(None, None, None, metrics=metrics, audit=audit)
    job = await MigrationCoordinator(InMemoryMigrationStore()).create("tenant-a", "session_memory", "memory", "sql")
    with pytest.raises(ValueError, match="redis -> sql only"):
        await provider.execute(job)
    rendered = metrics.render()
    assert 'trpc_service_migration_steps_total{phase="preparing",result="error"} 1' in rendered
    assert audit.events[0].tenant_id == "tenant-a"
    assert audit.events[0].action == "migration_phase"
    assert audit.events[0].decision == "error"


@pytest.mark.asyncio
@pytest.mark.unit
async def test_target_schema_is_initialized_before_compatibility_check():

    class Storage:

        def __init__(self):
            self.initialized = False

        async def create_sql_engine(self):
            self.initialized = True

    session_storage = Storage()
    memory_storage = Storage()

    class Pool:

        async def fetch(self, query, table):
            del query
            columns = {
                "sessions": SdkPostgresSnapshotWriter.REQUIRED_SESSION_COLUMNS,
                "events": SdkPostgresSnapshotWriter.REQUIRED_EVENT_COLUMNS,
                "mem_events": {"id", "save_key", "session_id", "timestamp", "content"},
            }
            assert session_storage.initialized and memory_storage.initialized
            return [{"column_name": name} for name in columns[table]]

    writer = object.__new__(SdkPostgresSnapshotWriter)
    writer._pool = Pool()
    writer._bundle = SimpleNamespace(
        session_service=SimpleNamespace(_delegate=SimpleNamespace(_sql_storage=session_storage)),
        memory_service=SimpleNamespace(_sql_storage=memory_storage),
    )
    await writer.validate_schema()


class _FakeRedis:

    def __init__(self, values):
        self.values = values

    async def scan(self, cursor, match, count):
        del cursor, match, count
        return 0, list(self.values)

    async def lrange(self, key, start, end):
        del start, end
        return self.values[key]

    async def pttl(self, key):
        del key
        return 60_000


@pytest.mark.asyncio
@pytest.mark.unit
async def test_memory_reader_keeps_full_colon_delimited_session_id():
    key = "memory:tenant:school:app:assistant/t:school:c:web:u:abc:t:school:a:assistant:s:def"
    redis = _FakeRedis({key: [_event("event-1", "remember me").model_dump_json()]})
    reader = SdkRedisSnapshotReader("redis://unused", client=redis)
    snapshots, _, complete = await reader.scan_memories(["tenant:school:app:assistant"], {}, 10)
    assert complete
    assert snapshots[0].save_key == "tenant:school:app:assistant/t:school:c:web:u:abc"
    assert snapshots[0].session_id == "t:school:a:assistant:s:def"


@pytest.mark.asyncio
@pytest.mark.unit
async def test_dual_write_session_reads_source_and_replaces_target():
    config = SessionServiceConfig(store_historical_events=True)
    source = InMemorySessionService(session_config=config)
    target = InMemorySessionService(session_config=config)
    routed = MigrationAwareSessionService(source, target, StorageRouteMode.DUAL_WRITE)
    session = await routed.create_session(app_name="app", user_id="user", session_id="session", state={"turn": 0})
    await routed.append_event(session, _event("event-1", "hello"))
    target_session = await target.get_session(app_name="app", user_id="user", session_id="session")
    assert target_session is not None
    assert [event.id for event in target_session.events] == ["event-1"]
    await routed.close()


@pytest.mark.unit
def test_direction_aware_factory_keeps_legacy_forward_default():
    policy = StoragePolicy(session=BackendType.REDIS,
                           memory=BackendType.REDIS,
                           redis_url="redis://127.0.0.1:6379/15",
                           sql_url="postgresql://user:password@127.0.0.1/db")
    factory = StorageProviderFactory()
    with pytest.raises(ValueError, match="unsupported migration route"):
        factory.create_migration_route(policy, "memory", "redis", StorageRouteMode.DUAL_WRITE)


@pytest.mark.asyncio
@pytest.mark.unit
async def test_batch_checkpoint_stays_in_phase_and_concurrent_advancer_is_rejected():
    entered = asyncio.Event()
    release = asyncio.Event()

    async def preparing(job):
        entered.set()
        await release.wait()
        job.checkpoint["_phase_complete"] = False
        job.checkpoint["cursor"] = 7
        return job

    store = InMemoryMigrationStore()
    coordinator = MigrationCoordinator(store, {MigrationPhase.PREPARING: preparing})
    job = await coordinator.create("tenant", "session_memory", "redis", "sql")
    first = asyncio.create_task(coordinator.advance(job.job_id))
    await entered.wait()
    with pytest.raises(RuntimeError, match="already being advanced"):
        await coordinator.advance(job.job_id)
    release.set()
    updated = await first
    assert updated.phase == MigrationPhase.PREPARING
    assert updated.checkpoint["cursor"] == 7


@pytest.mark.asyncio
@pytest.mark.integration
async def test_real_dual_write_route_updates_redis_and_postgres():
    redis_url = os.getenv("TRPC_TEST_REDIS_URL")
    postgres_url = os.getenv("TRPC_TEST_POSTGRES_URL")
    if not redis_url or not postgres_url:
        pytest.skip("TRPC_TEST_REDIS_URL and TRPC_TEST_POSTGRES_URL are required")
    policy = StoragePolicy(session=BackendType.REDIS,
                           memory=BackendType.REDIS,
                           redis_url=redis_url,
                           sql_url=postgres_url)
    bundle = StorageProviderFactory().create_migration(policy, StorageRouteMode.DUAL_WRITE)
    suffix = uuid.uuid4().hex
    app_name, user_id, session_id = f"dual:{suffix}", f"user:{suffix}", f"session:{suffix}"
    try:
        async with hold_storage_guards(bundle.write_guards, f"dual-write:{suffix}"):
            session = await bundle.session_service.create_session(app_name=app_name,
                                                                  user_id=user_id,
                                                                  session_id=session_id,
                                                                  state={"turn": 0})
            await bundle.session_service.append_event(session, _event("dual-event", "written to both"))
            await bundle.memory_service.store_session(session)
        from redis.asyncio import from_url
        redis = from_url(redis_url, decode_responses=True)
        try:
            assert await redis.exists(f"session:{app_name}:{user_id}:{session_id}") == 1
        finally:
            await redis.aclose()
        import asyncpg
        conn = await asyncpg.connect(postgres_url)
        try:
            assert await conn.fetchval("SELECT count(*) FROM events WHERE app_name=$1 AND user_id=$2 AND session_id=$3",
                                       app_name, user_id, session_id) == 1
            assert await conn.fetchval("SELECT count(*) FROM mem_events WHERE save_key=$1 AND session_id=$2",
                                       f"{app_name}/{user_id}", session_id) == 1
        finally:
            await conn.close()
    finally:
        await bundle.session_service.close()
        await bundle.memory_service.close()
        for guard in bundle.write_guards.values():
            await guard.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_real_redis_to_postgres_session_memory_migration():
    redis_url = os.getenv("TRPC_TEST_REDIS_URL")
    postgres_url = os.getenv("TRPC_TEST_POSTGRES_URL")
    if not redis_url or not postgres_url:
        pytest.skip("TRPC_TEST_REDIS_URL and TRPC_TEST_POSTGRES_URL are required")
    import asyncpg
    from redis.asyncio import from_url

    pool = await asyncpg.create_pool(postgres_url, min_size=1, max_size=6)
    redis = from_url(redis_url, decode_responses=True)
    tenant_id = f"migration-{uuid.uuid4().hex[:10]}"
    app_id = "assistant"
    app_name = sdk_app_name(tenant_id, app_id)
    user_id = f"t:{tenant_id}:c:web:u:test"
    session_id = f"t:{tenant_id}:a:{app_id}:s:test"
    event = _event("event-1", "migration secret 7391")
    summary = _summary_event()
    session = Session(id=session_id,
                      app_name=app_name,
                      user_id=user_id,
                      state={"turn": 2},
                      events=[event],
                      historical_events=[summary],
                      conversation_count=2,
                      save_key=f"{app_name}/{user_id}")
    try:
        # A missing table means the operator has not applied migration 006;
        # fail explicitly instead of silently treating this as an SDK issue.
        assert await pool.fetchval("SELECT to_regclass('storage_migration_route')")
        tenant = TenantConfig(tenant_id=tenant_id,
                              apps={app_id: AgentAppConfig(app_id=app_id, model={"model_name": "fake"})},
                              storage=StoragePolicy(session=BackendType.REDIS,
                                                    memory=BackendType.REDIS,
                                                    redis_url=redis_url,
                                                    sql_url=postgres_url))
        registry = PostgresTenantRegistry(pool)
        await registry.publish(tenant)
        await redis.set(f"session:{app_name}:{user_id}:{session_id}", session.model_dump_json(), ex=3600)
        await redis.hset(f"app_state:{app_name}", mapping={"policy": json.dumps("v1")})
        await redis.hset(f"user_state:{app_name}:{user_id}", mapping={"level": json.dumps(3)})
        await redis.rpush(f"memory:{session.save_key}:{session.id}", event.model_dump_json())
        await redis.expire(f"memory:{session.save_key}:{session.id}", 3600)

        control = PostgresMigrationControlStore(pool)
        provider = RedisPostgresMigrationProvider(registry, pool, control)
        coordinator = MigrationCoordinator(PostgresMigrationStore(pool), provider.steps)
        job = await coordinator.create(tenant_id,
                                       "session_memory",
                                       "redis",
                                       "sql",
                                       batch_size=1,
                                       rollback_window_seconds=0)
        late_sessions_added = False
        for _ in range(100):
            if job.phase == MigrationPhase.COMPLETED:
                break
            job = await coordinator.advance(job.job_id)
            # The initial Session scan has finished, but the migration is still
            # backfilling Memory. These writes must be caught by final verify.
            if (job.phase == MigrationPhase.BACKFILLING and job.checkpoint.get("resource") == "memory"
                    and not late_sessions_added):
                for index in range(20):
                    late_id = f"{session_id}-late-{index}"
                    late = session.model_copy(update={"id": late_id, "save_key": session.save_key})
                    await redis.set(f"session:{app_name}:{user_id}:{late_id}", late.model_dump_json(), ex=3600)
                late_sessions_added = True
                # A new Coordinator instance represents the Admin process
                # restarting; the next batch must continue the persisted cursor.
                coordinator = MigrationCoordinator(PostgresMigrationStore(pool), provider.steps)
        assert job.phase == MigrationPhase.COMPLETED
        assert job.mismatch_count == 0
        assert await control.dirty_count(job.job_id) == 0
        assert late_sessions_added
        assert job.source_count == 22 and job.target_count == 22

        writer = SdkPostgresSnapshotWriter(postgres_url,
                                           pool=pool,
                                           session_ttl_seconds=86400,
                                           memory_ttl_seconds=604800)
        migrated = await writer.read_session(app_name, user_id, session_id)
        assert migrated is not None
        assert [item.id for item in migrated.events] == ["event-1"]
        assert [item.id for item in migrated.historical_events] == ["summary-1"]
        assert migrated.historical_events[0].is_summary_event()
        assert migrated.session_state["turn"] == 2
        assert migrated.app_state == {"policy": '"v1"'}
        assert migrated.user_state == {"level": "3"}
        assert await writer.memory_event_ids(session.save_key, session.id) == ["event-1"]
        assert await redis.exists(f"session:{app_name}:{user_id}:{session_id}") == 1
        assert await pool.fetchval("SELECT count(*) FROM sessions WHERE app_name=$1", app_name) == 21
        active_route = await control.active_route(tenant_id)
        assert active_route is not None and active_route.mode == StorageRouteMode.TARGET_ONLY
        assert (await registry.get(tenant_id)).storage.session == BackendType.SQL

        # Migration verification must interpret PostgreSQL's naive timestamp as
        # UTC. On a UTC+8 Windows host the SDK public read used to treat this
        # still-valid 20-hour-old row as 28 hours old and return None.
        aged_session_id = f"{session_id}-near-ttl"
        aged_update_time = time.time() - 20 * 60 * 60
        aged = migrated.model_copy(update={
            "session_id": aged_session_id,
            "last_update_time": aged_update_time,
            "source_updated_at": aged_update_time,
        }).seal()
        await writer.write_session(
            aged,
            session_execution_key(tenant_id, app_id, aged_session_id),
        )
        aged_read = await writer.read_session(app_name, user_id, aged_session_id)
        assert aged_read is not None
        assert abs(aged_read.last_update_time - aged_update_time) < 0.01
        await writer.close()

        # Two independently constructed target runtimes can continue the
        # migrated Session without sticky routing to the migration process.
        first = StorageProviderFactory().create_migration(tenant.storage, StorageRouteMode.TARGET_ONLY)
        second = StorageProviderFactory().create_migration(tenant.storage, StorageRouteMode.TARGET_ONLY)
        try:
            async with hold_storage_guards(second.write_guards, f"post-cutover:{session_id}"):
                continued = await second.session_service.get_session(app_name=app_name,
                                                                     user_id=user_id,
                                                                     session_id=session_id)
                assert continued is not None
                await second.session_service.append_event(continued, _event("event-2", "continued on worker two"))
            async with hold_storage_guards(first.write_guards, f"post-cutover:{session_id}"):
                observed = await first.session_service.get_session(app_name=app_name,
                                                                   user_id=user_id,
                                                                   session_id=session_id)
                assert observed is not None
                assert [item.id for item in observed.events] == ["event-1", "event-2"]
            source_after_cutover = Session.model_validate_json(await
                                                               redis.get(f"session:{app_name}:{user_id}:{session_id}"))
            assert [item.id for item in source_after_cutover.events] == ["event-1"]
        finally:
            for item in (first, second):
                await item.session_service.close()
                await item.memory_service.close()
                for native_guard in item.write_guards.values():
                    await native_guard.close()
    finally:
        await redis.aclose()
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_real_postgres_to_redis_session_memory_migration():
    redis_url = os.getenv("TRPC_TEST_REDIS_URL")
    postgres_url = os.getenv("TRPC_TEST_POSTGRES_URL")
    if not redis_url or not postgres_url:
        pytest.skip("TRPC_TEST_REDIS_URL and TRPC_TEST_POSTGRES_URL are required")
    import asyncpg
    from redis.asyncio import from_url

    pool = await asyncpg.create_pool(postgres_url, min_size=1, max_size=6)
    redis = from_url(redis_url, decode_responses=True)
    tenant_id = f"reverse-{uuid.uuid4().hex[:10]}"
    app_id = "assistant"
    app_name = sdk_app_name(tenant_id, app_id)
    user_id = f"t:{tenant_id}:c:web:u:test"
    session_id = f"t:{tenant_id}:a:{app_id}:s:test"
    policy = StoragePolicy(session=BackendType.SQL, memory=BackendType.SQL, redis_url=redis_url, sql_url=postgres_url)
    tenant = TenantConfig(tenant_id=tenant_id,
                          apps={app_id: AgentAppConfig(app_id=app_id, model={"model_name": "fake"})},
                          storage=policy)
    source = StorageProviderFactory().create(policy)
    try:
        registry = PostgresTenantRegistry(pool)
        await registry.publish(tenant)
        lock_key = session_execution_key(tenant_id, app_id, session_id)
        async with hold_storage_guards(source.write_guards, lock_key):
            session = await source.session_service.create_session(app_name=app_name,
                                                                  user_id=user_id,
                                                                  session_id=session_id,
                                                                  state={
                                                                      "turn": "2",
                                                                      "app:policy": "v1",
                                                                      "user:level": "3"
                                                                  })
            event = _event("event-1", "reverse migration 7391")
            event.actions = EventActions(state_delta={"last_code": "7391"})
            event.usage_metadata = GenerateContentResponseUsageMetadata(prompt_token_count=12,
                                                                        candidates_token_count=3,
                                                                        total_token_count=15)
            await source.session_service.append_event(session, event)
            session.historical_events = [_summary_event()]
            session.conversation_count = 2
            await source.session_service.update_session(session)
            await source.memory_service.store_session(session)
        source_update_time = await pool.fetchval(
            "SELECT update_time FROM sessions WHERE app_name=$1 AND user_id=$2 AND id=$3", app_name, user_id,
            session_id)

        stale_id = f"{session_id}-stale-target-only"
        stale = session.model_copy(update={"id": stale_id, "save_key": session.save_key})
        await redis.set(f"session:{app_name}:{user_id}:{stale_id}", stale.model_dump_json(), ex=3600)

        control = PostgresMigrationControlStore(pool)
        provider = RedisPostgresMigrationProvider(registry, pool, control)
        coordinator = MigrationCoordinator(PostgresMigrationStore(pool), provider.steps)
        job = await coordinator.create(tenant_id,
                                       "session_memory",
                                       "sql",
                                       "redis",
                                       batch_size=1,
                                       rollback_window_seconds=0)
        late_sessions_added = False
        for _ in range(100):
            if job.phase == MigrationPhase.COMPLETED:
                break
            job = await coordinator.advance(job.job_id)
            if (job.phase == MigrationPhase.BACKFILLING and job.checkpoint.get("resource") == "memory"
                    and not late_sessions_added):
                for index in range(20):
                    late_id = f"{session_id}-late-{index}"
                    async with hold_storage_guards(source.write_guards,
                                                   session_execution_key(tenant_id, app_id, late_id)):
                        late = await source.session_service.create_session(app_name=app_name,
                                                                           user_id=user_id,
                                                                           session_id=late_id,
                                                                           state={"turn": str(index)})
                        await source.session_service.append_event(late, _event(f"late-event-{index}", f"late {index}"))
                late_sessions_added = True
                coordinator = MigrationCoordinator(PostgresMigrationStore(pool), provider.steps)
        assert job.phase == MigrationPhase.COMPLETED
        assert job.source_count == job.target_count == 22
        assert job.mismatch_count == 0
        assert await control.dirty_count(job.job_id) == 0
        raw = await redis.get(f"session:{app_name}:{user_id}:{session_id}")
        migrated = Session.model_validate_json(raw)
        assert [item.id for item in migrated.events] == ["event-1"]
        assert migrated.events[0].actions.state_delta == {"last_code": "7391"}
        assert migrated.events[0].usage_metadata.total_token_count == 15
        assert [item.id for item in migrated.historical_events] == ["summary-1"]
        assert migrated.historical_events[0].is_summary_event()
        assert await redis.lrange(f"memory:{app_name}/{user_id}:{session_id}", 0, -1)
        assert not await redis.exists(f"session:{app_name}:{user_id}:{stale_id}")
        assert await pool.fetchval("SELECT count(*) FROM migration_target_backup WHERE job_id=$1 AND redis_key=$2",
                                   job.job_id, f"session:{app_name}:{user_id}:{stale_id}") == 1
        route = await control.active_route(tenant_id)
        assert route is not None and route.mode == StorageRouteMode.TARGET_ONLY
        assert (await registry.get(tenant_id)).storage.session == BackendType.REDIS
        # Source data is deliberately retained for a later forward migration.
        assert await pool.fetchval("SELECT count(*) FROM sessions WHERE app_name=$1", app_name) == 21
        assert await pool.fetchval("SELECT update_time FROM sessions WHERE app_name=$1 AND user_id=$2 AND id=$3",
                                   app_name, user_id, session_id) == source_update_time
        assert late_sessions_added
    finally:
        await source.session_service.close()
        await source.memory_service.close()
        for guard in source.write_guards.values():
            await guard.close()
        await redis.aclose()
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_cutover_observation_can_rollback_with_continuous_session():
    redis_url = os.getenv("TRPC_TEST_REDIS_URL")
    postgres_url = os.getenv("TRPC_TEST_POSTGRES_URL")
    if not redis_url or not postgres_url:
        pytest.skip("TRPC_TEST_REDIS_URL and TRPC_TEST_POSTGRES_URL are required")
    import asyncpg
    from redis.asyncio import from_url

    pool = await asyncpg.create_pool(postgres_url, min_size=1, max_size=6)
    redis = from_url(redis_url, decode_responses=True)
    tenant_id = f"rollback-{uuid.uuid4().hex[:10]}"
    app_id = "assistant"
    app_name = sdk_app_name(tenant_id, app_id)
    user_id = f"t:{tenant_id}:c:web:u:test"
    session_id = f"t:{tenant_id}:a:{app_id}:s:test"
    policy = StoragePolicy(session=BackendType.REDIS,
                           memory=BackendType.REDIS,
                           redis_url=redis_url,
                           sql_url=postgres_url)
    tenant = TenantConfig(tenant_id=tenant_id,
                          apps={app_id: AgentAppConfig(app_id=app_id, model={"model_name": "fake"})},
                          storage=policy)
    session = Session(id=session_id,
                      app_name=app_name,
                      user_id=user_id,
                      events=[_event("before", "before")],
                      save_key=f"{app_name}/{user_id}")
    try:
        registry = PostgresTenantRegistry(pool)
        await registry.publish(tenant)
        await redis.set(f"session:{app_name}:{user_id}:{session_id}", session.model_dump_json(), ex=3600)
        control = PostgresMigrationControlStore(pool)
        provider = RedisPostgresMigrationProvider(registry, pool, control)
        coordinator = MigrationCoordinator(PostgresMigrationStore(pool), provider.steps)
        job = await coordinator.create(tenant_id,
                                       "session_memory",
                                       "redis",
                                       "sql",
                                       batch_size=10,
                                       rollback_window_seconds=3600)
        for _ in range(30):
            job = await coordinator.advance(job.job_id)
            route = await control.active_route(tenant_id)
            if job.phase == MigrationPhase.CUTOVER and route and route.mode == StorageRouteMode.TARGET_PRIMARY_MIRROR:
                break
        assert route is not None and route.mode == StorageRouteMode.TARGET_PRIMARY_MIRROR

        mirror = StorageProviderFactory().create_migration(policy, StorageRouteMode.TARGET_PRIMARY_MIRROR)
        try:
            async with hold_storage_guards(mirror.write_guards, f"rollback-observation:{session_id}"):
                current = await mirror.session_service.get_session(app_name=app_name,
                                                                   user_id=user_id,
                                                                   session_id=session_id)
                assert current is not None
                await mirror.session_service.append_event(current, _event("during", "during observation"))
        finally:
            await mirror.session_service.close()
            await mirror.memory_service.close()
            for native_guard in mirror.write_guards.values():
                await native_guard.close()

        rolled_back = await coordinator.rollback(job.job_id)
        assert rolled_back.phase == MigrationPhase.ROLLED_BACK
        assert (await control.active_route(tenant_id)).mode == StorageRouteMode.SOURCE_ONLY
        assert (await registry.get(tenant_id)).storage.session == BackendType.REDIS
        redis_session = Session.model_validate_json(await redis.get(f"session:{app_name}:{user_id}:{session_id}"))
        assert [item.id for item in redis_session.events] == ["before", "during"]
    finally:
        await redis.aclose()
        await pool.close()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_reverse_cutover_observation_can_rollback_to_postgres():
    redis_url = os.getenv("TRPC_TEST_REDIS_URL")
    postgres_url = os.getenv("TRPC_TEST_POSTGRES_URL")
    if not redis_url or not postgres_url:
        pytest.skip("TRPC_TEST_REDIS_URL and TRPC_TEST_POSTGRES_URL are required")
    import asyncpg
    from redis.asyncio import from_url

    pool = await asyncpg.create_pool(postgres_url, min_size=1, max_size=6)
    redis = from_url(redis_url, decode_responses=True)
    tenant_id = f"reverse-rollback-{uuid.uuid4().hex[:8]}"
    app_id = "assistant"
    app_name = sdk_app_name(tenant_id, app_id)
    user_id = f"t:{tenant_id}:c:web:u:test"
    session_id = f"t:{tenant_id}:a:{app_id}:s:test"
    policy = StoragePolicy(session=BackendType.SQL, memory=BackendType.SQL, redis_url=redis_url, sql_url=postgres_url)
    tenant = TenantConfig(tenant_id=tenant_id,
                          apps={app_id: AgentAppConfig(app_id=app_id, model={"model_name": "fake"})},
                          storage=policy)
    source = StorageProviderFactory().create(policy)
    try:
        registry = PostgresTenantRegistry(pool)
        await registry.publish(tenant)
        lock_key = session_execution_key(tenant_id, app_id, session_id)
        async with hold_storage_guards(source.write_guards, lock_key):
            session = await source.session_service.create_session(app_name=app_name,
                                                                  user_id=user_id,
                                                                  session_id=session_id,
                                                                  state={})
            await source.session_service.append_event(session, _event("before", "before reverse"))
        stale_id = f"{session_id}-restore-on-rollback"
        stale = session.model_copy(update={"id": stale_id, "save_key": session.save_key})
        await redis.set(f"session:{app_name}:{user_id}:{stale_id}", stale.model_dump_json(), ex=3600)
        control = PostgresMigrationControlStore(pool)
        provider = RedisPostgresMigrationProvider(registry, pool, control)
        coordinator = MigrationCoordinator(PostgresMigrationStore(pool), provider.steps)
        job = await coordinator.create(tenant_id,
                                       "session_memory",
                                       "sql",
                                       "redis",
                                       batch_size=10,
                                       rollback_window_seconds=3600)
        for _ in range(30):
            job = await coordinator.advance(job.job_id)
            route = await control.active_route(tenant_id)
            if (job.phase == MigrationPhase.CUTOVER and route and route.mode == StorageRouteMode.TARGET_PRIMARY_MIRROR):
                break
        assert route is not None and route.mode == StorageRouteMode.TARGET_PRIMARY_MIRROR
        assert not await redis.exists(f"session:{app_name}:{user_id}:{stale_id}")

        mirror = StorageProviderFactory().create_migration_route(policy, BackendType.SQL, BackendType.REDIS,
                                                                 StorageRouteMode.TARGET_PRIMARY_MIRROR)
        try:
            async with hold_storage_guards(mirror.write_guards, lock_key):
                current = await mirror.session_service.get_session(app_name=app_name,
                                                                   user_id=user_id,
                                                                   session_id=session_id)
                assert current is not None
                await mirror.session_service.append_event(current, _event("during", "during reverse observation"))
        finally:
            await mirror.session_service.close()
            await mirror.memory_service.close()
            for native_guard in mirror.write_guards.values():
                await native_guard.close()

        rolled_back = await coordinator.rollback(job.job_id)
        assert rolled_back.phase == MigrationPhase.ROLLED_BACK
        assert (await control.active_route(tenant_id)).mode == StorageRouteMode.SOURCE_ONLY
        assert (await registry.get(tenant_id)).storage.session == BackendType.SQL
        assert await redis.exists(f"session:{app_name}:{user_id}:{stale_id}") == 1
        rows = await pool.fetch(
            "SELECT id FROM events WHERE app_name=$1 AND user_id=$2 AND session_id=$3 ORDER BY timestamp,id", app_name,
            user_id, session_id)
        assert [row["id"] for row in rows] == ["before", "during"]
    finally:
        await source.session_service.close()
        await source.memory_service.close()
        for guard in source.write_guards.values():
            await guard.close()
        await redis.aclose()
        await pool.close()
