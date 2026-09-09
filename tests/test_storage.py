# storage 模块单元测试
import pytest

from trpc_service.storage import (
    InMemoryStorage,
    StorageFactory,
    create_sql_engine,
)
from trpc_service.tenant import DataBackendConfig


@pytest.fixture
def storage():
    return InMemoryStorage()


@pytest.mark.asyncio
async def test_session_crud_and_tenant_isolation(storage):
    await storage.session.save_session("t1", {"session_id": "s1", "state": {"x": 1}})
    sess = await storage.session.get_session("t1", "s1")
    assert sess["state"] == {"x": 1}
    # 跨租户隔离
    assert await storage.session.get_session("t2", "s1") is None
    # 乐观锁版本号
    await storage.session.update_state("t1", "s1", {"x": 2})
    updated = await storage.session.get_session("t1", "s1")
    assert updated["version"] == 1


@pytest.mark.asyncio
async def test_idempotency(storage):
    assert await storage.idempotency.try_acquire("idem:tg:m1")
    assert not await storage.idempotency.try_acquire("idem:tg:m1")
    await storage.idempotency.release("idem:tg:m1")
    assert await storage.idempotency.try_acquire("idem:tg:m1")


@pytest.mark.asyncio
async def test_distributed_lock(storage):
    assert await storage.lock.acquire("lock:t:s")
    assert not await storage.lock.acquire("lock:t:s")
    await storage.lock.release("lock:t:s")
    assert await storage.lock.acquire("lock:t:s")


@pytest.mark.asyncio
async def test_acquire_lock_with_retry_waits_for_release(storage):
    """锁被占时应等待重试直至释放，而非立即失败（并发写一致性的前置）。"""
    import asyncio

    from trpc_service.storage.base import acquire_lock_with_retry

    assert await storage.lock.acquire("lock:t:retry")
    released = asyncio.Event()

    async def holder():
        await asyncio.sleep(0.2)
        await storage.lock.release("lock:t:retry")
        released.set()

    task = asyncio.create_task(holder())
    got = await acquire_lock_with_retry(storage.lock, "lock:t:retry", wait_seconds=3.0, interval=0.02)
    assert got, "锁在租约内被释放后应重试成功"
    assert released.is_set()
    await storage.lock.release("lock:t:retry")
    await task


@pytest.mark.asyncio
async def test_acquire_lock_with_retry_timeout(storage):
    """锁持续被占时应在等待预算内返回 False（不抛错、不无限等）。"""
    from trpc_service.storage.base import acquire_lock_with_retry

    assert await storage.lock.acquire("lock:t:busy", timeout=30)  # 长租约，不会被释放
    got = await acquire_lock_with_retry(storage.lock, "lock:t:busy", wait_seconds=0.2, interval=0.02)
    assert got is False


@pytest.mark.asyncio
async def test_memory_search(storage):
    await storage.memory.add_memory("t1", "u1", {"content": "用户喜欢 Python"})
    await storage.memory.add_memory("t1", "u1", {"content": "用户讨厌香菜"})
    hits = await storage.memory.search_memory("t1", "u1", "Python", top_k=1)
    assert hits and "Python" in hits[0]["content"]
    # 跨租户隔离
    assert await storage.memory.search_memory("t2", "u1", "Python") == []


@pytest.mark.asyncio
async def test_summary_store(storage):
    await storage.summary.save_summary("t1", "s1", "会话摘要")
    assert await storage.summary.get_summary("t1", "s1") == "会话摘要"
    # 跨租户隔离
    assert await storage.summary.get_summary("t2", "s1") is None
    await storage.summary.delete_summary("t1", "s1")
    assert await storage.summary.get_summary("t1", "s1") is None


@pytest.mark.asyncio
async def test_audit_store(storage):
    await storage.audit.write_log("t1", {"decision": "allow", "tool_name": "search"})
    logs = await storage.audit.query_logs("t1", {"tool_name": "search"})
    assert len(logs) == 1
    assert await storage.audit.query_logs("t2", {}) == []


@pytest.mark.asyncio
async def test_artifact_store(storage):
    """Artifact 存储抽象（PRD 2.1/2.2）：put/get/delete + 跨租户隔离。"""
    aid = await storage.artifact.put_artifact("t1", "s1", "report.pdf", b"%PDF-bytes")
    assert aid == "s1/report.pdf"
    assert await storage.artifact.get_artifact("t1", aid) == b"%PDF-bytes"
    # 跨租户隔离
    assert await storage.artifact.get_artifact("t2", aid) is None
    await storage.artifact.delete_artifact("t1", aid)
    assert await storage.artifact.get_artifact("t1", aid) is None


@pytest.mark.asyncio
async def test_sql_summary_store(tmp_path):
    from trpc_service.storage import SqlSummaryStore

    dsn = f"sqlite+aiosqlite:///{tmp_path}/summary.db"
    engine = await create_sql_engine(dsn)
    store = SqlSummaryStore(engine)
    await store.save_summary("t1", "s1", "第一版")
    await store.save_summary("t1", "s1", "第二版")  # 幂等覆盖（每 session 一条）
    assert await store.get_summary("t1", "s1") == "第二版"
    assert await store.get_summary("t1", "s9") is None
    await store.delete_summary("t1", "s1")
    assert await store.get_summary("t1", "s1") is None
    await store.close()


@pytest.mark.asyncio
async def test_sql_audit_store(tmp_path):
    dsn = f"sqlite+aiosqlite:///{tmp_path}/audit.db"
    engine = await create_sql_engine(dsn)
    from trpc_service.storage import SqlAuditStore

    audit = SqlAuditStore(engine)
    await audit.write_log("t1", {"trace_id": "trc", "decision": "block", "latency_ms": 5})
    logs = await audit.query_logs("t1", {"decision": "block"})
    assert len(logs) == 1
    assert logs[0]["trace_id"] == "trc"
    assert logs[0]["latency_ms"] == 5
    await audit.close()


@pytest.mark.asyncio
async def test_sql_audit_query_rows_json_serializable(tmp_path):
    """query_logs 返回行必须 JSON 可序列化（Admin 端点直接 JSONResponse 返回）。

    联调发现: SQL 行 created_at 是 datetime，FastAPI JSONResponse 序列化抛
    TypeError 500——既有 bug，此前只测 query_logs 未测 Admin 端点 JSON 输出。
    """
    import json

    dsn = f"sqlite+aiosqlite:///{tmp_path}/audit_json.db"
    engine = await create_sql_engine(dsn)
    from trpc_service.storage import SqlAuditStore

    audit = SqlAuditStore(engine)
    await audit.write_log("t1", {"trace_id": "trc-x", "decision": "allow"})
    logs = await audit.query_logs("t1", {})
    # 直接对返回值做 json 序列化，复现 Admin JSONResponse 的失败场景
    json.dumps(logs)  # 修复前: TypeError: Object of type datetime is not JSON serializable
    assert isinstance(logs[0]["created_at"], str), "created_at 应转为 ISO 字符串"
    await audit.close()


@pytest.mark.asyncio
async def test_factory_missing_sql_engine():
    factory = StorageFactory()
    cfg = DataBackendConfig(session="inmemory", audit="sql")
    with pytest.raises(RuntimeError):
        await factory.create("t1", cfg)


@pytest.mark.asyncio
async def test_factory_composite(tmp_path):
    dsn = f"sqlite+aiosqlite:///{tmp_path}/f.db"
    engine = await create_sql_engine(dsn)
    factory = StorageFactory(sql_engine=engine)
    cfg = DataBackendConfig(session="inmemory", memory="inmemory", audit="sql")
    st = await factory.create("t1", cfg)
    await st.audit.write_log("t1", {"decision": "allow"})
    assert len(await st.audit.query_logs("t1", {})) == 1
    await st.close()


@pytest.mark.asyncio
async def test_factory_unsupported_backend_raises(tmp_path):
    """未配置依赖时显式报错：vector 无 Redis / s3 未实现，均不静默降级（PRD 2.2 口径）。"""
    dsn = f"sqlite+aiosqlite:///{tmp_path}/u.db"
    engine = await create_sql_engine(dsn)
    factory = StorageFactory(sql_engine=engine)
    with pytest.raises(RuntimeError, match="knowledge=vector"):
        await factory.create("t1", DataBackendConfig(session="inmemory", memory="inmemory", knowledge="vector"))
    with pytest.raises(NotImplementedError):
        await factory.create("t1", DataBackendConfig(session="inmemory", memory="inmemory", artifact="s3"))


@pytest.mark.asyncio
async def test_storage_manager_per_tenant_backends(tmp_path):
    """StorageManager 按租户 data_backend_config 懒建：audit 后端不同则落不同库。"""
    from trpc_service.storage import SqlAuditStore
    from trpc_service.storage.manager import StorageManager
    from trpc_service.tenant import TenantConfig

    dsn = f"sqlite+aiosqlite:///{tmp_path}/m.db"
    engine = await create_sql_engine(dsn)
    factory = StorageFactory(redis=None, sql_engine=engine)
    manager = StorageManager(factory)

    tenant_sql = TenantConfig(tenant_id="sql-tenant",
                              name="sql户",
                              backends={
                                  "session": "inmemory",
                                  "memory": "inmemory",
                                  "audit": "sql"
                              })
    tenant_mem = TenantConfig(tenant_id="mem-tenant",
                              name="mem户",
                              backends={
                                  "session": "inmemory",
                                  "memory": "inmemory",
                                  "audit": "inmemory"
                              })

    st_sql = await manager.get(tenant_sql)
    st_mem = await manager.get(tenant_mem)
    assert isinstance(st_sql.audit, SqlAuditStore), "audit=sql 租户应落 SqlAuditStore"
    assert not isinstance(st_mem.audit, SqlAuditStore), "audit=inmemory 租户不应用 SqlAuditStore"

    # 缓存复用: 同租户两次 get 同一实例
    assert await manager.get(tenant_sql) is st_sql
    # 失效后重建新实例
    manager.invalidate("sql-tenant")
    assert await manager.get(tenant_sql) is not st_sql

    await manager.close()


@pytest.mark.asyncio
async def test_sql_tenant_store(tmp_path):
    from trpc_service.storage import SqlTenantStore
    from trpc_service.tenant import TenantConfig

    dsn = f"sqlite+aiosqlite:///{tmp_path}/tenant.db"
    engine = await create_sql_engine(dsn)
    store = SqlTenantStore(engine)

    # create + get
    config = TenantConfig(tenant_id="t1", name="租户一", rate_limit_per_min=30)
    await store.create(config)
    loaded = await store.get("t1")
    assert loaded.name == "租户一"
    assert loaded.rate_limit_per_min == 30
    # 跨租户隔离
    assert await store.get("t2") is None
    assert [c.tenant_id for c in await store.list()] == ["t1"]

    # update
    config.status = "suspended"
    await store.update("t1", config)
    assert (await store.get("t1")).status == "suspended"

    # delete
    await store.delete("t1")
    assert await store.get("t1") is None
    await store.close()


@pytest.mark.asyncio
async def test_sql_tenant_store_secret_not_persisted(tmp_path):
    from trpc_service.storage import SqlTenantStore
    from trpc_service.tenant import ImChannelConfig, ModelConfig, TenantConfig

    dsn = f"sqlite+aiosqlite:///{tmp_path}/t2.db"
    engine = await create_sql_engine(dsn)
    store = SqlTenantStore(engine)
    config = TenantConfig(
        tenant_id="tk",
        name="带密钥",
        model=ModelConfig(api_key_ref="sk-secret-1234567890"),
        im=[ImChannelConfig(channel_type="wechat_work", token_ref="tok-abc-123456")],
    )
    await store.create(config)
    # 密钥字段不落库：重载后应为 None（PRD 4.5）
    reloaded = await store.get("tk")
    assert reloaded.model.api_key_ref is None
    assert reloaded.im[0].token_ref is None
    await store.close()


@pytest.mark.asyncio
async def test_sql_tenant_store_secret_ref_aes_not_persisted(tmp_path):
    """secret_ref / aes_key_ref 同样不落库且可正常创建（曾因 SecretStr 无法 JSON 序列化崩溃）。"""
    from trpc_service.storage import SqlTenantStore
    from trpc_service.tenant import ImChannelConfig, TenantConfig

    dsn = f"sqlite+aiosqlite:///{tmp_path}/t4.db"
    engine = await create_sql_engine(dsn)
    store = SqlTenantStore(engine)
    config = TenantConfig(
        tenant_id="tw",
        name="带企微密钥",
        im=[
            ImChannelConfig(
                channel_type="wechat_work",
                app_id="corp1",
                agent_id="1000002",
                token_ref="wx-token-123456",
                secret_ref="wx-secret-abcdef",
                aes_key_ref="jWmYm7qr5nMoAUwZRjGtBxmz3KA1tkAj3ykkR6q2B2C",
            )
        ],
    )
    await store.create(config)  # 修复前此处 TypeError: SecretStr not JSON serializable
    reloaded = await store.get("tw")
    assert reloaded.im[0].token_ref is None
    assert reloaded.im[0].secret_ref is None
    assert reloaded.im[0].aes_key_ref is None
    await store.close()


@pytest.mark.asyncio
async def test_sql_tenant_store_rate_limit_zero(tmp_path):
    """rate_limit_per_min=0（不限流）重载后语义不丢。"""
    from trpc_service.storage import SqlTenantStore
    from trpc_service.tenant import TenantConfig

    dsn = f"sqlite+aiosqlite:///{tmp_path}/t3.db"
    engine = await create_sql_engine(dsn)
    store = SqlTenantStore(engine)
    await store.create(TenantConfig(tenant_id="t0", name="不限流", rate_limit_per_min=0))
    assert (await store.get("t0")).rate_limit_per_min == 0
    await store.close()


@pytest.mark.asyncio
async def test_sql_tenant_store_increment_usage(tmp_path):
    """used_budget_usd 原子累加（PRD 6-10）: 正增量生效、负增量/零忽略。"""
    from trpc_service.storage import SqlTenantStore
    from trpc_service.tenant import TenantConfig

    dsn = f"sqlite+aiosqlite:///{tmp_path}/usage.db"
    engine = await create_sql_engine(dsn)
    store = SqlTenantStore(engine)
    await store.create(TenantConfig(tenant_id="tu", name="预算户", used_budget_usd=10.0))

    await store.increment_usage("tu", 2.5)
    await store.increment_usage("tu", -5)  # 负增量忽略
    await store.increment_usage("tu", 0)  # 零忽略
    assert (await store.get("tu")).used_budget_usd == pytest.approx(12.5)

    # 不存在的租户: 静默 no-op 不抛错
    await store.increment_usage("nope", 1.0)
    assert await store.get("nope") is None
    await store.close()


@pytest.mark.asyncio
async def test_model_price_fields_survive_sql_roundtrip(tmp_path):
    """模型单价字段经 SQL JSON 列往返不丢（成本闭环的配置来源）。"""
    from trpc_service.storage import SqlTenantStore
    from trpc_service.tenant import ModelConfig, TenantConfig

    dsn = f"sqlite+aiosqlite:///{tmp_path}/price.db"
    engine = await create_sql_engine(dsn)
    store = SqlTenantStore(engine)
    await store.create(
        TenantConfig(
            tenant_id="tp",
            name="带价",
            model=ModelConfig(provider="deepseek",
                              model_name="deepseek-chat",
                              input_price_per_1m_usd=0.27,
                              output_price_per_1m_usd=1.10),
        ))
    loaded = await store.get("tp")
    assert loaded.model.input_price_per_1m_usd == 0.27
    assert loaded.model.output_price_per_1m_usd == 1.10
    await store.close()


@pytest.mark.asyncio
async def test_runtime_writes_audit_to_tenant_sql_backend(tmp_path):
    """Runtime 经 StorageManager 按租户后端写执行审计：audit=sql 落 SQLite。"""
    from trpc_service.events import AgentEvent
    from trpc_service.runtime import MockAgentRunner, Runtime
    from trpc_service.storage.manager import StorageManager
    from trpc_service.tenant import TenantConfig, TenantRegistry

    dsn = f"sqlite+aiosqlite:///{tmp_path}/runtime_audit.db"
    engine = await create_sql_engine(dsn)
    factory = StorageFactory(sql_engine=engine)
    manager = StorageManager(factory)

    cfg = TenantConfig(tenant_id="audit-sql",
                       name="审计户",
                       backends={
                           "session": "inmemory",
                           "memory": "inmemory",
                           "audit": "sql"
                       })

    async def load_fn(tid):
        return cfg if tid == "audit-sql" else None

    registry = TenantRegistry(load_fn=load_fn)
    storage = InMemoryStorage()
    rt = Runtime(registry=registry,
                 storage=storage,
                 runner=MockAgentRunner(),
                 storage_manager=manager,
                 execution_audit_enabled=True)

    await rt.handle(
        AgentEvent(tenant_id="audit-sql",
                   session_id="s1",
                   user_id="u1",
                   content="你好",
                   channel_type="web",
                   trace_id="trc-sql-audit"))

    # 执行审计应落在 SqlAuditStore（共享 SQLite），而非进程内 InMemory
    from trpc_service.storage.sql_store import SqlAuditStore

    sql_audit = SqlAuditStore(engine)
    logs = await sql_audit.query_logs("audit-sql", {"decision": "executed"})
    assert len(logs) == 1, "执行审计应真实写入 SQL 后端"
    assert logs[0]["trace_id"] == "trc-sql-audit"
    await sql_audit.close()
    await manager.close()


# ------------------------------------------------------------------
# 全项目审查回归（2026-09-04 OCR findings）
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_factory_session_sql_raises_not_silent():
    """session=sql 未实现时必须显式抛错，不得静默降级 InMemory（PRD 2.2 口径）。

    缺陷背景：此前 session=sql 静默走 InMemory 分支——多节点会话一致性
    会静默失效，违反 vector/s3 同款「显式抛错」决策。
    """
    factory = StorageFactory()
    cfg = DataBackendConfig(session="sql", audit="inmemory", summary="inmemory")
    with pytest.raises(NotImplementedError, match="session=sql"):
        await factory.create("t-sql", cfg)


@pytest.mark.asyncio
async def test_factory_memory_redis_with_session_inmemory():
    """{session: inmemory, memory: redis} 此前 UnboundLocalError 崩溃（审查 09-04）。"""
    import redis.asyncio as aioredis

    try:
        client = aioredis.Redis.from_url("redis://localhost:6379/0")
        await client.ping()
    except Exception:  # noqa: BLE001 - 探测用途
        pytest.skip("Redis 不可用（本机未启动 redis-server；Docker 镜像内自动验证）")

    from trpc_service.storage.redis_memory import RedisMemoryStore

    try:
        factory = StorageFactory(redis=client)
        cfg = DataBackendConfig(session="inmemory", memory="redis", audit="inmemory", summary="inmemory")
        storage = await factory.create("t-mixed", cfg)
        from trpc_service.storage.inmemory import InMemorySessionStore

        assert isinstance(storage.session, InMemorySessionStore), "session=inmemory 应落内存会话"
        assert isinstance(storage.memory, RedisMemoryStore), "memory=redis 应落 Redis 记忆"
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_tenant_history_persisted_across_instances(tmp_path):
    """C4 回归：配置历史落 SQL——跨 store 实例（模拟重启/多节点）可弹出，
    且环形上限截断到 keep 条。"""
    from trpc_service.storage import SqlTenantStore
    from trpc_service.tenant import TenantConfig

    dsn = f"sqlite+aiosqlite:///{tmp_path}/hist.db"
    engine = await create_sql_engine(dsn)
    store_a = SqlTenantStore(engine)
    store_b = SqlTenantStore(engine)

    for i in range(7):
        await store_a.push_history("t-hist", TenantConfig(tenant_id="t-hist", name=f"v{i}"), keep=5)

    # 环形截断：仅剩最近 5 份（v2..v6），弹出顺序 LIFO
    for expected in ("v6", "v5", "v4"):
        prev = await store_b.pop_latest_history("t-hist")
        assert prev is not None and prev.name == expected, f"跨实例弹出应为 {expected}"

    # 弹空后返回 None（rollback 404 语义）
    while await store_b.pop_latest_history("t-hist") is not None:
        pass
    assert await store_b.pop_latest_history("t-hist") is None

    # 密钥不落历史（PRD 4.5）
    from trpc_service.tenant import ModelConfig

    secret_cfg = TenantConfig(tenant_id="t-hist2", name="带密钥", model=ModelConfig(api_key_ref="sk-secret-1234567890"))
    await store_a.push_history("t-hist2", secret_cfg)
    popped = await store_b.pop_latest_history("t-hist2")
    assert popped is not None
    assert popped.model.api_key_ref is None, "历史快照不应包含明文密钥"
