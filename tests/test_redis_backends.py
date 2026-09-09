# ===================================================================
# tests.test_redis_backends - Redis 后端集成测试
# ===================================================================
# 说明: 覆盖此前 0% 覆盖的共享后端路径（多节点部署的核心主张）：
#   - knowledge_redis.RedisKnowledgeStore（共享 RAG，租户隔离）
#   - redis_memory.RedisMemoryStore（Memory list + LTRIM 上限 + TTL）
#   - redis_store 全家族（Session/Summary/Idempotency/DistributedLock）
#   - tenant.broadcaster.ConfigBroadcaster（pub/sub 跨进程失效广播）
# 约定: 与 test_storage.py 同口径——连接真实本机 Redis（db15 隔离，
#   避免污染运行中的网关 db0），不可用则整文件 skip。
# ===================================================================

from __future__ import annotations

import asyncio
import uuid

import pytest
import redis.asyncio as aioredis

TEST_REDIS_URL = "redis://localhost:6379/15"


def _redis_available() -> bool:
    import redis

    try:
        client = redis.Redis.from_url(TEST_REDIS_URL, socket_connect_timeout=0.5)
        client.ping()
        client.close()
        return True
    except Exception:  # noqa: BLE001 - 探测用途
        return False


pytestmark = pytest.mark.skipif(not _redis_available(), reason="Redis 不可用（本机未启动 redis-server）")


@pytest.fixture()
async def redis_client():
    client = aioredis.Redis.from_url(TEST_REDIS_URL)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.aclose()


def _tid() -> str:
    """每个用例独立租户前缀，天然隔离。"""
    return f"t-{uuid.uuid4().hex[:8]}"


# ------------------------------------------------------------------
# RedisKnowledgeStore
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_knowledge_add_search_roundtrip(redis_client):
    from trpc_service.storage.knowledge_redis import RedisKnowledgeStore

    store = RedisKnowledgeStore(redis_client)
    tid = _tid()
    await store.add_document(tid, "faq", [
        {
            "content": "如何重置密码：在设置页点击重置密码按钮"
        },
        {
            "id": "chunk-2",
            "content": "如何绑定手机号：在账号安全页绑定",
            "metadata": {
                "page": 2
            }
        },
    ])

    hits = await store.search(tid, "怎么重置密码", top_k=5)
    assert hits, "关键词重叠应命中"
    assert hits[0]["doc_id"] == "faq"
    assert hits[0]["chunk_id"] == "chunk-2" or hits[0]["chunk_id"].startswith("faq:")
    assert hits[0]["score"] > 0


@pytest.mark.asyncio
async def test_knowledge_tenant_isolation(redis_client):
    """租户 A 录入的文档，租户 B 不可检索（PRD 4.5 数据隔离）。"""
    from trpc_service.storage.knowledge_redis import RedisKnowledgeStore

    store = RedisKnowledgeStore(redis_client)
    ta, tb = _tid(), _tid()
    await store.add_document(ta, "doc", [{"content": "机密内容只有A可见"}])

    assert await store.search(ta, "机密内容", top_k=5)
    assert not await store.search(tb, "机密内容", top_k=5)


@pytest.mark.asyncio
async def test_knowledge_re_add_is_idempotent_and_delete(redis_client):
    """重复 add 同一 doc_id 应整文档替换而非追加；delete 后检索为空。"""
    from trpc_service.storage.knowledge_redis import RedisKnowledgeStore

    store = RedisKnowledgeStore(redis_client)
    tid = _tid()
    await store.add_document(tid, "doc", [{"content": "legacy content alpha"}])
    await store.add_document(tid, "doc", [{"content": "fresh content bravo"}])

    # 分词按单字/ASCII 词，alpha 与 bravo 词集不相交——旧切块必须不可再检索
    assert not await store.search(tid, "legacy alpha", top_k=5), "旧切块应被替换"
    assert await store.search(tid, "fresh bravo", top_k=5)
    # 整文档仅剩 1 个新切块（先删后写，非追加）
    assert await redis_client.hlen(store._key(tid, "doc")) == 1

    await store.delete_document(tid, "doc")
    assert not await store.search(tid, "fresh bravo", top_k=5)


@pytest.mark.asyncio
async def test_knowledge_empty_query_returns_empty(redis_client):
    from trpc_service.storage.knowledge_redis import RedisKnowledgeStore

    store = RedisKnowledgeStore(redis_client)
    await store.add_document(_tid(), "doc", [{"content": "内容"}])
    assert await store.search(_tid(), "   ", top_k=5) == []


# ------------------------------------------------------------------
# RedisMemoryStore
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_memory_add_and_search_by_keyword(redis_client):
    from trpc_service.storage.redis_memory import RedisMemoryStore

    store = RedisMemoryStore(redis_client)
    tid, uid = _tid(), "u1"
    await store.add_memory(tid, uid, {"content": "用户喜欢喝美式咖啡"})
    await store.add_memory(tid, uid, {"content": "用户养了一只橘猫"})

    hits = await store.search_memory(tid, uid, "咖啡", top_k=5)
    assert hits and "美式咖啡" in hits[0]["content"]
    assert hits[0]["memory_id"], "应自动补 memory_id"
    assert hits[0]["created_at"] > 0, "应自动补 created_at"


@pytest.mark.asyncio
async def test_memory_empty_query_returns_recent(redis_client):
    from trpc_service.storage.redis_memory import RedisMemoryStore

    store = RedisMemoryStore(redis_client)
    tid, uid = _tid(), "u1"
    for i in range(3):
        await store.add_memory(tid, uid, {"content": f"记忆{i}"})

    hits = await store.search_memory(tid, uid, "", top_k=2)
    assert [h["content"] for h in hits] == ["记忆1", "记忆2"], "空查询返回最近 top_k 条"


@pytest.mark.asyncio
async def test_memory_ltrim_cap(redis_client):
    """审查 09-04：活跃用户 list 无界增长应被 LTRIM 裁剪到上限。"""
    from trpc_service.storage.redis_memory import _MAX_MEMORIES, RedisMemoryStore

    store = RedisMemoryStore(redis_client)
    tid, uid = _tid(), "bulk"
    for i in range(_MAX_MEMORIES + 10):
        await store.add_memory(tid, uid, {"content": f"m{i}"})

    length = await redis_client.llen(store._key(tid, uid))
    assert length == _MAX_MEMORIES, f"应裁剪到 {_MAX_MEMORIES}"
    # 裁剪保留的是最近的（列表尾部）
    hits = await store.search_memory(tid, uid, "", top_k=1)
    assert hits[0]["content"] == f"m{_MAX_MEMORIES + 9}"


@pytest.mark.asyncio
async def test_memory_ttl_applied(redis_client):
    from trpc_service.storage.redis_memory import RedisMemoryStore

    store = RedisMemoryStore(redis_client, ttl_seconds=120)
    tid, uid = _tid(), "ttl"
    await store.add_memory(tid, uid, {"content": "带过期"})
    ttl = await redis_client.ttl(store._key(tid, uid))
    assert 0 < ttl <= 120, "配置 TTL 后 key 应带过期时间"


# ------------------------------------------------------------------
# RedisSessionStore / RedisSummaryStore
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_session_roundtrip_and_extra_fields(redis_client):
    from trpc_service.storage.redis_store import RedisSessionStore

    store = RedisSessionStore(redis_client)
    tid, sid = _tid(), "s1"
    session = {
        "session_id": sid,
        "state": {
            "page": 2
        },
        "events": [{
            "role": "user",
            "text": "你好"
        }],
        "channel": "wecom",
        "version": 0,
    }
    await store.save_session(tid, session)

    loaded = await store.get_session(tid, sid)
    assert loaded["state"] == {"page": 2}
    assert loaded["events"] == [{"role": "user", "text": "你好"}]
    assert loaded["channel"] == "wecom", "非结构化字段应原样透传"
    assert loaded["version"] == 0

    assert await store.get_session(tid, "missing") is None


@pytest.mark.asyncio
async def test_session_update_state_increments_version(redis_client):
    from trpc_service.storage.redis_store import RedisSessionStore

    store = RedisSessionStore(redis_client)
    tid, sid = _tid(), "s1"

    s1 = await store.update_state(tid, sid, {"k": "v1"})
    assert s1["version"] == 1
    s2 = await store.update_state(tid, sid, {"k": "v2"})
    assert s2["version"] == 2, "乐观锁版本号应递增（PRD 2.3-A）"
    assert (await store.get_session(tid, sid))["state"] == {"k": "v2"}

    await store.delete_session(tid, sid)
    assert await store.get_session(tid, sid) is None


@pytest.mark.asyncio
async def test_summary_roundtrip_and_list(redis_client):
    from trpc_service.storage.redis_store import RedisSummaryStore

    store = RedisSummaryStore(redis_client)
    tid = _tid()
    await store.save_summary(tid, "s1", "摘要一")
    await store.save_summary(tid, "s2", "摘要二")

    assert await store.get_summary(tid, "s1") == "摘要一"
    assert await store.get_summary(tid, "missing") is None

    listed = dict(await store.list_summaries(tid))
    assert listed == {"s1": "摘要一", "s2": "摘要二"}

    await store.delete_summary(tid, "s1")
    assert await store.get_summary(tid, "s1") is None


# ------------------------------------------------------------------
# RedisIdempotencyStore / RedisDistributedLock
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_idempotency_nx_semantics(redis_client):
    """PRD 2.3-E：首次获取成功，重复获取失败，release 后可重新获取。"""
    from trpc_service.storage.redis_store import RedisIdempotencyStore

    store = RedisIdempotencyStore(redis_client)
    key = f"idem:{_tid()}"

    assert await store.try_acquire(key, ttl_seconds=60) is True
    assert await store.try_acquire(key, ttl_seconds=60) is False, "同 key 二次获取必须失败"

    await store.release(key)
    assert await store.try_acquire(key, ttl_seconds=60) is True


@pytest.mark.asyncio
async def test_lock_mutual_exclusion_and_release(redis_client):
    """PRD 2.3-A：持有期间他人不可获取；仅持有者能释放。"""
    from trpc_service.storage.redis_store import RedisDistributedLock

    lock_a = RedisDistributedLock(redis_client)
    lock_b = RedisDistributedLock(redis_client)
    key = f"lock:test:{_tid()}"

    assert await lock_a.acquire(key, timeout=10) is True
    assert await lock_b.acquire(key, timeout=10) is False, "持锁期间他人获取必须失败"

    # lock_b 未持有该锁，release 应为无操作（不能误删 lock_a 的锁）
    await lock_b.release(key)
    assert await redis_client.get(key) is not None, "非持有者释放不得删锁"

    await lock_a.release(key)
    assert await redis_client.get(key) is None
    assert await lock_b.acquire(key, timeout=10) is True, "释放后他人可获取"


@pytest.mark.asyncio
async def test_lock_release_without_token_is_noop(redis_client):
    from trpc_service.storage.redis_store import RedisDistributedLock

    lock = RedisDistributedLock(redis_client)
    key = f"lock:noop:{_tid()}"
    await lock.release(key)  # 从未 acquire，应静默无操作
    assert await redis_client.get(key) is None


# ------------------------------------------------------------------
# ConfigBroadcaster（Redis pub/sub 跨进程失效广播）
# ------------------------------------------------------------------


class _Recorder:
    """记录 invalidate 调用的替身（registry / storage_manager / channel_factory）。"""

    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: list[str] = []

    def invalidate(self, tenant_id: str) -> None:
        self.calls.append(tenant_id)


@pytest.mark.asyncio
async def test_broadcaster_pubsub_invalidates_all_caches(redis_client):
    """Admin 发布 → 订阅端失效 registry / storage / channel 三级缓存。"""
    from trpc_service.tenant.broadcaster import ConfigBroadcaster

    registry, storage_mgr, channel_factory = _Recorder("registry"), _Recorder("storage"), _Recorder("channel")
    broadcaster = ConfigBroadcaster(redis_client)

    stop = asyncio.Event()
    ready = asyncio.Event()
    loop_task = asyncio.create_task(
        broadcaster.subscribe_loop(registry,
                                   stop,
                                   ready=ready,
                                   storage_manager=storage_mgr,
                                   channel_factory=channel_factory))
    try:
        await asyncio.wait_for(ready.wait(), timeout=3), "订阅建立后 ready 才置位"
        await broadcaster.publish_invalidated("tenant-x")
        # 轮询间隔 0.5s，留出处理窗口
        for _ in range(20):
            if registry.calls:
                break
            await asyncio.sleep(0.1)
        assert registry.calls == ["tenant-x"]
        assert storage_mgr.calls == ["tenant-x"]
        assert channel_factory.calls == ["tenant-x"], "C6：热更新须一并失效通道适配器缓存"
    finally:
        stop.set()
        await asyncio.wait_for(loop_task, timeout=5)


@pytest.mark.asyncio
async def test_broadcaster_publish_failure_swallowed():
    """Redis 不可用时 publish 静默降级仅告警，不得抛错阻塞配置写入。"""
    from trpc_service.tenant.broadcaster import ConfigBroadcaster

    dead = aioredis.Redis(host="127.0.0.1", port=6390, socket_connect_timeout=0.2, socket_timeout=0.2)
    try:
        broadcaster = ConfigBroadcaster(dead)
        await broadcaster.publish_invalidated("t-x")  # 不应抛出
    finally:
        try:
            await dead.aclose()
        except Exception:  # noqa: BLE001 - 退出清理尽力而为
            pass


@pytest.mark.asyncio
async def test_broadcaster_decode_variants():
    from trpc_service.tenant.broadcaster import ConfigBroadcaster

    assert ConfigBroadcaster._decode(None) is None
    assert ConfigBroadcaster._decode(b"tenant-1") == "tenant-1"
    assert ConfigBroadcaster._decode("  tenant-2  ") == "tenant-2"
    assert ConfigBroadcaster._decode("   ") is None
    assert ConfigBroadcaster._decode(b"") is None
