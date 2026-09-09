# storage.migration 后端迁移单元测试（PRD 2.3-D）
import pytest

from trpc_service.storage.inmemory import InMemorySummaryStore
from trpc_service.storage.migration import copy_summaries, verify_summaries
from trpc_service.storage.sql_store import SqlSummaryStore, create_sql_engine

TENANT = "t-migrate"


async def _seed_inmemory(store, entries):
    for sid, content in entries:
        await store.save_summary(TENANT, sid, content)


@pytest.mark.asyncio
async def test_copy_inmemory_to_sql(tmp_path):
    src = InMemorySummaryStore()
    await _seed_inmemory(src, [("s1", "摘要一"), ("s2", "摘要二")])
    engine = await create_sql_engine(f"sqlite+aiosqlite:///{tmp_path}/mig.db")
    dst = SqlSummaryStore(engine)

    copied = await copy_summaries(src, dst, TENANT)
    assert copied == 2

    result = await verify_summaries(src, dst, TENANT)
    assert result["source"] == 2
    assert result["target"] == 2
    assert result["matched"] == 2
    assert result["mismatched"] == []

    # 目标端内容真实可读
    assert await dst.get_summary(TENANT, "s1") == "摘要一"
    await dst.close()


@pytest.mark.asyncio
async def test_verify_detects_missing_target(tmp_path):
    src = InMemorySummaryStore()
    await _seed_inmemory(src, [("s1", "摘要一"), ("s2", "摘要二")])
    engine = await create_sql_engine(f"sqlite+aiosqlite:///{tmp_path}/mig2.db")
    dst = SqlSummaryStore(engine)
    # 只迁一条 -> 校验应报差异
    await copy_summaries(src, dst, TENANT)  # 先全量
    await dst.delete_summary(TENANT, "s2")  # 模拟目标端丢一条

    result = await verify_summaries(src, dst, TENANT)
    assert result["mismatched"] == ["s2"]
    assert result["matched"] == 1
    await dst.close()


@pytest.mark.asyncio
async def test_copy_idempotent_rerun_converges(tmp_path):
    src = InMemorySummaryStore()
    await _seed_inmemory(src, [("s1", "摘要一"), ("s2", "摘要二"), ("s3", "摘要三")])
    engine = await create_sql_engine(f"sqlite+aiosqlite:///{tmp_path}/mig3.db")
    dst = SqlSummaryStore(engine)

    # 第一遍全量
    await copy_summaries(src, dst, TENANT)
    # 模拟源端更新一条内容，重跑后应收敛
    await src.save_summary(TENANT, "s2", "摘要二-v2")
    await copy_summaries(src, dst, TENANT)

    result = await verify_summaries(src, dst, TENANT)
    assert result["matched"] == 3
    assert result["mismatched"] == []
    assert await dst.get_summary(TENANT, "s2") == "摘要二-v2"
    await dst.close()


@pytest.mark.asyncio
async def test_copy_redis_to_sql_when_available(tmp_path):
    """Redis → SQLite（题面例子 Redis→SQL）。本机无 redis 自动跳过。"""
    try:
        import redis.asyncio as aioredis
        from trpc_service.storage.redis_store import RedisSummaryStore
    except ImportError:  # pragma: no cover
        pytest.skip("redis 不可用")
    client = aioredis.Redis.from_url("redis://localhost:6379/15")
    try:
        await client.ping()
    except Exception:  # noqa: BLE001 - 探测失败跳过
        pytest.skip("Redis 不可用（本机未启动 redis-server）")

    src = RedisSummaryStore(client)
    await client.flushdb()
    await src.save_summary(TENANT, "r1", "redis摘要一")
    await src.save_summary(TENANT, "r2", "redis摘要二")

    engine = await create_sql_engine(f"sqlite+aiosqlite:///{tmp_path}/mig_r.db")
    dst = SqlSummaryStore(engine)

    copied = await copy_summaries(src, dst, TENANT)
    assert copied == 2
    result = await verify_summaries(src, dst, TENANT)
    assert result["matched"] == 2
    assert result["mismatched"] == []

    await client.flushdb()
    await dst.close()
    await client.close()
