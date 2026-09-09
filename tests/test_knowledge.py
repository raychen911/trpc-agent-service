# ===================================================================
# Knowledge 域（RAG）测试: 存储 + 框架 ABC 适配 + 工具接线
# ===================================================================
# 说明: 验证「上传文档 -> 检索命中 -> 工具调用」链路（阶段三最小验证）。
#   检索为 InMemory 关键词占位（生产接向量库，接口不变）。
# ===================================================================

import pytest

from tests.fakes import FakeLLMModel
from trpc_service.storage.knowledge_inmemory import InMemoryKnowledgeStore
from trpc_service.tenant import TenantConfig


@pytest.mark.asyncio
async def test_knowledge_store_add_search_delete():
    store = InMemoryKnowledgeStore()
    await store.add_document("t1", "doc-1", [
        {
            "id": "c1",
            "content": "报销流程：提交飞书审批单，附发票，3 个工作日内到账。"
        },
        {
            "id": "c2",
            "content": "年假规则：入职满一年可休 5 天，未休自动作废。"
        },
    ])

    hits = await store.search("t1", "报销怎么走流程", top_k=3)
    assert hits, "应命中报销相关内容"
    assert hits[0]["doc_id"] == "doc-1"
    assert "报销流程" in hits[0]["content"]
    assert hits[0]["score"] > 0

    # 租户隔离: t2 查不到 t1 的文档
    assert await store.search("t2", "报销怎么走流程") == []

    # 删除后不再命中
    await store.delete_document("t1", "doc-1")
    assert await store.search("t1", "报销怎么走流程") == []


@pytest.mark.asyncio
async def test_platform_knowledge_base_search():
    """框架 KnowledgeBase ABC 适配: search 返回 SearchResult 命中列表。"""
    from trpc_agent_sdk.knowledge import KnowledgeBase, SearchParams, SearchRequest
    from trpc_agent_sdk.types import Part

    from trpc_service.storage.framework_adapter import PlatformKnowledgeBase

    store = InMemoryKnowledgeStore()
    await store.add_document("t1", "d1", [{"id": "c1", "content": "SLA：核心服务可用性 99.9%，故障 15 分钟内响应。"}])

    kb = PlatformKnowledgeBase(store, "t1")
    assert isinstance(kb, KnowledgeBase)
    req = SearchRequest(query=Part(text="SLA 是多少"), history=[], user_id="u1", session_id="s1", params=SearchParams())
    result = await kb.search(ctx=None, req=req)

    assert result.documents, "ABC 适配应返回命中"
    assert "99.9%" in result.documents[0].document
    assert result.documents[0].score > 0


def _build_with_knowledge(allowlist, knowledge=None):
    from trpc_service.agent.builder import build_framework_agent

    tenant = TenantConfig(tenant_id="t1", name="x", tools={"allowlist": allowlist})
    agent = build_framework_agent(tenant, model_factory=lambda _cfg: FakeLLMModel(), knowledge=knowledge)
    return {t.name: t for t in agent.tools}


@pytest.mark.asyncio
async def test_knowledge_search_tool_injected_when_allowed():
    """租户白名单含 knowledge_search 且配置知识库时，注入 RAG 工具。"""
    store = InMemoryKnowledgeStore()
    await store.add_document("t1", "d1", [{"id": "c1", "content": "打卡规则：工作日 9:00 前完成打卡。"}])

    tools = _build_with_knowledge(["echo", "knowledge_search"], knowledge=store)
    assert "knowledge_search" in tools

    # 参数声明应含 query
    decl = tools["knowledge_search"]._get_declaration()
    props = (decl.parameters.properties if decl.parameters else None) or {}
    assert "query" in props, f"knowledge_search 参数声明为空: {props}"

    # 经工具调用链路检索（模拟框架注入 tool_context）
    from types import SimpleNamespace

    ctx = SimpleNamespace(tenant_id="t1", user_id="u1", session_id="s1")
    out = await tools["knowledge_search"]._run_async_impl(tool_context=ctx, args={"query": "打卡时间"})
    assert out["hits"] == 1
    assert "打卡规则" in out["result"]


def test_knowledge_search_tool_not_injected_without_allowlist():
    """租户白名单不含 knowledge_search 时不注入（权限门控）。"""
    tools = _build_with_knowledge(["echo"], knowledge=InMemoryKnowledgeStore())
    assert "knowledge_search" not in tools


# ------------------------------------------------------------------
# Redis 后端（多节点共享，联调 2026-09-06 修复「知识库无数据」根因）
# ------------------------------------------------------------------


def _redis_available():
    import redis.asyncio as aioredis

    client = aioredis.Redis.from_url("redis://localhost:6379/15")
    return client


@pytest.mark.asyncio
async def test_redis_knowledge_store_roundtrip():
    """Redis 知识库: 写入 -> 检索命中 -> 租户隔离 -> 覆盖写幂等 -> 删除。"""
    import pytest as _pytest

    client = _redis_available()
    try:
        await client.ping()
    except Exception:  # noqa: BLE001 - 探测用途
        _pytest.skip("Redis 不可用（本机未启动 redis-server；Docker 镜像内自动验证）")

    from trpc_service.storage.knowledge_redis import RedisKnowledgeStore

    await client.flushdb()
    try:
        store = RedisKnowledgeStore(client)
        chunks = [
            {
                "id": "c1",
                "content": "报销流程：整理发票与行程单，OA 提交申请，5 个工作日内打款。"
            },
            {
                "id": "c2",
                "content": "年假规则：按入职年限每年 5 到 15 天。"
            },
        ]
        await store.add_document("kr1", "faq", chunks)

        hits = await store.search("kr1", "报销流程怎么走", top_k=3)
        assert hits, "应命中报销内容"
        assert hits[0]["doc_id"] == "faq"
        assert "发票" in hits[0]["content"]

        # 租户隔离
        assert await store.search("kr2", "报销流程怎么走") == []

        # 同 doc_id 重复录入为整体替换（幂等）
        await store.add_document("kr1", "faq", [{"id": "c9", "content": "报销流程更新：改为财务系统提交。"}])
        hits2 = await store.search("kr1", "报销流程", top_k=5)
        assert all("财务系统" not in h["content"] or "报销" in h["content"] for h in hits2)
        assert any(h["chunk_id"] == "c9" for h in hits2), "覆盖写后应检索到新切块"
        assert not any(h["chunk_id"] == "c1" for h in hits2), "旧切块不应残留"

        await store.delete_document("kr1", "faq")
        assert await store.search("kr1", "报销流程") == []
    finally:
        await client.flushdb()
        await client.aclose()


@pytest.mark.asyncio
async def test_factory_knowledge_redis_branch():
    """knowledge=redis 时工厂产出 RedisKnowledgeStore（多节点共享）。"""
    import redis.asyncio as aioredis

    try:
        client = aioredis.Redis.from_url("redis://localhost:6379/15")
        await client.ping()
    except Exception:  # noqa: BLE001 - 探测用途
        pytest.skip("Redis 不可用（本机未启动 redis-server；Docker 镜像内自动验证）")

    from trpc_service.storage import StorageFactory
    from trpc_service.storage.knowledge_redis import RedisKnowledgeStore
    from trpc_service.tenant.models import DataBackendConfig

    try:
        factory = StorageFactory(redis=client)
        cfg = DataBackendConfig(session="inmemory",
                                memory="inmemory",
                                audit="inmemory",
                                summary="inmemory",
                                knowledge="redis")
        storage = await factory.create("t-kredis", cfg)
        assert isinstance(storage.knowledge, RedisKnowledgeStore), "knowledge=redis 应落 Redis 知识库"
    finally:
        await client.aclose()


# ------------------------------------------------------------------
# 向量后端（C3：本地哈希 embedding + cosine top-k，多节点共享）
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_hash_vector_store_roundtrip():
    """向量知识库: 写入 -> cosine 检索命中 -> 租户隔离 -> 覆盖写幂等 -> 删除。"""
    import pytest as _pytest

    client = _redis_available()
    try:
        await client.ping()
    except Exception:  # noqa: BLE001 - 探测用途
        _pytest.skip("Redis 不可用（本机未启动 redis-server；Docker 镜像内自动验证）")

    from trpc_service.storage.knowledge_vector import HashVectorKnowledgeStore, _embed

    await client.flushdb()
    try:
        store = HashVectorKnowledgeStore(client)
        chunks = [
            {
                "id": "c1",
                "content": "报销流程：整理发票与行程单，OA 提交申请，5 个工作日内打款。"
            },
            {
                "id": "c2",
                "content": "年假规则：按入职年限每年 5 到 15 天。"
            },
        ]
        await store.add_document("kv1", "faq", chunks)

        hits = await store.search("kv1", "报销流程 发票", top_k=3)
        assert hits, "cosine 应命中报销内容"
        assert hits[0]["doc_id"] == "faq"
        assert "发票" in hits[0]["content"]

        # embedding 确定性：同文本同向量
        assert _embed("报销") == _embed("报销")

        # 零向量查询不召回（全无重叠内容时结果为空）
        assert await store.search("kv1", "！@#￥%……") == []

        # 租户隔离
        assert await store.search("kv2", "报销流程 发票") == []

        # 同 doc_id 重复录入为整体替换（幂等）
        await store.add_document("kv1", "faq", [{"id": "c9", "content": "报销流程更新：改为财务系统提交。"}])
        hits2 = await store.search("kv1", "报销流程", top_k=5)
        assert any(h["chunk_id"] == "c9" for h in hits2), "覆盖写后应检索到新切块"
        assert not any(h["chunk_id"] == "c1" for h in hits2), "旧切块不应残留"

        await store.delete_document("kv1", "faq")
        assert await store.search("kv1", "报销流程") == []
    finally:
        await client.flushdb()


@pytest.mark.asyncio
async def test_factory_knowledge_vector_branch():
    """factory: 配 redis 时 knowledge=vector 落 HashVectorKnowledgeStore。"""
    import pytest as _pytest

    client = _redis_available()
    try:
        await client.ping()
    except Exception:  # noqa: BLE001 - 探测用途
        _pytest.skip("Redis 不可用（本机未启动 redis-server；Docker 镜像内自动验证）")

    from trpc_service.storage.factory import StorageFactory
    from trpc_service.storage.knowledge_vector import HashVectorKnowledgeStore
    from trpc_service.tenant.models import DataBackendConfig

    await client.flushdb()
    try:
        factory = StorageFactory(redis=client)
        cfg = DataBackendConfig(session="inmemory",
                                memory="inmemory",
                                audit="inmemory",
                                summary="inmemory",
                                knowledge="vector")
        storage = await factory.create("t-kvec", cfg)
        assert isinstance(storage.knowledge, HashVectorKnowledgeStore), "knowledge=vector 应落向量知识库"
    finally:
        await client.flushdb()
        await client.aclose()
