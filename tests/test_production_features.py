import asyncio
import os

import fakeredis.aioredis
import httpx
from sqlalchemy import select

from trpc_service.channels.delivery import split_long_text
from trpc_service.config import CompositeSecretResolver, EnvironmentSecretResolver
from trpc_service.storage import Database
from trpc_service.storage.contracts import VectorDocument
from trpc_service.storage.migration import (
    RedisToSqlSessionMigrator,
    SqlMemoryToVectorMigrator,
)
from trpc_service.storage.models import AgentApp, AgentSession, Memory, Tenant
from trpc_service.storage.vector import HashingEmbedder, InMemoryVectorStore, QdrantVectorStore
from trpc_service.web.auth import AdminPrincipal


def test_admin_principal_rbac_is_tenant_scoped() -> None:
    platform = AdminPrincipal("root", frozenset({"platform-admin"}), frozenset())
    tenant = AdminPrincipal("alice", frozenset({"tenant-admin"}), frozenset({"t1"}))
    auditor = AdminPrincipal("audit", frozenset({"auditor"}), frozenset({"t1"}))
    assert platform.allows(write=True, tenant_id=None)
    assert tenant.allows(write=True, tenant_id="t1")
    assert not tenant.allows(write=True, tenant_id="t2")
    assert auditor.allows(write=False, tenant_id="t1")
    assert not auditor.allows(write=True, tenant_id="t1")


def test_composite_environment_secret_and_long_message_split() -> None:
    async def scenario() -> None:
        os.environ["TRPC_TEST_SECRET"] = "resolved"
        resolver = CompositeSecretResolver(EnvironmentSecretResolver())
        assert await resolver.resolve("env://TRPC_TEST_SECRET") == "resolved"

    asyncio.run(scenario())
    chunks = split_long_text("first paragraph\n" + "x" * 20, 12)
    actual = "".join(chunks).replace(" ", "").replace("\n", "")
    expected = ("first paragraph\n" + "x" * 20).replace(" ", "").replace("\n", "")
    assert actual == expected
    assert all(len(item) <= 12 for item in chunks)


def test_qdrant_vector_store_uses_namespace_filter() -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if request.method == "GET":
            return httpx.Response(200, json={"result": {"status": "green"}})
        if request.url.path.endswith("/points/search"):
            return httpx.Response(
                200,
                json={
                    "result": [
                        {
                            "score": 0.9,
                            "payload": {
                                "namespace": "tenant/t1",
                                "document_id": "doc-1",
                                "text": "hello",
                                "metadata": {"source": "test"},
                            },
                        }
                    ]
                },
            )
        return httpx.Response(200, json={"result": {"status": "ok"}})

    async def scenario() -> None:
        client = httpx.AsyncClient(
            transport=httpx.MockTransport(handler), base_url="http://qdrant"
        )
        store = QdrantVectorStore(
            "http://qdrant", embedder=HashingEmbedder(8), client=client
        )
        await store.upsert([VectorDocument("doc-1", "tenant/t1", "hello")])
        matches = await store.search("tenant/t1", "hello")
        await store.delete("tenant/t1", ["doc-1"])
        assert matches[0].document.id == "doc-1"
        await client.aclose()

    asyncio.run(scenario())
    search = next(item for item in requests if item.url.path.endswith("/points/search"))
    assert b'"namespace"' in search.content and b'"tenant/t1"' in search.content


def test_redis_session_migration_preserves_version() -> None:
    database = Database("sqlite+pysqlite:///:memory:")
    database.create_schema()
    with database.session_factory.begin() as session:
        session.add(Tenant(id="t1", slug="migrate", name="Migrate", key_namespace="t1"))
        session.flush()
        session.add(AgentApp(id="a1", tenant_id="t1", slug="app", name="App"))

    async def scenario() -> None:
        redis = fakeredis.aioredis.FakeRedis()
        await redis.set(
            "trpc:hot:session",
            '{"tenant_id":"t1","agent_app_id":"a1","user_id":"u1",'
            '"session_id":"s1","state":{"step":2},"version":7}',
        )
        report = await RedisToSqlSessionMigrator(redis, database.session_factory).run()
        assert report.migrated == 1 and report.failed == 0
        await redis.aclose()

    try:
        asyncio.run(scenario())
        with database.session_factory() as session:
            row = session.scalar(select(AgentSession))
            assert row is not None and row.version == 7 and row.state == {"step": 2}
    finally:
        database.dispose()


def test_sql_memory_can_rebuild_remote_vector_index() -> None:
    database = Database("sqlite+pysqlite:///:memory:")
    database.create_schema()
    with database.session_factory.begin() as session:
        session.add(Tenant(id="t1", slug="vector-migrate", name="T1", key_namespace="t1"))
        session.flush()
        session.add(AgentApp(id="a1", tenant_id="t1", slug="app", name="App"))
        session.flush()
        session.add(
            Memory(
                id="memory-1",
                tenant_id="t1",
                agent_app_id="a1",
                user_id="u1",
                memory_key="preference",
                content="用户喜欢蓝色",
                topics=["preference"],
                version=3,
            )
        )
    destination = InMemoryVectorStore()

    async def scenario() -> None:
        report = await SqlMemoryToVectorMigrator(
            database.session_factory, destination, batch_size=1
        ).run(tenant_id="t1")
        assert report.scanned == 1 and report.migrated == 1 and report.failed == 0
        matches = await destination.search("tenant/t1/app/a1/memory/u1", "蓝色")
        assert matches[0].document.id == "preference"
        assert matches[0].document.metadata["version"] == 3

    try:
        asyncio.run(scenario())
    finally:
        database.dispose()
