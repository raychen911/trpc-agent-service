"""Real Redis, PostgreSQL, Qdrant, and MinIO acceptance probe for Compose."""

from __future__ import annotations

import asyncio
import json
import os
import time
import uuid

from minio import Minio
from qdrant_client import QdrantClient
from qdrant_client.http import models
from redis import Redis
from sqlalchemy import text
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.memory import RedisMemoryService
from trpc_agent_sdk.memory import SqlMemoryService
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.sessions import RedisSessionService
from trpc_agent_sdk.sessions import SqlSessionService
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import EventActions
from trpc_agent_sdk.types import Part

from trpc_service.persistence import Database
from trpc_service.migration import VectorMigration
from trpc_service.migration import copy_and_verify_trpc_session
from trpc_service.reliability import SessionCoordinator
from trpc_service.storage import LocalVectorKnowledgeStore
from trpc_service.storage import QdrantKnowledgeStore
from trpc_service.storage import S3ArtifactService


def percentiles(samples: list[float]) -> dict[str, float]:
    ordered = sorted(samples)

    def value(fraction: float) -> float:
        index = min(len(ordered) - 1, max(0, int(len(ordered) * fraction + 0.999999) - 1))
        return round(ordered[index], 3)

    return {"count": len(ordered), "p50_ms": value(0.50), "p95_ms": value(0.95), "p99_ms": value(0.99)}


def measure_capacity(database: Database, redis: Redis, samples: int = 100) -> dict[str, dict[str, float]]:
    redis_latency = []
    sql_latency = []
    for _ in range(samples):
        started = time.perf_counter()
        assert redis.ping()
        redis_latency.append((time.perf_counter() - started) * 1000)

        started = time.perf_counter()
        with database.sessions() as session:
            assert session.execute(text("SELECT 1")).scalar_one() == 1
        sql_latency.append((time.perf_counter() - started) * 1000)
    return {"redis_ping": percentiles(redis_latency), "sql_select": percentiles(sql_latency)}


async def verify_sessions(redis_url: str, sql_url: str) -> None:
    suffix = uuid.uuid4().hex
    event = Event(
        id=f"compose-event-{suffix}",
        author="agent",
        content=Content(parts=[Part.from_text(text="persisted")]),
        actions=EventActions(state_delta={"turn": 1}),
    )
    services = [
        InMemorySessionService(),
        RedisSessionService(db_url=redis_url),
        SqlSessionService(db_url=sql_url),
    ]
    snapshots = []
    for index, service in enumerate(services):
        session = await service.create_session(
            app_name="tenant_compose:support",
            user_id="user-compose",
            session_id=f"session-{suffix}-{index}",
        )
        await service.append_event(session, event.model_copy(deep=True))
        loaded = await service.get_session(
            app_name="tenant_compose:support",
            user_id="user-compose",
            session_id=f"session-{suffix}-{index}",
        )
        assert loaded is not None
        snapshots.append((loaded.events[-1].id, loaded.events[-1].content.parts[0].text, loaded.state["turn"]))
        await service.close()
    assert snapshots == [(event.id, "persisted", 1)] * 3


async def verify_shared_memory(redis_url: str, sql_url: str) -> None:
    """A second service instance must see memory while another tenant remains isolated."""

    suffix = uuid.uuid4().hex
    session_service = InMemorySessionService()
    session = await session_service.create_session(
        app_name=f"tenant_compose:support:{suffix}",
        user_id="user-compose",
        session_id=f"memory-source-{suffix}",
    )
    await session_service.append_event(
        session,
        Event(
            id=f"memory-event-{suffix}",
            author="agent",
            content=Content(parts=[Part.from_text(text=f"durable-memory-{suffix}")]),
        ),
    )

    pairs = [
        (
            RedisMemoryService(db_url=redis_url, enabled=True, is_async=True),
            RedisMemoryService(db_url=redis_url, enabled=True, is_async=True),
        ),
        (
            SqlMemoryService(db_url=sql_url, enabled=True, is_async=True),
            SqlMemoryService(db_url=sql_url, enabled=True, is_async=True),
        ),
    ]
    for writer, reader in pairs:
        await writer.store_session(session)
        visible = await reader.search_memory(session.save_key, f"durable-memory-{suffix}")
        isolated = await reader.search_memory(f"other_tenant:{session.save_key}", f"durable-memory-{suffix}")
        assert len(visible.memories) == 1
        assert not isolated.memories
        await writer.close()
        await reader.close()
    await session_service.close()


async def verify_redis_to_sql_migration(redis_url: str, sql_url: str) -> None:
    suffix = uuid.uuid4().hex
    source = RedisSessionService(db_url=redis_url)
    target = SqlSessionService(db_url=sql_url)
    session = await source.create_session(
        app_name=f"tenant_compose:migration:{suffix}",
        user_id="user-compose",
        session_id=f"migration-{suffix}",
    )
    for index in range(2):
        await source.append_event(
            session,
            Event(
                id=f"migration-event-{suffix}-{index}",
                author="agent",
                actions=EventActions(state_delta={"turn": index + 1}),
                content=Content(parts=[Part.from_text(text=f"turn-{index + 1}")]),
            ),
        )
        migrated = await copy_and_verify_trpc_session(
            source,
            target, target_writes_paused=True,
            app_name=session.app_name,
            user_id=session.user_id,
            session_id=session.id,
        )
        assert migrated.revision == index + 1
    await source.close()
    await target.close()


async def main() -> None:
    database_url = os.environ["TRPC_DATABASE_URL"]
    session_database_url = os.environ["TRPC_SESSION_DATABASE_URL"]
    redis_url = os.environ["TRPC_REDIS_URL"]
    database = Database(database_url)
    with database.sessions() as session:
        assert session.execute(text("SELECT 1")).scalar_one() == 1
    redis = Redis.from_url(redis_url)
    assert redis.ping()
    await verify_sessions(redis_url, session_database_url)
    await verify_shared_memory(redis_url, session_database_url)
    await verify_redis_to_sql_migration(redis_url, session_database_url)

    coordinator = SessionCoordinator(database)
    lease = await coordinator.acquire("tenant_compose", "support", "user", "session", "worker-1", 30)
    assert await coordinator.commit(lease, expected_revision=0, event_id="event-1", event_payload={"ok": True}) == 1

    collection = f"compose_{uuid.uuid4().hex}"
    qdrant = QdrantClient(url=os.environ["TRPC_QDRANT_URL"])
    qdrant.create_collection(collection, vectors_config=models.VectorParams(size=2, distance=models.Distance.COSINE))
    vectors = QdrantKnowledgeStore(qdrant, collection)
    local_vectors = LocalVectorKnowledgeStore()
    local_vectors.upsert("tenant_compose", "kb", 1, "doc", str(uuid.uuid4()), [1.0, 0.0], "found")
    local_vectors.upsert("tenant_compose", "kb", 1, "deleted", str(uuid.uuid4()), [0.0, 1.0], "gone")
    local_vectors.delete_document("tenant_compose", "kb", 1, "deleted")
    vector_migration = VectorMigration(
        tenant_id="tenant_compose",
        knowledge_base_id="kb",
        source_index_version=1,
        target_index_version=2,
        source=local_vectors,
        target=vectors,
        source_embedding=("compose", 2),
        target_embedding=("compose", 2),
    )
    report = vector_migration.copy_and_verify()
    assert report.copied == 1 and report.tombstones == 1
    vector_migration.cutover()
    assert vectors.search("tenant_compose", "kb", 2, [1.0, 0.0])[0].text == "found"
    vector_migration.rollback()
    assert vector_migration.active_index_version == 1

    minio = Minio(
        os.environ["TRPC_MINIO_ENDPOINT"],
        access_key=os.environ["TRPC_MINIO_ACCESS_KEY"],
        secret_key=os.environ["TRPC_MINIO_SECRET_KEY"],
        secure=False,
    )
    bucket = f"artifacts-{uuid.uuid4().hex[:12]}"
    artifacts = S3ArtifactService(tenant_id="tenant_compose", bucket=bucket, client=minio, database=database)
    from trpc_agent_sdk.abc import ArtifactId

    artifact_id = ArtifactId(
        app_name="tenant_compose:support",
        user_id="user",
        session_id="session",
        filename="proof.txt",
    )
    await artifacts.save_artifact(artifact_id=artifact_id, artifact=Part.from_text(text="object-ok"), metadata={})
    assert (await artifacts.load_artifact(artifact_id=artifact_id)).data.text == "object-ok"
    print(json.dumps({
        "postgres": "ok",
        "redis": "ok",
        "qdrant": "ok",
        "minio": "ok",
        "workers": 2,
        "capacity": measure_capacity(database, redis),
    }))
    database.close()


if __name__ == "__main__":
    asyncio.run(main())
