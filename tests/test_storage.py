"""SQLite schema, tenant isolation, idempotency and optimistic-lock tests."""

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import inspect

from trpc_service.config.models import (
    AgentAppRecord,
    AuditLogRecord,
    ChannelBindingRecord,
    ChannelType,
    InboundMessageRecord,
    KnowledgeRecord,
    MemoryRecord,
    SessionEventRecord,
    SessionRecord,
    SummaryRecord,
    TenantRecord,
)
from trpc_service.storage.artifact import ArtifactNotFoundError, LocalArtifactStore
from trpc_service.storage.database import Database
from trpc_service.storage.repositories import (
    AgentAppRepository,
    AuditLogRepository,
    ChannelBindingRepository,
    InboundMessageRepository,
    KnowledgeRepository,
    MemoryRepository,
    SessionEventRepository,
    SessionRepository,
    SummaryRepository,
    TenantRepository,
)


@pytest.fixture
async def database():
    db = Database("sqlite+aiosqlite:///:memory:")
    await db.initialize()
    try:
        yield db
    finally:
        await db.dispose()


async def seed_tenant(database: Database, tenant_id: str = "tenant-a") -> None:
    await TenantRepository(database).create(TenantRecord(tenant_id=tenant_id, name=tenant_id))
    await AgentAppRepository(database).create(
        AgentAppRecord(
            tenant_id=tenant_id,
            app_id="assistant",
            name="Assistant",
            system_prompt="Be helpful.",
            model_config_data={"provider": "openai", "model": "company-model"},
            tool_policy={"allow": ["calculator"]},
        )
    )
    await ChannelBindingRepository(database).create(
        ChannelBindingRecord(
            tenant_id=tenant_id,
            binding_id="http-main",
            channel_type=ChannelType.HTTP,
            account_id=f"{tenant_id}-account",
        )
    )


def remove_sqlite_files(database_path: Path) -> None:
    for suffix in ("", "-shm", "-wal"):
        candidate = Path(f"{database_path}{suffix}")
        if candidate.exists():
            candidate.unlink()


@pytest.mark.asyncio
async def test_schema_contains_nine_required_tables(database: Database) -> None:
    async with database.engine.connect() as connection:
        table_names = await connection.run_sync(lambda sync: inspect(sync).get_table_names())
    assert {
        "tenant",
        "agent_app",
        "channel_binding",
        "inbound_message",
        "session",
        "session_event",
        "memory",
        "summary",
        "audit_log",
        "artifact",
        "execution_outbox",
        "knowledge",
    }.issubset(set(table_names))


@pytest.mark.asyncio
async def test_file_database_persists_after_engine_restart() -> None:
    database_path = Path("data") / f"test-persistence-{uuid4().hex}.db"
    database_url = f"sqlite+aiosqlite:///{database_path.as_posix()}"
    first = Database(database_url)
    try:
        await first.initialize()
        await TenantRepository(first).create(TenantRecord(tenant_id="persistent", name="Saved"))
    finally:
        await first.dispose()

    second = Database(database_url)
    try:
        await second.initialize()
        stored = await TenantRepository(second).get("persistent")
        assert stored is not None and stored.name == "Saved"
    finally:
        await second.dispose()
        await asyncio.to_thread(remove_sqlite_files, database_path)


@pytest.mark.asyncio
async def test_tenant_app_binding_and_session_are_tenant_scoped(database: Database) -> None:
    await seed_tenant(database, "tenant-a")
    await seed_tenant(database, "tenant-b")

    sessions = SessionRepository(database)
    await sessions.create(
        SessionRecord(
            tenant_id="tenant-a",
            session_id="same-session",
            app_id="assistant",
            principal_id="same-user",
            channel_type=ChannelType.HTTP,
            state={"owner": "a"},
        )
    )
    await sessions.create(
        SessionRecord(
            tenant_id="tenant-b",
            session_id="same-session",
            app_id="assistant",
            principal_id="same-user",
            channel_type=ChannelType.HTTP,
            state={"owner": "b"},
        )
    )

    tenant_a = await sessions.get("tenant-a", "same-session")
    tenant_b = await sessions.get("tenant-b", "same-session")
    assert tenant_a is not None and tenant_a.state == {"owner": "a"}
    assert tenant_b is not None and tenant_b.state == {"owner": "b"}


@pytest.mark.asyncio
async def test_inbound_message_registration_is_idempotent(database: Database) -> None:
    await seed_tenant(database)
    repository = InboundMessageRepository(database)
    first_record = InboundMessageRecord(
        inbound_id="inbound-1",
        tenant_id="tenant-a",
        binding_id="http-main",
        external_message_id="provider-message-1",
        session_id="session-1",
        trace_id="trace-1",
    )
    duplicate_record = first_record.model_copy(update={"inbound_id": "inbound-2"})

    stored, created = await repository.register(first_record)
    duplicate, duplicate_created = await repository.register(duplicate_record)

    assert created is True
    assert duplicate_created is False
    assert duplicate.inbound_id == stored.inbound_id == "inbound-1"


@pytest.mark.asyncio
async def test_session_update_uses_optimistic_version(database: Database) -> None:
    await seed_tenant(database)
    repository = SessionRepository(database)
    await repository.create(
        SessionRecord(
            tenant_id="tenant-a",
            session_id="session-1",
            app_id="assistant",
            principal_id="user-1",
            channel_type=ChannelType.HTTP,
        )
    )

    first = await repository.update_state("tenant-a", "session-1", 0, {"turn": 1})
    stale = await repository.update_state("tenant-a", "session-1", 0, {"turn": 2})
    stored = await repository.get("tenant-a", "session-1")

    assert first is True
    assert stale is False
    assert stored is not None
    assert stored.version == 1
    assert stored.state == {"turn": 1}


@pytest.mark.asyncio
async def test_event_memory_summary_and_audit_persist(database: Database) -> None:
    await seed_tenant(database)
    await SessionRepository(database).create(
        SessionRecord(
            tenant_id="tenant-a",
            session_id="session-1",
            app_id="assistant",
            principal_id="user-1",
            channel_type=ChannelType.HTTP,
        )
    )

    event = await SessionEventRepository(database).append(
        SessionEventRecord(
            event_id="event-1",
            tenant_id="tenant-a",
            session_id="session-1",
            sequence=1,
            event_type="user_message",
            payload={"text": "remember me"},
            trace_id="trace-1",
        )
    )
    await MemoryRepository(database).create(
        MemoryRecord(
            memory_id="memory-1",
            tenant_id="tenant-a",
            principal_id="user-1",
            content="The user prefers concise answers.",
            source_event_id=event.event_id,
        )
    )
    await SummaryRepository(database).create(
        SummaryRecord(
            summary_id="summary-1",
            tenant_id="tenant-a",
            session_id="session-1",
            content="The user introduced a preference.",
            source_end_sequence=1,
        )
    )
    await AuditLogRepository(database).create(
        AuditLogRecord(
            log_id="audit-1",
            trace_id="trace-1",
            tenant_id="tenant-a",
            channel=ChannelType.HTTP,
            user_id="user-1",
            session_id="session-1",
            agent_name="assistant",
        )
    )

    memories = await MemoryRepository(database).list_for_principal("tenant-a", "user-1")
    summary = await SummaryRepository(database).latest("tenant-a", "session-1")
    audit = await AuditLogRepository(database).list_for_tenant("tenant-a")
    events = await SessionEventRepository(database).list_for_session("tenant-a", "session-1")

    assert [item.content for item in memories] == ["The user prefers concise answers."]
    assert summary is not None and summary.source_end_sequence == 1
    assert [item.log_id for item in audit] == ["audit-1"]
    assert [item.event_id for item in events] == ["event-1"]


@pytest.mark.asyncio
async def test_knowledge_and_artifact_stores_enforce_tenant_scope(
    database: Database, tmp_path: Path
) -> None:
    await seed_tenant(database, "tenant-a")
    await seed_tenant(database, "tenant-b")
    knowledge = KnowledgeRepository(database)
    await knowledge.create(
        KnowledgeRecord(
            knowledge_id="guide-1",
            tenant_id="tenant-a",
            app_id="assistant",
            title="Tenant guide",
            content="Only tenant A can retrieve this phrase.",
        )
    )
    assert len(await knowledge.search("tenant-a", "retrieve", "assistant")) == 1
    assert await knowledge.search("tenant-b", "retrieve", "assistant") == []

    artifacts = LocalArtifactStore(database, tmp_path / "artifacts")
    stored = await artifacts.put(
        tenant_id="tenant-a",
        artifact_id="file-1",
        filename="answer.txt",
        data=b"verified content",
        media_type="text/plain",
        session_id="session-1",
    )
    assert stored.storage_uri == "artifact://tenant-a/file-1"
    assert await artifacts.read("tenant-a", "file-1") == b"verified content"
    with pytest.raises(ArtifactNotFoundError):
        await artifacts.read("tenant-b", "file-1")
    with pytest.raises(ValueError, match="path"):
        await artifacts.put(
            tenant_id="tenant-a",
            artifact_id="file-2",
            filename="../escape.txt",
            data=b"no",
        )
