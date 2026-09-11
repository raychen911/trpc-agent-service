"""Restartable migration state machines and versioned vector cutover."""

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.sessions import SqlSessionService
from trpc_agent_sdk.types import Content
from trpc_agent_sdk.types import EventActions
from trpc_agent_sdk.types import Part

from trpc_service.migration import MigrationCoordinator
from trpc_service.migration import MigrationState
from trpc_service.migration import SessionRecord
from trpc_service.migration import SessionStore
from trpc_service.migration import VectorMigration
from trpc_service.migration import copy_and_verify_trpc_session
from trpc_service.persistence import Database
from trpc_service.storage import ConflictError
from trpc_service.storage import LocalVectorKnowledgeStore


@pytest.fixture
def database(tmp_path):
    database = Database(f"sqlite+pysqlite:///{tmp_path / 'migration.db'}")
    database.create_schema()
    yield database
    database.close()


def test_session_migration_pauses_resumes_verifies_cuts_over_and_rolls_back(database: Database) -> None:
    source = SessionStore([
        SessionRecord("ses-a", 2, {"events": [1, 2]}),
        SessionRecord("ses-b", 1, {"events": [1]}),
    ])
    target = SessionStore()
    coordinator = MigrationCoordinator.create(
        database,
        tenant_id="tenant_acme",
        resource_type="session",
        source_profile_id="redis_main",
        target_profile_id="sql_main",
        config_version=3,
        storage_revision=4,
        source=source,
        target=target,
    )
    coordinator.prepare()
    assert coordinator.backfill(batch_size=1) is False
    coordinator.pause()
    restarted = MigrationCoordinator.load(database, coordinator.job_id, source=source, target=target)
    restarted.resume()
    assert restarted.backfill(batch_size=10) is True
    restarted.begin_dual_write()
    source.put(SessionRecord("ses-c", 1, {"events": [1]}))
    restarted.dual_write(SessionRecord("ses-c", 1, {"events": [1]}))
    assert restarted.verify().matches
    restarted.shadow_read()
    restarted.cutover(rollback_seconds=300)
    assert restarted.state is MigrationState.ROLLBACK_WINDOW
    restarted.rollback()
    assert restarted.state is MigrationState.ROLLED_BACK


def test_shadow_difference_blocks_cutover(database: Database) -> None:
    source = SessionStore([SessionRecord("ses-a", 1, {"value": 1})])
    target = SessionStore([SessionRecord("ses-a", 1, {"value": 2})])
    coordinator = MigrationCoordinator.create(
        database,
        tenant_id="tenant_acme",
        resource_type="session",
        source_profile_id="redis_main",
        target_profile_id="sql_main",
        config_version=3,
        storage_revision=4,
        source=source,
        target=target,
    )
    coordinator.prepare()
    coordinator.backfill(10)
    coordinator.begin_dual_write()
    target.put(SessionRecord("ses-a", 1, {"value": 2}))
    assert not coordinator.verify().matches
    with pytest.raises(ConflictError, match="verification"):
        coordinator.shadow_read()


def test_vector_migration_uses_new_index_version_and_tombstones() -> None:
    source = LocalVectorKnowledgeStore()
    target = LocalVectorKnowledgeStore()
    source.upsert("tenant_acme", "kb", 1, "doc-1", "chunk-1", [1.0, 0.0], "hello")
    source.upsert("tenant_acme", "kb", 1, "doc-2", "chunk-2", [0.0, 1.0], "deleted")
    source.delete_document("tenant_acme", "kb", 1, "doc-2")

    migration = VectorMigration(
        tenant_id="tenant_acme",
        knowledge_base_id="kb",
        source_index_version=1,
        target_index_version=2,
        source=source,
        target=target,
        source_embedding=("embed-v1", 2),
        target_embedding=("embed-v1", 2),
    )
    report = migration.copy_and_verify()
    assert report.copied == 1
    assert report.tombstones == 1
    assert [item.text for item in target.search("tenant_acme", "kb", 2, [1.0, 0.0])] == ["hello"]
    migration.cutover()
    assert migration.active_index_version == 2
    migration.rollback()
    assert migration.active_index_version == 1


@pytest.mark.asyncio
async def test_public_trpc_session_copy_preserves_ids_state_and_is_idempotent(tmp_path) -> None:
    source = InMemorySessionService()
    target = SqlSessionService(db_url=f"sqlite:///{tmp_path / 'trpc-target.db'}")
    session = await source.create_session(
        app_name="tenant_acme:support_app",
        user_id="user-1",
        session_id="session-1",
    )
    await source.append_event(
        session,
        Event(
            id="event-1",
            author="agent",
            actions=EventActions(state_delta={"turn": 1}),
            content=Content(parts=[Part.from_text(text="migrated")]),
        ),
    )
    first = await copy_and_verify_trpc_session(
        source,
        target, target_writes_paused=True,
        app_name=session.app_name,
        user_id=session.user_id,
        session_id=session.id,
    )
    second = await copy_and_verify_trpc_session(
        source,
        target, target_writes_paused=True,
        app_name=session.app_name,
        user_id=session.user_id,
        session_id=session.id,
    )
    assert first.digest == second.digest
    assert first.payload["events"][0]["id"] == "event-1"
    assert first.payload["state"] == {"turn": 1}
    await target.close()
