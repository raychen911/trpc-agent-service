"""Published profiles and portable import retain the protected storage contract."""

import pytest
from pydantic import ValidationError
from trpc_agent_sdk.sessions import InMemorySessionService
from trpc_agent_sdk.memory import SqlMemoryService
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, Part

from trpc_service.protected_demo import documents
from trpc_service.management import ConfigurationStore, ConfigDraft, ProfileRegistration
from trpc_service.persistence import Database
from trpc_service.migration.offline import OfflineMigrations
from trpc_service.migration.session_migration import snapshot_trpc_session
from trpc_service.storage.protected_migration import ProtectedMigrationStore
from trpc_service.storage.protected_session import ProtectedSessionService


def test_publish_two_tenants_with_distinct_session_placement(tmp_path):
    database = Database("sqlite:///" + (tmp_path / "config.db").as_posix())
    database.create_schema()
    try:
        store = ConfigurationStore(database)
        profiles, tenants = documents("example_wecom", "cli_example")
        for profile in profiles:
            store.register_profile(profile, "test")
        for tenant in tenants:
            store.save_draft(
                ConfigDraft(config=tenant,
                            profile_revisions={role: 1
                                               for role in type(tenant.data_backends).model_fields}), "test")
            store.publish(tenant.tenant_id, 1, 0, "test")
        assert len(store.active()) == 2
        assert store.resource_profile("demo_wecom", 1, "session").kind.value == "sql"
        assert store.resource_profile("demo_feishu", 1, "session").kind.value == "redis"
        for tenant in tenants:
            assert store.resource_profile(tenant.tenant_id, 1, "summary").options == {"session_format": "protected_v1"}
        invalid = profiles[0].model_dump()
        invalid["options"] = {"session_format": "trust_legacy"}
        with pytest.raises(ValidationError):
            ProfileRegistration.model_validate(invalid)
    finally:
        database.close()


@pytest.mark.asyncio
async def test_offline_public_snapshot_import_and_retry(tmp_path):
    source = InMemorySessionService()
    target = ProtectedMigrationStore("sql", "sqlite:///" + (tmp_path / "import.db").as_posix(), ["tenant:app"])
    identity = dict(app_name="tenant:app", user_id="u", session_id="s")
    try:
        session = await source.create_session(**identity, state={"a": 1})
        await source.append_event(
            session,
            Event(id="event", author="model", content=Content(role="model", parts=[Part.from_text(text="portable")])))
        record = await snapshot_trpc_session(source, **identity)
        await OfflineMigrations._restore_prepared(target, record, identity)
        await OfflineMigrations._restore_prepared(target, record, identity)
        assert (await snapshot_trpc_session(target, **identity)).digest == record.digest
        with pytest.raises(PermissionError):
            await target.service("other:app")
    finally:
        await source.close()
        await target.close()


@pytest.mark.asyncio
async def test_public_sql_memory_can_find_protected_session(tmp_path):
    native = ProtectedSessionService.sql("sqlite:///" + (tmp_path / "native.db").as_posix(), "tenant:app")
    memory = SqlMemoryService(db_url="sqlite:///" + (tmp_path / "memory.db").as_posix(),
                              enabled=True,
                              is_async=False,
                              expire_on_commit=False)
    await native.initialize()
    try:
        async with native.execution(user_id="user", session_id="session"):
            session = await native.create_session(app_name="tenant:app", user_id="user", session_id="session")
            await native.append_event(
                session,
                Event(id="event",
                      author="model",
                      content=Content(role="model", parts=[Part.from_text(text="pineapple")])))
            await memory.store_session(session)
            found = await memory.search_memory("tenant:app/user", "pineapple")
            assert found.memories
            assert not (await memory.search_memory("other:app/user", "pineapple")).memories
    finally:
        await memory.close()
        await native.close()


@pytest.mark.asyncio
async def test_upstream_summary_preserves_immutable_originals(tmp_path):
    from trpc_agent_sdk.sessions import SummarizerSessionManager
    from trpc_service.service_runtime import BoundarySummarizer, SimulationModel
    native = ProtectedSessionService.sql("sqlite:///" + (tmp_path / "summary.db").as_posix(), "tenant:app")
    await native.initialize()
    try:
        async with native.execution(user_id="u", session_id="s"):
            session = await native.create_session(app_name="tenant:app", user_id="u", session_id="s")
            for index in range(6):
                await native.append_event(
                    session,
                    Event(id=str(index),
                          author="user" if index % 2 == 0 else "agent",
                          content=Content(role="user" if index % 2 == 0 else "model",
                                          parts=[Part.from_text(text=str(index))])))
            model = SimulationModel()
            manager = SummarizerSessionManager(model=model,
                                               auto_summarize=False,
                                               summarizer=BoundarySummarizer(
                                                   model=model,
                                                   keep_recent_count=2,
                                                   boundary=6,
                                                   native_ids=[item.id for item in session.events]))
            manager.set_session_service(native)
            await manager.create_session_summary(session, force=True)
            stored = await native.get_session(app_name="tenant:app", user_id="u", session_id="s")
            assert any(item.is_summary_event() for item in stored.events)
            assert {item.id
                    for item in [*stored.historical_events, *stored.events]
                    if not item.is_summary_event()} == {str(index)
                                                        for index in range(6)}
    finally:
        await native.close()
