"""Restart, checksum rejection and active-pointer cutover against native storage."""

import os
import uuid

import pytest
from trpc_agent_sdk.events import Event
from trpc_agent_sdk.types import Content, Part

from trpc_service.management import ConfigurationStore, ProfileRegistration, ConfigDraft, ConfigConflict
from trpc_service.migration.offline import OfflineMigrations
from trpc_service.migration.session_migration import snapshot_trpc_session
from trpc_service.persistence.models import SessionRow, OfflineMigrationRow
from trpc_service.protected_demo import documents
from trpc_service.storage.protected_session import ProtectedSessionService
from trpc_service.agent import tenant_app_name


def protected_documents(tenant):
    profiles, tenants = documents("acceptance-wecom", "acceptance-feishu")
    config = tenants[1].model_dump(mode="json")
    config["tenant_id"] = tenant
    config["channel_bindings"][0]["tenant_id"] = tenant
    for ref in config["data_backends"].values():
        ref["namespace"] = tenant
    return [profile.model_dump(mode="json") for profile in profiles], config


@pytest.mark.asyncio
async def test_managed_migration_restart_archive_and_transactional_cutover(platform_database, monkeypatch, request):
    real = request.config.getoption("--backend-mode") == "real"
    if not real:
        pytest.skip("managed Redis-to-SQL migration requires real mode")
    database, tenant = platform_database, "tenant_" + uuid.uuid4().hex[:12]
    store, jobs = ConfigurationStore(database), OfflineMigrations(database)
    profiles, config = protected_documents(tenant)
    profiles.append(dict(profile_id="sql_target", profile_revision=1, kind="sql", provider="postgresql",
                         endpoint="postgres:5432", secret_ref={"uri": "env://TRPC_SESSION_DATABASE_URL"},
                         capabilities=["session", "summary"], options={"session_format": "protected_v1"}))
    for profile in profiles:
        store.register_profile(ProfileRegistration.model_validate(profile), "test")
    config["tenant_id"] = tenant
    config["channel_bindings"][0]["tenant_id"] = tenant
    for ref in config["data_backends"].values():
        ref["namespace"] = tenant
    version = config["config_version"]
    revisions = {role: 1 for role in config["data_backends"]}
    store.save_draft(ConfigDraft(config=config, profile_revisions=revisions), "test")
    store.publish(tenant, version, 0, "test")
    config["config_version"], config["storage_revision"] = version + 1, config["storage_revision"] + 1
    for role in ("session", "summary"):
        config["data_backends"][role].update(kind="sql", profile_id="sql_target")
    store.save_draft(ConfigDraft(config=config, profile_revisions=revisions), "test")
    key = dict(app_name=tenant_app_name(tenant, "assistant"),
               user_id="usr_migrate", session_id="ses_migrate")
    source = ProtectedSessionService.redis(os.environ["TRPC_REDIS_URL"], key["app_name"])
    target = ProtectedSessionService.sql(os.environ["TRPC_SESSION_DATABASE_URL"], key["app_name"])
    try:
        async with source.execution(user_id=key["user_id"], session_id=key["session_id"]):
            native = await source.create_session(**key, state={"value": 8})
            native.events = [Event(id="current", author="user",
                                   content=Content(parts=[Part.from_text(text="current")]))]
            native.historical_events = [Event(id="archived", author="user",
                                              content=Content(parts=[Part.from_text(text="original history")]))]
            native.conversation_count = 7
            await source.update_session(native)
        with database.sessions.begin() as session:
            session.add(SessionRow(tenant_id=tenant, app_id="assistant", user_id=key["user_id"],
                                   session_id=key["session_id"]))
        with pytest.raises(ConfigConflict):
            jobs.create(tenant, version, version + 1, "test")
        jobs.maintenance(tenant, True, "test")
        job_id = jobs.create(tenant, version, version + 1, "test")
        with pytest.raises(ConfigConflict):
            await jobs.run(tenant, job_id, writers_stopped=False)
        with pytest.raises(ConfigConflict):
            jobs.maintenance(tenant, False, "test")

        original = jobs._restore_prepared

        async def fail(target_service, record, identity):
            await original(target_service, record, identity)
            raise ConnectionError("lost acknowledgement after protected commit")

        monkeypatch.setattr(jobs, "_restore_prepared", fail)
        with pytest.raises(ConnectionError):
            await jobs.run(tenant, job_id, writers_stopped=True)
        with database.sessions() as session:
            row = session.get(OfflineMigrationRow, job_id)
            assert row.copied == 0 and "snapshot" in row.items_json[0]
        restarted = OfflineMigrations(database)
        result = await restarted.run(tenant, job_id, writers_stopped=True)
        assert result["status"] == "verified" and result["copied"] == 1
        expected = await snapshot_trpc_session(source, **key)
        assert (await snapshot_trpc_session(target, **key)).digest == expected.digest
        async with target.execution(user_id=key["user_id"], session_id=key["session_id"]):
            changed = await target.get_session(**key)
            changed.state["injected"] = True
            await target.update_session(changed)
        with pytest.raises(ConfigConflict):
            await restarted.run(tenant, job_id, writers_stopped=True, cutover=True)
        assert store.active()[0].config_version == version
        async with target.execution(user_id=key["user_id"], session_id=key["session_id"]):
            changed = await target.get_session(**key)
            del changed.state["injected"]
            await target.update_session(changed)
        assert (await restarted.run(tenant, job_id, writers_stopped=True, cutover=True))["status"] == "completed"
        assert store.active()[0].config_version == version + 1
        assert store.published(tenant, version + 1).storage_revision == config["storage_revision"]
        restarted.maintenance(tenant, False, "test")
        with pytest.raises(ConfigConflict):
            store.publish(tenant, version, version + 1, "test")
    finally:
        for service in (source, target):
            async with service.execution(user_id=key["user_id"], session_id=key["session_id"]):
                await service.delete_session(**key)
        await source.close()
        await target.close()


@pytest.mark.asyncio
async def test_managed_vector_job_restart_tombstone_and_cutover(
        platform_database, vector_backend, monkeypatch, request):
    if request.config.getoption("--backend-mode") != "real":
        pytest.skip("managed index migration requires real mode")
    database, tenant = platform_database, "tenant_" + uuid.uuid4().hex[:12]
    store, jobs = ConfigurationStore(database), OfflineMigrations(database)
    profiles, config = protected_documents(tenant)
    for profile in profiles:
        if profile["kind"] == "vector":
            profile["options"] = {"collection": vector_backend._collection, "index_version": 1,
                                  "embedding_id": "test-embedding-v1"}
        store.register_profile(ProfileRegistration.model_validate(profile), "test")
    profile = next(profile for profile in profiles if profile["kind"] == "vector")
    profile["profile_revision"], profile["options"]["index_version"] = 2, 2
    store.register_profile(ProfileRegistration.model_validate(profile), "test")
    config["tenant_id"] = tenant
    config["channel_bindings"][0]["tenant_id"] = tenant
    config["applications"][0]["knowledge_base_ids"] = ["kb_migrate"]
    for ref in config["data_backends"].values():
        ref["namespace"] = tenant
    version = config["config_version"]
    revisions = {role: 1 for role in config["data_backends"]}
    store.save_draft(ConfigDraft(config=config, profile_revisions=revisions), "test")
    store.publish(tenant, version, 0, "test")
    config["config_version"], config["storage_revision"] = version + 1, config["storage_revision"] + 1
    revisions["knowledge"] = 2
    store.save_draft(ConfigDraft(config=config, profile_revisions=revisions), "test")
    for doc in ("visible", "deleted"):
        vector_backend.upsert(tenant, "kb_migrate", 1, doc, doc + "-chunk", [1.0, 0.0], doc, {})
    vector_backend.delete_document(tenant, "kb_migrate", 1, "deleted")
    jobs.maintenance(tenant, True, "test")
    job_id = jobs.create(tenant, version, version + 1, "test", resource_type="knowledge")
    from trpc_service.storage.knowledge import QdrantKnowledgeStore
    original = QdrantKnowledgeStore.upsert

    def lost_ack(self, *args, **kwargs):
        original(self, *args, **kwargs)
        raise ConnectionError("lost vector acknowledgement")

    with monkeypatch.context() as patch:
        patch.setattr(QdrantKnowledgeStore, "upsert", lost_ack)
        with pytest.raises(ConnectionError):
            await jobs.run(tenant, job_id, writers_stopped=True)
    resumed = await OfflineMigrations(database).run(tenant, job_id, writers_stopped=True)
    assert resumed["status"] == "verified"
    assert [hit.document_id for hit in vector_backend.search(tenant, "kb_migrate", 2, [1.0, 0.0])] == ["visible"]
    vector_backend.upsert(tenant, "kb_migrate", 2, "visible", "visible-chunk", [1.0, 0.0], "tampered", {})
    with pytest.raises(ConfigConflict):
        await jobs.run(tenant, job_id, writers_stopped=True, cutover=True)
    assert store.active()[0].config_version == version
    vector_backend.upsert(tenant, "kb_migrate", 2, "visible", "visible-chunk", [1.0, 0.0], "visible", {})
    assert (await jobs.run(tenant, job_id, writers_stopped=True, cutover=True))["status"] == "completed"
    assert store.active()[0].config_version == version + 1
