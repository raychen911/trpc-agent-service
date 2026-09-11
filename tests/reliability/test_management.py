"""Management publishes immutable, auditable configuration snapshots."""

import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from trpc_service.persistence import Database
from trpc_service.web.app import create_app


@pytest.fixture
def managed(tmp_path, monkeypatch):
    monkeypatch.delenv("TRPC_RUNTIME_MODE", raising=False)
    monkeypatch.setenv("TRPC_ADMIN_TOKEN", "operator-test-token-" * 3)
    url = f"sqlite:///{tmp_path / 'management.db'}"
    db = Database(url)
    db.create_schema()
    with TestClient(create_app(url)) as client:
        client.headers["Authorization"] = "Bearer " + "operator-test-token-" * 3
        yield client, db
    db.close()


def register_config(client, version=3):
    config = json.loads(Path("deploy/simulation-tenants.json").read_text(encoding="utf-8"))[0]
    config["config_version"] = version
    profiles = {}
    for resource, ref in config["data_backends"].items():
        profile = profiles.setdefault(
            ref["profile_id"],
            dict(profile_id=ref["profile_id"],
                 profile_revision=1,
                 kind=ref["kind"],
                 provider="test",
                 endpoint="localhost",
                 secret_ref={"uri": "env://BACKEND_URL"},
                 capabilities=[]))
        profile["capabilities"].append(resource)
    if version == 3:
        for profile in profiles.values():
            assert client.post("/admin/backend-profiles", json=profile).status_code == 201
    body = {"config": config, "profile_revisions": {role: 1 for role in config["data_backends"]}}
    return body


def test_auth_and_immutable_publish(managed):
    client, db = managed
    assert client.get("/admin/tenants", headers={"Authorization": "Bearer wrong"}).status_code == 401
    body = register_config(client)
    path = "/admin/tenants/tenant_acme/configs"
    assert client.post(path, json=body).status_code == 201
    assert client.post(path, json=body).status_code == 409
    assert client.post(path + "/3/publish", json={"expected_active_version": 0}).status_code == 200
    assert client.post(path + "/3/publish", json={"expected_active_version": 0}).status_code == 409
    body2 = register_config(client, 4)
    body2["config"]["applications"][0]["instruction"] = "Updated instruction"
    assert client.post(path, json=body2).status_code == 201
    assert client.post(path + "/4/publish", json={"expected_active_version": 3}).status_code == 200
    saved = client.get(path + "/3").json()["config"]
    assert saved["applications"][0]["instruction"] == body["config"]["applications"][0]["instruction"]
    assert client.get("/admin/tenants").json()[0]["active_config_version"] == 4
    logs = client.get("/admin/tenants/tenant_acme/audit").json()
    assert [item["decision"] for item in logs].count("config_published") == 2
    assert "Updated instruction" not in json.dumps(logs)
    assert "secret_ref" not in client.get("/admin/backend-profiles").text
    # Safe rollback changes the active pointer, never the immutable snapshot.
    assert client.post(path + "/3/publish", json={"expected_active_version": 4}).status_code == 200
    from trpc_service.management import ConfigurationStore
    independent = Database(str(db.engine.url))
    assert ConfigurationStore(independent).active()[0].config_version == 3
    independent.close()


def test_resource_status_reports_effective_policy_and_queue(managed):
    client, _ = managed
    body = register_config(client)
    body["config"]["resource_policy"] = {
        "max_concurrent_runs": 2,
        "max_queued_messages": 10,
        "requests_per_minute": 20,
        "priority": 7,
    }
    path = "/admin/tenants/tenant_acme/configs"
    assert client.post(path, json=body).status_code == 201
    assert client.post(path + "/3/publish", json={"expected_active_version": 0}).status_code == 200
    response = client.get("/admin/tenants/tenant_acme/resources")
    assert response.status_code == 200
    assert response.json()["resource_policy"]["priority"] == 7
    assert response.json()["usage"]["queued"] == 0


def test_invalid_placement_and_storage_switch_rejected(managed):
    client, db = managed
    body = register_config(client)
    path = "/admin/tenants/tenant_acme/configs"
    body["profile_revisions"]["session"] = 99
    assert client.post(path, json=body).status_code == 422
    body["profile_revisions"]["session"] = 1
    assert client.post(path, json=body).status_code == 201
    assert client.post(path + "/3/publish", json={"expected_active_version": 0}).status_code == 200
    body["config"]["config_version"] = 4
    body["config"]["storage_revision"] += 1
    assert client.post(path, json=body).status_code == 201
    assert client.post(path + "/4/publish", json={"expected_active_version": 3}).status_code == 409
    assert client.get("/admin/tenants").json()[0]["active_config_version"] == 3


def test_profile_credentials_rejected(managed):
    client, _ = managed
    body = dict(profile_id="sql_test",
                profile_revision=1,
                kind="sql",
                provider="sql",
                endpoint="postgresql://user:password@host/db",
                secret_ref={"uri": "env://DB"},
                capabilities=["session"])
    assert client.post("/admin/backend-profiles", json=body).status_code == 422


def test_dead_letter_replay_is_scoped_conditional_and_audited(managed):
    from trpc_service.reliability import OutboxRepository
    from trpc_service.persistence.models import OutboxMessageRow
    client, db = managed
    outbox = OutboxRepository(db)
    identity = outbox.enqueue("tenant_acme", "message", 0, {"text": "private response"})
    outbox.claim(limit=1, worker_id="sender", lease_seconds=30)
    outbox.dead_letter(identity, "sender", "failed")
    suffix = f"/deliveries/{identity}/retry"
    assert client.post("/admin/tenants/tenant_other" + suffix, json={"expected_attempt": 1}).status_code == 409
    assert client.post("/admin/tenants/tenant_acme" + suffix, json={"expected_attempt": 1}).status_code == 200
    assert client.post("/admin/tenants/tenant_acme" + suffix, json={"expected_attempt": 1}).status_code == 409
    assert "private response" not in client.get("/admin/tenants/tenant_acme/deliveries").text
    assert client.get("/admin/tenants/tenant_acme/audit").json()[0]["decision"] == "delivery_requeued"
    with db.sessions() as session:
        row = session.get(OutboxMessageRow, identity)
        assert row.status == "retry" and row.attempt == 0


def test_channel_routes_each_request_using_active_database_config(managed, monkeypatch):
    from datetime import datetime, timezone
    from trpc_service.management import ConfigurationStore
    from trpc_service.channels import NormalizedInboundMessage

    client, db = managed
    body = register_config(client)
    path = "/admin/tenants/tenant_acme/configs"
    monkeypatch.setenv("TRPC_IDENTITY_KEY", "identity-test-" * 4)
    for version, previous in ((3, 0), (4, 3)):
        body["config"]["config_version"] = version
        assert client.post(path, json=body).status_code == 201
        assert client.post(path + f"/{version}/publish", json={"expected_active_version": previous}).status_code == 200
        message = NormalizedInboundMessage(channel="wecom",
                                           webhook_public_id="callback_acme",
                                           external_message_id=f"message-{version}",
                                           external_user_id="user-1",
                                           conversation_type="direct",
                                           text="hello",
                                           received_at=datetime.now(timezone.utc),
                                           request_id=f"request-{version}")
        assert ConfigurationStore(db).route_message(message).config_version == version
    assert client.post("/simulation/inbound", json={}).status_code == 404


def test_admin_disabled_without_operator_credential(tmp_path, monkeypatch):
    monkeypatch.delenv("TRPC_ADMIN_TOKEN", raising=False)
    monkeypatch.delenv("TRPC_RUNTIME_MODE", raising=False)
    url = f"sqlite:///{tmp_path / 'disabled.db'}"
    db = Database(url)
    db.create_schema()
    with TestClient(create_app(url)) as client:
        assert client.get("/admin/tenants").status_code == 404
    db.close()


def test_callback_collision_rolls_back_publish_and_audit(managed):
    client, _ = managed
    body = register_config(client)
    path = "/admin/tenants/tenant_acme/configs"
    assert client.post(path, json=body).status_code == 201
    assert client.post(path + "/3/publish", json={"expected_active_version": 0}).status_code == 200
    other = json.loads(json.dumps(body).replace("tenant_acme", "tenant_other"))
    other_path = "/admin/tenants/tenant_other/configs"
    assert client.post(other_path, json=other).status_code == 201
    assert client.post(other_path + "/3/publish", json={"expected_active_version": 0}).status_code == 409
    tenants = {item["tenant_id"]: item for item in client.get("/admin/tenants").json()}
    assert tenants["tenant_other"]["active_config_version"] == 0
    assert [item["decision"] for item in client.get("/admin/tenants/tenant_other/audit").json()] == ["config_drafted"]


@pytest.mark.asyncio
async def test_two_runtimes_load_pinned_versions_after_publish(managed, tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from trpc_service.channels import NormalizedInboundMessage
    from trpc_service.management import ConfigurationStore
    from trpc_service.service_runtime import ServiceRuntime, message_router

    client, db = managed

    async def no_external_resources(*args):
        return None

    monkeypatch.setattr("trpc_service.storage.runtime_resources.build_resources", no_external_resources)
    monkeypatch.setenv("TRPC_CONFIG_SOURCE", "database")
    monkeypatch.setenv("TRPC_RUNTIME_MODE", "simulation")
    monkeypatch.setenv("TRPC_IDENTITY_KEY", "identity-key-" * 4)
    monkeypatch.setenv("BACKEND_URL", f"sqlite:///{tmp_path / 'native.db'}")
    body = register_config(client)
    for role in ("session", "summary"):
        body["config"]["data_backends"][role].update(kind="sql", profile_id="sql_session")
    assert client.post("/admin/backend-profiles",
                       json=dict(profile_id="sql_session",
                                 profile_revision=1,
                                 kind="sql",
                                 provider="sql",
                                 endpoint="local",
                                 secret_ref={"uri": "env://BACKEND_URL"},
                                 capabilities=["session", "summary"])).status_code == 201
    path = "/admin/tenants/tenant_acme/configs"
    assert client.post(path, json=body).status_code == 201
    assert client.post(path + "/3/publish", json={"expected_active_version": 0}).status_code == 200
    store = ConfigurationStore(db)
    message = NormalizedInboundMessage(channel="wecom",
                                       webhook_public_id="callback_acme",
                                       external_message_id="old",
                                       external_user_id="user-1",
                                       conversation_type="direct",
                                       text="remember lighthouse",
                                       received_at=datetime.now(timezone.utc),
                                       request_id="request-old")
    old_route = message_router(store.active()).route_message(message)
    runtime1 = ServiceRuntime(db)
    await runtime1.start()
    runtime1.pipeline.ingest(message, old_route, trace_id="old")
    body["config"]["config_version"] = 4
    body["config"]["applications"][0]["instruction"] = "New instruction"
    assert client.post(path, json=body).status_code == 201
    assert client.post(path + "/4/publish", json={"expected_active_version": 3}).status_code == 200
    # A newly started worker loads active v4 but still resolves queued v3.
    runtime2 = ServiceRuntime(db)
    await runtime2.start()
    try:
        work = runtime2.inbox.claim(worker_id="second", lease_seconds=30)
        assert work.config_version == 3
        result = await runtime2.pipeline.execute(work, runtime2.registry, worker_id="second")
        assert "lighthouse" in result[-1].text
        new_message = message.model_copy(update={"external_message_id": "new", "request_id": "new", "text": "continue"})
        new_route = message_router(store.active()).route_message(new_message)
        assert new_route.config_version == 4
        runtime1.pipeline.ingest(new_message, new_route, trace_id="new")
        work = runtime1.inbox.claim(worker_id="first", lease_seconds=30)
        result = await runtime1.pipeline.execute(work, runtime1.registry, worker_id="first")
        assert "lighthouse" in result[-1].text
        assert runtime1.configs[("tenant_acme", 4)].applications[0].instruction == "New instruction"
        assert runtime2.configs[("tenant_acme", 3)].applications[0].instruction != "New instruction"
        for task in runtime2.post.claim(limit=100, worker_id="post", lease_seconds=60):
            await runtime2.process_post_turn(task)
            runtime2.post.complete(task.task_id, "post")
    finally:
        await runtime1.close()
        await runtime2.close()
