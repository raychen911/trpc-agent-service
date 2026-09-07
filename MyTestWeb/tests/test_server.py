import sys
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import server
from trpc_agent_sdk.memory import InMemoryMemoryService
from trpc_agent_sdk.sessions import InMemorySessionService

client = TestClient(server.app)
TEST_SESSION = InMemorySessionService()
TEST_MEMORY = InMemoryMemoryService()
for worker in (server.MOCK_WORKER, server.DEEPSEEK_WORKER):
    worker._session_factory = lambda _tenant: TEST_SESSION
    worker._memory_factory = lambda _tenant: TEST_MEMORY


def test_health_and_bootstrap_do_not_expose_secrets():
    health = client.get("/healthz")
    assert health.status_code == 200
    assert health.json()["mode"] == "local"

    response = client.get("/api/bootstrap")
    assert response.status_code == 200
    body = response.json()
    assert len(body["tenants"]) == 3
    assert "api_key" not in response.text.lower()
    assert {item["tenant_id"] for item in body["tenants"]} == {"acme-retail", "nova-finance", "orbit-lab"}


def test_local_admin_can_register_tenant_and_duplicate_is_rejected():
    response = client.post("/api/tenants",
                           json={
                               "tenant_id": "tenant-created",
                               "name": "新建演示租户",
                               "model_name": "mock-local",
                               "storage_backend": "redis",
                               "tools": ["knowledge_lookup"],
                           })
    assert response.status_code == 201
    assert response.json()["tenant_id"] == "tenant-created"
    assert response.json()["storage_config"]["session_backend"] == "redis"
    duplicate = client.post("/api/tenants", json={
        "tenant_id": "tenant-created",
        "name": "重复租户",
    })
    assert duplicate.status_code == 409
    assert client.get("/api/tenants/tenant-created/storage").status_code == 200


def test_mock_chat_is_tenant_scoped_and_audited():
    response = client.post("/api/chat", json={
        "tenant_id": "acme-retail",
        "message": "订单在哪里？",
        "mode": "mock",
    })
    assert response.status_code == 200
    body = response.json()
    assert "Acme 零售" in body["reply"]
    assert body["tenant_id"] == "acme-retail"
    assert len(body["session_id"]) == 64

    audit = client.get("/api/audit", params={"tenant_id": "acme-retail"}).json()
    assert audit["items"][0]["tenant_id"] == "acme-retail"
    metrics = client.get("/api/metrics", params={"tenant_id": "acme-retail"})
    assert metrics.status_code == 200


def test_unknown_tenant_and_missing_key_errors(monkeypatch):
    assert client.post("/api/chat", json={"tenant_id": "missing", "message": "x"}).status_code == 404
    monkeypatch.delenv("DEEPSEEK_API_KEY", raising=False)
    monkeypatch.delenv("TRPC_AGENT_API_KEY", raising=False)
    response = client.post("/api/chat", json={"tenant_id": "acme-retail", "message": "hello", "mode": "deepseek"})
    assert response.status_code == 400
    assert "DEEPSEEK_API_KEY" in response.json()["detail"]


def test_local_tools_and_deepseek_agent_factory(monkeypatch):
    assert "退款规则" in server.knowledge_lookup("退款规则")
    assert server.calculator("(2 + 3) * 4") == "20"
    assert "仅支持" in server.calculator("")
    assert "仅支持" in server.calculator("open('/etc/passwd')")

    monkeypatch.setenv("DEEPSEEK_API_KEY", "unit-test-secret")
    normal = server.create_deepseek_agent(server.TENANTS[0])
    laboratory = server.create_deepseek_agent(server.TENANTS[2])
    assert normal.name == "acme_retail"
    assert laboratory.name == "orbit_lab"


def test_scenarios_are_isolated_and_idempotent():
    isolated = client.post("/api/scenarios/isolation", json={"tenant_id": "acme-retail"})
    assert isolated.status_code == 200
    assert isolated.json()["isolated"] is True
    assert isolated.json()["sessions"][0] != isolated.json()["sessions"][1]

    duplicate = client.post("/api/scenarios/duplicate", json={"tenant_id": "acme-retail"})
    assert duplicate.status_code == 200
    assert duplicate.json()["accepted"] == [True, False]

    assert client.post("/api/scenarios/isolation", json={"tenant_id": "missing"}).status_code == 404
    assert client.post("/api/scenarios/duplicate", json={"tenant_id": "missing"}).status_code == 404


def test_storage_configuration_update_validation_dry_run_and_rollback(monkeypatch):
    tenant_id = "acme-retail"
    initial = client.get(f"/api/tenants/{tenant_id}/storage")
    assert initial.status_code == 200
    assert initial.json()["session_backend"] in {"redis", "mysql"}

    updated = client.patch(f"/api/tenants/{tenant_id}/storage",
                           json={
                               "session_backend": "mysql",
                               "memory_backend": "redis",
                               "summary_backend": "mysql",
                               "audit_backend": "mysql",
                               "redis_url": "redis://test/0",
                               "mysql_url": "mysql+aiomysql://user:secret@mysql/test",
                           })
    assert updated.status_code == 200
    assert updated.json()["session_backend"] == "mysql"
    assert "secret" not in updated.text
    assert client.patch(f"/api/tenants/{tenant_id}/storage",
                        json={
                            "session_backend": "inmemory",
                            "memory_backend": "redis",
                            "summary_backend": "mysql",
                            "audit_backend": "mysql",
                        }).status_code == 422

    async def healthy(_tenant, backend, redis_url=None, mysql_url=None):
        return {"backend": backend, "ok": True, "latency_ms": 1.0}

    monkeypatch.setattr(server, "_test_storage_connection", healthy)
    checked = client.post(f"/api/tenants/{tenant_id}/storage/test", json={"backend": "redis"})
    assert checked.json()["ok"] is True
    dry = client.post(f"/api/tenants/{tenant_id}/storage/migrate/dry-run",
                      json={
                          "source": "redis",
                          "target": "mysql",
                      })
    assert dry.json()["ready"] is True
    assert client.post(f"/api/tenants/{tenant_id}/storage/migrate/dry-run",
                       json={
                           "source": "redis",
                           "target": "redis",
                       }).status_code == 422

    rolled = client.post(f"/api/tenants/{tenant_id}/rollback")
    assert rolled.status_code == 200
    assert "回滚" in rolled.json()["message"]
    assert client.get("/api/tenants/missing/storage").status_code == 404


@pytest.mark.asyncio
async def test_real_storage_connection_helpers_are_redacted(monkeypatch):
    tenant = server.MANAGER.get("acme-retail")

    class FakeRedis:
        pinged = False
        closed = False

        async def ping(self):
            self.pinged = True

        async def aclose(self):
            self.closed = True

    fake_redis = FakeRedis()
    monkeypatch.setattr("redis.asyncio.from_url", lambda _url: fake_redis)
    result = await server._test_storage_connection(tenant, "redis", redis_url="redis://ok/0")
    assert result["ok"] is True and fake_redis.pinged and fake_redis.closed

    class FakeConnection:

        async def execute(self, _statement):
            return 1

    class ConnectContext:

        async def __aenter__(self):
            return FakeConnection()

        async def __aexit__(self, *_args):
            return None

    class FakeEngine:
        disposed = False

        def connect(self):
            return ConnectContext()

        async def dispose(self):
            self.disposed = True

    engine = FakeEngine()
    monkeypatch.setattr("sqlalchemy.ext.asyncio.create_async_engine", lambda *_args, **_kwargs: engine)
    result = await server._test_storage_connection(tenant, "mysql", mysql_url="mysql+aiomysql://user:secret@host/db")
    assert result["ok"] is True and engine.disposed
    invalid = await server._test_storage_connection(tenant, "mysql", mysql_url="sqlite:///unsupported.db")
    assert invalid["ok"] is False
    assert "secret" not in invalid["error"]


def test_five_channel_configs_are_versioned_and_never_return_secrets():
    tenant_id = "orbit-lab"
    listed = client.get(f"/api/tenants/{tenant_id}/channels")
    assert listed.status_code == 200
    assert {item["channel"] for item in listed.json()["items"]} == {"wecom", "wechat_kf", "dingtalk", "feishu", "qq"}
    invalid = client.patch(
        f"/api/tenants/{tenant_id}/channels/wecom",
        json={"token": "incomplete-secret"},
    )
    assert invalid.status_code == 422
    assert "incomplete-secret" not in invalid.text
    assert {error["loc"][0] for error in invalid.json()["detail"]} >= {"aes_key", "corp_id", "agent_id"}
    payloads = {
        "wecom": {
            "corp_id": "corp",
            "agent_id": "1",
            "token": "wecom-secret",
            "aes_key": "aes"
        },
        "wechat_kf": {
            "corp_id": "corp",
            "open_kfid": "wk-1",
            "token": "kf-secret",
            "aes_key": "aes"
        },
        "dingtalk": {
            "app_id": "ding",
            "robot_code": "robot",
            "secret": "ding-secret"
        },
        "feishu": {
            "app_id": "fei",
            "verification_token": "verify-secret",
            "encrypt_key": "encrypt"
        },
        "qq": {
            "app_id": "qq-app",
            "secret": "qq-secret",
            "access_token": "qq-token"
        },
    }
    for channel, payload in payloads.items():
        response = client.patch(f"/api/tenants/{tenant_id}/channels/{channel}", json=payload)
        assert response.status_code == 200
        assert response.json()["item"]["configured"] is True
        assert not any(value in response.text for value in ("wecom-secret", "kf-secret", "ding-secret", "verify-secret",
                                                            "encrypt", "qq-secret", "qq-token"))
    # Empty secret fields retain the previous SecretStr without exposing it.
    retained = client.patch(f"/api/tenants/{tenant_id}/channels/wecom", json={"corp_id": "corp-new"})
    assert retained.status_code == 200
    assert retained.json()["item"]["secret_configured"] is True
    assert client.patch(f"/api/tenants/{tenant_id}/channels/slack", json={}).status_code == 422


def test_local_im_simulator_covers_all_supported_channels_and_dedup():
    for index, channel in enumerate(("wecom", "wechat_kf", "dingtalk", "feishu", "qq")):
        message_id = f"im-{channel}-{index}"
        response = client.post("/api/im/simulate",
                               json={
                                   "tenant_id": "orbit-lab",
                                   "channel": channel,
                                   "text": "本地 IM 验证",
                                   "user_id": "same-user",
                                   "chat_id": "group-1",
                                   "chat_type": "group",
                                   "message_id": message_id,
                               })
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["inbound"]["channel"] == channel
        assert body["outbound_payloads"]
        if channel == "qq":
            assert body["outbound_payloads"][0]["msg_type"] == 0
            assert body["outbound_payloads"][0]["msg_id"] == message_id
        assert len(body["session_id"]) == 64
        duplicate = client.post("/api/im/simulate",
                                json={
                                    "tenant_id": "orbit-lab",
                                    "channel": channel,
                                    "text": "重复",
                                    "message_id": message_id,
                                })
        assert duplicate.json()["duplicate"] is True
    assert client.post("/api/im/simulate", json={
        "tenant_id": "missing",
        "channel": "wecom",
        "text": "x",
    }).status_code == 404


def test_im_simulator_redacts_execution_and_delivery_failures(monkeypatch):

    async def fail_worker(*_args, **_kwargs):
        raise RuntimeError("mysql://user:plaintext@host/db")

    monkeypatch.setattr(server.MOCK_WORKER, "handle", fail_worker)
    failed = client.post("/api/im/simulate",
                         json={
                             "tenant_id": "acme-retail",
                             "channel": "wecom",
                             "text": "x",
                             "message_id": "fail-worker",
                         })
    assert failed.status_code == 503
    assert "plaintext" not in failed.text

    async def ok_worker(*_args, **_kwargs):
        return "reply"

    async def fail_reply(*_args, **_kwargs):
        return server.SendResult(ok=False, error="delivery")

    monkeypatch.setattr(server.MOCK_WORKER, "handle", ok_worker)
    monkeypatch.setattr(server.WecomAdapter, "reply_text", fail_reply)
    failed = client.post("/api/im/simulate",
                         json={
                             "tenant_id": "acme-retail",
                             "channel": "wecom",
                             "text": "x",
                             "message_id": "fail-reply",
                         })
    assert failed.status_code == 502


def _migration_tenant(tenant_id: str, *, session_backend: str = "redis"):
    tenant = server.Tenant(
        tenant_id=tenant_id,
        name=tenant_id,
        model=server.ModelEndpoint(model_name="mock"),
        storage_config=server.StorageBackendConfig(
            session_backend=session_backend,
            memory_backend="redis",
            summary_backend=session_backend,
            audit_backend="mysql",
            redis_url="redis://fixture/0",
            mysql_url="mysql+aiomysql://user:password@fixture/db",
        ),
    )
    server.MANAGER.register(tenant, reason="migration test")
    return tenant


@pytest.mark.asyncio
async def test_missing_storage_urls_and_new_tenant_rollback_guard():
    tenant = server.Tenant(
        tenant_id="storage-missing-urls",
        name="missing",
        model=server.ModelEndpoint(model_name="mock"),
        storage_config=server.StorageBackendConfig(),
    )
    server.MANAGER.register(tenant, reason="storage error test")

    redis = await server._test_storage_connection(tenant, "redis")
    mysql = await server._test_storage_connection(tenant, "mysql")

    assert redis["ok"] is mysql["ok"] is False
    assert client.post("/api/tenants/storage-missing-urls/rollback").status_code == 409


@pytest.mark.asyncio
async def test_create_tenant_normalizes_manager_conflict(monkeypatch):
    monkeypatch.setattr(server.MANAGER, "get", lambda _tenant_id: None)

    def conflict(*_args, **_kwargs):
        raise ValueError("version conflict")

    monkeypatch.setattr(server.MANAGER, "register", conflict)
    request = server.TenantCreateRequest(tenant_id="manager-conflict", name="conflict")

    with pytest.raises(server.HTTPException) as error:
        await server.create_local_tenant(request)
    assert error.value.status_code == 409


@pytest.mark.asyncio
async def test_storage_migration_worker_switches_routes_and_closes_adapters(monkeypatch):
    tenant = _migration_tenant("migration-success")
    adapters = []

    class FakeAdapter:

        def __init__(self, _tenant, backend):
            self.backend = backend
            self.closed = False
            adapters.append(self)

        async def close(self):
            self.closed = True

    report = SimpleNamespace(
        copied_by_kind={
            "session": 2,
            "memory": 3
        },
        source_checksums={
            "session": "a",
            "memory": "b"
        },
        target_checksums={
            "session": "a",
            "memory": "b"
        },
        verified=True,
    )

    class FakeMigrator:

        def __init__(self, source, target):
            assert source.backend == "redis" and target.backend == "mysql"

        async def migrate(self, tenant_id, kinds):
            assert tenant_id == "migration-success"
            assert kinds == ["session", "memory"]
            return report

    monkeypatch.setattr(server, "TenantBackendMigrationAdapter", FakeAdapter)
    monkeypatch.setattr(server, "TenantDataMigrator", FakeMigrator)
    server.MIGRATION_JOBS["success-job"] = {
        "tenant_id": tenant.tenant_id,
        "status": "pending",
    }
    request = server.StorageMigrationRequest(source="redis", target="mysql")

    await server._execute_storage_migration("success-job", tenant.tenant_id, request)

    job = server.MIGRATION_JOBS["success-job"]
    assert job["status"] == "completed" and job["verified"] is True
    assert job["storage"]["session_backend"] == "mysql"
    assert job["storage"]["summary_backend"] == "mysql"
    assert job["storage"]["memory_backend"] == "mysql"
    assert len(adapters) == 2 and all(adapter.closed for adapter in adapters)


@pytest.mark.asyncio
async def test_storage_migration_worker_keeps_routes_when_verification_fails(monkeypatch):
    tenant = _migration_tenant("migration-unverified")

    class FakeAdapter:

        def __init__(self, *_args):
            self.closed = False

        async def close(self):
            self.closed = True

    report = SimpleNamespace(
        copied_by_kind={"session": 1},
        source_checksums={"session": "source"},
        target_checksums={"session": "target"},
        verified=False,
    )

    class FakeMigrator:

        def __init__(self, *_args):
            pass

        async def migrate(self, *_args):
            return report

    monkeypatch.setattr(server, "TenantBackendMigrationAdapter", FakeAdapter)
    monkeypatch.setattr(server, "TenantDataMigrator", FakeMigrator)
    server.MIGRATION_JOBS["unverified-job"] = {
        "tenant_id": tenant.tenant_id,
        "status": "pending",
    }

    await server._execute_storage_migration(
        "unverified-job",
        tenant.tenant_id,
        server.StorageMigrationRequest(source="redis", target="mysql", kinds=["session"]),
    )

    job = server.MIGRATION_JOBS["unverified-job"]
    assert job["status"] == "failed" and job["stage"] == "verification_failed"
    assert server.MANAGER.get(tenant.tenant_id).storage_config.session_backend == "redis"


@pytest.mark.asyncio
async def test_storage_migration_worker_redacts_failure(monkeypatch):
    tenant = _migration_tenant("migration-error")
    adapters = []

    class FakeAdapter:

        def __init__(self, *_args):
            self.closed = False
            adapters.append(self)

        async def close(self):
            self.closed = True

    class FailingMigrator:

        def __init__(self, *_args):
            pass

        async def migrate(self, *_args):
            raise RuntimeError("mysql://user:plaintext@host/db")

    monkeypatch.setattr(server, "TenantBackendMigrationAdapter", FakeAdapter)
    monkeypatch.setattr(server, "TenantDataMigrator", FailingMigrator)
    server.MIGRATION_JOBS["error-job"] = {
        "tenant_id": tenant.tenant_id,
        "status": "pending",
    }

    await server._execute_storage_migration(
        "error-job",
        tenant.tenant_id,
        server.StorageMigrationRequest(source="redis", target="mysql"),
    )

    job = server.MIGRATION_JOBS["error-job"]
    assert job["status"] == "failed" and job["stage"] == "error"
    assert "plaintext" not in job["error"]
    assert all(adapter.closed for adapter in adapters)


def test_storage_migration_endpoint_guards_enqueue_and_lookup(monkeypatch):
    stale = _migration_tenant("migration-stale", session_backend="mysql")
    healthy = _migration_tenant("migration-endpoint")

    assert client.post(
        f"/api/tenants/{healthy.tenant_id}/storage/migrate",
        json={
            "source": "redis",
            "target": "redis"
        },
    ).status_code == 422
    assert client.post(
        f"/api/tenants/{healthy.tenant_id}/storage/migrate",
        json={
            "source": "mysql",
            "target": "redis"
        },
    ).status_code == 422
    assert client.post(
        f"/api/tenants/{healthy.tenant_id}/storage/migrate",
        json={
            "source": "redis",
            "target": "mysql",
            "kinds": ["session", "session"]
        },
    ).status_code == 400
    assert client.post(
        f"/api/tenants/{stale.tenant_id}/storage/migrate",
        json={
            "source": "redis",
            "target": "mysql",
            "kinds": ["session"]
        },
    ).status_code == 409

    async def unhealthy(_tenant, backend, **_kwargs):
        return {"backend": backend, "ok": False}

    monkeypatch.setattr(server, "_test_storage_connection", unhealthy)
    assert client.post(
        f"/api/tenants/{healthy.tenant_id}/storage/migrate",
        json={
            "source": "redis",
            "target": "mysql"
        },
    ).status_code == 409

    async def storage_ok(_tenant, backend, **_kwargs):
        return {"backend": backend, "ok": True}

    monkeypatch.setattr(server, "_test_storage_connection", storage_ok)
    server.MIGRATION_JOBS["already-running"] = {
        "job_id": "already-running",
        "tenant_id": healthy.tenant_id,
        "status": "running",
    }
    assert client.post(
        f"/api/tenants/{healthy.tenant_id}/storage/migrate",
        json={
            "source": "redis",
            "target": "mysql"
        },
    ).status_code == 409
    server.MIGRATION_JOBS.pop("already-running")

    async def no_op_job(*_args):
        return None

    monkeypatch.setattr(server, "_execute_storage_migration", no_op_job)
    queued = client.post(
        f"/api/tenants/{healthy.tenant_id}/storage/migrate",
        json={
            "source": "redis",
            "target": "mysql"
        },
    )
    assert queued.status_code == 202
    job_id = queued.json()["job_id"]
    assert client.get(f"/api/tenants/{healthy.tenant_id}/storage/migrate/{job_id}").status_code == 200
    assert client.get(f"/api/tenants/wrong-tenant/storage/migrate/{job_id}").status_code == 404
    assert client.get(f"/api/tenants/{healthy.tenant_id}/storage/migrate/missing").status_code == 404


def test_qq_private_fixture_uses_c2c_identifiers():
    response = client.post(
        "/api/im/simulate",
        json={
            "tenant_id": "orbit-lab",
            "channel": "qq",
            "text": "private QQ",
            "user_id": "qq-user",
            "chat_type": "private",
            "message_id": "qq-private-fixture",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["platform_payload"]["t"] == "C2C_MESSAGE_CREATE"
    assert body["inbound"]["sender_id"] == "qq-user"
    assert body["inbound"]["chat_type"] == "private"
