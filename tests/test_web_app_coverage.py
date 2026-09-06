from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from trpc_service.agent.model_client import CodexCliModelClient, ModelResponse, ResponsesModelClient
from trpc_service.channels.base import Attachment, ChannelVerificationError
from trpc_service.policy.quota import QuotaExceeded
from trpc_service.policy.tenant_filter import PolicyDenied
from trpc_service.tenant.models import ChannelBinding
from trpc_service.tenant.repository import TenantNotFound, TenantRepositoryError
from trpc_service.web.app import create_app


def make_client(tmp_path, monkeypatch, *, raise_server_exceptions=True, **env):
    monkeypatch.setenv("TENANT_DB_PATH", str(tmp_path / "tenant.sqlite3"))
    monkeypatch.delenv("TRPC_AGENT_RUNTIME_MODE", raising=False)
    monkeypatch.setenv("ADMIN_API_KEY", "root-key")
    monkeypatch.setenv("ADMIN_API_KEYS", "viewer-key=viewer:tenant_demo,operator-key=operator:tenant_demo")
    monkeypatch.setenv("REDIS_URL", "")
    monkeypatch.setenv("WORKER_QUEUE_URL", "")
    monkeypatch.setenv("COMPENSATION_WORKER", "0")
    monkeypatch.setenv("TRPC_AGENT_RUNTIME_MODE", "local")
    monkeypatch.setenv("PUBLIC_SURFACE_AUTH_REQUIRED", "0")
    monkeypatch.delenv("CPA_BASE_URL", raising=False)
    monkeypatch.delenv("OPENAI_API_KEY", raising=False)
    monkeypatch.delenv("CPA_API_KEY_REF", raising=False)
    monkeypatch.delenv("CPA_USE_CODEX_CLI", raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return TestClient(create_app(), raise_server_exceptions=raise_server_exceptions)


class FakeWebhookQueue:
    def __init__(self, *args, **kwargs):
        self.client = SimpleNamespace(ping=lambda: True)
        self.closed = False
        self.items = {}

    def submit(self, channel, account_id, payload, traceparent, tenant_id=None):
        self.items["task-1"] = {
            "task_id": "task-1",
            "tenant_id": tenant_id,
            "channel": channel,
            "account_id": account_id,
            "status": "accepted",
            "payload": payload,
            "traceparent": traceparent,
        }
        return "task-1"

    def status(self, task_id):
        return self.items.get(task_id)

    def consume_once(self, handler, timeout=1):
        return False

    def close(self):
        self.closed = True


class FakeOutboundQueue:
    def __init__(self, *args, **kwargs):
        self.client = SimpleNamespace(ping=lambda: True)
        self.closed = False
        self.items = []

    def enqueue(self, item, task_id=None):
        self.items.append({**item, "task_id": task_id or "outbound-1"})
        return task_id or "outbound-1"

    def consume_once(self, handler, on_dead_letter=None, timeout=1):
        return False

    def update(self, item):
        return None

    def close(self):
        self.closed = True


def test_http_auth_roles_etag_errors_and_audit_filters(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as client:
        assert client.get("/admin/v1/tenants/tenant_demo").status_code == 401
        viewer = {"X-Admin-API-Key": "viewer-key"}
        operator = {"X-Admin-API-Key": "operator-key"}
        assert client.get("/admin/v1/tenants/tenant_demo", headers=viewer).status_code == 200
        assert client.put("/admin/v1/tenants/tenant_demo/config", json={}, headers=viewer).status_code == 403
        assert client.get("/admin/v1/tenants/nope", headers=viewer).status_code == 403
        assert client.get("/admin/v1/tenants/tenant_demo/audit?decision=missing", headers=viewer).json()["count"] == 0
        assert client.get("/admin/v1/tenants/tenant_demo/audit?limit=501", headers=viewer).status_code == 400
        assert client.get("/admin/v1/tenants/missing/health", headers={"X-Admin-API-Key": "root-key"}).status_code == 404
        assert client.post("/admin/v1/tenants", json={}, headers=viewer).status_code == 403

        monkeypatch.setenv("REQUIRE_ETAG", "1")
        assert client.put("/admin/v1/tenants/tenant_demo/config", json={}, headers=operator).status_code == 428
        assert client.put(
            "/admin/v1/tenants/tenant_demo/config", json={}, headers={**operator, "If-Match": "bad"}
        ).status_code == 400
        assert client.put(
            "/admin/v1/tenants/tenant_demo/config", json={}, headers={**operator, "If-Match": '"99"'}
        ).status_code == 412
        assert client.put(
            "/admin/v1/tenants/tenant_demo/config", json={}, headers={**operator, "If-Match": "*"}
        ).status_code == 200


def test_http_webhook_body_limits_and_verification_errors(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as client:
        assert client.get("/webhooks/unknown/account").status_code == 404
        assert client.post(
            "/webhooks/web/web_demo", content=b"not-json", headers={"content-type": "application/json"}
        ).status_code == 400
        assert client.post(
            "/webhooks/web/web_demo", content=b"{}", headers={"content-length": "invalid"}
        ).status_code == 400
        monkeypatch.setenv("WEBHOOK_MAX_BODY_BYTES", "1")
        assert client.post(
            "/webhooks/web/web_demo", content=b"{}", headers={"content-type": "application/json"}
        ).status_code == 400
        monkeypatch.setenv("WEBHOOK_MAX_BODY_BYTES", "1048576")
        accepted = client.post(
            "/webhooks/web/web_demo",
            json={"message_id": "webhook-1", "from_user_id": "user", "text": "hello"},
        )
        assert accepted.status_code == 200
        assert accepted.json()["accepted"] is True
        assert accepted.json()["durable"] is False


def test_http_ui_model_mode_and_webhook_task_unavailable(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as client:
        response = client.post("/ui/api/chat", json={"text": "hello"})
        assert response.status_code == 200
        assert response.json()["model_mode"] == "fallback"

        monkeypatch.setenv("CPA_USE_CODEX_CLI", "1")
        with patch.object(
            CodexCliModelClient,
            "generate_with_usage",
            return_value=ModelResponse("mock codex response"),
        ):
            response = client.post("/ui/api/chat", json={"text": "hello again"})
        assert response.status_code == 200
        assert response.json()["model_mode"] == "codex_cli"
        assert client.get("/admin/v1/webhook-tasks/task", headers={"X-Admin-API-Key": "root-key"}).status_code == 503


def test_http_admin_mutation_error_mapping(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as client:
        headers = {"X-Admin-API-Key": "operator-key"}
        assert client.post(
            "/admin/v1/tenants/tenant_demo/publish", json={"version": "bad"}, headers=headers
        ).status_code == 400
        assert client.post(
            "/admin/v1/tenants/tenant_demo/rollback", json={"version": 99}, headers=headers
        ).status_code == 404
        assert client.post(
            "/admin/v1/tenants/tenant_demo/gray-release", json={"percent": 101}, headers=headers
        ).status_code == 400
        assert client.post(
            "/admin/v1/tenants/tenant_demo/channels", json={"channel": "web"}, headers=headers
        ).status_code == 400
        assert client.post(
            "/admin/v1/tenants/missing/compensations/replay", json={}, headers={"X-Admin-API-Key": "root-key"}
        ).status_code == 404


def test_http_health_liveness_metrics_ui_and_successful_admin_workflow(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as client:
        live = client.get("/livez")
        assert live.status_code == 200
        assert live.json() == {"status": "ok"}

        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"
        assert health.json()["active_tenants"] >= 1
        ready = client.get("/readyz")
        assert ready.status_code == 200
        assert client.get("/metrics").status_code == 200
        assert "trpc_" in client.get("/metrics").text or "python_info" in client.get("/metrics").text
        ui = client.get("/ui")
        assert ui.status_code == 200
        assert "<!doctype html>" in ui.text.lower()

        root = {"X-Admin-API-Key": "root-key"}
        monkeypatch.setenv("PUBLIC_SURFACE_AUTH_REQUIRED", "1")
        assert client.get("/metrics").status_code == 401
        assert client.get("/ui").status_code == 401
        assert client.post("/ui/api/chat", json={"text": "blocked"}).status_code == 401
        assert client.get("/metrics", headers=root).status_code == 200
        assert client.get("/ui", headers=root).status_code == 200
        assert client.post(
            "/ui/api/chat",
            json={"text": "viewer must be tenant-scoped"},
            headers={"X-Admin-API-Key": "viewer-key"},
        ).status_code == 200
        monkeypatch.setenv("PUBLIC_SURFACE_AUTH_REQUIRED", "0")

        new_tenant = {
            "tenant_id": "tenant-new",
            "apps": [
                {
                    "agent_app_id": "app-new",
                    "agent_name": "new-agent",
                    "model_config": {"provider": "test", "model": "test-model"},
                }
            ],
            "channel_bindings": [
                {
                    "channel": "web",
                    "account_id": "web-new",
                    "agent_app_id": "app-new",
                }
            ],
        }
        created = client.post("/admin/v1/tenants", json=new_tenant, headers=root)
        assert created.status_code == 200
        assert created.json()["tenant_id"] == "tenant-new"
        version = created.json()["config_version"]
        fetched = client.get("/admin/v1/tenants/tenant-new", headers=root)
        assert fetched.status_code == 200
        assert fetched.headers["etag"] == f'"{version}"'

        updated = client.put(
            "/admin/v1/tenants/tenant-new/config",
            json={"quota_policy": {"qps_limit": 8}},
            headers={**root, "If-Match": str(version)},
        )
        assert updated.status_code == 200
        next_version = updated.json()["config_version"]
        assert next_version == version + 1
        published = client.post(
            "/admin/v1/tenants/tenant-new/publish",
            json={"version": next_version},
            headers={**root, "If-Match": str(version)},
        )
        assert published.status_code == 200
        gray = client.post(
            "/admin/v1/tenants/tenant-new/gray-release",
            json={"candidate_version": next_version, "percent": 10},
            headers={**root, "If-Match": str(next_version)},
        )
        assert gray.status_code == 200
        added = client.post(
            "/admin/v1/tenants/tenant-new/channels",
            json={"channel": "web", "account_id": "web-new-2", "agent_app_id": "app-new"},
            headers={**root, "If-Match": str(gray.json()["config_version"])},
        )
        assert added.status_code == 200
        assert any(item["account_id"] == "web-new-2" for item in added.json()["channel_bindings"])
        tenant_health = client.get("/admin/v1/tenants/tenant-new/health", headers=root)
        assert tenant_health.status_code == 200
        assert tenant_health.json()["tenant_id"] == "tenant-new"


def test_http_webhook_verification_and_admin_compensation_success(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as client:
        assert client.get("/webhooks/web/web_demo").status_code == 200
        assert client.get("/webhooks/web/web_demo").text == "ok"
        assert client.get("/webhooks/unknown/web_demo").status_code == 404
        root = {"X-Admin-API-Key": "root-key"}
        replay = client.post("/admin/v1/tenants/tenant_demo/compensations/replay", json={"limit": 1}, headers=root)
        assert replay.status_code == 200
        assert replay.json()["tenant_id"] == "tenant_demo"


def test_http_ui_chat_supports_groups_attachments_revoke_and_configured_model(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as client:
        attached = client.post(
            "/ui/api/chat",
            json={
                "message_id": "ui-attachment",
                "user_id": "ui-user",
                "group_id": "group-1",
                "text": "inspect this",
                "attachments": [
                    {
                        "kind": "file",
                        "name": "note.txt",
                        "content_type": "text/plain",
                        "metadata": {"content_base64": "aGVsbG8="},
                    }
                ],
            },
        )
        assert attached.status_code == 200
        assert attached.json()["session_id"]

        revoked = client.post(
            "/ui/api/chat",
            json={"message_id": "ui-revoke", "user_id": "ui-user", "text": "", "event": "revoke"},
        )
        assert revoked.status_code == 200

        monkeypatch.setenv("CPA_BASE_URL", "https://model.example")
        monkeypatch.setenv("OPENAI_API_KEY", "configured-for-test")
        with patch.object(
            ResponsesModelClient,
            "generate_with_usage",
            return_value=ModelResponse("configured response"),
        ):
            configured = client.post("/ui/api/chat", json={"text": "configured"})
        assert configured.status_code == 200
        assert configured.json()["model_mode"] == "responses"


def test_http_ui_attachment_url_is_materialized_only_for_allowlisted_host(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")

    class Response:
        headers = {"Content-Type": "text/plain", "Content-Length": "5"}

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return False

        def read(self, _limit):
            return b"hello"

    with make_client(tmp_path, monkeypatch, raise_server_exceptions=False) as client:
        root = {"X-Admin-API-Key": "root-key"}
        created = client.post(
            "/admin/v1/tenants",
            json={
                "tenant_id": "tenant-attachments",
                "apps": [
                    {
                        "agent_app_id": "app-attachments",
                        "agent_name": "attachments",
                        "model_config": {"provider": "test", "model": "test"},
                    }
                ],
                "channel_bindings": [
                    {
                        "channel": "web",
                        "account_id": "web-attachments",
                        "agent_app_id": "app-attachments",
                        "config": {"attachment_host_allowlist": ["cdn.example"]},
                    }
                ],
            },
            headers=root,
        )
        assert created.status_code == 200

        monkeypatch.setattr(web_app, "urlopen", lambda *args, **kwargs: Response())
        materialized = client.post(
            "/ui/api/chat",
            json={
                "channel": "web",
                "account_id": "web-attachments",
                "message_id": "url-attachment",
                "text": "inspect",
                "attachments": [{"kind": "file", "url": "https://cdn.example/report.txt"}],
            },
        )
        assert materialized.status_code == 200

        rejected = client.post(
            "/ui/api/chat",
            json={
                "channel": "web",
                "account_id": "web-attachments",
                "message_id": "blocked-attachment",
                "text": "blocked",
                "attachments": [{"kind": "file", "url": "https://other.example/report.txt"}],
            },
        )
        assert rejected.status_code == 500


def test_http_ui_duplicate_message_returns_idempotent_result(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as client:
        payload = {"channel": "web", "account_id": "web_demo", "message_id": "duplicate-ui", "text": "same"}
        first = client.post("/ui/api/chat", json=payload)
        second = client.post("/ui/api/chat", json=payload)
        assert first.status_code == 200
        assert second.status_code == 200
        assert second.json()["duplicate"] is True
        assert second.json()["answer"] == first.json()["answer"]


def test_http_durable_webhook_queue_accepts_and_authorizes_task_status(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")
    with patch.object(web_app, "DurableWebhookQueue", FakeWebhookQueue), make_client(
        tmp_path,
        monkeypatch,
        REDIS_URL="redis://protocol-test",
        WEBHOOK_DURABLE_QUEUE="1",
        COMPENSATION_WORKER="0",
    ) as client:
        response = client.post(
            "/webhooks/web/web_demo",
            json={"message_id": "durable-1", "from_user_id": "user", "text": "hello"},
        )
        assert response.status_code == 200
        assert response.json()["durable"] is True
        task_id = response.json()["task_id"]
        status = client.get(
            f"/admin/v1/webhook-tasks/{task_id}",
            headers={"X-Admin-API-Key": "viewer-key"},
        )
        assert status.status_code == 200
        assert status.json()["tenant_id"] == "tenant_demo"
        assert client.get(
            "/admin/v1/webhook-tasks/missing",
            headers={"X-Admin-API-Key": "viewer-key"},
        ).status_code == 404
        assert client.app.state.webhook_queue.closed is False


def test_http_outbound_queue_accepts_non_web_reply(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")
    from trpc_service.gateway import AgentGateway

    with patch.object(web_app, "OutboundDeliveryQueue", FakeOutboundQueue), patch.object(
        AgentGateway, "_durable_inbox_enabled", return_value=False
    ), make_client(
        tmp_path,
        monkeypatch,
        OUTBOUND_QUEUE_URL="redis://protocol-test",
        OUTBOUND_QUEUE_ENABLED="1",
        OUTBOUND_QUEUE_CONSUMER="0",
        COMPENSATION_WORKER="0",
    ) as client:
        response = client.post(
            "/ui/api/chat",
            json={"channel": "telegram", "account_id": "corp_account_1", "text": "queued"},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["accepted"] is True
        assert body["queued"] is True
        assert body["durable"] is True
        assert client.app.state.outbound_queue.items[0]["channel"] == "telegram"


def test_http_outbound_consumer_delivers_resumes_and_dead_letters(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")
    from trpc_service.channels.base import SendResult
    from trpc_service.gateway import AgentGateway

    class ManualOutboundQueue(FakeOutboundQueue):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.handler = None
            self.dead_letter = None

        def consume_once(self, handler, on_dead_letter=None, timeout=1):
            del timeout
            self.handler = handler
            self.dead_letter = on_dead_letter
            __import__("time").sleep(0.005)
            return False

    with patch.object(web_app, "OutboundDeliveryQueue", ManualOutboundQueue), patch.object(
        AgentGateway, "_durable_inbox_enabled", return_value=False
    ), patch.object(
        web_app, "send_with_retry", return_value=SendResult(True, "sent", metadata={"message_type": "text"})
    ):
        with make_client(
            tmp_path,
            monkeypatch,
            OUTBOUND_QUEUE_URL="redis://protocol-test",
            OUTBOUND_QUEUE_ENABLED="1",
            OUTBOUND_QUEUE_CONSUMER="1",
            COMPENSATION_WORKER="0",
        ) as client:
            response = client.post(
                "/ui/api/chat",
                json={"channel": "telegram", "account_id": "corp_account_1", "message_id": "consume-1", "text": "queued"},
            )
            assert response.status_code == 200
            queue = client.app.state.outbound_queue
            assert queue.handler is not None
            item = queue.items[0]
            queue.handler(item)
            assert item["completed_parts"] == [0]
            assert item["results"] == [{"message_type": "text"}]

            # A retry can safely resume from the persisted completed-part set.
            queue.handler(item)
            assert item["completed_parts"] == [0]

            second = client.post(
                "/ui/api/chat",
                json={"channel": "telegram", "account_id": "corp_account_1", "message_id": "consume-2", "text": "dead-letter"},
            )
            assert second.status_code == 200
            dead_item = queue.items[1]
            queue.dead_letter(dead_item, "provider unavailable")
            monkeypatch.setattr(
                client.app.state.gateway.storage_manager,
                "get",
                lambda *_args: (_ for _ in ()).throw(RuntimeError("storage unavailable")),
            )
            queue.dead_letter(dead_item, "storage failure")
            assert queue.dead_letter is not None


def test_http_outbound_delivery_failure_and_exception_are_recorded(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")
    from trpc_service.channels.base import SendResult

    with make_client(tmp_path, monkeypatch, raise_server_exceptions=False) as client:
        with patch.object(web_app, "send_with_retry", return_value=SendResult(False, "", "provider down")):
            failed = client.post("/ui/api/chat", json={"message_id": "failed-1", "text": "fail"})
        assert failed.status_code == 200
        assert failed.json()["ok"] is False
        assert failed.json()["reply"]["parts"] == 1
        assert failed.json()["reply"]["results"] == [{}]

        with patch.object(web_app, "send_with_retry", side_effect=RuntimeError("transport down")):
            errored = client.post(
                "/ui/api/chat",
                json={"message_id": "error-1", "text": "error"},
            )
        assert errored.status_code == 500


def test_http_health_reports_dependency_failure(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch) as client:
        client.app.state.gateway.worker_queue = SimpleNamespace(
            client=SimpleNamespace(ping=lambda: (_ for _ in ()).throw(RuntimeError("redis down")))
        )
        response = client.get("/health")
        assert response.status_code == 503
        assert response.json()["status"] == "degraded"
        assert response.json()["errors"]["worker_queue"] == "RuntimeError"


def test_create_app_queue_failure_is_optional_or_fatal_by_policy(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")

    with patch.object(web_app, "DurableWebhookQueue", side_effect=RuntimeError("redis unavailable")), make_client(
        tmp_path,
        monkeypatch,
        REDIS_URL="redis://protocol-test",
        WEBHOOK_DURABLE_QUEUE="1",
        REQUIRE_DURABLE_WEBHOOK="0",
        COMPENSATION_WORKER="0",
    ) as client:
        assert client.app.state.webhook_queue is None
        assert client.app.state.webhook_queue_error == "RuntimeError"

    with patch.object(web_app, "DurableWebhookQueue", side_effect=RuntimeError("redis unavailable")):
        with pytest.raises(RuntimeError, match="required but unavailable"):
            make_client(
                tmp_path,
                monkeypatch,
                REDIS_URL="redis://protocol-test",
                WEBHOOK_DURABLE_QUEUE="1",
                REQUIRE_DURABLE_WEBHOOK="1",
                COMPENSATION_WORKER="0",
            )


def test_create_app_outbound_queue_failure_is_optional_or_fatal_by_policy(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")

    with patch.object(web_app, "OutboundDeliveryQueue", side_effect=RuntimeError("queue unavailable")), make_client(
        tmp_path,
        monkeypatch,
        OUTBOUND_QUEUE_URL="redis://protocol-test",
        OUTBOUND_QUEUE_ENABLED="1",
        REQUIRE_OUTBOUND_QUEUE="0",
        COMPENSATION_WORKER="0",
    ) as client:
        assert client.app.state.outbound_queue is None
        assert client.app.state.outbound_queue_error == "RuntimeError"

    with patch.object(web_app, "OutboundDeliveryQueue", side_effect=RuntimeError("queue unavailable")):
        with pytest.raises(RuntimeError, match="required but unavailable"):
            make_client(
                tmp_path,
                monkeypatch,
                OUTBOUND_QUEUE_URL="redis://protocol-test",
                OUTBOUND_QUEUE_ENABLED="1",
                REQUIRE_OUTBOUND_QUEUE="1",
                COMPENSATION_WORKER="0",
            )


def test_create_app_wecom_ai_bot_binding_starts_and_stops_connector(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")

    async def run_connector(*args, **kwargs):
        return None

    with patch.object(web_app.WeComAIBotConnector, "run", side_effect=run_connector) as run, make_client(
        tmp_path,
        monkeypatch,
        WECOM_AI_BOT_ENABLED="1",
        WECOM_AI_BOT_ACCOUNT_ID="bot-account",
        COMPENSATION_WORKER="0",
    ) as client:
        assert run.called


def test_http_ui_materializes_telegram_and_wechat_media(monkeypatch, tmp_path):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")
    from trpc_service.channels.base import SendResult

    class Response:
        def __init__(self, body, content_type):
            self.body = body
            self.headers = {"Content-Type": content_type}

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, limit=-1):
            return self.body[:limit] if limit >= 0 else self.body

    def fake_urlopen(request, timeout=15):
        del timeout
        url = request.full_url
        if "/getFile?" in url:
            return Response(b'{"ok":true,"result":{"file_path":"docs/report.txt"}}', "application/json")
        if "/file/bot" in url:
            return Response(b"telegram-content", "text/plain")
        return Response(b"wechat-content", "application/octet-stream")

    monkeypatch.setenv(
        "SECRETS_JSON",
        '{"secret://tenant_demo/telegram/token":"telegram-token",'
        '"secret://tenant_demo/telegram/secret":"telegram-secret",'
        '"secret://tenant_demo/wecom/token":"wecom-token"}',
    )
    monkeypatch.setattr(web_app, "urlopen", fake_urlopen)
    monkeypatch.setattr(
        web_app,
        "send_with_retry",
        lambda *_args, **_kwargs: SendResult(True, "sent", metadata={"message_type": "text"}),
    )
    with make_client(
        tmp_path,
        monkeypatch,
        raise_server_exceptions=False,
        ENABLE_LEGACY_WECOM="1",
    ) as client:
        telegram = client.post(
            "/ui/api/chat",
            json={
                "channel": "telegram",
                "account_id": "corp_account_1",
                "message_id": "telegram-media",
                "user_id": "user-1",
                "text": "summarize",
                "attachments": [
                    {"kind": "file", "metadata": {"file_id": "file-1"}},
                ],
            },
        )
        assert telegram.status_code == 200
        assert telegram.json()["ok"] is True

        wechat = client.post(
            "/ui/api/chat",
            json={
                "channel": "wecom",
                "account_id": "corp_account_1",
                "message_id": "wechat-media",
                "user_id": "user-2",
                "text": "inspect",
                "attachments": [
                    {"kind": "file", "metadata": {"media_id": "media-1"}},
                ],
            },
        )
        assert wechat.status_code == 200

        monkeypatch.setattr(
            web_app,
            "urlopen",
            lambda *_args, **_kwargs: Response(b'{"ok":false}', "application/json"),
        )
        bad_telegram = client.post(
            "/ui/api/chat",
            json={
                "channel": "telegram",
                "account_id": "corp_account_1",
                "message_id": "telegram-bad-media",
                "user_id": "user-1",
                "text": "bad",
                "attachments": [{"kind": "file", "metadata": {"file_id": "missing"}}],
            },
        )
        assert bad_telegram.status_code == 500

        monkeypatch.setattr(
            web_app,
            "urlopen",
            lambda *_args, **_kwargs: Response(b"{invalid", "application/json"),
        )
        bad_wechat = client.post(
            "/ui/api/chat",
            json={
                "channel": "wecom",
                "account_id": "corp_account_1",
                "message_id": "wechat-bad-media",
                "user_id": "user-2",
                "text": "bad",
                "attachments": [{"kind": "file", "metadata": {"media_id": "missing"}}],
            },
        )
        assert bad_wechat.status_code == 500

        monkeypatch.setattr(
            web_app,
            "urlopen",
            lambda *_args, **_kwargs: Response(b"too-large", "text/plain"),
        )
        monkeypatch.setenv("MAX_ATTACHMENT_BYTES", "1")
        too_large = client.post(
            "/ui/api/chat",
            json={
                "channel": "wecom",
                "account_id": "corp_account_1",
                "message_id": "wechat-large-media",
                "user_id": "user-2",
                "text": "large",
                "attachments": [{"kind": "file", "metadata": {"media_id": "large"}}],
            },
        )
        assert too_large.status_code == 500


def test_http_admin_exception_mappings_cover_repository_conflicts(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")
    from trpc_service.tenant.repository import TenantNotFound, TenantRepositoryError
    from trpc_service.tenant.service import TenantConfigConflict

    with make_client(tmp_path, monkeypatch) as client:
        root = {"X-Admin-API-Key": "root-key"}
        admin = client.app.state.admin

        monkeypatch.setattr(admin, "get_tenant", lambda *_args: (_ for _ in ()).throw(TenantNotFound("gone")))
        assert client.get("/admin/v1/tenants/tenant_demo", headers=root).status_code == 404
        monkeypatch.setattr(admin, "get_tenant", lambda *_args: (_ for _ in ()).throw(KeyError("bad-version")))
        assert client.get("/admin/v1/tenants/tenant_demo", headers=root).status_code == 400

        monkeypatch.setattr(
            admin,
            "update_config",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(TenantConfigConflict("conflict")),
        )
        assert client.put("/admin/v1/tenants/tenant_demo/config", json={}, headers=root).status_code == 412
        monkeypatch.setattr(
            admin,
            "update_config",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(TenantRepositoryError("repository")),
        )
        assert client.put("/admin/v1/tenants/tenant_demo/config", json={}, headers=root).status_code == 400

        monkeypatch.setattr(
            admin,
            "publish",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(TenantConfigConflict("publish-conflict")),
        )
        assert client.post(
            "/admin/v1/tenants/tenant_demo/publish", json={"version": 1}, headers=root
        ).status_code == 412
        monkeypatch.setattr(
            admin,
            "rollback",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(TenantNotFound("rollback-missing")),
        )
        assert client.post(
            "/admin/v1/tenants/tenant_demo/rollback", json={"version": 1}, headers=root
        ).status_code == 404
        monkeypatch.setattr(
            admin,
            "configure_gray_release",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(TenantRepositoryError("gray-error")),
        )
        assert client.post(
            "/admin/v1/tenants/tenant_demo/gray-release", json={"percent": 10}, headers=root
        ).status_code == 400
        monkeypatch.setattr(
            admin,
            "add_channel",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(TenantConfigConflict("channel-conflict")),
        )
        assert client.post(
            "/admin/v1/tenants/tenant_demo/channels",
            json={"channel": "web", "account_id": "new", "agent_app_id": "app_support"},
            headers=root,
        ).status_code == 412

        monkeypatch.setattr(
            client.app.state.gateway.tenants,
            "resolve_binding",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(TenantRepositoryError("webhook-error")),
        )
        webhook = client.post(
            "/webhooks/web/web_demo",
            json={"message_id": "exception-map", "from_user_id": "user", "text": "hello"},
        )
        assert webhook.status_code == 400


def test_http_webhook_verification_maps_value_and_unexpected_errors(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")

    class ValueErrorAdapter:
        def verify_handshake(self, payload, binding):
            del payload, binding
            raise ValueError("bad handshake")

    with patch.object(web_app, "default_channel_adapters", return_value={"web": ValueErrorAdapter()}):
        with make_client(tmp_path, monkeypatch) as client:
            response = client.get("/webhooks/web/web_demo")
            assert response.status_code == 400
            assert response.json()["detail"] == "webhook verification failed"

    class RuntimeErrorAdapter:
        def verify_handshake(self, payload, binding):
            del payload, binding
            raise RuntimeError("unexpected")

    with patch.object(web_app, "default_channel_adapters", return_value={"web": RuntimeErrorAdapter()}):
        with make_client(tmp_path, monkeypatch) as client:
            response = client.get("/webhooks/web/web_demo")
            assert response.status_code == 400
            assert response.json()["detail"] == "webhook processing failed"


def test_app_lifespan_starts_and_stops_all_background_consumers(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")

    class SlowWebhookQueue(FakeWebhookQueue):
        def consume_once(self, handler, timeout=1):
            del handler, timeout
            __import__("time").sleep(0.005)
            return False

    class SlowOutboundQueue(FakeOutboundQueue):
        def consume_once(self, handler, on_dead_letter=None, timeout=1):
            del handler, on_dead_letter, timeout
            __import__("time").sleep(0.005)
            return False

    with patch.object(web_app, "DurableWebhookQueue", SlowWebhookQueue), patch.object(
        web_app, "OutboundDeliveryQueue", SlowOutboundQueue
    ):
        with make_client(
            tmp_path,
            monkeypatch,
            REDIS_URL="redis://protocol-test",
            WEBHOOK_DURABLE_QUEUE="1",
            OUTBOUND_QUEUE_URL="redis://protocol-test",
            OUTBOUND_QUEUE_ENABLED="1",
            OUTBOUND_QUEUE_CONSUMER="1",
            COMPENSATION_WORKER="1",
            COMPENSATION_INTERVAL_SECONDS="0.01",
        ) as client:
            assert client.app.state.webhook_consumer_thread.is_alive()
            assert client.app.state.outbound_consumer_thread.is_alive()
            assert client.app.state.compensation_thread.is_alive()
            assert client.get("/health").status_code == 200
        assert client.app.state.webhook_queue.closed
        assert client.app.state.outbound_queue.closed


def test_app_lifespan_consumes_webhooks_and_routes_wecom_ai_bot_to_queue(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")

    class InvokingWebhookQueue(FakeWebhookQueue):
        invoked = False

        def consume_once(self, handler, timeout=1):
            del timeout
            if not self.invoked:
                self.invoked = True
                handler(
                    {
                        "channel": "web",
                        "account_id": "web_demo",
                        "payload": {
                            "message_id": "queue-message",
                            "from_user_id": "queue-user",
                            "text": "queued",
                            "callback_verified": True,
                        },
                        "traceparent": "00-queue-trace",
                    }
                )
            __import__("time").sleep(0.005)
            return False

    class Connector:
        _stop_events = {}

        async def run(self, binding, sink, stop):
            await sink(
                SimpleNamespace(
                    external_message_id="ai-message",
                    external_user_id="ai-user",
                    group_id=None,
                    text="hello from bot",
                    attachments=[],
                    raw_event={"source": "test"},
                ),
                binding,
            )
            stop.set()

        def stop(self, binding_id):
            self._stop_events.pop(binding_id, None)

    with patch.object(web_app, "DurableWebhookQueue", InvokingWebhookQueue), patch.object(
        web_app, "WeComAIBotConnector", Connector
    ), make_client(
        tmp_path,
        monkeypatch,
        REDIS_URL="redis://protocol-test",
        WEBHOOK_DURABLE_QUEUE="1",
        WECOM_AI_BOT_ENABLED="1",
        WECOM_AI_BOT_ACCOUNT_ID="bot-account",
        COMPENSATION_WORKER="0",
    ) as client:
        __import__("time").sleep(0.03)
        queue = client.app.state.webhook_queue
        assert queue.invoked
        assert any(item["payload"]["message_id"] == "ai-message" for item in queue.items.values())


def _attachment_helpers(client):
    """Return the closure-bound attachment helpers used by the UI pipeline."""
    ui_route = next(route for route in client.app.routes if route.path == "/ui/api/chat")
    build = next(cell.cell_contents for cell in ui_route.endpoint.__closure__
                 if getattr(cell.cell_contents, "__name__", "") == "build_ui_result")
    persist = next(cell.cell_contents for cell in build.__closure__
                   if getattr(cell.cell_contents, "__name__", "") == "persist_inbound_attachments")
    helpers = {
        cell.cell_contents.__name__: cell.cell_contents
        for cell in persist.__closure__
        if callable(cell.cell_contents)
    }
    helpers["persist_inbound_attachments"] = persist
    return helpers


def test_attachment_helpers_cover_url_and_provider_error_protocols(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")

    class ContentTypeHeaders(dict):
        def get_content_type(self):
            return "application/custom"

    class Response:
        def __init__(self, body, headers=None):
            self.body = body
            self.headers = headers if headers is not None else {"Content-Type": "text/plain; charset=utf-8"}

        def __enter__(self): return self
        def __exit__(self, *_args): return False
        def read(self, limit=-1): return self.body[:limit] if limit >= 0 else self.body

    with make_client(tmp_path, monkeypatch, COMPENSATION_WORKER="0") as client:
        helpers = _attachment_helpers(client)
        binding = ChannelBinding(
            tenant_id="tenant_demo", binding_id="web:web_demo", channel="web",
            account_id="web_demo", agent_app_id="app_support",
            config={"attachment_host_allowlist": ["cdn.example"]},
        )
        monkeypatch.setattr(web_app, "urlopen", lambda *_args, **_kwargs: Response(
            b"content", ContentTypeHeaders()
        ))
        content, content_type = helpers["download_allowed_url"](
            "https://cdn.example/file.bin", {"cdn.example"}, 10
        )
        assert content == b"content" and content_type == "application/custom"
        with pytest.raises(ValueError, match="http or https"):
            helpers["download_allowed_url"]("ftp://cdn.example/file", {"cdn.example"}, 10)
        with pytest.raises(ValueError, match="allowlist"):
            helpers["download_allowed_url"]("https://other.example/file", {"cdn.example"}, 10)

        no_token = ChannelBinding(
            tenant_id="tenant_demo", binding_id="telegram:corp_account_1", channel="telegram",
            account_id="corp_account_1", agent_app_id="app_support",
        )
        assert helpers["download_telegram_file"](no_token, "file", 10) == (None, None, None)
        assert helpers["download_wechat_media"](no_token, "media", 10) == (None, None)

        token_binding = ChannelBinding(
            tenant_id="tenant_demo", binding_id="telegram:corp_account_1", channel="telegram",
            account_id="corp_account_1", agent_app_id="app_support",
            token_ref="secret://tenant_demo/telegram/token",
        )
        monkeypatch.setenv(
            "SECRETS_JSON",
            '{"secret://tenant_demo/telegram/token":"token"}',
        )
        monkeypatch.setattr(web_app, "urlopen", lambda *_args, **_kwargs: Response(
            b'{"ok":true,"result":{}}', {"Content-Type": "application/json"}
        ))
        with pytest.raises(ValueError, match="file_path"):
            helpers["download_telegram_file"](token_binding, "file", 100)

        wechat_binding = ChannelBinding(
            tenant_id="tenant_demo", binding_id="wecom:corp_account_1", channel="wecom",
            account_id="corp_account_1", agent_app_id="app_support",
            token_ref="secret://tenant_demo/telegram/token",
            config={"api_base_url": "https://wechat.example"},
        )
        monkeypatch.setattr(web_app, "urlopen", lambda *_args, **_kwargs: Response(
            b'{"errcode":40001}', {"Content-Type": "application/json"}
        ))
        with pytest.raises(ValueError, match="WeChat media download failed"):
            helpers["download_wechat_media"](wechat_binding, "media", 100)


def test_attachment_materialization_skips_and_enforces_limits(tmp_path, monkeypatch):
    with make_client(tmp_path, monkeypatch, COMPENSATION_WORKER="0") as client:
        helpers = _attachment_helpers(client)
        class Artifacts:
            def __init__(self): self.items = []
            def put(self, tenant_id, content, content_type):
                self.items.append((tenant_id, content, content_type))
                return SimpleNamespace(
                    object_id="artifact-1", tenant_id=tenant_id,
                    size=len(content), content_type=content_type,
                )

        storage = SimpleNamespace(artifacts=Artifacts())
        binding = ChannelBinding(
            tenant_id="tenant_demo", binding_id="web:web_demo", channel="web",
            account_id="web_demo", agent_app_id="app_support",
        )
        skipped = Attachment(kind="file", name="skip", metadata={})
        inline = Attachment(
            kind="file", name="inline", content_type="text/plain",
            metadata={"content_base64": "aGVsbG8="},
        )
        inbound = SimpleNamespace(attachments=[skipped, inline])
        helpers["persist_inbound_attachments"](inbound, storage, "tenant_demo", binding)
        assert len(storage.artifacts.items) == 1
        assert inline.url is None and "content_base64" not in inline.metadata
        assert inline.metadata["materialized_from"] == "inline_base64"

        monkeypatch.setenv("MAX_ATTACHMENT_BYTES", "2")
        oversized = Attachment(kind="file", metadata={"content_base64": "aGVsbG8="})
        with pytest.raises(ValueError, match="exceeds"):
            helpers["persist_inbound_attachments"](
                SimpleNamespace(attachments=[oversized]), storage, "tenant_demo", binding
            )


def test_http_health_and_webhook_error_mappings(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")
    with make_client(tmp_path, monkeypatch, COMPENSATION_WORKER="0") as client:
        repository = client.app.state.admin.tenants.repository
        monkeypatch.setattr(repository, "all_active", lambda: (_ for _ in ()).throw(RuntimeError("db down")))
        response = client.get("/health")
        assert response.status_code == 503
        assert response.json()["errors"]["tenant_repository"] == "RuntimeError"

        gateway = client.app.state.gateway
        for error, status in (
            (QuotaExceeded("quota"), 429),
            (ChannelVerificationError("invalid signature"), 403),
            (PolicyDenied("blocked"), 403),
            (TenantNotFound("missing"), 404),
            (TenantRepositoryError("repository"), 400),
            (KeyError("unknown"), 404),
        ):
            monkeypatch.setattr(
                gateway.tenants,
                "resolve_binding",
                lambda *_args, error=error: (_ for _ in ()).throw(error),
            )
            response = client.get("/webhooks/web/web_demo")
            assert response.status_code == status

        monkeypatch.setattr(gateway.tenants, "resolve_binding", lambda *_args: (_ for _ in ()).throw(KeyError("unknown")))
        response = client.post("/webhooks/web/web_demo", json={"message_id": "x"})
        assert response.status_code == 404

        with patch.object(web_app, "default_channel_adapters", return_value={}):
            pass


def test_http_webhook_task_tenant_binding_and_authorization_errors(tmp_path, monkeypatch):
    import importlib

    web_app = importlib.import_module("trpc_service.web.app")
    with patch.object(web_app, "DurableWebhookQueue", FakeWebhookQueue), make_client(
        tmp_path,
        monkeypatch,
        REDIS_URL="redis://protocol-test",
        WEBHOOK_DURABLE_QUEUE="1",
        COMPENSATION_WORKER="0",
    ) as client:
        queue = client.app.state.webhook_queue
        queue.status = lambda _task: {"task_id": "x", "tenant_id": ""}
        assert client.get(
            "/admin/v1/webhook-tasks/x", headers={"X-Admin-API-Key": "viewer-key"}
        ).status_code == 404
        queue.status = lambda _task: {"task_id": "x", "tenant_id": "tenant_other"}
        assert client.get(
            "/admin/v1/webhook-tasks/x", headers={"X-Admin-API-Key": "viewer-key"}
        ).status_code == 403
