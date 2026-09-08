import base64
import json
from pathlib import Path

from fastapi.testclient import TestClient
from sqlalchemy import func, select

from trpc_service.channels import WeComCrypto
from trpc_service.config import Settings
from trpc_service.gateway import GatewayResponse
from trpc_service.storage.models import InboundMessage
from trpc_service.web import create_app


def make_client() -> TestClient:
    settings = Settings(
        environment="test",
        log_level="CRITICAL",
        database_url="sqlite+pysqlite:///:memory:",
        outbox_worker_enabled=False,
        inbound_worker_enabled=False,
        _env_file=None,
    )
    return TestClient(create_app(settings))


def test_service_info() -> None:
    with make_client() as client:
        response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {
        "name": "trpc-agent-service",
        "version": "0.6.0",
        "environment": "test",
        "docs_url": "/docs",
    }


def test_liveness_preserves_request_id() -> None:
    with make_client() as client:
        response = client.get("/health/live", headers={"x-request-id": "test-request"})

    assert response.status_code == 200
    assert response.headers["x-request-id"] == "test-request"
    assert len(response.headers["x-trace-id"]) == 32
    assert int(response.headers["x-trace-id"], 16) > 0
    assert response.json()["status"] == "ok"


def test_prometheus_metrics_are_exposed() -> None:
    with make_client() as client:
        client.get("/health/live")
        response = client.get("/metrics/")

    assert response.status_code == 200
    assert "trpc_http_requests_total" in response.text


def test_artifact_upload_and_download(tmp_path: Path) -> None:
    settings = Settings(
        environment="test",
        log_level="CRITICAL",
        database_url="sqlite+pysqlite:///:memory:",
        artifact_root=str(tmp_path),
        outbox_worker_enabled=False,
        inbound_worker_enabled=False,
        _env_file=None,
    )
    headers = {"x-gateway-token": "development-only-change-me"}
    with TestClient(create_app(settings)) as client:
        upload = client.post(
            "/gateway/v1/artifacts",
            headers=headers,
            json={
                "tenant_id": "tenant-1",
                "object_key": "reports/result.txt",
                "mime_type": "text/plain",
                "content_base64": base64.b64encode(b"artifact-body").decode(),
            },
        )
        download = client.get(
            "/gateway/v1/artifacts/tenant-1/reports/result.txt", headers=headers
        )
    assert upload.status_code == 201
    assert download.content == b"artifact-body"
    assert download.headers["etag"] == upload.json()["checksum"]


def test_readiness() -> None:
    with make_client() as client:
        response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json()["checks"] == {"application": "ok", "database": "ok"}


def test_gateway_node_registration_and_route_resolution() -> None:
    with make_client() as client:
        unauthorized = client.get("/gateway/v1/nodes")
        nodes = client.get(
            "/gateway/v1/nodes",
            headers={"x-gateway-token": "development-only-change-me"},
        )
        route = client.get(
            "/gateway/v1/routes/resolve",
            params={"tenant_id": "tenant", "agent_app_id": "app", "session_id": "s1"},
            headers={"x-gateway-token": "development-only-change-me"},
        )

    assert unauthorized.status_code == 401
    assert nodes.status_code == 200
    assert len(nodes.json()) == 1
    assert route.status_code == 200
    assert route.json()["node_id"] == nodes.json()[0]["node_id"]


class StubMessageHandler:
    async def handle(self, message, node_id: str) -> GatewayResponse:
        return GatewayResponse(
            "processed", node_id, message.session_id, message.trace_id, "stub reply", 1
        )


def test_gateway_auth_internal_forward_and_telegram_webhook() -> None:
    direct_payload = {
        "tenant_id": "tenant-1",
        "agent_app_id": "app-1",
        "channel": "http",
        "account_id": "api",
        "external_message_id": "message-1",
        "sender_user_id": "user-1",
        "conversation_id": "user-1",
        "conversation_type": "direct",
        "text": "hello",
        "metadata": {},
    }
    with make_client() as client:
        client.app.state.services.gateway_router._handler = StubMessageHandler()

        assert client.post("/gateway/v1/messages", json=direct_payload).status_code == 401
        direct = client.post(
            "/gateway/v1/messages",
            json=direct_payload,
            headers={"x-gateway-token": "development-only-change-me"},
        )
        assert direct.status_code == 200
        assert direct.json()["reply_text"] == "stub reply"

        internal_body = {"message": direct_payload}
        assert client.post("/internal/v1/messages", json=internal_body).status_code == 401
        forwarded = client.post(
            "/internal/v1/messages",
            json=internal_body,
            headers={"x-internal-token": "development-only-change-me"},
        )
        assert forwarded.status_code == 200

        tenant = client.post(
            "/admin/v1/tenants", json={"slug": "telegram-tenant", "name": "Telegram"}
        ).json()
        app = client.post(
            f"/admin/v1/tenants/{tenant['id']}/apps",
            json={"slug": "telegram-agent", "name": "Telegram Agent"},
        ).json()
        draft = {
            "expected_lock_version": app["lock_version"],
            "instruction": "Reply briefly.",
            "model": {
                "provider": "openai-compatible",
                "model_name": "test-model",
                "api_key_secret_ref": "env://TRPC_AGENT_API_KEY",
            },
            "channels": [
                {
                    "channel_type": "telegram",
                    "account_id": "test-bot",
                    "webhook_path": "/webhooks/telegram/test-bot",
                }
            ],
        }
        updated = client.put(f"/admin/v1/tenants/{tenant['id']}/apps/{app['id']}/draft", json=draft)
        assert updated.status_code == 200
        published = client.post(
            f"/admin/v1/tenants/{tenant['id']}/apps/{app['id']}/publish",
            json={"expected_lock_version": app["lock_version"] + 1},
        )
        assert published.status_code == 200

        telegram = client.post(
            "/webhooks/telegram/test-bot",
            json={
                "update_id": 77,
                "message": {
                    "message_id": 5,
                    "text": "ping",
                    "chat": {"id": 99, "type": "private"},
                    "from": {"id": 88},
                },
            },
        )
        assert telegram.status_code == 200
        assert telegram.json()["status"] == "accepted"
        assert telegram.json()["message_id"]
        queued = client.get(
            f"/gateway/v1/messages/{telegram.json()['message_id']}",
            headers={"x-gateway-token": "development-only-change-me"},
        )
        assert queued.status_code == 200
        assert queued.json()["status"] == "pending"
        assert queued.json()["execution_id"]


def test_wecom_aibot_url_verification_callback_and_deduplication(monkeypatch) -> None:
    aes_key = base64.b64encode(bytes(range(32))).decode().rstrip("=")
    monkeypatch.setenv("WECOM_AIBOT_CALLBACK_TOKEN", "callback-token")
    monkeypatch.setenv("WECOM_AIBOT_ENCODING_AES_KEY", aes_key)
    crypto = WeComCrypto("callback-token", aes_key, "")

    with make_client() as client:
        tenant = client.post(
            "/admin/v1/tenants", json={"slug": "aibot-tenant", "name": "AIBot"}
        ).json()
        app = client.post(
            f"/admin/v1/tenants/{tenant['id']}/apps",
            json={"slug": "aibot-agent", "name": "AIBot Agent"},
        ).json()
        draft = {
            "expected_lock_version": app["lock_version"],
            "instruction": "Reply briefly.",
            "model": {
                "provider": "openai-compatible",
                "model_name": "test-model",
                "api_key_secret_ref": "env://TRPC_AGENT_API_KEY",
            },
            "channels": [
                {
                    "channel_type": "wecom",
                    "account_id": "smart-support",
                    "webhook_path": "/webhooks/wecom/smart-support",
                    "token_secret_ref": "env://WECOM_AIBOT_CALLBACK_TOKEN",
                    "secret_ref": "env://WECOM_AIBOT_ENCODING_AES_KEY",
                    "options": {"mode": "aibot", "aibot_id": "bot-123"},
                }
            ],
        }
        updated = client.put(
            f"/admin/v1/tenants/{tenant['id']}/apps/{app['id']}/draft", json=draft
        )
        assert updated.status_code == 200
        published = client.post(
            f"/admin/v1/tenants/{tenant['id']}/apps/{app['id']}/publish",
            json={"expected_lock_version": app["lock_version"] + 1},
        )
        assert published.status_code == 200

        encrypted_challenge, challenge_signature = crypto.encrypt(
            "verified-challenge", "1700000100", "nonce-get"
        )
        verification = client.get(
            "/webhooks/wecom/smart-support",
            params={
                "msg_signature": challenge_signature,
                "timestamp": "1700000100",
                "nonce": "nonce-get",
                "echostr": encrypted_challenge,
            },
        )
        assert verification.status_code == 200
        assert verification.text == "verified-challenge"

        message = {
            "msgid": "aibot-web-message-1",
            "aibotid": "bot-123",
            "chattype": "single",
            "from": {"userid": "user-9"},
            "response_url": (
                "https://qyapi.weixin.qq.com/cgi-bin/aibot/response?response_code=code"
            ),
            "msgtype": "text",
            "text": {"content": "hello through webhook"},
        }
        encrypted, signature = crypto.encrypt(
            json.dumps(message), "1700000101", "nonce-post"
        )
        request = {
            "params": {
                "msg_signature": signature,
                "timestamp": "1700000101",
                "nonce": "nonce-post",
            },
            "json": {"encrypt": encrypted},
        }
        first = client.post("/webhooks/wecom/smart-support", **request)
        duplicate = client.post("/webhooks/wecom/smart-support", **request)
        assert first.status_code == 200
        assert first.json() == {}
        assert duplicate.status_code == 200
        with client.app.state.database.session_factory() as session:
            count = session.scalar(
                select(func.count()).select_from(InboundMessage).where(
                    InboundMessage.channel == "wecom",
                    InboundMessage.external_message_id == "aibot-web-message-1",
                )
            )
        assert count == 1

        control_event = {
            "msgid": "aibot-enter-chat-1",
            "aibotid": "bot-123",
            "chattype": "single",
            "from": {"userid": "user-9"},
            "msgtype": "event",
            "event": {"eventtype": "enter_chat"},
        }
        event_encrypted, event_signature = crypto.encrypt(
            json.dumps(control_event), "1700000102", "nonce-event"
        )
        event_response = client.post(
            "/webhooks/wecom/smart-support",
            params={
                "msg_signature": event_signature,
                "timestamp": "1700000102",
                "nonce": "nonce-event",
            },
            json={"encrypt": event_encrypted},
        )
        assert event_response.status_code == 200
        assert event_response.json() == {}
        with client.app.state.database.session_factory() as session:
            assert session.scalar(select(func.count()).select_from(InboundMessage)) == 1
