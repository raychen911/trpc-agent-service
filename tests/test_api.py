from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import yaml
from asgi_lifespan import LifespanManager

from tenant_agent.main import create_app
from tenant_agent.models import AgentEvent, AgentEventType, ModelConfig, SecretRef
from tenant_agent.services.broker import BrokerCapacityError, BrokerMessage, InlineBroker
from tenant_agent.settings import ServiceRole, Settings
from tenant_agent.storage.base import OutboxItem
from tests.helpers import make_envelope, make_tenant


@pytest.mark.asyncio
async def test_web_sync_api_admin_and_metrics(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TENANT_ALPHA_WEBHOOK_TOKEN", "test-web-token")
    tenant = make_tenant(summary_every_events="2")
    bootstrap = tmp_path / "tenants.yaml"
    bootstrap.write_text(
        yaml.safe_dump({"tenants": [tenant.model_dump(mode="json")]}, sort_keys=False),
        encoding="utf-8",
    )
    settings = Settings(
        control_database_url="inmemory://",
        bootstrap_config_path=bootstrap,
        session_hmac_key="a-long-enough-test-session-hmac-key",
        admin_bearer_token="admin-test-token",
        internal_bearer_token="internal-test-token",
        worker_poll_ms=50,
        outbox_poll_seconds=0.05,
    )
    app = create_app(settings)
    async with LifespanManager(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            assert (await client.get("/health/live")).status_code == 200
            assert (await client.get("/health/ready")).status_code == 200
            response = await client.post(
                f"/v1/channels/web/{tenant.channels[0].binding_id}/webhook?synchronous=true",
                headers={"x-webhook-token": "test-web-token"},
                json={
                    "user_id": "browser-user",
                    "conversation_id": "browser-conversation",
                    "message_id": "browser-message-1",
                    "text": "hello browser",
                },
            )
            assert response.status_code == 200
            payload = response.json()["results"][0]
            assert payload["status"] == "processed"
            assert payload["messages"][0]["text"].startswith("Hello! I am TestAssistant")
            assert "hello browser" not in payload["messages"][0]["text"]
            session_id = payload["session_id"]

            unauthorized = await client.get("/admin/v1/tenants")
            assert unauthorized.status_code == 401
            tenants = await client.get(
                "/admin/v1/tenants", headers={"authorization": "Bearer admin-test-token"}
            )
            assert tenants.status_code == 200
            assert tenants.json()["items"][0]["tenant_id"] == "alpha"
            session = await client.get(
                f"/admin/v1/tenants/alpha/sessions/{session_id}",
                headers={"authorization": "Bearer admin-test-token"},
            )
            assert session.status_code == 200
            assert len(session.json()["events"]) == 2
            audit = await client.get(
                "/admin/v1/tenants/alpha/audit",
                headers={"authorization": "Bearer admin-test-token"},
            )
            assert audit.status_code == 200 and audit.json()["items"]

            revision_two = tenant.model_copy(update={"revision": 2, "display_name": "Alpha Revision Two"})
            created = await client.post(
                "/admin/v1/tenants/config-versions",
                headers={
                    "authorization": "Bearer admin-test-token",
                    "x-admin-actor": "test-suite",
                },
                json=revision_two.model_dump(mode="json"),
            )
            assert created.status_code == 201
            activated = await client.post(
                "/admin/v1/tenants/alpha/config-versions/2/activate",
                headers={"authorization": "Bearer admin-test-token"},
            )
            assert activated.status_code == 200
            rollback = await client.post(
                "/admin/v1/tenants/alpha/rollback/1",
                headers={"authorization": "Bearer admin-test-token"},
            )
            assert rollback.status_code == 200
            versions = await client.get(
                "/admin/v1/tenants/alpha/config-versions",
                headers={"authorization": "Bearer admin-test-token"},
            )
            assert {item["revision"] for item in versions.json()["items"]} == {1, 2}
            stored_version = await app.state.container.configs.repository.get_config_version("alpha", 2)
            assert stored_version is not None
            assert stored_version.created_by.startswith("admin-credential:")
            assert stored_version.created_by != "test-suite"
            admin_audit = await client.get(
                "/admin/v1/tenants/alpha/audit",
                headers={"authorization": "Bearer admin-test-token"},
            )
            assert {item["decision"] for item in admin_audit.json()["items"]} >= {
                "config_activate_requested",
                "config_rollback_requested",
            }
            unknown_revision = await client.post(
                "/admin/v1/tenants/alpha/config-versions/999/activate",
                headers={"authorization": "Bearer admin-test-token"},
            )
            assert unknown_revision.status_code == 404

            unknown_tool = tenant.model_copy(
                update={
                    "revision": 3,
                    "apps": {
                        "assistant": tenant.apps["assistant"].model_copy(
                            update={"allowed_tools": frozenset({"unregistered_tool"})}
                        )
                    },
                    "governance": tenant.governance.model_copy(
                        update={
                            "tools": tenant.governance.tools.model_copy(
                                update={"allow": frozenset({"unregistered_tool"})}
                            )
                        }
                    ),
                }
            )
            rejected_activation = await client.post(
                "/admin/v1/tenants/config-versions?activate=true",
                headers={"authorization": "Bearer admin-test-token"},
                json=unknown_tool.model_dump(mode="json"),
            )
            assert rejected_activation.status_code == 422
            active_after_rejection = await client.get(
                "/admin/v1/tenants",
                headers={"authorization": "Bearer admin-test-token"},
            )
            assert active_after_rejection.json()["items"][0]["revision"] == 1

            monkeypatch.setenv("TENANT_ALPHA_MODEL_KEY", "model-key")
            invalid_litellm = tenant.model_copy(
                update={
                    "revision": 4,
                    "models": {
                        "offline": ModelConfig(
                            provider="litellm",
                            model_name="invalid model name",
                            api_key_ref=SecretRef(uri="env://TENANT_ALPHA_MODEL_KEY"),
                        )
                    },
                }
            )
            invalid_runtime = await client.post(
                "/admin/v1/tenants/config-versions?activate=true",
                headers={"authorization": "Bearer admin-test-token"},
                json=invalid_litellm.model_dump(mode="json"),
            )
            assert invalid_runtime.status_code == 422
            active_after_runtime_rejection = await client.get(
                "/admin/v1/tenants",
                headers={"authorization": "Bearer admin-test-token"},
            )
            assert active_after_runtime_rejection.json()["items"][0]["revision"] == 1

            invalid = tenant.model_dump(mode="json")
            invalid["models"]["offline"] = {
                "provider": "openai-compatible",
                "model_name": "remote",
                "api_key_ref": {"uri": "accidentally-pasted-secret-value"},
            }
            validation = await client.post(
                "/admin/v1/tenants/config-versions",
                headers={"authorization": "Bearer admin-test-token"},
                json=invalid,
            )
            assert validation.status_code == 422
            assert "accidentally-pasted-secret-value" not in validation.text

            async with client.stream(
                "POST",
                f"/v1/chat/stream/{tenant.channels[0].binding_id}",
                headers={"x-webhook-token": "test-web-token"},
                json={
                    "user_id": "browser-user",
                    "conversation_id": "stream-conversation",
                    "message_id": "stream-message",
                    "text": "stream safely",
                },
            ) as streamed:
                stream_body = (await streamed.aread()).decode()
            assert streamed.status_code == 200
            assert '"event_type": "text_final"' in stream_body
            assert '"event_type": "text_delta"' not in stream_body
            metrics = await client.get("/metrics")
            assert metrics.status_code == 200
            assert "tenant_agent_requests_total" in metrics.text


@pytest.mark.asyncio
async def test_web_api_rejects_wrong_binding_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TENANT_ALPHA_WEBHOOK_TOKEN", "right")
    tenant = make_tenant()
    bootstrap = tmp_path / "tenants.yaml"
    bootstrap.write_text(
        yaml.safe_dump({"tenants": [tenant.model_dump(mode="json")]}, sort_keys=False),
        encoding="utf-8",
    )
    app = create_app(
        Settings(
            control_database_url="inmemory://",
            bootstrap_config_path=bootstrap,
            session_hmac_key="a-long-enough-test-session-hmac-key",
            admin_bearer_token="admin-test-token",
            internal_bearer_token="internal-test-token",
            worker_poll_ms=50,
        )
    )
    async with LifespanManager(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                f"/v1/channels/web/{tenant.channels[0].binding_id}/webhook?synchronous=true",
                headers={"x-webhook-token": "wrong"},
                json={"user_id": "u", "conversation_id": "c", "text": "hello"},
            )
            assert response.status_code == 401
            assert "right" not in response.text
            stream_response = await client.post(
                f"/v1/chat/stream/{tenant.channels[0].binding_id}",
                headers={"x-webhook-token": "wrong"},
                json={"user_id": "u", "conversation_id": "c", "text": "hello"},
            )
            assert stream_response.status_code == 401
            malformed_length = await client.post(
                f"/v1/channels/web/{tenant.channels[0].binding_id}/webhook?synchronous=true",
                headers={
                    "x-webhook-token": "right",
                    "content-length": "not-a-number",
                },
                content=b"{}",
            )
            assert malformed_length.status_code == 400
            invalid_shape = await client.post(
                f"/v1/channels/web/{tenant.channels[0].binding_id}/webhook?synchronous=true",
                headers={"x-webhook-token": "right"},
                json=["not", "an", "object"],
            )
            assert invalid_shape.status_code == 422

            async def oversized_chunks() -> object:
                yield b"x" * (1024 * 1024)
                yield b"y" * (1024 * 1024 + 1)

            oversized = await client.post(
                f"/v1/channels/web/{tenant.channels[0].binding_id}/webhook?synchronous=true",
                headers={"x-webhook-token": "right"},
                content=oversized_chunks(),
            )
            assert oversized.status_code == 413

            async def reject_at_capacity(*args: object, **kwargs: object) -> str:
                del args, kwargs
                raise BrokerCapacityError("tenant broker queue capacity reached")

            monkeypatch.setattr(app.state.container.broker, "publish", reject_at_capacity)
            routed = app.state.container.gateway.route(make_envelope(tenant), tenant)
            overloaded = await client.post(
                "/internal/v1/inbound",
                headers={"authorization": "Bearer internal-test-token"},
                json=routed.model_dump(mode="json"),
            )
            assert overloaded.status_code == 503
            assert overloaded.headers["retry-after"] == "5"


@pytest.mark.asyncio
async def test_sse_disconnect_cancels_and_awaits_background_turn(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TENANT_ALPHA_WEBHOOK_TOKEN", "right")
    tenant = make_tenant()
    bootstrap = tmp_path / "tenants.yaml"
    bootstrap.write_text(
        yaml.safe_dump({"tenants": [tenant.model_dump(mode="json")]}, sort_keys=False),
        encoding="utf-8",
    )
    app = create_app(
        Settings(
            control_database_url="inmemory://",
            bootstrap_config_path=bootstrap,
            session_hmac_key="a-long-enough-test-session-hmac-key",
            admin_bearer_token="admin-test-token",
            internal_bearer_token="internal-test-token",
            worker_poll_ms=50,
        )
    )
    started = asyncio.Event()
    cancelled = asyncio.Event()

    async with LifespanManager(app):
        container = app.state.container

        async def slow_process(**kwargs: object) -> object:
            sink = kwargs["event_sink"]
            assert callable(sink)
            started.set()
            await sink(  # type: ignore[misc]
                AgentEvent(
                    event_id="partial",
                    event_type=AgentEventType.TEXT_DELTA,
                    text="partial",
                    partial=True,
                )
            )
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        monkeypatch.setattr(container.dispatcher, "process", slow_process)
        request_body = json.dumps(
            {
                "user_id": "user",
                "conversation_id": "conversation",
                "message_id": "disconnect-message",
                "text": "hello",
            }
        ).encode()
        disconnect = asyncio.Event()
        request_sent = False

        async def receive() -> dict[str, object]:
            nonlocal request_sent
            if not request_sent:
                request_sent = True
                return {"type": "http.request", "body": request_body, "more_body": False}
            await disconnect.wait()
            return {"type": "http.disconnect"}

        async def send(message: dict[str, object]) -> None:
            if message["type"] == "http.response.body" and message.get("more_body"):
                disconnect.set()

        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": f"/v1/chat/stream/{tenant.channels[0].binding_id}",
            "raw_path": f"/v1/chat/stream/{tenant.channels[0].binding_id}".encode(),
            "query_string": b"",
            "root_path": "",
            "headers": [
                (b"host", b"test"),
                (b"content-type", b"application/json"),
                (b"content-length", str(len(request_body)).encode()),
                (b"x-webhook-token", b"right"),
            ],
            "client": ("127.0.0.1", 12345),
            "server": ("test", 80),
        }
        await asyncio.wait_for(app(scope, receive, send), timeout=2)  # type: ignore[arg-type]
        await asyncio.wait_for(started.wait(), timeout=1)
        await asyncio.wait_for(cancelled.wait(), timeout=1)


@pytest.mark.asyncio
async def test_admin_can_auditably_recover_tenant_scoped_dead_letters(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("TENANT_ALPHA_WEBHOOK_TOKEN", "right")
    tenant = make_tenant()
    bootstrap = tmp_path / "tenants.yaml"
    bootstrap.write_text(
        yaml.safe_dump({"tenants": [tenant.model_dump(mode="json")]}, sort_keys=False),
        encoding="utf-8",
    )
    app = create_app(
        Settings(
            service_role=ServiceRole.ADMIN,
            control_database_url="inmemory://",
            bootstrap_config_path=bootstrap,
            admin_bearer_token="admin-test-token",
            broker_global_queue_limit=1_000,
        )
    )
    async with LifespanManager(app):
        container = app.state.container
        now = datetime.now(UTC)
        await container.control.enqueue_outbox(  # type: ignore[attr-defined]
            OutboxItem(
                outbox_id="dead-outbox",
                tenant_id=tenant.tenant_id,
                kind="im-delivery",
                payload={"next_segment": 1},
                status="pending",
                attempts=0,
                available_at=now,
            )
        )
        await container.control.claim_outbox("dead-owner", limit=1, now=now)  # type: ignore[attr-defined]
        await container.control.retry_outbox(  # type: ignore[attr-defined]
            "dead-outbox",
            "dead-owner",
            error_type="PermanentDeliveryError",
            available_at=now,
            terminal=True,
        )
        broker = container.broker
        assert isinstance(broker, InlineBroker)
        routed = container.gateway.route(make_envelope(tenant), tenant)
        broker.dead_letters.append(BrokerMessage("dead-broker", routed, attempts=8))

        headers = {
            "authorization": "Bearer admin-test-token",
            "x-admin-actor": "spoofed-actor",
        }
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://test",
        ) as client:
            outbox_rows = await client.get(
                "/admin/v1/tenants/alpha/outbox/dead",
                headers=headers,
            )
            assert outbox_rows.status_code == 200
            assert outbox_rows.json()["items"][0]["next_segment"] == 1
            broker_rows = await client.get(
                "/admin/v1/tenants/alpha/broker/dead",
                headers=headers,
            )
            assert broker_rows.status_code == 200
            assert broker_rows.json()["items"][0]["broker_id"] == "dead-broker"

            outbox_requeue = await client.post(
                "/admin/v1/tenants/alpha/outbox/dead-outbox/requeue",
                headers=headers,
            )
            assert outbox_requeue.status_code == 200
            broker_requeue = await client.post(
                "/admin/v1/tenants/alpha/broker/dead/dead-broker/requeue",
                headers=headers,
            )
            assert broker_requeue.status_code == 200
            missing = await client.post(
                "/admin/v1/tenants/beta/outbox/dead-outbox/requeue",
                headers=headers,
            )
            assert missing.status_code == 404

        audit_repository = await container.storage.audit_for_tenant(tenant)
        audits = await audit_repository.query_audit("alpha", limit=10)
        assert {row.decision for row in audits} >= {
            "outbox_requeue_requested",
            "broker_requeue_requested",
        }
        assert all(row.user_id.startswith("admin-credential:") for row in audits)
        assert all(row.user_id != "spoofed-actor" for row in audits)
