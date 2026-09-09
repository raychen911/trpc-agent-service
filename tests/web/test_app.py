"""Gateway lifecycle, health, and Admin API tests."""

from __future__ import annotations

import asyncio

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from trpc_service.config import Environment, Settings
from trpc_service.storage import Database
from trpc_service.web import create_app


@pytest.fixture
def client(tmp_path):
    database_url = f"sqlite+aiosqlite:///{tmp_path / 'web.db'}"

    async def prepare() -> None:
        database = Database(database_url)
        await database.create_schema()
        await database.dispose()

    asyncio.run(prepare())
    settings = Settings(
        _env_file=None,
        env=Environment.TEST,
        database_url=database_url,
        secret_key=SecretStr("x" * 32),
        admin_api_key=SecretStr("test-admin-key"),
    )
    with TestClient(create_app(settings)) as test_client:
        yield test_client


def _spec() -> dict[str, object]:
    return {
        "tenant_id": "tenant-web",
        "revision": 1,
        "display_name": "Web tenant",
        "apps": [
            {
                "app_id": "assistant",
                "revision": 1,
                "name": "assistant_agent",
                "prompt": "Answer from trusted context.",
                "model": {"provider": "mock", "model": "deterministic"},
            }
        ],
        "channels": [
            {
                "binding_id": "telegram-web",
                "app_id": "assistant",
                "app_revision": 1,
                "channel": "telegram",
                "external_account_id": "bot-web",
                "callback_path": "/v1/channels/telegram/cb-web/callback",
                "public_callback_id": "cb-web",
                "secret_refs": {
                    "bot_token": "secret://env/TEST_TELEGRAM_TOKEN",
                    "webhook_secret": "secret://env/TEST_TELEGRAM_WEBHOOK_SECRET",
                },
            }
        ],
    }


def test_health_and_correlation_headers(client: TestClient) -> None:
    response = client.get("/health/ready", headers={"x-request-id": "req-safe-1"})

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}
    assert response.headers["x-request-id"] == "req-safe-1"
    assert len(response.headers["x-trace-id"]) == 32
    assert response.headers["cache-control"] == "no-store"


def test_console_is_packaged_and_hardened(client: TestClient) -> None:
    root = client.get("/", follow_redirects=False)
    assert root.status_code == 307
    assert root.headers["location"] == "/console"

    console = client.get("/console")
    assert console.status_code == 200
    assert "Agent 平台运行总览" in console.text
    assert "/console/assets/console.js" in console.text
    assert "default-src 'self'" in console.headers["content-security-policy"]
    assert console.headers["x-frame-options"] == "DENY"

    stylesheet = client.get("/console/assets/console.css")
    script = client.get("/console/assets/console.js")
    favicon = client.get("/console/assets/favicon.svg")
    assert stylesheet.status_code == 200
    assert "--accent" in stylesheet.text
    assert script.status_code == 200
    assert "sessionStorage" in script.text
    assert favicon.status_code == 200
    assert favicon.headers["content-type"].startswith("image/svg+xml")


def test_admin_api_requires_key_and_publishes_revision(client: TestClient) -> None:
    path = "/v1/admin/tenants/tenant-web/revisions"
    assert client.post(path, json=_spec()).status_code == 401

    response = client.post(
        path,
        json=_spec(),
        headers={"x-admin-key": "test-admin-key", "x-admin-actor": "admin:test"},
    )
    assert response.status_code == 201
    assert response.json()["revision"] == 1
    assert response.json()["idempotent"] is False

    active = client.get(
        "/v1/admin/tenants/tenant-web/active",
        headers={"x-admin-key": "test-admin-key"},
    )
    assert active.status_code == 200
    assert active.json()["apps"][0]["app_id"] == "assistant"

    overview = client.get(
        "/v1/admin/overview",
        headers={"x-admin-key": "test-admin-key"},
    )
    assert overview.status_code == 200
    payload = overview.json()
    assert payload["environment"] == "test"
    assert payload["database"] == "sqlite"
    assert payload["tenant_count"] == 1
    assert payload["active_tenant_count"] == 1
    assert payload["tenants"][0]["tenant_id"] == "tenant-web"
    assert payload["tenants"][0]["channels"] == ["telegram"]
    assert payload["tenants"][0]["model_routes"] == ["mock / deterministic"]
    assert payload["totals"]["sessions"] == 0

    revisions = client.get(
        "/v1/admin/tenants/tenant-web/revisions",
        headers={"x-admin-key": "test-admin-key"},
    )
    assert revisions.status_code == 200
    assert revisions.json()[0]["revision"] == 1
    assert revisions.json()[0]["active"] is True
    assert len(revisions.json()[0]["content_hash"]) == 64

    activity = client.get(
        "/v1/admin/tenants/tenant-web/activity",
        headers={"x-admin-key": "test-admin-key"},
    )
    assert activity.status_code == 200
    assert activity.json() == []


def test_console_api_requires_admin_key_and_bounds_activity(client: TestClient) -> None:
    assert client.get("/v1/admin/overview").status_code == 401
    response = client.get(
        "/v1/admin/tenants/tenant-web/activity?limit=101",
        headers={"x-admin-key": "test-admin-key"},
    )
    assert response.status_code == 422


def test_admin_rejects_path_body_tenant_mismatch(client: TestClient) -> None:
    response = client.post(
        "/v1/admin/tenants/another/revisions",
        json=_spec(),
        headers={"x-admin-key": "test-admin-key"},
    )
    assert response.status_code == 400
