import pytest
from fastapi.testclient import TestClient
from sqlalchemy.exc import IntegrityError

from trpc_service.config import Settings
from trpc_service.storage.models import ModelConfig
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


def create_tenant(client: TestClient, slug: str = "acme") -> dict[str, object]:
    response = client.post(
        "/admin/v1/tenants",
        json={"slug": slug, "name": f"Tenant {slug}", "audit_policy": {"retention_days": 90}},
    )
    assert response.status_code == 201
    return response.json()


def create_agent_app(client: TestClient, tenant_id: str) -> dict[str, object]:
    response = client.post(
        f"/admin/v1/tenants/{tenant_id}/apps",
        json={
            "slug": "support-agent",
            "name": "Support Agent",
            "description": "Customer support",
            "instruction": "Help the customer.",
        },
    )
    assert response.status_code == 201
    return response.json()


def draft_payload(expected_lock_version: int, model_name: str = "gpt-demo") -> dict[str, object]:
    return {
        "expected_lock_version": expected_lock_version,
        "description": "Customer support",
        "instruction": "Help the customer and use approved tools only.",
        "application_config": {"max_turns": 20},
        "model": {
            "provider": "openai-compatible",
            "model_name": model_name,
            "api_key_secret_ref": "env://TRPC_AGENT_API_KEY",
            "parameters": {"temperature": 0.2},
        },
        "tools": [
            {
                "tool_name": "ticket_lookup",
                "effect": "allow",
                "requires_confirmation": False,
                "constraints": {"timeout_seconds": 5},
            }
        ],
        "channels": [
            {
                "channel_type": "wecom",
                "account_id": "corp-account",
                "webhook_path": "/hooks/wecom/corp-account",
                "token_secret_ref": "vault://channels/wecom/token",
                "secret_ref": "vault://channels/wecom/secret",
                "options": {"reply_timeout_seconds": 4},
            }
        ],
        "backends": [
            {
                "backend_kind": "session",
                "backend_type": "redis",
                "secret_ref": "vault://storage/redis/url",
                "options": {"key_prefix": "sessions"},
            },
            {
                "backend_kind": "audit",
                "backend_type": "sql",
                "secret_ref": "vault://storage/sql/url",
                "options": {},
            },
        ],
    }


def test_tenant_crud_and_conflicts() -> None:
    with make_client() as client:
        tenant = create_tenant(client)

        listed = client.get("/admin/v1/tenants")
        assert listed.status_code == 200
        assert [item["id"] for item in listed.json()] == [tenant["id"]]

        updated = client.patch(
            f"/admin/v1/tenants/{tenant['id']}",
            json={"expected_version": 1, "name": "Acme Updated"},
        )
        assert updated.status_code == 200
        assert updated.json()["version"] == 2

        stale = client.patch(
            f"/admin/v1/tenants/{tenant['id']}",
            json={"expected_version": 1, "name": "Stale"},
        )
        assert stale.status_code == 409
        assert stale.json()["error"]["code"] == "conflict"

        duplicate = client.post("/admin/v1/tenants", json={"slug": "acme", "name": "Duplicate"})
        assert duplicate.status_code == 409


def test_versioned_app_publish_and_rollback() -> None:
    with make_client() as client:
        tenant = create_tenant(client)
        app = create_agent_app(client, str(tenant["id"]))
        tenant_id = str(tenant["id"])
        app_id = str(app["id"])

        draft = client.put(
            f"/admin/v1/tenants/{tenant_id}/apps/{app_id}/draft",
            json=draft_payload(expected_lock_version=1),
        )
        assert draft.status_code == 200
        assert draft.json()["version"] == 1
        assert draft.json()["model"]["model_name"] == "gpt-demo"

        published = client.post(
            f"/admin/v1/tenants/{tenant_id}/apps/{app_id}/publish",
            json={"expected_lock_version": 2},
        )
        assert published.status_code == 200
        assert published.json()["active_version"] == 1
        assert published.json()["draft_version"] == 2
        assert published.json()["lock_version"] == 3

        cloned = client.get(f"/admin/v1/tenants/{tenant_id}/apps/{app_id}/draft")
        assert cloned.status_code == 200
        assert cloned.json()["version"] == 2
        assert cloned.json()["model"]["model_name"] == "gpt-demo"

        updated = client.put(
            f"/admin/v1/tenants/{tenant_id}/apps/{app_id}/draft",
            json=draft_payload(expected_lock_version=3, model_name="gpt-demo-v2"),
        )
        assert updated.status_code == 200

        published_v2 = client.post(
            f"/admin/v1/tenants/{tenant_id}/apps/{app_id}/publish",
            json={"expected_lock_version": 4},
        )
        assert published_v2.status_code == 200
        assert published_v2.json()["active_version"] == 2

        rolled_back = client.post(
            f"/admin/v1/tenants/{tenant_id}/apps/{app_id}/rollback",
            json={"expected_lock_version": 5, "target_version": 1},
        )
        assert rolled_back.status_code == 200
        assert rolled_back.json()["active_version"] == 1
        assert rolled_back.json()["draft_version"] == 3


def test_publish_requires_model_and_rejects_inline_secret() -> None:
    with make_client() as client:
        tenant = create_tenant(client)
        app = create_agent_app(client, str(tenant["id"]))
        path = f"/admin/v1/tenants/{tenant['id']}/apps/{app['id']}"

        missing_model = client.post(f"{path}/publish", json={"expected_lock_version": 1})
        assert missing_model.status_code == 422
        assert missing_model.json()["error"]["code"] == "invalid_state"

        inline_secret = draft_payload(expected_lock_version=1)
        inline_secret["model"]["parameters"] = {"api_key": "plaintext"}  # type: ignore[index]
        rejected = client.put(f"{path}/draft", json=inline_secret)
        assert rejected.status_code == 422
        assert "secret_ref" in rejected.text


def test_database_rejects_cross_tenant_app_reference() -> None:
    with make_client() as client:
        first_tenant = create_tenant(client, "tenant-one")
        second_tenant = create_tenant(client, "tenant-two")
        app = create_agent_app(client, str(first_tenant["id"]))

        with client.app.state.database.session_factory() as session:
            session.add(
                ModelConfig(
                    tenant_id=second_tenant["id"],
                    agent_app_id=app["id"],
                    config_version=1,
                    provider="test",
                    model_name="test",
                    parameters={},
                )
            )
            with pytest.raises(IntegrityError):
                session.commit()


def test_effective_tenant_backend_and_im_identity_admin_api() -> None:
    with make_client() as client:
        tenant = create_tenant(client)
        app = create_agent_app(client, str(tenant["id"]))
        tenant_id = str(tenant["id"])
        app_id = str(app["id"])
        payload = draft_payload(expected_lock_version=1)
        payload["backends"] = [
            {"backend_kind": "session", "backend_type": "inmemory", "options": {}}
        ]
        payload["channels"][0]["options"] = {"identity_mode": "strict"}  # type: ignore[index]
        assert client.put(
            f"/admin/v1/tenants/{tenant_id}/apps/{app_id}/draft", json=payload
        ).status_code == 200
        assert client.post(
            f"/admin/v1/tenants/{tenant_id}/apps/{app_id}/publish",
            json={"expected_lock_version": 2},
        ).status_code == 200

        effective = client.get(
            f"/admin/v1/tenants/{tenant_id}/apps/{app_id}/backends/effective"
        )
        assert effective.status_code == 200
        session_backend = next(
            item for item in effective.json() if item["backend_kind"] == "session"
        )
        assert session_backend["effective_type"] == "inmemory"
        assert session_backend["source"] == "agent-app"

        bindings = client.get(
            f"/admin/v1/tenants/{tenant_id}/apps/{app_id}/channel-bindings"
        )
        assert bindings.status_code == 200
        assert bindings.json()[0]["identity_mode"] == "strict"

        mapping = client.put(
            f"/admin/v1/tenants/{tenant_id}/im-identities",
            json={
                "channel_type": "wecom",
                "account_id": "corp-account",
                "external_user_id": "zhangsan",
                "internal_user_id": "employee-10086",
                "display_name": "Zhang San",
                "attributes": {"department": "engineering"},
            },
        )
        assert mapping.status_code == 200
        assert mapping.json()["internal_user_id"] == "employee-10086"
        listed = client.get(
            f"/admin/v1/tenants/{tenant_id}/im-identities",
            params={"channel_type": "wecom", "account_id": "corp-account"},
        )
        assert listed.status_code == 200
        assert len(listed.json()) == 1


def test_production_rejects_tenant_inmemory_session_publish() -> None:
    settings = Settings(
        environment="production",
        log_level="CRITICAL",
        database_url="sqlite+pysqlite:///:memory:",
        gateway_internal_secret="production-test-secret",
        outbox_worker_enabled=False,
        inbound_worker_enabled=False,
        _env_file=None,
    )
    with TestClient(create_app(settings)) as client:
        tenant = create_tenant(client, "production-tenant")
        app = create_agent_app(client, str(tenant["id"]))
        payload = draft_payload(expected_lock_version=1)
        payload["backends"] = [
            {"backend_kind": "session", "backend_type": "inmemory", "options": {}}
        ]
        path = f"/admin/v1/tenants/{tenant['id']}/apps/{app['id']}"
        assert client.put(f"{path}/draft", json=payload).status_code == 200

        published = client.post(
            f"{path}/publish", json={"expected_lock_version": 2}
        )

        assert published.status_code == 422
        assert "does not allow" in published.text
