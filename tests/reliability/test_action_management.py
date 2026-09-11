"""Operator APIs never accept a caller-selected acting identity or reveal capabilities."""

from fastapi.testclient import TestClient
from tests.reliability.test_actions import action_system as action_fixture, system as system_fixture, proposal, command
from trpc_service.web.app import create_app

action_system, system = action_fixture, system_fixture


def test_action_api_requires_authenticated_bound_principal(action_system, monkeypatch):
    _, service, route, execution = action_system
    action = proposal(service, route, execution)
    token = command(service, action["action_id"])
    monkeypatch.setenv("TRPC_ADMIN_TOKEN", "synthetic-operator-token-" * 2)
    monkeypatch.setenv("TRPC_ADMIN_ACTOR", "platform_operator")
    monkeypatch.setenv("TRPC_IM_CONTEXT_KEYS", "configured-by-test")
    monkeypatch.delenv("TRPC_RUNTIME_MODE", raising=False)
    monkeypatch.setattr("trpc_service.governance.action_management.ContextCipher", lambda _: service.cipher)
    body = {"decision": "approve", "nonce": token.split(":")[2], "expected_revision": 1}
    base = f"/admin/tenants/{route.tenant_id}/actions"
    with TestClient(create_app(service.database.url)) as client:
        assert client.get(base).status_code == 401
        client.headers["Authorization"] = "Bearer " + "synthetic-operator-token-" * 2
        assert client.get(base).json()[0]["action_id"] == action["action_id"]
        path = base + "/" + action["action_id"]
        detail = client.get(path)
        assert detail.status_code == 200
        assert all(field not in detail.text for field in (body["nonce"], "encrypted_payload", "arguments", "reply"))
        assert client.get(path.replace(route.tenant_id, "tenant_other")).status_code == 404
        assert client.post(path + "/decision", json=body).status_code == 403
        forged = client.post(path + "/decision", json={**body, "actor_id": route.actor_id})
        assert forged.status_code == 422 and body["nonce"] not in forged.text
    # Trusted configuration binds the operator credential to an actually enrolled
    # IM approver. It is not a per-request override.
    monkeypatch.setenv("TRPC_ADMIN_ACTOR", route.actor_id)
    with TestClient(create_app(service.database.url)) as client:
        client.headers["Authorization"] = "Bearer " + "synthetic-operator-token-" * 2
        assert client.post(path + "/decision", json=body).json()["status"] == "approved"
        assert client.post(path + "/decision", json=body).json()["revision"] == 2
