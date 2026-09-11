"""Sticky routing is computed before durable acceptance, never at retry time."""

from datetime import datetime, timezone

from tests.reliability.test_management import managed, register_config  # noqa: F401
from trpc_service.channels import NormalizedInboundMessage
from trpc_service.management import ConfigurationStore


def test_canary_sticky_percentage_rollback_and_storage_rejection(managed, monkeypatch):  # noqa: F811
    client, database = managed
    monkeypatch.setenv("TRPC_IDENTITY_KEY", "canary-identity-key-" * 3)
    base = register_config(client)
    path = "/admin/tenants/tenant_acme"
    assert client.post(path + "/configs", json=base).status_code == 201
    assert client.post(path + "/configs/3/publish", json={"expected_active_version": 0}).status_code == 200
    target = register_config(client, 4)
    target["config"]["applications"][0]["instruction"] = "Canary instruction"
    assert client.post(path + "/configs", json=target).status_code == 201
    body = {"target_version": 4, "expected_active_version": 3, "percent": 35}
    assert client.post(path + "/rollout", json=body).status_code == 200
    store = ConfigurationStore(database)
    versions = []
    for index in range(100):
        message = NormalizedInboundMessage(
            channel="wecom", webhook_public_id="callback_acme",
            external_message_id=f"msg-{index}", external_user_id=f"user-{index}", conversation_type="direct",
            text="hello", received_at=datetime.now(timezone.utc), request_id="request")
        route = store.route_message(message)
        versions.append(route.config_version)
        assert store.route_message(message.model_copy(update={"external_message_id": "next"})) == route
    assert 15 <= versions.count(4) <= 55
    assert client.post(path + "/rollout", json={**body, "percent": 0}).status_code == 200
    assert store.route_message(message).config_version == 3
    assert client.post(path + "/rollout", json={**body, "percent": 100}).status_code == 200
    assert store.route_message(message).config_version == 4
    assert client.post(path + "/configs/4/publish", json={"expected_active_version": 3}).status_code == 200
    assert client.get(path + "/rollout").json()["active"] is False
    assert client.post(path + "/rollout", json=body).status_code == 409
    target = register_config(client, 5)
    target["config"]["storage_revision"] += 1
    assert client.post(path + "/configs", json=target).status_code == 201
    assert client.post(path + "/rollout", json={"target_version": 5, "expected_active_version": 4,
                                                "percent": 10}).status_code == 409
