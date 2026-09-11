"""Knowledge HTTP authorization and bounded document parsing before any provider call."""

import json

from tests.reliability.test_management import managed as managed_fixture, register_config

managed = managed_fixture


def test_upload_scope_content_limits_and_missing_embedding(managed, monkeypatch):
    client, _ = managed
    body = register_config(client)
    body["config"]["applications"][0]["knowledge_base_ids"] = ["manual"]
    config_path = "/admin/tenants/tenant_acme/configs"
    assert client.post(config_path, json=body).status_code == 201
    assert client.post(config_path + "/3/publish", json={"expected_active_version": 0}).status_code == 200
    app_id = body["config"]["applications"][0]["app_id"]
    root = f"/admin/tenants/tenant_acme/apps/{app_id}/knowledge/manual"
    document = root + "/documents/check.txt"
    assert client.put(document, content=b"text", headers={"Content-Type": "application/pdf"}).status_code == 415
    headers = {"Content-Type": "text/plain"}
    assert client.put(document, content=b"\xff", headers=headers).status_code == 422
    assert client.put(document, content=b"x" * 256001, headers=headers).status_code == 413
    assert client.put(document, content=b"text", headers=headers).status_code == 409
    assert client.put(document.replace("/manual/", "/private/"), content=b"text", headers=headers).status_code == 403
    monkeypatch.setenv("VIEWER_TOKEN", "viewer-token-" * 4)
    monkeypatch.setenv("TRPC_ADMIN_PRINCIPALS", json.dumps([{
        "actor": "reader", "role": "viewer", "tenant_ids": ["tenant_acme"], "token_ref": "env://VIEWER_TOKEN"}]))
    client.headers["Authorization"] = "Bearer " + "viewer-token-" * 4
    assert client.put(document, content=b"text", headers=headers).status_code == 403
    assert client.post(root + "/search", json={"query": "secret"}).status_code == 403
    assert client.get(root.replace("tenant_acme", "tenant_other")).status_code in {403, 404}


def test_new_secret_references_cannot_escape_tenant_assignment(managed, monkeypatch):
    client, _ = managed
    body = register_config(client)
    monkeypatch.setenv("TENANT_TOKEN", "tenant-admin-" * 4)
    identity = {"actor": "admin", "role": "tenant_admin", "tenant_ids": ["tenant_acme"],
                "token_ref": "env://TENANT_TOKEN",
                "profile_ids": [ref["profile_id"] for ref in body["config"]["data_backends"].values()],
                "secret_refs": [model["api_key_ref"]["uri"] for model in body["config"]["models"]] + [
                    item["secret_ref"]["uri"] for binding in body["config"]["channel_bindings"]
                    for item in binding["credential_refs"]]}
    monkeypatch.setenv("TRPC_ADMIN_PRINCIPALS", json.dumps([identity]))
    client.headers["Authorization"] = "Bearer " + "tenant-admin-" * 4
    path = "/admin/tenants/tenant_acme/configs"
    body["config"]["embedding"] = {
        "model_name": "embedding", "dimensions": 3, "api_key_ref": {"uri": "env://OTHER_KEY"}}
    assert client.post(path, json=body).status_code == 403
    body["config"].pop("embedding")
    body["config"]["applications"][0]["mcp_servers"] = [{
        "server_id": "tools", "endpoint": "https://example.test/mcp", "secret_ref": {"uri": "env://OTHER_KEY"}}]
    assert client.post(path, json=body).status_code == 403
