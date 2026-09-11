"""Management requests cannot cross tenant, privilege or credential boundaries."""

import json

from sqlalchemy import select

from tests.reliability.test_management import managed as managed_fixture, register_config
from trpc_service.persistence.models import AuditLogRow

managed = managed_fixture


def test_tenant_management_is_scoped_and_audited(managed, monkeypatch):
    client, database = managed
    body = register_config(client)
    monkeypatch.setenv("TENANT_ADMIN_TEST", "tenant-admin-" * 4)
    identity = {
        "actor":
        "alice",
        "role":
        "tenant_admin",
        "tenant_ids": ["tenant_acme"],
        "token_ref":
        "env://TENANT_ADMIN_TEST",
        "profile_ids": [ref["profile_id"] for ref in body["config"]["data_backends"].values()],
        "secret_refs": [model["api_key_ref"]["uri"] for model in body["config"]["models"]] + [
            credential["secret_ref"]["uri"] for binding in body["config"]["channel_bindings"]
            for credential in binding["credential_refs"]
        ]
    }
    monkeypatch.setenv("TRPC_ADMIN_PRINCIPALS", json.dumps([identity]))
    client.headers["Authorization"] = "Bearer " + "tenant-admin-" * 4
    assert client.get("/admin/tenants/tenant_other/configs").status_code == 403
    assert client.get("/admin/backend-profiles").status_code == 403
    assert client.get("/admin/tenants").status_code == 403
    path = "/admin/tenants/tenant_acme/configs"
    assert client.post(path, json=body).status_code == 201
    with database.sessions() as session:
        assert session.scalar(select(AuditLogRow.user_id).where(AuditLogRow.decision == "config_drafted")) == "alice"
    body["config"]["config_version"] += 1
    body["config"]["models"][0]["api_key_ref"]["uri"] = "env://OTHER_TENANT_KEY"
    assert client.post(path, json=body).status_code == 403
    identity["role"] = "viewer"
    monkeypatch.setenv("TRPC_ADMIN_PRINCIPALS", json.dumps([identity]))
    assert client.get(path).status_code == 200
    assert client.post(path, json=body).status_code == 403
    # A subsequent operator request must not inherit Alice's actor/scope.
    client.headers["Authorization"] = "Bearer " + "operator-test-token-" * 3
    assert client.get("/admin/tenants").status_code == 200
