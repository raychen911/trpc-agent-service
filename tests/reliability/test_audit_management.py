"""Audit visibility includes executions and enforces the published tenant policy."""

from datetime import datetime, timezone
import json

from sqlalchemy import select

from tests.reliability.test_management import managed as managed_fixture, register_config
from trpc_service.persistence.models import AuditLogRow

managed = managed_fixture


def seed(database):
    with database.sessions.begin() as session:
        for index, tenant in enumerate(("tenant_acme", "tenant_acme", "tenant_other")):
            session.add(
                AuditLogRow(audit_id=f"evidence_{index}",
                            tenant_id=tenant,
                            decision="tool_succeeded",
                            trace_id="a" * 32,
                            user_id="usr_123",
                            tool_name="write_artifact",
                            cost=0.01,
                            created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
                            metadata_json={
                                "execution_id": "exec_1",
                                "prompt": "private-body",
                                "unknown": {
                                    "secret": "private-body"
                                }
                            },
                            redacted=True))


def test_full_evidence_export_filter_and_access_receipts(managed):
    client, database = managed
    seed(database)
    path = "/admin/tenants/tenant_acme/audit"
    query = {"decision": "tool_succeeded", "trace_id": "a" * 32, "limit": 1}
    result = client.get(path, params=query)
    assert result.status_code == 200 and len(result.json()) == 1
    assert result.json()[0]["cost"] == 0.01
    assert "private-body" not in result.text
    first = client.get(path + "/export", params=query)
    assert first.status_code == 200
    query.update(offset=first.headers["X-Next-Offset"], until=first.headers["X-Audit-Until"])
    second = client.get(path + "/export", params=query)
    assert second.headers["X-Next-Offset"] == ""
    assert {json.loads(first.text)["audit_id"], json.loads(second.text)["audit_id"]} == {"evidence_0", "evidence_1"}
    assert client.get(path, params={"since": "2026-01-01T00:00:00"}).status_code == 422
    with database.sessions() as session:
        decisions = list(session.scalars(
            select(AuditLogRow.decision).where(AuditLogRow.user_id == "platform_operator")))
        assert decisions.count("audit_exported") == 2 and decisions.count("audit_read") == 1


def test_auditor_role_and_policy_restrict_access(managed, monkeypatch):
    client, database = managed
    body = register_config(client)
    assert client.post("/admin/tenants/tenant_acme/configs", json=body).status_code == 201
    assert client.post("/admin/tenants/tenant_acme/configs/3/publish", json={
        "expected_active_version": 0
    }).status_code == 200
    seed(database)
    monkeypatch.setenv("AUDITOR_TEST_TOKEN", "auditor-secret-" * 4)
    identity = dict(actor="audit_alice",
                    role="tenant_auditor",
                    tenant_ids=["tenant_acme"],
                    token_ref="env://AUDITOR_TEST_TOKEN")
    monkeypatch.setenv("TRPC_ADMIN_PRINCIPALS", json.dumps([identity]))
    client.headers["Authorization"] = "Bearer " + "auditor-secret-" * 4
    assert client.get("/admin/tenants/tenant_acme/audit").status_code == 200
    assert client.get("/admin/tenants/tenant_other/audit/export").status_code == 403
    assert client.get("/admin/tenants/tenant_acme/configs").status_code == 403
    assert client.post("/admin/tenants/tenant_acme/rollout", json={}).status_code == 403
    identity["role"] = "viewer"
    monkeypatch.setenv("TRPC_ADMIN_PRINCIPALS", json.dumps([identity]))
    assert client.get("/admin/tenants/tenant_acme/audit").status_code == 403
