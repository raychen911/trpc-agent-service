# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Admin API tests for tenant lifecycle, rollback, auth and audit isolation."""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from trpc_service import AuditLogEntry
from trpc_service import AuditLogger
from trpc_service import TenantConfigManager
from trpc_service import create_admin_router


def _payload(tenant_id: str = "tenant_a", model_name: str = "model-a") -> dict:
    return {
        "tenant": {
            "tenant_id": tenant_id,
            "name": tenant_id,
            "model": {
                "model_name": model_name
            },
            "channel_configs": {
                "feishu": {
                    "channel_type": "feishu",
                    "app_id": "app-id",
                    "verification_token": "verification-token",
                    "secret": "super-secret-token",
                },
            },
        },
        "metadata": {
            "by": "tester",
            "reason": "test"
        },
    }


def _client(manager=None, audit_logger=None, api_key="admin-secret") -> TestClient:
    app = FastAPI()
    app.include_router(
        create_admin_router(
            manager=manager or TenantConfigManager(),
            audit_logger=audit_logger,
            api_key=api_key,
        ))
    return TestClient(app)


def test_admin_requires_configured_api_key():
    client = _client()
    assert client.get("/admin/health").status_code == 401
    assert client.get("/admin/health", headers={"X-Admin-API-Key": "wrong"}).status_code == 401
    response = client.get("/admin/health", headers={"X-Admin-API-Key": "admin-secret"})
    assert response.status_code == 200


def test_admin_ui_is_local_read_only_console_and_does_not_embed_secret():
    client = _client()

    redirect = client.get("/admin", follow_redirects=False)
    assert redirect.status_code == 307
    assert redirect.headers["location"] == "/admin/ui"

    response = client.get("/admin/ui")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/html")
    assert response.headers["cache-control"] == "no-store"
    assert "frame-ancestors 'none'" in response.headers["content-security-policy"]
    assert "tRPC Agent Console" in response.text
    assert "/admin/tenants" in response.text
    assert "/admin/audit" in response.text
    assert "/admin/metrics" in response.text
    assert "sessionStorage" in response.text
    assert "admin-secret" not in response.text


def test_admin_tenant_crud_history_and_rollback_masks_secrets():
    manager = TenantConfigManager()
    client = _client(manager=manager)
    headers = {"X-Admin-API-Key": "admin-secret"}

    created = client.post("/admin/tenants", json=_payload(), headers=headers)
    assert created.status_code == 201
    assert "super-secret-token" not in created.text
    assert client.post("/admin/tenants", json=_payload(), headers=headers).status_code == 409

    update = _payload(model_name="model-b")
    updated = client.put("/admin/tenants/tenant_a", json=update, headers=headers)
    assert updated.status_code == 200
    assert updated.json()["model"]["model_name"] == "model-b"

    history = client.get("/admin/tenants/tenant_a/history", headers=headers)
    assert [item["version"] for item in history.json()] == [1, 2]
    rolled_back = client.post(
        "/admin/tenants/tenant_a/rollback",
        json={
            "version": 1,
            "by": "tester"
        },
        headers=headers,
    )
    assert rolled_back.status_code == 200
    assert rolled_back.json()["model"]["model_name"] == "model-a"

    assert client.delete("/admin/tenants/tenant_a", headers=headers).status_code == 204
    assert client.get("/admin/tenants/tenant_a", headers=headers).status_code == 404


def test_admin_rejects_path_body_mismatch_and_missing_tenant():
    client = _client()
    headers = {"X-Admin-API-Key": "admin-secret"}
    assert client.put("/admin/tenants/other", json=_payload(), headers=headers).status_code == 400
    assert client.delete("/admin/tenants/missing", headers=headers).status_code == 404
    assert client.post(
        "/admin/tenants/missing/rollback",
        json={
            "version": 1
        },
        headers=headers,
    ).status_code == 404


async def test_admin_queries_audit_by_tenant():
    audit = AuditLogger()
    await audit.log(AuditLogEntry(tenant_id="tenant_a", decision="allow"))
    await audit.log(AuditLogEntry(tenant_id="tenant_b", decision="deny"))
    client = _client(audit_logger=audit, api_key=None)

    response = client.get("/admin/audit", params={"tenant_id": "tenant_a"})
    assert response.status_code == 200
    assert [entry["tenant_id"] for entry in response.json()] == ["tenant_a"]
