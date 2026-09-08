# Tencent is pleased to support the open source community by making tRPC-Agent-Python available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Agent-Python is licensed under Apache-2.0.
"""Admin API tests for tenant lifecycle, rollback, auth and audit isolation."""

from __future__ import annotations

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient

from trpc_service import AuditLogEntry
from trpc_service import AuditLogger
from trpc_service import EnterpriseMetrics
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


def _client(manager=None,
            audit_logger=None,
            api_key="admin-secret",
            metrics=None,
            prometheus_reader=None) -> TestClient:
    app = FastAPI()
    app.include_router(
        create_admin_router(
            manager=manager or TenantConfigManager(),
            audit_logger=audit_logger,
            api_key=api_key,
            metrics=metrics,
            prometheus_reader=prometheus_reader,
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
    assert 'id="metricKpis"' in response.text
    assert "运行概览" in response.text
    assert "Gateway 回调" in response.text
    assert "Worker 消费" in response.text
    assert 'totalCounter("agent_worker_unavailable_total")' in response.text
    assert "无可用 Worker 拒绝" in response.text
    assert "回复成功率" in response.text
    assert "Token 消耗" in response.text
    assert 'totalCounter("agent_callback_total", "success")' in response.text
    assert 'totalCounter("agent_callback_total", "challenge")' in response.text
    assert 'kpi("消息回调"' in response.text
    assert 'kpi("验证回调"' in response.text
    assert 'histogramTotal("agent_callback_duration_ms", "success")' in response.text
    assert "模型成本" in response.text
    assert "Token 预算" in response.text
    assert "Prometheus 全局指标" in response.text
    assert "histogramChart" in response.text
    assert "histogram-bar" in response.text
    assert "metric-details" in response.text
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
    staged = client.put("/admin/tenants/tenant_a/draft", json=update, headers=headers)
    assert staged.status_code == 200
    assert staged.json()["based_on_version"] == 1
    assert client.get("/admin/tenants/tenant_a", headers=headers).json()["model"]["model_name"] == "model-a"
    updated = client.post(
        "/admin/tenants/tenant_a/publish",
        json={
            "expected_version": 1,
            "by": "tester"
        },
        headers=headers,
    )
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
    assert client.put("/admin/tenants/other/draft", json=_payload(), headers=headers).status_code == 400
    assert client.delete("/admin/tenants/missing", headers=headers).status_code == 404
    assert client.post(
        "/admin/tenants/missing/rollback",
        json={
            "version": 1
        },
        headers=headers,
    ).status_code == 404


def test_admin_draft_query_conflict_and_discard():
    manager = TenantConfigManager()
    client = _client(manager=manager)
    headers = {"X-Admin-API-Key": "admin-secret"}
    assert client.post("/admin/tenants", json=_payload(), headers=headers).status_code == 201
    update = _payload(model_name="model-b")
    update["metadata"]["expected_version"] = 9
    assert client.put("/admin/tenants/tenant_a/draft", json=update, headers=headers).status_code == 409
    update["metadata"]["expected_version"] = 1
    assert client.put("/admin/tenants/tenant_a/draft", json=update, headers=headers).status_code == 200
    draft = client.get("/admin/tenants/tenant_a/draft", headers=headers)
    assert draft.status_code == 200
    assert draft.json()["config_snapshot"]["model"]["model_name"] == "model-b"
    assert client.post(
        "/admin/tenants/tenant_a/publish",
        json={
            "expected_version": 9
        },
        headers=headers,
    ).status_code == 409
    assert client.delete("/admin/tenants/tenant_a/draft", headers=headers).status_code == 204
    assert client.get("/admin/tenants/tenant_a/draft", headers=headers).status_code == 404
    assert client.delete("/admin/tenants/tenant_a/draft", headers=headers).status_code == 404


async def test_admin_queries_audit_by_tenant():
    audit = AuditLogger()
    await audit.log(AuditLogEntry(tenant_id="tenant_a", decision="allow"))
    await audit.log(AuditLogEntry(tenant_id="tenant_b", decision="deny"))
    client = _client(audit_logger=audit, api_key=None)

    response = client.get("/admin/audit", params={"tenant_id": "tenant_a"})
    assert response.status_code == 200
    assert [entry["tenant_id"] for entry in response.json()] == ["tenant_a"]


def test_admin_metrics_are_explicitly_process_local(monkeypatch):
    metrics = EnterpriseMetrics(meter=False)
    metrics.increment("agent_requests_total", tenant_id="tenant_a", outcome="success")
    metrics.increment("agent_requests_total", tenant_id="tenant_b", outcome="error")
    metrics.observe("agent_runner_latency_ms", 75, tenant_id="tenant_a", outcome="success")
    monkeypatch.setenv("OTEL_SERVICE_NAME", "gateway-test")
    monkeypatch.setenv("OTEL_SERVICE_INSTANCE_ID", "instance-test")
    client = _client(api_key=None, metrics=metrics)

    response = client.get("/admin/metrics", params={"tenant_id": "tenant_a"})

    assert response.status_code == 200
    body = response.json()
    assert body["scope"] == "process"
    assert body["service_name"] == "gateway-test"
    assert body["instance_id"] == "instance-test"
    assert len(body["counters"]) == 1
    assert body["counters"][0]["attributes"]["tenant_id"] == "tenant_a"
    assert body["histograms"][0]["unit"] == "ms"
    assert sum(bucket["count"] for bucket in body["histograms"][0]["buckets"]) == 1


def test_admin_metrics_use_prometheus_cross_process_snapshot():

    class FakePrometheusReader:

        async def snapshot(self, tenant_id):
            assert tenant_id == "tenant_a"
            return {
                "counters": [{
                    "name": "agent_requests_total",
                    "attributes": {
                        "tenant_id": tenant_id
                    },
                    "value": 7,
                }],
                "gauges": [{
                    "name": "agent_budget_tokens_used",
                    "attributes": {
                        "tenant_id": tenant_id
                    },
                    "value": 120,
                }],
                "histograms": [],
            }

    client = _client(api_key=None, prometheus_reader=FakePrometheusReader())

    body = client.get("/admin/metrics", params={"tenant_id": "tenant_a"}).json()

    assert body["scope"] == "prometheus"
    assert body["service_name"] == "all"
    assert body["instance_id"] is None
    assert body["source_error"] is None
    assert body["counters"][0]["value"] == 7
    assert body["gauges"][0]["value"] == 120


def test_admin_metrics_fall_back_to_local_snapshot_when_prometheus_fails():

    class FailingPrometheusReader:

        async def snapshot(self, tenant_id):
            raise httpx.ConnectError("offline")

    metrics = EnterpriseMetrics(meter=False)
    metrics.increment("agent_callback_total", tenant_id="tenant_a", outcome="success")
    client = _client(api_key=None, metrics=metrics, prometheus_reader=FailingPrometheusReader())

    body = client.get("/admin/metrics").json()

    assert body["scope"] == "process"
    assert body["source_error"] == "ConnectError"
    assert body["counters"][0]["name"] == "agent_callback_total"
