"""
End-to-End API and Webhook integration tests.
"""

import pytest
from starlette.testclient import TestClient
from trpc_service.web.app import app
from trpc_service.tenant.manager import tenant_manager
from trpc_service.config.models import TenantConfig, AgentAppConfig


@pytest.fixture
def client():
    with TestClient(app) as c:
        yield c


def test_health_endpoint(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_metrics_endpoint(client):
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert "trpc_agent_requests_total" in resp.text


def test_admin_tenant_lifecycle(client):
    tenant_payload = {
        "tenant_id": "test_e2e_tenant",
        "name": "E2E Test Tenant",
        "quota_policy": {"daily_token_budget": 50000},
    }
    resp = client.post("/admin/tenants", json=tenant_payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["tenant_id"] == "test_e2e_tenant"

    # List tenants
    resp_list = client.get("/admin/tenants")
    assert resp_list.status_code == 200
    ids = [t["tenant_id"] for t in resp_list.json()]
    assert "test_e2e_tenant" in ids


def test_direct_chat_completion(client):
    payload = {
        "message": "Calculate 15 * 6",
        "user_id": "tester_1",
    }
    resp = client.post("/v1/chat/completions", json=payload, headers={"x-tenant-id": "default_corp"})
    assert resp.status_code == 200
    res = resp.json()
    assert "reply" in res
    assert "90" in res["reply"]  # Calculator tool should calculate 15*6 = 90


def test_wecom_webhook_e2e(client):
    xml_body = b"""<xml>
        <ToUserName><![CDATA[mock_wecom_bot]]></ToUserName>
        <FromUserName><![CDATA[emp_001]]></FromUserName>
        <CreateTime>1700000000</CreateTime>
        <MsgType><![CDATA[text]]></MsgType>
        <Content><![CDATA[hello agent]]></Content>
        <MsgId>msg_uniq_999</MsgId>
    </xml>"""

    resp = client.post("/webhook/wecom/default_corp", content=xml_body)
    assert resp.status_code == 200
    assert resp.text == "success"

    # Idempotency duplicate check
    resp_dup = client.post("/webhook/wecom/default_corp", content=xml_body)
    assert resp_dup.status_code == 200
    assert resp_dup.text == "success"
