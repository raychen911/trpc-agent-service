"""Admin API / chat 端点的 API-Key 鉴权测试。"""
import pytest
from fastapi.testclient import TestClient

from trpc_service.web.app import app


@pytest.fixture()
def client():
    return TestClient(app)


def test_admin_requires_key_when_configured(client, monkeypatch):
    monkeypatch.setenv("ADMIN_API_KEY", "secret123")
    r = client.get("/api/v1/tenants")
    assert r.status_code == 401
    r = client.get("/api/v1/tenants", headers={"X-API-Key": "wrong"})
    assert r.status_code == 401
    r = client.get("/api/v1/tenants", headers={"X-API-Key": "secret123"})
    assert r.status_code == 200


def test_admin_open_when_not_configured(client, monkeypatch):
    """未配置 ADMIN_API_KEY = 开发模式放行（向后兼容）。"""
    monkeypatch.delenv("ADMIN_API_KEY", raising=False)
    assert client.get("/api/v1/tenants").status_code == 200


def test_admin_key_covers_audit_and_metrics(client, monkeypatch):
    monkeypatch.setenv("ADMIN_API_KEY", "k1")
    assert client.get("/api/v1/audit").status_code == 401
    assert client.get("/api/v1/metrics").status_code == 401
    assert client.get("/api/v1/audit", headers={"X-API-Key": "k1"}).status_code == 200
    assert client.get("/api/v1/metrics", headers={"X-API-Key": "k1"}).status_code == 200


def test_chat_key_enforced_when_configured(client, monkeypatch):
    monkeypatch.setenv("CHAT_API_KEY", "ck1")
    r = client.post("/api/v1/chat", json={"tenant_id": "t", "message": "hi"})
    assert r.status_code == 401
    # 带正确 Key 后进入业务校验（租户不存在 404，而非鉴权失败）
    r = client.post(
        "/api/v1/chat",
        json={"tenant_id": "no_such_tenant", "message": "hi"},
        headers={"X-API-Key": "ck1"},
    )
    assert r.status_code == 404


def test_webhook_not_api_key_protected(client, monkeypatch):
    """飞书回调不走 API-Key（信任模型：验签由 adapter 负责）。"""
    monkeypatch.setenv("ADMIN_API_KEY", "k1")
    # 未启用飞书通道的租户返回 404（业务态），而不是 401（鉴权态）
    r = client.post("/api/v1/channels/feishu/webhook/no_such_tenant", json={})
    assert r.status_code == 404
