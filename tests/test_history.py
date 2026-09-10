"""会话历史端点测试：事件收敛为 user/agent 消息流。"""
from types import SimpleNamespace

from fastapi.testclient import TestClient

from trpc_service.web.app import app, state


class FakeSessionService:
    async def get_session(self, *, app_name, user_id, session_id):
        if session_id == "ghost":
            return None
        return SimpleNamespace(
            events=[
                SimpleNamespace(author="user", timestamp=1.0,
                                get_text=lambda: "你好"),
                SimpleNamespace(author="customer_service", timestamp=2.0,
                                get_text=lambda: "你好，需要查订单吗？"),
                SimpleNamespace(author="customer_service", timestamp=3.0,
                                get_text=lambda: ""),  # 空事件跳过
            ]
        )


def test_chat_history_returns_events(client=None):
    client = client or TestClient(app)
    # 注入 fake runner（绕过 lifespan 装配）
    state.runners["tenant_001"] = SimpleNamespace(
        app_name="tenant_001_customer_service",
        runner=SimpleNamespace(session_service=FakeSessionService()),
    )
    try:
        res = client.get(
            "/api/v1/chat/history",
            params={"tenant_id": "tenant_001", "session_id": "sess1", "user_id": "web_user"},
        )
        assert res.status_code == 200
        items = res.json()
        assert [i["role"] for i in items] == ["user", "agent"]
        assert items[0]["text"] == "你好"
        assert items[1]["text"] == "你好，需要查订单吗？"
    finally:
        state.runners.pop("tenant_001", None)


def test_chat_history_unknown_session_returns_empty():
    client = TestClient(app)
    state.runners["tenant_001"] = SimpleNamespace(
        app_name="tenant_001_customer_service",
        runner=SimpleNamespace(session_service=FakeSessionService()),
    )
    try:
        res = client.get(
            "/api/v1/chat/history",
            params={"tenant_id": "tenant_001", "session_id": "ghost"},
        )
        assert res.status_code == 200
        assert res.json() == []
    finally:
        state.runners.pop("tenant_001", None)


def test_chat_history_unknown_tenant_404():
    client = TestClient(app)
    res = client.get(
        "/api/v1/chat/history",
        params={"tenant_id": "no_such", "session_id": "s"},
    )
    assert res.status_code == 404
