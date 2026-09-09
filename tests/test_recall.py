# IM 消息撤回事件（PRD 3.7）单元测试
import asyncio
import json

import pytest
from fastapi.testclient import TestClient

from trpc_service.channels import ChannelFactory, FeishuAdapter
from trpc_service.channels.base import mark_message_revoked
from trpc_service.runtime import MockAgentRunner, Runtime
from trpc_service.storage import InMemoryStorage
from trpc_service.tenant import ImChannelConfig, TenantRegistry
from trpc_service.web import build_gateway_app

# ------------------------------------------------------------------
# mark_message_revoked 纯函数
# ------------------------------------------------------------------


def test_mark_message_revoked_marks_matching_user_msg():
    state = {
        "history": [
            {
                "role": "user",
                "msg_id": "m1",
                "content": "你好"
            },
            {
                "role": "assistant",
                "content": "回复"
            },
        ]
    }
    assert mark_message_revoked(state, "m1") is True
    assert state["history"][0]["revoked"] is True
    assert "revoked" not in state["history"][1]


def test_mark_message_revoked_no_match():
    state = {"history": [{"role": "user", "msg_id": "m1", "content": "你好"}]}
    assert mark_message_revoked(state, "nope") is False
    assert "revoked" not in state["history"][0]


def test_mark_message_revoked_missing_history():
    assert mark_message_revoked({}, "m1") is False
    assert mark_message_revoked({"history": []}, "m1") is False


# ------------------------------------------------------------------
# 飞书撤回协议解析（im.message.recalled_v1）
# ------------------------------------------------------------------


def _feishu_recall_body(msg_id="msg-recall-1", operator_open_id="ou_op"):
    return json.dumps({
        "schema": "2.0",
        "header": {
            "event_id": "evt-recall-1",
            "event_type": "im.message.recalled_v1",
            "app_id": "cli_test",
            "create_time": "1700000000000",
            "token": "",
        },
        "event": {
            "message_id": msg_id,
            "operator_id": {
                "open_id": operator_open_id,
            },
            "operator_tenant_key": "",
        },
    }).encode()


def test_feishu_parse_recall_event():
    adapter = FeishuAdapter(ImChannelConfig(channel_type="feishu", app_id="cli_test"))
    recall = adapter.parse_recall_event(_feishu_recall_body(), {})
    assert recall is not None
    assert recall.msg_id == "msg-recall-1"
    assert recall.user_id == "ou_op"
    assert recall.event_type == "im.message.recalled_v1"
    assert recall.channel_type == "feishu"


def test_feishu_parse_recall_event_returns_none_for_message():
    """普通消息事件（receive_v1）不应被识别为撤回。"""
    adapter = FeishuAdapter(ImChannelConfig(channel_type="feishu", app_id="cli_test"))
    body = json.dumps({
        "schema": "2.0",
        "header": {
            "event_type": "im.message.receive_v1",
            "token": "",
        },
        "event": {
            "message": {
                "message_id": "m-1",
                "chat_id": "oc_1",
                "message_type": "text",
                "content": '{"text":"hi"}',
            },
            "sender": {
                "sender_id": {
                    "open_id": "ou_1"
                }
            },
        },
    }).encode()
    assert adapter.parse_recall_event(body, {}) is None


# ------------------------------------------------------------------
# webhook 撤回短路：不触发 Agent、历史标记 revoked、审计 recall
# ------------------------------------------------------------------


async def _load_tenant(tid):
    if tid == "demo":
        return {
            "tenant_id": "demo",
            "name": "演示租户",
            "status": "active",
            "app": {
                "system_prompt": "演示助手"
            },
            "model": {
                "model_name": "mock"
            },
            "tools": {
                "allowlist": ["echo"]
            },
            "im": [{
                "channel_type": "feishu",
                "webhook_path": "/webhook/feishu/demo__fb",
                "app_id": "cli_test",
            }],
            "rate_limit_per_min": 60,
        }
    return None


@pytest.fixture
def recall_gateway():
    storage = InMemoryStorage()
    registry = TenantRegistry(load_fn=_load_tenant)
    rt = Runtime(registry=registry, storage=storage, runner=MockAgentRunner())
    app = build_gateway_app(
        registry=registry,
        storage=storage,
        runtime=rt,
        channel_factory=ChannelFactory(),
    )
    return TestClient(app), storage, rt


def test_recall_does_not_trigger_agent_and_marks_history(recall_gateway):
    """撤回回调: 不触发 Agent、审计 recall、会话历史 user 消息标记 revoked。"""
    from trpc_service.tenant.resolver import generate_session_id

    client, storage, rt = recall_gateway

    # 撤回会话按单聊规则定位: generate_session_id(demo, feishu, app_id, open_id)
    sid = generate_session_id(tenant_id="demo", channel_type="feishu", channel_id="cli_test", external_user_id="ou_1")

    # 先造一条带 msg_id 的会话历史（模拟之前用户发过该消息）
    state = {
        "history": [{
            "role": "user",
            "msg_id": "recalled-msg-9",
            "content": "要被撤回的话"
        }],
    }
    asyncio.run(storage.session.update_state("demo", sid, state))

    # 发撤回回调
    resp = client.post(
        "/webhook/feishu/demo__fb",
        content=_feishu_recall_body(msg_id="recalled-msg-9", operator_open_id="ou_1"),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 200

    # 该消息历史被标记 revoked（撤回处理在历史中定位并改写）
    sess = asyncio.run(storage.session.get_session("demo", sid))
    assert sess is not None
    assert sess["state"]["history"][0].get("revoked") is True

    # 审计出现 recall（字段与其余审计写入同一 schema）
    logs = asyncio.run(storage.audit.query_logs("demo", {"decision": "recall"}))
    assert len(logs) == 1
    assert logs[0]["payload"]["recalled_msg_id"] == "recalled-msg-9"
    assert logs[0]["agent_name"] == "演示租户"
    assert logs[0]["tool_name"] is None
    assert logs[0]["latency_ms"] is None
    # trace_id 非空：撤回短路不经过 TraceFilter，审计行自行生成（Problem 4.4）
    assert logs[0]["trace_id"]


def test_recall_webhook_binding_mismatch_rejected(recall_gateway):
    """binding 与租户声明的 webhook_path 不一致时 404（PRD 3.4 绑定校验）。"""
    client, _, _ = recall_gateway
    # 任意伪造后缀不应命中 demo 的 feishu 绑定（声明为 /webhook/feishu/demo__fb）
    resp = client.post(
        "/webhook/feishu/demo__forged",
        content=_feishu_recall_body(msg_id="m-x", operator_open_id="ou_1"),
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 404
