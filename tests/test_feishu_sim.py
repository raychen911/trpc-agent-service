"""飞书通道模拟验证：不依赖真实凭证，fake API 传输层做协议回环测试。"""
import json

import pytest

from trpc_service.agent.runner import RunResult
from trpc_service.channels.base import WebhookRequest
from trpc_service.channels.feishu import FeishuAdapter
from trpc_service.config.tenant_config import ChannelConfig, TenantConfig
from trpc_service.agent.routing import SessionRouter
from trpc_service.tenant.governance.user_authz import user_authz
from trpc_service.tenant.storage.database import Database
from trpc_service.tenant.storage.tables import ChannelBindingRow, IdempotencyRow


def _event(message_id, text, chat_type="p2p", event_id=None):
    return json.dumps(
        {
            "schema": "2.0",
            "header": {
                "event_id": event_id or f"ev-{message_id}",
                "event_type": "im.message.receive_v1",
                "token": "verify_token",
            },
            "event": {
                "sender": {"sender_id": {"open_id": "ou_userA"}},
                "message": {
                    "message_id": message_id,
                    "chat_id": "oc_chat",
                    "chat_type": chat_type,
                    "message_type": "text",
                    "content": json.dumps({"text": text}),
                },
            },
        }
    )


class FakeRunner:
    app_name = "customer_service"

    def __init__(self):
        self.calls = []

    async def run(self, user_id, session_id, message, files=None, agent_context=None):
        self.calls.append({"user_id": user_id, "session_id": session_id, "message": message})
        return RunResult(text="订单已发货，预计明天送达。")


def _build_adapter(runner, database=None):
    tenant = TenantConfig(
        tenant_id="tenant_feishu_test",
        name="测试租户",
        channels={
            "feishu": ChannelConfig(
                enabled=True, app_id="cli_abc", app_secret="secret",
                token="verify_token", encrypt_key="",
            )
        },
    )
    adapter = FeishuAdapter(
        tenant, tenant.channels["feishu"], runner_getter=lambda _tid: runner, database=database
    )
    sent = []
    adapter._http_post = lambda url, payload, token="": (
        sent.append({"url": url, "payload": payload, "token": token})
        or {"code": 0, "tenant_access_token": "tok", "expire": 7200, "data": {}}
    )
    return adapter, sent


async def test_url_verification_challenge():
    """开放平台 URL 验证：原样回传 challenge。"""
    adapter, _ = _build_adapter(FakeRunner())
    body = json.dumps({"type": "url_verification", "challenge": "ajls384kdjx98XX"})
    resp = await adapter.handle_webhook("tenant_feishu_test", WebhookRequest(body=body))
    assert resp.status_code == 200
    assert json.loads(resp.body) == {"challenge": "ajls384kdjx98XX"}


async def test_verification_token_rejected():
    """verification token 不匹配 → 403。"""
    adapter, _ = _build_adapter(FakeRunner())
    body = json.dumps(
        {
            "schema": "2.0",
            "header": {"event_id": "ev-x", "event_type": "im.message.receive_v1", "token": "wrong"},
            "event": {},
        }
    )
    resp = await adapter.handle_webhook("tenant_feishu_test", WebhookRequest(body=body))
    assert resp.status_code == 403


async def test_message_flow_and_reply_via_api():
    """消息事件 → ACK → 异步执行 → API 发送回复（内容回环）。"""
    runner = FakeRunner()
    adapter, sent = _build_adapter(runner)
    resp = await adapter.handle_webhook(
        "tenant_feishu_test", WebhookRequest(body=_event("om_10001", "帮我查订单 O1001"))
    )
    assert resp.status_code == 200
    for task in list(adapter._pending_tasks):
        await task

    # 回复经 IM API 发送，内容可回环
    msg_posts = [p for p in sent if "/im/v1/messages" in p["url"]]
    assert len(msg_posts) == 1
    assert msg_posts[0]["token"] == "tok"  # tenant_access_token 鉴权
    content = json.loads(msg_posts[0]["payload"]["content"])
    assert "订单已发货" in content["text"]

    # 身份映射与会话路由（单聊以用户为维度）
    assert user_authz.check("feishu", "ou_userA", "tenant_feishu_test")
    expected = SessionRouter.session_id("tenant_feishu_test", "feishu", "ou_userA")
    assert runner.calls and runner.calls[0]["session_id"] == expected


async def test_idempotent_redelivery():
    """同 message_id 重复投递只执行一次。"""
    runner = FakeRunner()
    adapter, sent = _build_adapter(runner)
    body = _event("om_20001", "再发一次")
    for _ in range(2):
        await adapter.handle_webhook("tenant_feishu_test", WebhookRequest(body=body))
    for task in list(adapter._pending_tasks):
        await task
    assert len(runner.calls) == 1
    msg_posts = [p for p in sent if "/im/v1/messages" in p["url"]]
    assert len(msg_posts) == 1


async def test_group_chat_session_isolated():
    """群聊 session 追加 chat_id 维度，与单聊隔离。"""
    runner = FakeRunner()
    adapter, _ = _build_adapter(runner)
    await adapter.handle_webhook(
        "tenant_feishu_test", WebhookRequest(body=_event("om_30001", "群里问一下", chat_type="group"))
    )
    for task in list(adapter._pending_tasks):
        await task
    single = SessionRouter.session_id("tenant_feishu_test", "feishu", "ou_userA")
    group = SessionRouter.session_id("tenant_feishu_test", "feishu", "ou_userA", chat_id="oc_chat")
    assert runner.calls[0]["session_id"] == group
    assert single != group


@pytest.fixture()
def db(tmp_path):
    url = f"sqlite:///{(tmp_path / 'feishu.db').as_posix()}"
    database = Database(url)
    database.create_all()
    yield database
    database.dispose()


async def test_binding_and_idempotency_written(db):
    """channel_binding 落一行 + idempotency 落幂等键（SQL 第三层接线）。"""
    runner = FakeRunner()
    adapter, _ = _build_adapter(runner, database=db)
    await adapter.handle_webhook(
        "tenant_feishu_test", WebhookRequest(body=_event("om_40001", "你好"))
    )
    for task in list(adapter._pending_tasks):
        await task
    with db.session() as s:
        bindings = s.query(ChannelBindingRow).all()
        assert len(bindings) == 1
        assert bindings[0].channel_type == "feishu"
        assert bindings[0].external_user_id == "ou_userA"
        assert bindings[0].session_id
        assert [i.idempotency_key for i in s.query(IdempotencyRow).all()] == ["feishu:om_40001"]


async def test_sql_idempotency_rejects_after_restart(db):
    """新 adapter（去重缓存为空）+ 共享数据库 → SQL 唯一索引仍拦截。"""
    runner = FakeRunner()
    body = _event("om_50001", "重启后重发")
    adapter1, _ = _build_adapter(runner, database=db)
    await adapter1.handle_webhook("tenant_feishu_test", WebhookRequest(body=body))
    for task in list(adapter1._pending_tasks):
        await task
    adapter2, _ = _build_adapter(runner, database=db)
    await adapter2.handle_webhook("tenant_feishu_test", WebhookRequest(body=body))
    for task in list(adapter2._pending_tasks):
        await task
    assert len(runner.calls) == 1
