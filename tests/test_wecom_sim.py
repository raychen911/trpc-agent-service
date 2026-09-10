"""企业微信通道模拟验证：不依赖真实凭证，构造报文做协议回环测试。"""
import base64
import os
import time
import xml.etree.ElementTree as ET

from trpc_service.agent.runner import RunResult
from trpc_service.channels import wecom_crypto
from trpc_service.channels.base import WebhookRequest
from trpc_service.channels.wecom import WeComAdapter
from trpc_service.config.tenant_config import ChannelConfig, TenantConfig
from trpc_service.tenant.governance.user_authz import user_authz
from trpc_service.agent.routing import SessionRouter


def _make_aes_key() -> str:
    return base64.b64encode(os.urandom(32)).decode()[:43]


def _wrap(encrypt: str, token: str, nonce: str) -> tuple[str, str]:
    ts = str(int(time.time()))
    sig = wecom_crypto.sha1_signature(token, ts, nonce, encrypt)
    body = (
        "<xml>"
        f"<Encrypt><![CDATA[{encrypt}]]></Encrypt>"
        f"<MsgSignature><![CDATA[{sig}]]></MsgSignature>"
        f"<TimeStamp>{ts}</TimeStamp>"
        f"<Nonce><![CDATA[{nonce}]]></Nonce>"
        "</xml>"
    )
    return {"msg_signature": sig, "timestamp": ts, "nonce": nonce}, body


def _inner_message(user: str, content: str, msg_id: str, to_user: str = "corp") -> str:
    return (
        "<xml>"
        f"<ToUserName><![CDATA[{to_user}]]></ToUserName>"
        f"<FromUserName><![CDATA[{user}]]></FromUserName>"
        f"<CreateTime>{int(time.time())}</CreateTime>"
        "<MsgType><![CDATA[text]]></MsgType>"
        f"<Content><![CDATA[{content}]]></Content>"
        f"<MsgId>{msg_id}</MsgId>"
        "</xml>"
    )


class FakeRunner:
    app_name = "customer_service"

    def __init__(self):
        self.calls = []

    async def run(self, user_id, session_id, message, files=None, agent_context=None):
        self.calls.append({"user_id": user_id, "session_id": session_id, "message": message})
        return RunResult(text="订单已发货，预计明天送达。")


def _build_adapter(token: str, aes_key: str) -> tuple[WeComAdapter, FakeRunner]:
    tenant = TenantConfig(
        tenant_id="tenant_wecom_test",
        name="测试租户",
        channels={
            "wecom": ChannelConfig(
                enabled=True, token=token, encoding_aes_key=aes_key, corp_id="corp_abc", bot_id="bot_001"
            )
        },
    )
    runner = FakeRunner()
    adapter = WeComAdapter(tenant, tenant.channels["wecom"], runner_getter=lambda _tid: runner)
    return adapter, runner


async def test_url_verify_roundtrip():
    token, aes_key = _make_aes_key(), _make_aes_key()
    adapter, _ = _build_adapter(token, aes_key)
    nonce = "n1"
    echo_plain = "echo-1234567890"
    echo = wecom_crypto.encrypt_message(aes_key, echo_plain, "corp_abc")
    query, _ = _wrap(echo, token, nonce)
    query["echostr"] = echo
    resp = await adapter.handle_webhook(
        "tenant_wecom_test", WebhookRequest(method="GET", query=query)
    )
    assert resp.status_code == 200
    assert resp.body == echo_plain


async def test_bad_signature_rejected():
    token, aes_key = _make_aes_key(), _make_aes_key()
    adapter, _ = _build_adapter(token, aes_key)
    nonce = "n1"
    echo = wecom_crypto.encrypt_message(aes_key, "echo", "corp_abc")
    query, _ = _wrap(echo, token, nonce)
    query["msg_signature"] = "deadbeef" * 5
    resp = await adapter.handle_webhook(
        "tenant_wecom_test", WebhookRequest(method="GET", query=query)
    )
    assert resp.status_code == 403


async def test_message_flow_and_reply_roundtrip():
    token, aes_key = _make_aes_key(), _make_aes_key()
    adapter, runner = _build_adapter(token, aes_key)
    nonce = "n2"
    inner = _inner_message("userA", "帮我查订单 O1001", "10001")
    encrypt = wecom_crypto.encrypt_message(aes_key, inner, "corp_abc")
    query, body = _wrap(encrypt, token, nonce)
    resp = await adapter.handle_webhook(
        "tenant_wecom_test", WebhookRequest(method="POST", query=query, body=body)
    )
    assert resp.status_code == 200
    assert resp.delivered_reply and "订单已发货" in resp.delivered_reply

    # 回复报文可解密回环
    reply_encrypt = ET.fromstring(resp.body).find("Encrypt").text
    reply_xml, receiveid = wecom_crypto.decrypt_message(aes_key, reply_encrypt)
    assert receiveid == "corp_abc"
    assert "订单已发货" in reply_xml

    # 用户绑定与会话路由
    assert user_authz.check("wecom", "userA", "tenant_wecom_test")
    expected_session = SessionRouter.session_id("tenant_wecom_test", "wecom", "userA")
    assert runner.calls and runner.calls[0]["session_id"] == expected_session

    # 幂等：同一 MsgId 重复投递不再执行
    resp2 = await adapter.handle_webhook(
        "tenant_wecom_test", WebhookRequest(method="POST", query=query, body=body)
    )
    assert resp2.body == "success"
    assert len(runner.calls) == 1


async def test_group_chat_session_isolated():
    token, aes_key = _make_aes_key(), _make_aes_key()
    adapter, runner = _build_adapter(token, aes_key)
    nonce = "n3"
    inner = _inner_message("userB", "群里问一下", "10002")
    encrypt = wecom_crypto.encrypt_message(aes_key, inner, "corp_abc")
    query, body = _wrap(encrypt, token, nonce)
    await adapter.handle_webhook(
        "tenant_wecom_test", WebhookRequest(method="POST", query=query, body=body)
    )
    session_single = SessionRouter.session_id("tenant_wecom_test", "wecom", "userB")
    session_room = SessionRouter.session_id("tenant_wecom_test", "wecom", "userB", chat_id="room9")
    assert runner.calls[0]["session_id"] == session_single
    assert session_single != session_room


async def test_revoke_message_silent_ack():
    """撤回事件：静默 ACK，不回复、不触发 Agent。"""
    token, aes_key = _make_aes_key(), _make_aes_key()
    adapter, runner = _build_adapter(token, aes_key)
    nonce = "n4"
    inner = (
        "<xml>"
        f"<ToUserName><![CDATA[corp]]></ToUserName>"
        f"<FromUserName><![CDATA[userC]]></FromUserName>"
        f"<CreateTime>{int(time.time())}</CreateTime>"
        "<MsgType><![CDATA[revoke]]></MsgType>"
        "<RevokedMsg><Content>撤回了一条消息</Content></RevokedMsg>"
        f"<MsgId>10003</MsgId>"
        "</xml>"
    )
    encrypt = wecom_crypto.encrypt_message(aes_key, inner, "corp_abc")
    query, body = _wrap(encrypt, token, nonce)
    resp = await adapter.handle_webhook(
        "tenant_wecom_test", WebhookRequest(method="POST", query=query, body=body)
    )
    assert resp.body == "success"
    assert resp.delivered_reply is None
    assert len(runner.calls) == 0
