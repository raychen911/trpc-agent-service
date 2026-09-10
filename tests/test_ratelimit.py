"""频率限制与飞书多媒体消息处理测试。"""
import json

import pytest
import redis as redis_lib
from fakeredis import FakeRedis

from trpc_service.agent.runner import RunResult
from trpc_service.channels.base import WebhookRequest
from trpc_service.channels.feishu import FeishuAdapter, sha256_signature
from trpc_service.config.tenant_config import ChannelConfig, TenantConfig
from trpc_service.tenant.ratelimit import RateLimitExceeded, RateLimiter, rate_limiter


@pytest.fixture(autouse=True)
def _reset_global_limiter():
    """全局 rate_limiter 单例在测试间复位，避免窗口计数跨用例污染。"""
    rate_limiter.reset()
    yield
    rate_limiter.reset()


# ---- 频率限制器 ----

def test_memory_window_counts_and_blocks():
    limiter = RateLimiter()
    for _ in range(3):
        limiter.check("t1", "u1", limit_per_minute=3)
    try:
        limiter.check("t1", "u1", 3)
        raise AssertionError("should raise")
    except RateLimitExceeded:
        pass
    limiter.check("t1", "u2", 3)  # 其他用户不受影响


def test_zero_limit_means_unlimited():
    limiter = RateLimiter()
    for _ in range(100):
        limiter.check("t1", "u1", limit_per_minute=0)


def test_redis_shared_window():
    """两个"节点"共享同一 Redis：窗口计数合并。"""
    shared = FakeRedis(decode_responses=True)
    r1, r2 = RateLimiter(), RateLimiter()
    r1._redis = shared
    r2._redis = shared
    for _ in range(3):
        r1.check("t1", "u1", 3)
    try:
        r2.check("t1", "u1", 3)  # 两"节点"共享窗口计数，第 4 次超限
        raise AssertionError("should raise")
    except RateLimitExceeded:
        pass


def test_redis_failure_degrades_to_memory():
    class Exploding:
        def __getattr__(self, name):
            raise redis_lib.RedisError("down")

    limiter = RateLimiter()
    limiter._redis = Exploding()
    limiter.check("t1", "u1", 2)  # Redis 炸 → 降级内存，本次按内存计数
    assert limiter._redis is None, "故障后应永久降级"
    limiter.check("t1", "u1", 2)
    try:
        limiter.check("t1", "u1", 2)  # 内存第 3 次 → 超限
        raise AssertionError("should raise")
    except RateLimitExceeded:
        pass


# ---- 飞书多媒体消息与限流（fake API 传输层） ----

class FakeRunner:
    app_name = "t"

    async def run(self, user_id, session_id, message, files=None, agent_context=None):
        return RunResult(text="ok")


def _event(message_id: str, msg_type: str, text: str = "", chat_type: str = "p2p") -> str:
    content = json.dumps({"text": text}) if msg_type == "text" else "{}"
    return json.dumps(
        {
            "schema": "2.0",
            "header": {"event_id": f"ev-{message_id}", "event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_test"}},
                "message": {
                    "message_id": message_id,
                    "chat_id": "oc_chat1",
                    "chat_type": chat_type,
                    "message_type": msg_type,
                    "content": content,
                },
            },
        }
    )


def _adapter(sent: list) -> FeishuAdapter:
    tenant = TenantConfig(tenant_id="tenant_fs", name="t", rate_limit_per_minute=2)

    def fake_post(url, payload, token=""):
        sent.append({"url": url, "payload": payload, "token": token})
        if "tenant_access_token" in url:
            return {"code": 0, "tenant_access_token": "fake-token", "expire": 7200}
        return {"code": 0, "data": {}}

    cfg = ChannelConfig(enabled=True, app_id="cli_x", app_secret="s", encrypt_key="")
    adapter = FeishuAdapter(tenant, cfg, runner_getter=lambda _t: FakeRunner())
    adapter._http_post = fake_post
    return adapter


async def _deliver(adapter: FeishuAdapter, raw: str):
    resp = await adapter.handle_webhook(
        "tenant_fs", WebhookRequest(method="POST", body=raw)
    )
    for task in list(adapter._pending_tasks):
        await task
    return resp


async def test_media_message_gets_friendly_reply():
    """图片消息不再静默 ACK，经 API 发送引导文案。"""
    sent: list = []
    adapter = _adapter(sent)
    resp = await _deliver(adapter, _event("om_1", "image"))
    assert resp.status_code == 200
    texts = [json.loads(p["payload"]["content"])["text"] for p in sent if "messages" in p["url"]]
    assert any("文本" in t for t in texts)


async def test_text_message_flow_sends_reply():
    sent: list = []
    adapter = _adapter(sent)
    raw = _event("om_2", "text", text="你好")
    await _deliver(adapter, raw)
    texts = [json.loads(p["payload"]["content"])["text"] for p in sent if "messages" in p["url"]]
    assert texts == ["ok"]


async def test_group_mention_stripped():
    """群聊 @机器人 占位符剥离后执行。"""
    calls = []

    class RecordingRunner(FakeRunner):
        async def run(self, user_id, session_id, message, files=None, agent_context=None):
            calls.append(message)
            return RunResult(text="ok")

    tenant = TenantConfig(tenant_id="tenant_fs", name="t")
    cfg = ChannelConfig(enabled=True, app_id="cli_x", app_secret="s")
    adapter = FeishuAdapter(tenant, cfg, runner_getter=lambda _t: RecordingRunner())
    adapter._http_post = (
        lambda url, payload, token="": {"code": 0, "tenant_access_token": "t", "data": {}}
    )
    raw = json.dumps(
        {
            "schema": "2.0",
            "header": {"event_id": "ev-3", "event_type": "im.message.receive_v1"},
            "event": {
                "sender": {"sender_id": {"open_id": "ou_test"}},
                "message": {
                    "message_id": "om_3",
                    "chat_id": "oc_g1",
                    "chat_type": "group",
                    "message_type": "text",
                    "content": json.dumps({"text": "@_user_1 帮我查订单"}),
                },
            },
        }
    )
    await _deliver(adapter, raw)
    assert calls == ["帮我查订单"]


async def test_rate_limit_blocks_third_message():
    """rate_limit_per_minute=2：第 3 条文本被限流提示。"""
    sent: list = []
    adapter = _adapter(sent)
    await _deliver(adapter, _event("om_10", "text", text="m1"))
    await _deliver(adapter, _event("om_11", "text", text="m2"))
    await _deliver(adapter, _event("om_12", "text", text="m3"))
    texts = [json.loads(p["payload"]["content"])["text"] for p in sent if "messages" in p["url"]]
    assert texts[:2] == ["ok", "ok"]
    assert "频繁" in texts[2]


def test_signature_verification():
    """配置 encrypt_key 时验签：合法放行，伪造 403。"""
    tenant = TenantConfig(tenant_id="tenant_fs", name="t")
    cfg = ChannelConfig(enabled=True, app_id="a", app_secret="s", encrypt_key="key123")
    adapter = FeishuAdapter(tenant, cfg, runner_getter=lambda _t: FakeRunner())
    body = _event("om_20", "text", text="hi")
    sig = sha256_signature("111", "nonce1", "key123", body)
    headers = {"X-Lark-Signature": sig, "X-Lark-Timestamp": "111", "X-Lark-Nonce": "nonce1"}

    import asyncio

    ok = asyncio.run(
        adapter.handle_webhook("tenant_fs", WebhookRequest(body=body, headers=headers))
    )
    assert ok.status_code == 200
    bad = dict(headers, **{"X-Lark-Signature": "deadbeef"})
    rejected = asyncio.run(
        adapter.handle_webhook("tenant_fs", WebhookRequest(body=body, headers=bad))
    )
    assert rejected.status_code == 403


# ---- 企微：媒体消息与限流（加密被动回复） ----

import base64  # noqa: E402
import os  # noqa: E402
import time  # noqa: E402

from trpc_service.channels import wecom_crypto  # noqa: E402
from trpc_service.channels.wecom import WeComAdapter  # noqa: E402


def _make_aes_key() -> str:
    return base64.b64encode(os.urandom(32)).decode()[:43]


def _wecom_inner(user: str, msg_type: str, msg_id: str, content: str = "") -> str:
    extra = f"<Content><![CDATA[{content}]]></Content>" if content else ""
    return (
        "<xml><ToUserName><![CDATA[corp]]></ToUserName>"
        f"<FromUserName><![CDATA[{user}]]></FromUserName>"
        f"<CreateTime>{int(time.time())}</CreateTime>"
        f"<MsgType><![CDATA[{msg_type}]]></MsgType>"
        f"{extra}<MsgId>{msg_id}</MsgId></xml>"
    )


def _wecom_wrap(encrypt: str, token: str, nonce: str):
    ts = str(int(time.time()))
    sig = wecom_crypto.sha1_signature(token, ts, nonce, encrypt)
    body = (
        "<xml><Encrypt><![CDATA[" + encrypt + "]]></Encrypt>"
        "<MsgSignature><![CDATA[" + sig + "]]></MsgSignature>"
        "<TimeStamp>" + ts + "</TimeStamp><Nonce><![CDATA[" + nonce + "]]></Nonce></xml>"
    )
    return {"msg_signature": sig, "timestamp": ts, "nonce": nonce}, body


def _wecom_adapter() -> WeComAdapter:
    token, aes_key = _make_aes_key(), _make_aes_key()
    tenant = TenantConfig(
        tenant_id="tenant_wecom_rl",
        name="t",
        rate_limit_per_minute=2,
        channels={
            "wecom": ChannelConfig(
                enabled=True, token=token, encoding_aes_key=aes_key,
                corp_id="corp", bot_id="bot",
            )
        },
    )
    return WeComAdapter(tenant, tenant.channels["wecom"], runner_getter=lambda _t: FakeRunner())


async def test_wecom_media_message_gets_friendly_reply():
    """图片消息不再静默 ACK，返回引导文案（加密报文可解密回环）。"""
    adapter = _wecom_adapter()
    nonce = "w1"
    encrypt = wecom_crypto.encrypt_message(
        adapter.channel_config.encoding_aes_key, _wecom_inner("u1", "image", "70001"), "corp"
    )
    query, body = _wecom_wrap(encrypt, adapter.channel_config.token, nonce)
    resp = await adapter.handle_webhook(
        "tenant_wecom_rl", WebhookRequest(method="POST", query=query, body=body)
    )
    assert resp.status_code == 200
    assert resp.delivered_reply and "文本" in resp.delivered_reply


async def test_wecom_rate_limit_blocks_third_message():
    """rate_limit_per_minute=2：第 3 条文本被限流提示。"""
    adapter = _wecom_adapter()
    for i, msg_id in enumerate(["70011", "70012", "70013"]):
        nonce = f"w2{i}"
        encrypt = wecom_crypto.encrypt_message(
            adapter.channel_config.encoding_aes_key,
            _wecom_inner("u1", "text", msg_id, content=f"msg{i}"),
            "corp",
        )
        query, body = _wecom_wrap(encrypt, adapter.channel_config.token, nonce)
        resp = await adapter.handle_webhook(
            "tenant_wecom_rl", WebhookRequest(method="POST", query=query, body=body)
        )
        if i < 2:
            assert resp.delivered_reply == "ok"
        else:
            assert resp.delivered_reply and "频繁" in resp.delivered_reply
