"""企微智能机器人长连接通道测试（fake frame 回环，不依赖真实网络）。"""

import pytest

from trpc_service.agent.runner import RunResult
from trpc_service.channels.base import WebhookRequest  # noqa: F401  (口径对齐)
from trpc_service.channels.wecom_smartbot import WeComSmartBotChannel
from trpc_service.config.tenant_config import ChannelConfig, TenantConfig
from trpc_service.tenant.ratelimit import rate_limiter


class FakeRunner:
    app_name = "tenant_wsb_cs"

    async def run(self, user_id, session_id, message, files=None, agent_context=None):
        return RunResult(text=f"echo:{message}")


class FakeWsClient:
    def __init__(self, sent: list):
        self.sent = sent

    async def reply_stream(self, frame, stream_id, content, finish=False, **kw):
        self.sent.append({"stream_id": stream_id, "content": content, "finish": finish})
        return frame


def _channel(sent: list) -> WeComSmartBotChannel:
    tenant = TenantConfig(tenant_id="tenant_wsb", name="t", rate_limit_per_minute=100)
    channel = WeComSmartBotChannel(
        tenant, ChannelConfig(enabled=True, bot_id="bot_x", secret="s"),
        runner_getter=lambda _t: FakeRunner(),
    )
    channel._client = FakeWsClient(sent)
    return channel


def _frame(msg_id, text, chat_type="single", chatid=""):
    body = {
        "msgid": msg_id,
        "msgtype": "text",
        "from": {"userid": "ou_user1"},
        "chattype": chat_type,
        "chatid": chatid,
        "text": {"content": text},
    }
    from wecom_aibot_sdk import WsFrame

    return WsFrame(headers={"req_id": f"r-{msg_id}"}, body=body)


@pytest.fixture(autouse=True)
def _reset_limiter():
    rate_limiter.reset()
    yield
    rate_limiter.reset()


async def test_text_message_roundtrip():
    """文本消息 → execute_chat → reply_stream 回复。"""
    sent: list = []
    channel = _channel(sent)
    await channel._on_message(_frame("sm_1", "你好"))
    assert len(sent) == 1
    assert sent[0]["content"] == "echo:你好"
    assert sent[0]["finish"] is True


async def test_idempotent_redelivery():
    """同 msgid 重复投递只执行一次（第二帧直接丢弃）。"""
    sent: list = []
    channel = _channel(sent)
    await channel._on_message(_frame("sm_2", "重复投递"))
    await channel._on_message(_frame("sm_2", "重复投递"))
    assert len(sent) == 1


async def test_group_session_isolated():
    """群聊 session 追加 chatid 维度，与单聊隔离。"""

    from trpc_service.agent.routing import SessionRouter

    calls = []

    class RecordingRunner(FakeRunner):
        async def run(self, user_id, session_id, message, files=None, agent_context=None):
            calls.append(session_id)
            return RunResult(text="ok")

    sent: list = []
    tenant = TenantConfig(tenant_id="tenant_wsb", name="t")
    channel = WeComSmartBotChannel(
        tenant, ChannelConfig(enabled=True, bot_id="b", secret="s"),
        runner_getter=lambda _t: RecordingRunner(),
    )
    channel._client = FakeWsClient(sent)
    await channel._on_message(_frame("sm_g1", "@客服 帮我查订单", chat_type="group", chatid="oc_g1"))
    expected_group = SessionRouter.session_id("tenant_wsb", "wecom_smartbot", "ou_user1", "oc_g1")
    single = SessionRouter.session_id("tenant_wsb", "wecom_smartbot", "ou_user1")
    assert calls == [expected_group]
    assert single != expected_group


async def test_rate_limit_third_message_blocked():
    sent: list = []
    tenant = TenantConfig(tenant_id="tenant_wsb", name="t", rate_limit_per_minute=2)
    channel = WeComSmartBotChannel(
        tenant, ChannelConfig(enabled=True, bot_id="b", secret="s"),
        runner_getter=lambda _t: FakeRunner(),
    )
    channel._client = FakeWsClient(sent)
    for i, mid in enumerate(["sm_r1", "sm_r2", "sm_r3"]):
        await channel._on_message(_frame(mid, f"m{i}"))
    assert "频繁" in sent[2]["content"]


async def test_media_message_hint():
    sent: list = []
    channel = _channel(sent)
    from wecom_aibot_sdk import WsFrame

    body = {"msgid": "sm_img1", "msgtype": "image", "from": {"userid": "ou_user1"}}
    await channel._on_message(WsFrame(headers={"req_id": "r"}, body=body))
    assert "文本" in sent[0]["content"]
