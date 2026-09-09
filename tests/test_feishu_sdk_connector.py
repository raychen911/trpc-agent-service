# ===================================================================
# feishu_sdk 长连接驱动测试（无网络）
# ===================================================================
# 覆盖: InboundMessage -> 治理链 -> send 回发、幂等、身份映射审计、
#   单/群 session 确定性、分段、ERROR 不回发、生命周期。
# ===================================================================
import asyncio

import pytest

from trpc_service.channels.feishu_sdk import FeishuSdkConnector
from trpc_service.filters.impl import build_filter_chain
from trpc_service.metrics.metrics import get_metrics
from trpc_service.runtime import MockAgentRunner, Runtime
from trpc_service.runtime.pipeline import process_event
from trpc_service.storage import InMemoryStorage
from trpc_service.tenant import TenantRegistry


async def _load_tenant_fs(tid):
    if tid == "fs1":
        return {
            "tenant_id":
            "fs1",
            "name":
            "飞书SDK租户",
            "status":
            "active",
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
                "channel_type": "feishu_sdk",
                "app_id": "cli_app1",
                "secret_ref": "app_secret",
                "user_id_mapping": {
                    "ou_u1": "emp_001"
                },
                "user_acl": {
                    "enabled": True,
                    "blocklist": ["ou_blocked"]
                },
            }],
            "rate_limit_per_min":
            60,
        }
    return None


def _inbound(chat_type="p2p",
             content="你好",
             sender="ou_u1",
             msg_id="om_1",
             chat_id="oc_p2p",
             mentioned=False,
             raw_ctype="text",
             mentions=None):
    """构造鸭子类型 InboundMessage（对齐 SDK 字段）。"""

    class _Inbound:
        pass

    m = _Inbound()
    m.chat_type = chat_type
    m.content_text = content
    m.sender_id = sender
    m.message_id = msg_id
    m.chat_id = chat_id
    m.mentioned_bot = mentioned
    m.raw_content_type = raw_ctype
    m.content = None
    m.reply_to_message_id = ""
    m.mentions = mentions or []
    return m


def _mention(mention_type="bot", is_bot=True):
    """鸭子类型 mention（对齐 SDK: is_bot + mentioned_type 兼容）。"""

    class _Mention:
        pass

    mt = _Mention()
    mt.is_bot = is_bot
    mt.mentioned_type = mention_type
    return mt


class _FakeChannel:
    """无网替身 FeishuChannel（收集 send，注册 handler）。"""

    def __init__(self) -> None:
        self.handlers: dict[str, object] = {}
        self.sent: list[tuple] = []
        self.ready = False
        self.disconnected = False

    def on(self, event: str, handler=None):
        if handler is None:

            def deco(fn):
                self.handlers[event] = fn
                return fn

            return deco
        self.handlers[event] = handler

    async def connect_until_ready(self, timeout=None) -> None:
        self.ready = True

    async def disconnect(self) -> None:
        self.disconnected = True

    async def send(self, chat_id, message, opts=None):
        self.sent.append((chat_id, message, opts))

    async def fire(self, event: str, *args) -> None:
        handler = self.handlers.get(event)
        if handler is not None:
            result = handler(*args)
            if asyncio.iscoroutine(result):
                await result


@pytest.fixture
def pipeline_env():
    storage = InMemoryStorage()
    registry = TenantRegistry(load_fn=_load_tenant_fs)
    runtime = Runtime(registry=registry, storage=storage, runner=MockAgentRunner())
    chain, ctx = build_filter_chain(registry=registry, storage=storage, metrics=get_metrics())

    async def processor(event):
        return await process_event(event, chain=chain, ctx=ctx, runtime=runtime)

    return {"storage": storage, "registry": registry, "processor": processor}


def _config():
    from trpc_service.tenant import ImChannelConfig
    return ImChannelConfig(channel_type="feishu_sdk",
                           app_id="cli_app1",
                           secret_ref="s1",
                           user_id_mapping={"ou_u1": "emp_001"})


def _audit(storage, tenant="fs1"):
    return asyncio.run(storage.audit.query_logs(tenant, {}))


def test_p2p_reply_and_audit(pipeline_env):
    fake = _FakeChannel()
    conn = FeishuSdkConnector(tenant_id="fs1", config=_config(), processor=pipeline_env["processor"], channel=fake)
    asyncio.run(conn.handle_inbound(_inbound()))
    assert len(fake.sent) == 1
    chat_id, body, opts = fake.sent[0]
    assert chat_id == "oc_p2p"
    assert body == {"text": "（本地回声） 你好"}
    assert opts == {"reply_to": "om_1", "receive_id_type": "chat_id"}
    logs = _audit(pipeline_env["storage"])
    assert any(e["decision"] == "allow" and e["channel"] == "feishu_sdk" for e in logs)
    assert any(e["user_id"] == "emp_001" for e in logs)  # 身份映射生效


def test_group_mention_strip(pipeline_env):
    fake = _FakeChannel()
    conn = FeishuSdkConnector(tenant_id="fs1", config=_config(), processor=pipeline_env["processor"], channel=fake)
    asyncio.run(
        conn.handle_inbound(
            _inbound(chat_type="group", content="@Teneuris 报销流程", msg_id="om_g", chat_id="oc_g", mentioned=True)))
    assert len(fake.sent) == 1
    chat_id, body, _ = fake.sent[0]
    assert chat_id == "oc_g"
    assert "报销流程" in body["text"] and "@Teneuris" not in body["text"]


def test_group_plain_processed_without_sdk_policy(pipeline_env):
    """群 @ 门控由 SDK group policy 承担（未 @ 消息在 handler 前被 reject）。

    此处直接喂 handler（绕过 SDK 层）的群消息仍应处理并剥 @，用于覆盖
    mentioned_bot 不可靠但消息确已通过 SDK @ 校验的真实场景。
    """
    fake = _FakeChannel()
    conn = FeishuSdkConnector(tenant_id="fs1", config=_config(), processor=pipeline_env["processor"], channel=fake)
    asyncio.run(
        conn.handle_inbound(
            _inbound(chat_type="group", content="@星辰 你好", msg_id="om_g2", chat_id="oc_g", mentioned=False)))
    assert len(fake.sent) == 1
    chat_id, body, _ = fake.sent[0]
    assert chat_id == "oc_g"
    assert body["text"].endswith("你好") and "@星辰" not in body["text"]


def test_group_mention_via_mentions_field(pipeline_env):
    """SDK mentioned_bot=False 但 mentions 含 bot（个人环境实测）也应视为 @。"""
    fake = _FakeChannel()
    conn = FeishuSdkConnector(tenant_id="fs1", config=_config(), processor=pipeline_env["processor"], channel=fake)
    inbound = _inbound(chat_type="group",
                       content="@星辰 报销流程",
                       msg_id="om_g3",
                       chat_id="oc_g",
                       mentioned=False,
                       mentions=[_mention("bot")])
    asyncio.run(conn.handle_inbound(inbound))
    assert len(fake.sent) == 1
    chat_id, body, _ = fake.sent[0]
    assert chat_id == "oc_g"
    assert "报销流程" in body["text"] and "@星辰" not in body["text"]


def test_non_text_ignored(pipeline_env):
    fake = _FakeChannel()
    conn = FeishuSdkConnector(tenant_id="fs1", config=_config(), processor=pipeline_env["processor"], channel=fake)
    asyncio.run(conn.handle_inbound(_inbound(content="[图片]", raw_ctype="image")))
    assert fake.sent == []


def test_duplicate_msgid_single_reply(pipeline_env):
    fake = _FakeChannel()
    conn = FeishuSdkConnector(tenant_id="fs1", config=_config(), processor=pipeline_env["processor"], channel=fake)
    asyncio.run(conn.handle_inbound(_inbound(msg_id="dup_1")))
    asyncio.run(conn.handle_inbound(_inbound(msg_id="dup_1")))
    assert len(fake.sent) == 1


def test_p2p_session_deterministic(pipeline_env):
    fake = _FakeChannel()
    seen: list[str] = []

    async def recorder(event):
        resp = await pipeline_env["processor"](event)
        seen.append(resp.session_id)
        return resp

    conn = FeishuSdkConnector(tenant_id="fs1", config=_config(), processor=recorder, channel=fake)
    asyncio.run(conn.handle_inbound(_inbound(msg_id="a", sender="ou_u1")))
    asyncio.run(conn.handle_inbound(_inbound(msg_id="b", sender="ou_u1")))
    asyncio.run(conn.handle_inbound(_inbound(msg_id="c", sender="ou_other")))
    assert seen[0] == seen[1]
    assert seen[0] != seen[2]


def test_long_text_split_into_multiple_sends(pipeline_env):
    fake = _FakeChannel()
    conn = FeishuSdkConnector(tenant_id="fs1", config=_config(), processor=pipeline_env["processor"], channel=fake)
    long_content = "长" * 5000
    asyncio.run(conn.handle_inbound(_inbound(content=long_content, msg_id="long_1")))
    assert len(fake.sent) > 1
    assert all(len(body["text"]) <= 2000 for _, body, _ in fake.sent)


def test_blocked_user_no_reply_audit_block(pipeline_env):
    fake = _FakeChannel()
    conn = FeishuSdkConnector(tenant_id="fs1", config=_config(), processor=pipeline_env["processor"], channel=fake)
    asyncio.run(conn.handle_inbound(_inbound(sender="ou_blocked", msg_id="blk_1")))
    assert fake.sent == []
    logs = _audit(pipeline_env["storage"])
    assert any(e["decision"] == "block" for e in logs)


def test_run_stop_lifecycle(pipeline_env):
    fake = _FakeChannel()
    conn = FeishuSdkConnector(tenant_id="fs1", config=_config(), processor=pipeline_env["processor"], channel=fake)

    async def main():
        stop = asyncio.Event()
        task = asyncio.create_task(conn.run(stop))
        for _ in range(50):
            if fake.ready:
                break
            await asyncio.sleep(0.01)
        assert fake.ready
        stop.set()
        await task
        assert fake.disconnected

    asyncio.run(main())


def test_run_start_failure_does_not_raise(pipeline_env):

    class _BadChannel(_FakeChannel):

        async def connect_until_ready(self, timeout=None):
            raise RuntimeError("连接失败")

    bad = _BadChannel()
    conn = FeishuSdkConnector(tenant_id="fs1", config=_config(), processor=pipeline_env["processor"], channel=bad)

    async def main():
        stop = asyncio.Event()
        task = asyncio.create_task(conn.run(stop))
        await asyncio.sleep(0.05)
        stop.set()
        await task  # 不抛异常

    asyncio.run(main())


def test_real_sdk_api_shape(pipeline_env):
    """真实 lark-oapi FeishuChannel API 形状（不实例化，避免长连接资源泄漏）。

    09-02 实测约束: on(event, handler) 必须显式传 handler，不支持装饰器形态。
    """
    pytest.importorskip("lark_oapi")
    import inspect

    from lark_oapi.channel import FeishuChannel
    sig = inspect.signature(FeishuChannel.on)
    assert sig.parameters["handler"].default is None  # on(event, handler)
    assert hasattr(FeishuChannel, "connect_until_ready")
    assert hasattr(FeishuChannel, "disconnect")
    assert hasattr(FeishuChannel, "send")
