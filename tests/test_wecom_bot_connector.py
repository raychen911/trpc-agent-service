# ===================================================================
# wecom_bot 长连接驱动集成测试（无网络）
# ===================================================================
# 覆盖: 帧->治理链->回复（与 webhook 同一条 process_event 链路）、
#   幂等去重、身份映射审计、单聊/群聊 session 确定性、SDK 事件派发接线。
# ===================================================================
import asyncio

import pytest

from trpc_service.channels.wecom_bot import WecomBotConnector
from trpc_service.filters.impl import build_filter_chain
from trpc_service.metrics.metrics import get_metrics
from trpc_service.runtime import MockAgentRunner, Runtime
from trpc_service.runtime.pipeline import process_event
from trpc_service.storage import InMemoryStorage
from trpc_service.tenant import TenantRegistry


async def _load_tenant_wb(tid):
    if tid == "wb1":
        return {
            "tenant_id":
            "wb1",
            "name":
            "智能机器人租户",
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
                "channel_type": "wecom_bot",
                "app_id": "bot_1",
                "secret_ref": "bot-secret",
                "user_id_mapping": {
                    "wx_u1": "emp_001"
                },
            }],
            "rate_limit_per_min":
            60,
        }
    return None


def _frame_single(content="你好", msgid="m1", userid="u1", req_id="r1"):
    return {
        "cmd": "aibot_msg_callback",
        "headers": {
            "req_id": req_id
        },
        "body": {
            "msgid": msgid,
            "aibotid": "bot_1",
            "chattype": "single",
            "from": {
                "userid": userid
            },
            "msgtype": "text",
            "text": {
                "content": content
            },
        },
    }


def _frame_group(content="@Teneuris 报销流程", msgid="m2", chatid="chat_9", userid="u1"):
    return {
        "cmd": "aibot_msg_callback",
        "headers": {
            "req_id": "r2"
        },
        "body": {
            "msgid": msgid,
            "aibotid": "bot_1",
            "chatid": chatid,
            "chattype": "group",
            "from": {
                "userid": userid
            },
            "msgtype": "text",
            "text": {
                "content": content
            },
        },
    }


class _FakeWS:
    """Fake 官方 WSClient（收集 reply，不发网络）。"""

    def __init__(self) -> None:
        self.replies: list[tuple[str, dict]] = []
        self.connected = False
        self.disconnected = False

    async def connect(self) -> None:
        self.connected = True

    async def reply(self, frame, body):
        req_id = (frame.get("headers") or {}).get("req_id", "")
        self.replies.append((req_id, body))
        return {"errcode": 0}

    def disconnect(self) -> None:
        self.disconnected = True


@pytest.fixture
def pipeline_env():
    """与 test_web.gateway fixture 同构的装配（InMemory + MockAgentRunner）。"""
    storage = InMemoryStorage()
    registry = TenantRegistry(load_fn=_load_tenant_wb)
    runtime = Runtime(registry=registry, storage=storage, runner=MockAgentRunner())
    chain, ctx = build_filter_chain(registry=registry, storage=storage, metrics=get_metrics())

    async def processor(event):
        return await process_event(event, chain=chain, ctx=ctx, runtime=runtime)

    return {"storage": storage, "registry": registry, "processor": processor}


def _config():
    from trpc_service.tenant import ImChannelConfig
    return ImChannelConfig(channel_type="wecom_bot", app_id="bot_1", secret_ref="s1", user_id_mapping={"u1": "emp_001"})


def _audit(storage, tenant="wb1"):
    return asyncio.run(storage.audit.query_logs(tenant, {}))


def test_group_frame_reply_and_audit_allow(pipeline_env):
    fake = _FakeWS()
    conn = WecomBotConnector(tenant_id="wb1", config=_config(), processor=pipeline_env["processor"], ws_client=fake)
    asyncio.run(conn.handle_text_frame(_frame_group()))
    # 群聊 @ 帧 -> 1 条 text 回复，req_id 与入站一致
    assert len(fake.replies) == 1
    req_id, body = fake.replies[0]
    assert req_id == "r2"
    assert body["msgtype"] == "stream"
    assert body["stream"]["finish"] is True
    assert "报销流程" in body["stream"]["content"]
    # 审计 allow + 身份映射后的内部 user_id（emp_001）
    logs = _audit(pipeline_env["storage"])
    assert any(e["decision"] == "allow" for e in logs)
    assert any(e["channel"] == "wecom_bot" for e in logs)
    assert any(e["user_id"] == "emp_001" for e in logs)


def test_duplicate_msgid_no_second_reply(pipeline_env):
    fake = _FakeWS()
    conn = WecomBotConnector(tenant_id="wb1", config=_config(), processor=pipeline_env["processor"], ws_client=fake)
    asyncio.run(conn.handle_text_frame(_frame_group()))
    # 同 msgid 重复推送（企微/网络重试）-> 幂等拦截，不产生第二次回复
    asyncio.run(conn.handle_text_frame(_frame_group()))
    assert len(fake.replies) == 1


def test_single_chat_session_deterministic(pipeline_env):
    """单聊: 同 bot+同人 -> session 恒定；不同人 -> 不同 session。"""
    fake = _FakeWS()
    seen: list[str] = []

    async def recorder(event):
        resp = await pipeline_env["processor"](event)
        seen.append(resp.session_id)
        return resp

    conn = WecomBotConnector(tenant_id="wb1", config=_config(), processor=recorder, ws_client=fake)
    asyncio.run(conn.handle_text_frame(_frame_single(msgid="a", req_id="ra")))
    asyncio.run(conn.handle_text_frame(_frame_single(msgid="b", req_id="rb")))
    asyncio.run(conn.handle_text_frame(_frame_single(msgid="c", userid="u2", req_id="rc")))
    assert seen[0] == seen[1]
    assert seen[0] != seen[2]


def test_start_stop_and_sdk_wiring(pipeline_env):
    """真实 WSClient 对象（不连网）验证 SDK 事件派发能到达我们的处理链。"""
    pytest.importorskip("aibot")
    from aibot import WSClient, WSClientOptions

    captured: list[tuple[str, dict]] = []

    async def fake_send_reply(req_id, body, cmd=None):
        captured.append((req_id, body))
        return {"errcode": 0}

    ws = WSClient(WSClientOptions(bot_id="bot_1", secret="s1"))
    ws._ws_manager.send_reply = fake_send_reply  # 仅桩出站，不发网络
    conn = WecomBotConnector(tenant_id="wb1", config=_config(), processor=pipeline_env["processor"], ws_client=ws)
    conn.attach()

    async def main():
        # 模拟 SDK 内部收到帧后的同步派发
        ws._message_handler.handle_frame(_frame_single(content="你好", msgid="w1", req_id="wr1"), ws)
        for _ in range(50):
            if captured:
                break
            await asyncio.sleep(0.01)
        # connector.run 生命周期: 挂起等待 -> 停止
        stop = asyncio.Event()
        task = asyncio.create_task(conn.run(stop))
        await asyncio.sleep(0)
        stop.set()
        await task

    asyncio.run(main())
    assert captured, "SDK 派发未到达 handler"
    assert captured[0][0] == "wr1"
    assert "你好" in captured[0][1]["stream"]["content"]
