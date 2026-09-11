"""Bounded, opt-in WeCom transport probe. No model, database, or ordinary-message replies."""

import asyncio
import logging
import os
import secrets
from types import SimpleNamespace

from .wecom import WecomAdapter
from .models import ChannelType

REPLY = "企微连接测试成功：已收到你的测试口令，并完成固定文本回复。这是连通性测试，尚未调用 Agent 或模型。"


async def probe(adapter, bot_id, *, timeout=180, challenge=None, emit=print):
    if not 5 <= timeout <= 600:
        raise ValueError("test timeout must be between 5 and 600 seconds")
    phrase = challenge or "连通测试 " + secrets.token_hex(5)
    authenticated, disconnected = asyncio.Event(), asyncio.Event()
    messages = asyncio.Queue(maxsize=1)
    claimed = False
    binding = SimpleNamespace(channel=ChannelType.WECOM,
                              external_account_id=bot_id,
                              webhook_public_id="local-probe-only")

    async def ready(_):
        authenticated.set()

    async def lost(_):
        disconnected.set()

    async def receive(frame):
        nonlocal claimed
        if claimed:
            return
        try:
            event = adapter.normalize(frame, binding)
        except Exception:
            return
        if event.kind != "chat" or event.message.conversation_type.value != "direct" or event.message.text != phrase:
            return
        claimed = True
        # Reply outside the SDK receive callback so its reader can process the ACK.
        messages.put_nowait(event)

    adapter.client.on("authenticated", ready)
    adapter.client.on("disconnected", lost)
    adapter.client.on("error", lost)
    adapter.client.on("message", receive)
    deadline = asyncio.get_running_loop().time() + timeout
    stage = "connecting"

    async def wait_for(awaitable, limit):
        task = asyncio.ensure_future(awaitable)
        broken = asyncio.create_task(disconnected.wait())
        try:
            done, _ = await asyncio.wait({task, broken},
                                         timeout=max(0, min(limit, deadline - asyncio.get_running_loop().time())),
                                         return_when=asyncio.FIRST_COMPLETED)
            if broken in done:
                raise ConnectionError()
            if task not in done:
                raise TimeoutError()
            return task.result()
        finally:
            task.cancel()
            broken.cancel()
            await asyncio.gather(task, broken, return_exceptions=True)

    try:
        emit("正在连接企业微信，日志不会输出 Bot ID、Secret 或消息正文。")
        await wait_for(adapter.client.connect_async(), 20)
        await wait_for(authenticated.wait(), 20)
        if not adapter.client.is_authenticated:
            raise ConnectionError()
        stage = "waiting_message"
        emit("长连接认证成功。请打开这个机器人的单聊窗口，发送下面这整行：")
        emit(phrase)
        event = await wait_for(messages.get(), timeout)
        stage = "sending_reply"
        emit("已收到匹配的单聊测试口令，正在回复一次固定文本。")
        result = await wait_for(adapter.send_text(event.reply_context, REPLY, stream_id=secrets.token_hex(16)), 15)
        if result.outcome == "accepted":
            emit("回复已获企微协议确认。请在机器人聊天窗口查看测试回复。")
            return {"status": "protocol_ack", "authenticated": True, "received": True, "reply_outcome": "accepted"}
        emit("未确认回复成功；本次不会自动重发。")
        return {
            "status": "reply_" + result.outcome,
            "authenticated": True,
            "received": True,
            "reply_outcome": result.outcome
        }
    except (TimeoutError, ConnectionError):
        outcome = "unknown" if stage == "sending_reply" else "not_sent"
        emit("测试超时或连接中断；已停止本次测试，不自动重发。")
        return {
            "status": stage + "_incomplete",
            "authenticated": authenticated.is_set(),
            "received": claimed,
            "reply_outcome": outcome
        }
    except Exception:
        emit("企微测试未完成。请核对长连接模式、凭据和网络；原始异常已隐藏。")
        return {
            "status": stage + "_failed",
            "authenticated": authenticated.is_set(),
            "received": claimed,
            "reply_outcome": "unknown" if stage == "sending_reply" else "not_sent"
        }
    finally:
        try:
            await asyncio.wait_for(adapter.close(), 10)
        except Exception:
            emit("连接清理未确认，测试进程退出后请检查机器人连接状态。")


async def run_from_environment(timeout=180):
    bot_id, secret = os.getenv("TRPC_WECOM_BOT_ID"), os.getenv("TRPC_WECOM_BOT_SECRET")
    if not bot_id or not secret:
        print("缺少企微凭据。请先运行 configure-im，再使用 --secrets-file .secrets/im.json。")
        return False
    # The public protocol client has a quiet logger; silence low-level frame loggers too.
    for name in ("websockets", "httpx", "httpcore", "wecom_aibot_sdk"):
        logging.getLogger(name).setLevel(logging.CRITICAL + 1)
    result = await probe(WecomAdapter.create(bot_id, secret), bot_id, timeout=timeout)
    return result["status"] == "protocol_ack"
