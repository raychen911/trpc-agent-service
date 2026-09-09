# ===================================================================
# channels.feishu_sdk - 飞书官方 SDK 版通道（FeishuChannel 长连接）
# ===================================================================
# 说明: 与手写 webhook 版（channels/feishu.py，channel_type="feishu"）
#   **并存**的第二实现形态（PRD 3.3）。基于官方 lark-oapi 的高层
#   `lark_oapi.channel.FeishuChannel`:
#   - transport 默认 WebSocket 长连接（出站，免公网回调 URL）;
#     事件订阅二选一（长连接 vs webhook），同一应用不可双活。
#   - `channel.on("message")` 收归一化 InboundMessage: chat_type(p2p/group/topic)、
#     sender_id(open_id)、message_id、content_text、mentioned_bot。
#   - 回复 `channel.send(chat_id, {"text": ...}, {"reply_to": message_id,
#     "receive_id_type": "chat_id"})`。
# 规范: 治理链复用 runtime.pipeline.process_event（与 webhook/wecom_bot 同源）;
#   本模块不进 IMAdapter/factory（非 HTTP webhook 形态）。SDK 内置去重关闭
#   （SafetyConfig.dedup.enabled=False），让位平台 msg_id 幂等，统一各通道重复消息语义。
# ===================================================================

from __future__ import annotations

import asyncio
import os
import re
from typing import Any, Awaitable, Callable, Optional

from ..events import AgentEvent, AgentResponse, MessageType, ResponseType
from ..log.logger import get_logger
from ..tenant.models import ImChannelConfig

log = get_logger("channels.feishu_sdk")

CHANNEL_TYPE = "feishu_sdk"
_TEXT_MAX = 2000  # 对齐 feishu.py platform_limits().max_message_len
_GROUP_CHAT_TYPES = {"group", "topic"}

# 群聊文本开头 @机器人 mention（可多个 @/夹空白）
_MENTION_RE = re.compile(r"^\s*(?:@[^\s@]+\s*)+")

# ----------------------------------------------------------------------
# 纯函数（无 IO，单测直接覆盖）
# ----------------------------------------------------------------------


def strip_mention(content: str) -> str:
    """剥除群聊回调文本开头的 @机器人 mention（只剥开头，不依赖 bot 名）。"""
    if not content:
        return content
    return _MENTION_RE.sub("", content, count=1)


def split_text(content: str, max_len: int = _TEXT_MAX) -> list[str]:
    """超长文本分段（对齐 channels.base.split_long_message 语义）。"""
    if not content or len(content) <= max_len:
        return [content] if content else []
    return [content[i:i + max_len] for i in range(0, len(content), max_len)]


def _is_mentioned_bot(inbound: Any) -> bool:
    """判定群消息是否 @了当前机器人（双保险）。

    SDK 的 InboundMessage.mentioned_bot 在部分场景（如个人开发者环境 bot
    user_id 为空）计算为 False，即使 mentions 里确实 @了机器人（09-02 实测）。
    这里叠加校验 mentions 中 mentioned_type == "bot" 的条目。
    """
    if bool(getattr(inbound, "mentioned_bot", False)):
        return True
    mentions = getattr(inbound, "mentions", None) or []
    for m in mentions:
        if bool(getattr(m, "is_bot", False)):
            return True
        # 兼容其它实现/测试形态
        mtype = getattr(m, "mentioned_type", None)
        if mtype is None:
            mtype = getattr(m, "mentionedType", None)
        if mtype == "bot":
            return True
    return False


def inbound_to_event(inbound: Any, *, tenant_id: str, app_id: str = "") -> Optional[AgentEvent]:
    """SDK InboundMessage -> AgentEvent（鸭子类型，只读属性，测试无需引 lark-oapi）。

    门控:
      - 仅文本（raw_content_type 为空或 "text" 且 content 非空）;
      - 群聊/话题（group/topic）先剥开头的 @ 前缀（@门控由 SDK group policy 承担，
        能到达 handler 即已通过 @ 校验）;
      - 剥后内容为空 -> None（不触发 Agent）。

    映射（对齐 feishu.py webhook 语义）:
      channel_id = chat_id or app_id；user_id = sender_id(open_id)；msg_id = message_id
    """
    if inbound is None:
        return None
    chat_type = str(getattr(inbound, "chat_type", "") or "").lower()
    raw_ctype = str(getattr(inbound, "raw_content_type", "") or "").lower()
    if raw_ctype and raw_ctype != "text":
        return None

    content = (getattr(inbound, "content_text", "") or "").strip()
    if not content:
        # 兼容 content_text 缺失：回退 typed content.text
        typed = getattr(inbound, "content", None)
        if typed is not None:
            content = str(getattr(typed, "text", "") or "").strip()
    if not content:
        return None

    is_group = chat_type in _GROUP_CHAT_TYPES
    # 注意: 群聊 @ 门控交给 SDK group policy（默认需显式 @，未 @ 消息在到达
    # handler 前即被 reject，09-02 实测 policy_no_mention）。此处不再自设门控，
    # 因为 InboundMessage.mentioned_bot 对机器人 open_id 匹配不可靠
    # （个人环境实测为 False）；能到达 handler 的群消息即已通过 @ 校验。
    mentioned_bot = _is_mentioned_bot(inbound)
    if is_group:
        content = strip_mention(content).strip()
        if not content:
            return None

    chat_id = str(getattr(inbound, "chat_id", "") or "")
    sender_id = str(getattr(inbound, "sender_id", "") or "")
    message_id = str(getattr(inbound, "message_id", "") or getattr(inbound, "id", "") or "")
    if not message_id and not content:
        return None

    return AgentEvent(
        tenant_id=tenant_id,
        channel_type=CHANNEL_TYPE,
        channel_id=chat_id or app_id or CHANNEL_TYPE,
        user_id=sender_id,
        msg_id=message_id,
        content=content,
        msg_type=MessageType.TEXT,
        is_group=is_group,
        metadata={
            "chat_id": chat_id,
            "open_id": sender_id,
            "message_id": message_id,
            "chat_type": chat_type,
            "mentioned_bot": mentioned_bot,
            "reply_to_message_id": str(getattr(inbound, "reply_to_message_id", "") or ""),
            "raw_content_type": raw_ctype,
        },
    )


# ----------------------------------------------------------------------
# 驱动类
# ----------------------------------------------------------------------


class FeishuSdkConnector:
    """飞书官方 SDK 长连接驱动（feishu_sdk 通道）。

    持有官方 FeishuChannel（连接/重连/事件分发由 SDK 维护），只负责:
      InboundMessage -> AgentEvent -> process_event（与 webhook 同治理链）
      -> 回复经 channel.send(chat_id, ..., reply_to=message_id) 回原会话。

    Args:
        tenant_id: 绑定租户（demo）
        config: ImChannelConfig（feishu_sdk 形态: app_id=AppID, secret_ref=AppSecret）
        processor: process_event 闭包
        channel: 测试注入 fake；None 时懒建官方 FeishuChannel
    """

    def __init__(
        self,
        *,
        tenant_id: str,
        config: ImChannelConfig,
        processor: Callable[[AgentEvent], Awaitable[AgentResponse]],
        channel: Any = None,
    ) -> None:
        self._tenant_id = tenant_id
        self._config = config
        self._processor = processor
        self._channel: Any = channel
        self._started = False
        # 记录构造时所在事件循环（Gateway 主 loop）。FeishuChannel 在自己的后台
        # loop 线程分发事件，处理须桥接回此 loop 访问共享异步存储。
        try:
            self._loop: Optional[asyncio.AbstractEventLoop] = asyncio.get_running_loop()
        except RuntimeError:
            self._loop = None  # 同步上下文（单测）: 不桥接，直接执行

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def _build_channel(self) -> Any:
        """懒建官方 FeishuChannel（集中 import，缺失给出清晰错误）。

        SDK 日志级别经环境变量 FEISHU_SDK_LOG_LEVEL 可调（DEBUG 排查入站帧）。
        """
        import os

        try:
            from lark_oapi.channel import FeishuChannel
            from lark_oapi.channel.config import DedupConfig, SafetyConfig
        except ImportError as exc:  # pragma: no cover - 依赖缺失路径
            raise RuntimeError("feishu_sdk 通道需要 lark-oapi（pip install 'lark-oapi>=1.7.3,<1.8.0'），"
                               "请先安装依赖") from exc
        secret = ""
        if self._config.secret_ref is not None:
            secret = self._config.secret_ref.get_secret_value()
        # SDK 日志级别经环境变量 FEISHU_SDK_LOG_LEVEL 调整（DEBUG 排查入站帧）
        log_level = None
        level_name = os.environ.get("FEISHU_SDK_LOG_LEVEL", "").upper()
        if level_name in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            from lark_oapi.core.enum import LogLevel

            log_level = LogLevel[level_name]
        kwargs: dict[str, Any] = {"safety": SafetyConfig(dedup=DedupConfig(enabled=False))}
        if log_level is not None:
            kwargs["log_level"] = log_level
        # 关闭 SDK 两层去重 -> 让位 platform process_event msg_id 幂等
        return FeishuChannel(app_id=self._config.app_id, app_secret=secret, **kwargs)

    def attach(self, channel: Any = None) -> Any:
        """注册事件监听（message / error / reject）。

        注意: FeishuChannel.on(event, handler) 要求显式传 handler，
        不支持 @channel.on("message") 装饰器形态（实测 TypeError）。
        """
        channel = channel or self._channel
        if channel is None:
            channel = self._build_channel()
            self._channel = channel

        async def _on_message(inbound: Any) -> None:
            try:
                if os.environ.get("FEISHU_SDK_LOG_LEVEL", "").upper() == "DEBUG":
                    log.info(
                        "feishu_sdk inbound: chat_type=%r sender=%r msg_id=%r mentioned=%r "
                        "raw_type=%r content_text=%r content=%r", getattr(inbound, "chat_type", None),
                        getattr(inbound, "sender_id", None), getattr(inbound, "message_id", None),
                        getattr(inbound, "mentioned_bot", None), getattr(inbound, "raw_content_type", None),
                        getattr(inbound, "content_text", None), getattr(inbound, "content", None))
                # FeishuChannel 在自身后台 loop 线程分发事件，而平台存储（redis 等）
                # 绑定在 Gateway 主 loop —— 跨 loop 直接 await 会报
                # "Future attached to a different loop"（09-02 实测）。桥接回主 loop 执行。
                if self._loop is not None and asyncio.get_running_loop() is not self._loop:
                    await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(self.handle_inbound(inbound),
                                                                               self._loop))
                else:
                    await self.handle_inbound(inbound)
            except Exception:  # noqa: BLE001 - 单条消息异常不影响连接
                log.warning("feishu_sdk 处理消息异常", extra={"tenant": self._tenant_id}, exc_info=True)

        async def _on_error(error: Any) -> None:
            log.warning(f"feishu_sdk 事件 error: {error}")

        async def _on_reject(reason: Any) -> None:
            # 群聊未 @ 等 policy 丢弃（SDK 去重已关闭，剩余 reject 多为 policy）
            log.info(f"feishu_sdk 事件 reject: {reason}")

        channel.on("message", _on_message)
        channel.on("error", _on_error)
        channel.on("reject", _on_reject)
        return channel

    async def start(self) -> None:
        """建连并保持（SDK 维护后台连接/重连）；失败仅告警不拖垮 Gateway。"""
        if self._started:
            return
        channel = self._channel or self._build_channel()
        self._channel = channel
        self.attach(channel)
        self._started = True
        # async Web 框架下用 connect_until_ready：就绪即返回，连接由 SDK 后台维持
        await channel.connect_until_ready(timeout=30.0)

    async def stop(self) -> None:
        """断开长连接（注意: 飞书 disconnect 是协程，与 wecom_bot 的同步不同）。"""
        self._started = False
        if self._channel is not None:
            try:
                await self._channel.disconnect()
            except Exception:  # noqa: BLE001 - 断开失败仅告警
                log.warning("feishu_sdk disconnect 异常", exc_info=True)

    async def run(self, stop_event: asyncio.Event) -> None:
        """supervisor 任务（供 _cli create_task）: start -> 等待停止 -> 断开。"""
        try:
            await self.start()
        except Exception:  # noqa: BLE001 - 启动失败不阻断 Gateway
            log.warning("feishu_sdk 启动失败（仅告警）", extra={"tenant": self._tenant_id}, exc_info=True)
        try:
            await stop_event.wait()
        finally:
            await self.stop()

    # ------------------------------------------------------------------
    # 消息处理
    # ------------------------------------------------------------------

    async def handle_inbound(self, inbound: Any) -> None:
        """InboundMessage -> AgentEvent -> 治理链 -> 回复（ERROR 不回复，仅留痕）。"""
        event = inbound_to_event(inbound, tenant_id=self._tenant_id, app_id=self._config.app_id)
        if event is None:
            return
        # 身份映射（对齐 webhook 语义，PRD 3.4）
        mapped = self._config.user_id_mapping.get(event.user_id, event.user_id)
        if mapped != event.user_id:
            event.metadata["external_user_id"] = event.user_id
            event.user_id = mapped

        response = await self._processor(event)
        if response.response_type == ResponseType.TEXT and response.content:
            chat_id = event.metadata.get("chat_id") or event.channel_id
            if not chat_id:
                return
            for chunk in split_text(response.content):
                if not await self._reply_chunk(chat_id, chunk, event.msg_id):
                    break  # 投递失败即停

    async def _reply_chunk(self, chat_id: str, chunk: str, message_id: str) -> bool:
        """单段回复；返回 False 表示投递失败（调用方应停止后续分段）。"""
        if self._channel is None:
            return False
        try:
            await self._channel.send(
                chat_id,
                {"text": chunk},
                {
                    "reply_to": message_id,
                    "receive_id_type": "chat_id"
                },
            )
            return True
        except Exception as exc:  # noqa: BLE001 - 单段投递失败仅告警
            log.warning(f"feishu_sdk send 失败: {type(exc).__name__}: {exc}")
            return False
