# ===================================================================
# channels.wecom_bot - 企业微信智能机器人通道（长连接形态，群聊 @）
# ===================================================================
# 说明: 企微「智能机器人」（工作台创建，API 模式·长连接）第二接入形态
#   （PRD 3.3/3.5）。与 HTTP webhook 形态的 wechat_work（自建应用）不同:
#   - 平台以出站 WSS 连接 openws.work.weixin.qq.com（官方 asyncio SDK，
#     import 名 `aibot`），无需公网回调 URL / echostr 验签。
#   - 入站帧: {"cmd":"aibot_msg_callback","headers":{"req_id"},
#              "body":{"msgid","aibotid","chatid(仅群)","chattype":"single|group",
#                      "from":{"userid"},"msgtype":"text","text":{"content":"@Bot hi"}}}
#   - chattype=single/group 提供群聊标识（补 PRD 3.5「应用消息无群聊」缺口）；
#     回复经 reply(frame, body) 透传 req_id，由企微路由回原会话（群/单）。
# 规范: 治理链入口与 webhook 共用 runtime.pipeline.process_event；
#   本类只做 帧->AgentEvent 映射 + 回复接线，不实现 IMAdapter（非 HTTP 形态）。
# ===================================================================

from __future__ import annotations

import asyncio
import re
import secrets
from typing import Any, Awaitable, Callable, Optional

from ..events import AgentEvent, AgentResponse, MessageType, ResponseType
from ..log.logger import get_logger
from ..tenant.models import ImChannelConfig

log = get_logger("channels.wecom_bot")

CHANNEL_TYPE = "wecom_bot"
_TEXT_MAX = 2048  # 与企微通道一致（PRD 3.6 消息长度）

# 群聊回调文本开头的 @机器人 mention（可多个 @/夹空白）
_MENTION_RE = re.compile(r"^\s*(?:@[^\s@]+\s*)+")

# ----------------------------------------------------------------------
# 纯函数（无 IO，单测直接覆盖）
# ----------------------------------------------------------------------


def strip_mention(content: str) -> str:
    """剥除群聊回调文本开头的 @机器人 mention（只剥开头，不依赖 bot 名）。

    如 "@Teneuris 你好" -> "你好"；"  @A @B hi " -> "hi "。
    不命中保留原文（群消息未带 @ 时按原文处理）。
    """
    if not content:
        return content
    return _MENTION_RE.sub("", content, count=1)


def split_text(content: str, max_len: int = _TEXT_MAX) -> list[str]:
    """超长文本分段（对齐 channels.base.split_long_message 语义）。"""
    if not content or len(content) <= max_len:
        return [content] if content else []
    return [content[i:i + max_len] for i in range(0, len(content), max_len)]


def frame_to_event(frame: dict[str, Any], *, tenant_id: str) -> Optional[AgentEvent]:
    """入站 aibot_msg_callback 帧 -> AgentEvent。

    仅处理文本消息；非消息 / 非 text / 剥 @ 后为空返回 None（不触发 Agent）。
    单聊: channel_id=aibotid, is_group=False；
    群聊: channel_id=chatid, is_group=True，content 先剥 @ mention。
    """
    body = frame.get("body") if isinstance(frame, dict) else None
    if not isinstance(body, dict):
        return None
    if body.get("msgtype") != "text":
        return None

    chat_type = body.get("chattype") or "single"
    text_obj = body.get("text")
    content = (text_obj or {}).get("content", "") if isinstance(text_obj, dict) else ""
    if chat_type == "group":
        content = strip_mention(content)
    content = content.strip()
    if not content:
        return None

    aibotid = str(body.get("aibotid") or "")
    chat_id = str(body.get("chatid") or "")
    userinfo = body.get("from") or {}
    user_id = userinfo.get("userid", "") if isinstance(userinfo, dict) else ""
    headers = frame.get("headers") if isinstance(frame, dict) else None
    req_id = (headers or {}).get("req_id", "") if isinstance(headers, dict) else ""
    is_group = chat_type == "group"

    return AgentEvent(
        tenant_id=tenant_id,
        channel_type=CHANNEL_TYPE,
        channel_id=chat_id if is_group and chat_id else (aibotid or CHANNEL_TYPE),
        user_id=user_id,
        msg_id=str(body.get("msgid") or ""),
        content=content,
        msg_type=MessageType.TEXT,
        is_group=is_group,
        metadata={
            "chat_id": chat_id,
            "aibotid": aibotid,
            "chat_type": chat_type,
            "req_id": req_id,
        },
    )


def build_stream_reply(content: str, stream_id: str) -> dict[str, Any]:
    """AgentResponse 文本 -> 长连接被动回复 body。

    企微智能机器人对 aibot_respond_msg 的文本回复需走流式消息
    （msgtype=stream + finish=true）；msgtype=text 仅欢迎语支持
    （实测 errcode=40008 invalid message type）。
    """
    return {
        "msgtype": "stream",
        "stream": {
            "id": stream_id,
            "finish": True,
            "content": content,
        },
    }


def new_stream_id() -> str:
    """生成本次被动回复的流式消息 ID（对齐 SDK generate_req_id 语义）。"""
    return f"s_{secrets.token_hex(8)}"


# ----------------------------------------------------------------------
# 驱动类
# ----------------------------------------------------------------------


class WecomBotConnector:
    """企业微信智能机器人·长连接驱动。

    持有官方 SDK WSClient（认证/心跳/指数退避重连交给 SDK），只负责:
      帧 -> AgentEvent -> process_event（与 webhook 同一条治理+执行链）
      -> 回复经 reply(frame, body) 透传 req_id 回原会话。

    Args:
        tenant_id: 绑定租户（demo）
        config: ImChannelConfig（wecom_bot 形态: app_id=bot_id, secret_ref=secret）
        processor: 处理函数（webhook 同款 process_event 闭包）
        ws_client: 测试注入 fake；None 时懒建官方 WSClient
    """

    def __init__(
        self,
        *,
        tenant_id: str,
        config: ImChannelConfig,
        processor: Callable[[AgentEvent], Awaitable[AgentResponse]],
        ws_client: Any = None,
    ) -> None:
        self._tenant_id = tenant_id
        self._config = config
        self._processor = processor
        self._ws: Any = ws_client
        self._started = False

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    def _build_client(self) -> Any:
        """懒建官方 WSClient（集中 import，缺失时给出清晰错误）。"""
        try:
            from aibot import WSClient, WSClientOptions
        except ImportError as exc:  # pragma: no cover - 依赖缺失路径
            raise RuntimeError("企微智能机器人通道需要 wecom-aibot-python-sdk（pip install wecom-aibot-python-sdk），"
                               "请先安装依赖") from exc
        secret = ""
        if self._config.secret_ref is not None:
            secret = self._config.secret_ref.get_secret_value()
        # max_reconnect_attempts=-1: Gateway 长驻进程，无限重连（心跳/退避由 SDK 内建）
        return WSClient(WSClientOptions(
            bot_id=self._config.app_id,
            secret=secret,
            max_reconnect_attempts=-1,
        ))

    def attach(self, ws: Any = None) -> Any:
        """注册事件监听（authenticated / message.text / event.enter_chat）。

        幂等：同一 ws 实例只注册一次（审查 09-04——重启重连重复 attach 会
        注册重复 listener，单条消息触发多次处理）。
        """
        ws = ws or self._ws
        if ws is None:
            ws = self._build_client()
            self._ws = ws
        if getattr(ws, "_teneuris_attached", False):
            return ws
        ws._teneuris_attached = True

        @ws.on("authenticated")
        def _on_auth() -> None:
            log.info("wecom_bot authenticated", extra={"tenant": self._tenant_id, "channel": CHANNEL_TYPE})

        @ws.on("message.text")
        async def _on_text(frame: dict[str, Any]) -> None:
            try:
                await self.handle_text_frame(frame)
            except Exception:  # noqa: BLE001 - 单条消息异常不影响连接
                log.warning("wecom_bot 处理消息异常", extra={"tenant": self._tenant_id}, exc_info=True)

        @ws.on("event.enter_chat")
        async def _on_enter_chat(frame: dict[str, Any]) -> None:
            body = frame.get("body") or {}
            log.info("wecom_bot enter_chat",
                     extra={
                         "tenant": self._tenant_id,
                         "chat_id": (body.get("chatid") or "")[:32]
                     })

        return ws

    async def start(self) -> None:
        """建连并保持（认证/重连由 SDK 内建）；失败仅告警不拖垮 Gateway。

        _started 在 connect 成功后才置位（审查 09-04：此前 connect 抛异常
        时 _started 已为 True，重试 start() 会直接 return 不再重连）。
        """
        if self._started:
            return
        self.attach()
        await self._ws.connect()
        self._started = True

    def stop(self) -> None:
        """断开长连接（SDK disconnect 为同步方法）。"""
        self._started = False
        if self._ws is not None:
            try:
                self._ws.disconnect()
            except Exception:  # noqa: BLE001 - 断开失败仅告警
                log.warning("wecom_bot disconnect 异常", exc_info=True)

    async def run(self, stop_event: asyncio.Event) -> None:
        """supervisor 任务（供 _cli create_task）: start -> 等待停止 -> 断开。"""
        try:
            await self.start()
        except Exception:  # noqa: BLE001 - 启动失败不阻断 Gateway
            log.warning("wecom_bot 启动失败（仅告警）", extra={"tenant": self._tenant_id}, exc_info=True)
        try:
            await stop_event.wait()
        finally:
            self.stop()

    # ------------------------------------------------------------------
    # 消息处理
    # ------------------------------------------------------------------

    async def handle_text_frame(self, frame: dict[str, Any]) -> None:
        """帧 -> AgentEvent -> 治理链 -> 回复（ERROR 不回复，仅留痕）。"""
        event = frame_to_event(frame, tenant_id=self._tenant_id)
        if event is None:
            return
        # 身份映射（对齐 webhook 语义，PRD 3.4）
        mapped = self._config.user_id_mapping.get(event.user_id, event.user_id)
        if mapped != event.user_id:
            event.metadata["external_user_id"] = event.user_id
            event.user_id = mapped

        response = await self._processor(event)
        if response.response_type == ResponseType.TEXT and response.content:
            for chunk in split_text(response.content):
                if not await self._reply_chunk(frame, chunk):
                    break  # 投递失败即停（避免死循环重试同一 req_id）

    async def _reply_chunk(self, frame: dict[str, Any], chunk: str) -> bool:
        """单段回复；返回 False 表示投递失败（调用方应停止后续分段）。"""
        if self._ws is None:
            return False
        try:
            await self._ws.reply(frame, build_stream_reply(chunk, new_stream_id()))
            return True
        except Exception as exc:  # noqa: BLE001 - 单段投递失败仅告警
            log.warning(f"wecom_bot reply 失败: {type(exc).__name__}: {exc}")
            return False
