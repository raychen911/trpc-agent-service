"""企微智能机器人长连接通道（API 模式，免公网）。

基于 wecom-aibot-sdk-python 的 WSClient（BotID/Secret 登录 WebSocket，
SDK 内置心跳/重连/加解密），消息桥接进平台统一管线：
dedupe → SessionRouter → 频率限制 → execute_chat（韧性/指标/审计/脱敏）
→ reply_stream 回复。与飞书/HTTP 回调企微构成三条平级通道。

与被动回复（HTTP 回调）的差异：长连接由本进程主动发起，无需公网地址
与备案域名；同一租户同一 Bot 仅允许一个连接（多节点见后续 Redis 抢占锁）。
"""
from __future__ import annotations

import asyncio
import os
import re
from typing import Callable, Optional

from wecom_aibot_sdk import (
    WSClient,
    WSClientOptions,
    WsFrame,
    generate_req_id,
)

from trpc_service.channels.dedupe import Deduper
from trpc_service.config.tenant_config import ChannelConfig, TenantConfig

MENTION_PATTERN = re.compile(r"@\S+\s")  # 群聊 @机器人 名字剥离


class WeComSmartBotChannel:
    """企微智能机器人通道（长连接，平台内嵌桥接）。"""

    channel_type = "wecom_smartbot"

    def __init__(
        self,
        tenant_config: TenantConfig,
        channel_config: ChannelConfig,
        runner_getter: Callable[[str], object],
        deduper: Optional[Deduper] = None,
        database=None,
    ):
        self.tenant_config = tenant_config
        self.channel_config = channel_config
        self.get_runner = runner_getter
        self.deduper = deduper or Deduper()
        self.database = database
        secret = channel_config.secret or os.getenv("WECOM_BOT_SECRET", "")
        self._client = WSClient(
            WSClientOptions(
                bot_id=channel_config.bot_id,
                secret=secret,
                reconnect_interval=3,
                max_reconnect_attempts=50,
            )
        )
        self._task: Optional[asyncio.Task] = None

    # ---- 生命周期 ----

    async def start(self) -> None:
        """注册消息回调并建立长连接（SDK 内置断线重连）。"""
        self._client.on("message", self._on_message)
        await self._client.connect_async()
        # connect_async 建连后返回；接收循环在 SDK 内部线程/任务里自转
        from trpc_service.log import get_logger

        get_logger("channels.wecom_smartbot").info(
            "smartbot connected tenant=%s bot=%s",
            self.tenant_config.tenant_id,
            self.channel_config.bot_id,
        )

    async def stop(self) -> None:
        await self._client.disconnect()

    # ---- 消息处理 ----

    async def _on_message(self, frame: WsFrame) -> None:
        body = frame.body
        msg_type = body.get("msgtype", "")
        if msg_type != "text":
            # 非文本：引导文案（图片/语音/文件，与 HTTP 版口径一致）
            hints = {
                "image": "已收到您的图片。当前仅支持文本对话，文字描述需求即可。",
                "voice": "已收到您的语音。当前仅支持文本对话，请以文字发送。",
                "mixed": "已收到您的多媒体消息。当前仅支持文本对话。",
                "file": "已收到您的文件。当前仅支持文本对话。",
            }
            await self._client.reply_stream(
                frame, generate_req_id("stream"),
                hints.get(msg_type, "暂不支持该消息类型，请发送文本消息。"), finish=True,
            )
            return

        msg_id = str(body.get("msgid") or "")
        from_user = (
            (body.get("from") or {}).get("userid")
            or body.get("from_userid")
            or ""
        )
        chat_id = body.get("chatid") or ""
        chat_type = body.get("chattype") or "single"
        content = str((body.get("text") or {}).get("content") or "").strip()

        # 幂等去重（第一层内存/Redis；SQL 兜底同 HTTP 版）
        if msg_id and self.deduper.seen(f"wecom_smartbot:{msg_id}"):
            return
        if msg_id and not await asyncio.to_thread(self._sql_first_seen, msg_id):
            return
        if not from_user or not content:
            return

        # 群聊去除 @机器人 前缀
        content = MENTION_PATTERN.sub("", content, count=1).strip()
        if not content:
            return

        runner = self.get_runner(self.tenant_config.tenant_id)
        if runner is None:
            await self._client.reply_stream(
                frame, generate_req_id("stream"), "服务正在初始化，请稍后再试。", finish=True
            )
            return

        session_id, result = await self._execute(
            frame, from_user, chat_id, chat_type, content
        )
        reply = result["reply"] or "服务暂时不可用，请稍后重试。"
        try:
            await self._client.reply_stream(
                frame, generate_req_id("stream"), reply, finish=True
            )
            delivered = True
        except Exception:
            delivered = False

        from trpc_service.metrics.collector import metrics_collector

        metrics_collector.inc_im_delivery(self.tenant_config.tenant_id, delivered=delivered)
        await asyncio.to_thread(
            self._record_binding, self.tenant_config.tenant_id, from_user, chat_id, session_id
        )

    async def _execute(self, frame, from_user: str, chat_id: str, chat_type: str, content: str) -> tuple:
        """幂等后的业务管线：路由 → 限流 → execute_chat（预算/韧性/审计全在内）。"""
        from trpc_service.agent.routing import SessionRouter
        from trpc_service.chat import ChatBlocked, execute_chat
        from trpc_service.metrics.context import new_trace_id
        from trpc_service.tenant.governance.user_authz import user_authz
        from trpc_service.tenant.ratelimit import RateLimitExceeded, rate_limiter

        tenant_id = self.tenant_config.tenant_id
        group_key = chat_id if chat_type == "group" else ""
        session_id = SessionRouter.session_id(
            tenant_id, self.channel_type, from_user, group_key
        )
        user_authz.bind(tenant_id, self.channel_type, from_user, session_id)

        try:
            rate_limiter.check(tenant_id, from_user, self.tenant_config.rate_limit_per_minute)
        except RateLimitExceeded:
            await self._client.reply_stream(
                frame, generate_req_id("stream"), "消息发送过于频繁，请稍后再试。", finish=True
            )
            return session_id, {"reply": "", "trace_id": ""}

        try:
            result = await execute_chat(
                self.tenant_config,
                self.get_runner(tenant_id),
                message=content,
                user_id=from_user,
                session_id=session_id,
                channel=self.channel_type,
                trace_id=new_trace_id(),
            )
        except ChatBlocked:
            await self._client.reply_stream(
                frame, generate_req_id("stream"), "今日使用额度已用完，请明日再试。", finish=True
            )
            return session_id, {"reply": "", "trace_id": ""}
        return session_id, result

    # ---- best-effort 落库 ----

    def _sql_first_seen(self, msg_id: str) -> bool:
        if not msg_id or self.database is None:
            return True
        from sqlalchemy.exc import IntegrityError

        from trpc_service.tenant.storage.tables import IdempotencyRow

        try:
            with self.database.session() as session:
                session.add(IdempotencyRow(idempotency_key=f"wecom_smartbot:{msg_id}"))
            return True
        except IntegrityError:
            return False
        except Exception:
            return True

    def _record_binding(
        self, tenant_id: str, external_user_id: str, chat_id: str, session_id: str
    ) -> None:
        if self.database is None:
            return
        from trpc_service.tenant.storage.tables import ChannelBindingRow

        try:
            with self.database.session() as session:
                exists = (
                    session.query(ChannelBindingRow)
                    .filter_by(
                        tenant_id=tenant_id,
                        channel_type=self.channel_type,
                        external_user_id=external_user_id,
                        chat_id=chat_id,
                    )
                    .first()
                )
                if exists is None:
                    session.add(
                        ChannelBindingRow(
                            tenant_id=tenant_id,
                            channel_type=self.channel_type,
                            external_user_id=external_user_id,
                            chat_id=chat_id,
                            session_id=session_id,
                        )
                    )
        except Exception:
            pass
