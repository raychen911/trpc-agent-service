"""飞书 Channel Adapter（事件订阅 v2.0 + 主动回复 API）。"""
from __future__ import annotations

import asyncio
import hashlib
import json
import re
import time
from typing import Callable, Optional
from urllib import request as urllib_request

from trpc_service.channels.base import WebhookRequest, WebhookResponse
from trpc_service.channels.dedupe import Deduper
from trpc_service.config.tenant_config import ChannelConfig, TenantConfig

REPLY_MAX_CHARS = 4000  # 飞书文本消息保守分片上限
TOKEN_TTL_SECONDS = 3600  # tenant_access_token 官方有效期 2h，提前刷新

MENTION_PATTERN = re.compile(r"@_user_\d+")  # 群聊 @机器人 占位符


def sha256_signature(timestamp: str, nonce: str, encrypt_key: str, body: str) -> str:
    """飞书回调验签算法：sha256(timestamp + nonce + encrypt_key + body)。"""
    return hashlib.sha256(f"{timestamp}{nonce}{encrypt_key}{body}".encode()).hexdigest()


def _urllib_post(url: str, payload: dict, token: str = "") -> dict:
    """同步 JSON POST（标准库）；经 asyncio.to_thread 调用。"""
    data = json.dumps(payload).encode("utf-8")
    req = urllib_request.Request(url, data=data, method="POST")
    req.add_header("Content-Type", "application/json; charset=utf-8")
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    with urllib_request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode("utf-8"))


class FeishuAdapter:
    """飞书通道适配器（事件订阅 + 主动回复）。"""

    channel_type = "feishu"

    def __init__(
        self,
        tenant_config: TenantConfig,
        channel_config: ChannelConfig,
        runner_getter: Callable[[str], object],
        deduper: Optional[Deduper] = None,
        database=None,
        api_base: str = "https://open.feishu.cn",
    ):
        self.tenant_config = tenant_config
        self.channel_config = channel_config
        self.get_runner = runner_getter
        self.deduper = deduper or Deduper()
        self.database = database  # 平台 Database（channel_binding / idempotency）
        self.api_base = api_base.rstrip("/")
        self._http_post = _urllib_post  # DI 点：测试注入 fake
        self._token_cache: tuple[str, float] = ("", 0.0)  # (token, 过期时刻)
        self._pending_tasks: set = set()  # 后台执行任务（测试等待用）

    # ---- 对外入口 ----

    async def handle_webhook(self, tenant_id: str, request: WebhookRequest) -> WebhookResponse:
        cfg = self.channel_config
        body = request.body

        # 验签（配置 encrypt_key 时强制）
        if cfg.encrypt_key:
            signature = request.headers.get("X-Lark-Signature", "")
            timestamp = request.headers.get("X-Lark-Timestamp", "")
            nonce = request.headers.get("X-Lark-Nonce", "")
            expected = sha256_signature(timestamp, nonce, cfg.encrypt_key, body)
            if signature != expected:
                return WebhookResponse(status_code=403, body="bad signature")

        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            return WebhookResponse(status_code=400, body="bad json")

        # URL 可靠性验证：原样回传 challenge
        if payload.get("type") == "url_verification":
            return WebhookResponse(
                content_type="application/json",
                body=json.dumps({"challenge": payload.get("challenge", "")}),
            )

        header = payload.get("header", {})
        # verification token 校验（配置 token 时）
        if cfg.token and header.get("token") != cfg.token:
            return WebhookResponse(status_code=403, body="bad token")
        if header.get("event_type") != "im.message.receive_v1":
            return WebhookResponse(body="success")  # 非消息事件直接 ACK

        event = payload.get("event", {})
        message = event.get("message", {})
        sender_id = (event.get("sender", {}).get("sender_id", {}) or {}).get("open_id", "")
        message_id = message.get("message_id", "")
        chat_id = message.get("chat_id", "")
        chat_type = message.get("chat_type", "p2p")
        msg_type = message.get("message_type", "")

        # 幂等去重：第一层进程内/Redis，第二层 SQL 唯一索引兜底
        if message_id and self.deduper.seen(f"feishu:{message_id}"):
            return WebhookResponse(body="success")
        if message_id and not await asyncio.to_thread(self._sql_first_seen, message_id):
            return WebhookResponse(body="success")
        if not sender_id:
            return WebhookResponse(body="success")

        # 立即 ACK，Agent 执行与回复异步进行（飞书 3 秒 ACK 限制）
        task = asyncio.create_task(
            self._process_and_reply(
                tenant_id, sender_id, chat_id, chat_type, msg_type, message_id, body_content(payload)
            )
        )
        self._pending_tasks.add(task)
        task.add_done_callback(self._pending_tasks.discard)
        return WebhookResponse(body="success")

    # ---- 内部流程 ----

    def _sql_first_seen(self, message_id: str) -> bool:
        """第二层幂等兜底：SQL idempotency 唯一索引。SQL 不可用时放行。"""
        if self.database is None:
            return True
        from sqlalchemy.exc import IntegrityError

        from trpc_service.tenant.storage.tables import IdempotencyRow

        try:
            with self.database.session() as session:
                session.add(IdempotencyRow(idempotency_key=f"feishu:{message_id}"))
            return True
        except IntegrityError:
            return False
        except Exception:
            return True

    def _record_binding(
        self, tenant_id: str, external_user_id: str, chat_id: str, session_id: str
    ) -> None:
        """IM 用户与租户绑定落 channel_binding 表（已存在则跳过）。"""
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
            pass  # 绑定落库失败不影响消息主链路

    async def _process_and_reply(
        self,
        tenant_id: str,
        sender_id: str,
        chat_id: str,
        chat_type: str,
        msg_type: str,
        message_id: str,
        content_raw: str = "",
    ) -> None:
        from trpc_service.agent.routing import SessionRouter
        from trpc_service.metrics.collector import metrics_collector
        from trpc_service.tenant.audit.model import AuditEvent
        from trpc_service.tenant.audit.service import audit_service
        from trpc_service.tenant.governance.user_authz import user_authz
        from trpc_service.tenant.ratelimit import RateLimitExceeded, rate_limiter

        runner = self.get_runner(tenant_id)
        if runner is None:
            await self._send_text(chat_id, "服务正在初始化，请稍后再试。")
            return

        # 非文本消息：识别类型并友好回复
        content_text = ""
        if msg_type == "text":
            try:
                content_text = json.loads(content_raw or "{}").get("text", "")
            except json.JSONDecodeError:
                content_text = ""
        if msg_type != "text" or not content_text:
            media_hint = {
                "image": "已收到您的图片。当前仅支持文本对话，文字描述需求即可。",
                "audio": "已收到您的语音。当前仅支持文本对话，请以文字发送。",
                "media": "已收到您的视频。当前仅支持文本对话。",
                "file": "已收到您的文件。当前仅支持文本对话。",
            }
            await self._send_text(
                chat_id, media_hint.get(msg_type, "暂不支持该消息类型，请发送文本消息。")
            )
            return

        # 群聊去除 @机器人 占位符
        content_text = MENTION_PATTERN.sub("", content_text).strip()
        if not content_text:
            return
        # 群聊以 chat_id 维度隔离，单聊以用户维度（与 SessionRouter 规则一致）
        group_key = chat_id if chat_type == "group" else ""

        # 身份映射 + 会话路由
        session_id = SessionRouter.session_id(
            tenant_id, self.channel_type, sender_id, group_key
        )
        user_authz.bind(tenant_id, self.channel_type, sender_id, session_id)

        # 频率限制（IM 洪峰防护）
        try:
            rate_limiter.check(
                tenant_id, sender_id, self.tenant_config.rate_limit_per_minute
            )
        except RateLimitExceeded:
            await self._send_text(chat_id, "消息发送过于频繁，请稍后再试。")
            return

        from trpc_service.chat import ChatBlocked, execute_chat
        from trpc_service.metrics.context import new_trace_id

        try:
            result = await execute_chat(
                self.tenant_config,
                runner,
                message=content_text,
                user_id=sender_id,
                session_id=session_id,
                channel=self.channel_type,
                trace_id=new_trace_id(),
            )
        except ChatBlocked:
            await self._send_text(chat_id, "今日使用额度已用完，请明日再试。")
            return

        reply = result["reply"] or "服务暂时不可用，请稍后重试。"
        reasoning = (result.get("reasoning") or "").strip()
        delivered = True
        if reasoning and len(reply) <= REPLY_MAX_CHARS:
            # 思考与回答分离：卡片折叠面板展示思考（灰字），正文为回答
            try:
                await self._send_card(chat_id, reply, reasoning)
            except Exception:
                delivered = False  # 卡片失败 → 纯文本兜底
                for chunk in _chunk_text(reply):
                    try:
                        await self._send_text(chat_id, chunk)
                    except Exception:
                        delivered = False
        else:
            for chunk in _chunk_text(reply):
                try:
                    await self._send_text(chat_id, chunk)
                except Exception:
                    delivered = False
        metrics_collector.inc_im_delivery(tenant_id, delivered=delivered)

        # 用户绑定落库（best-effort）
        await asyncio.to_thread(self._record_binding, tenant_id, sender_id, chat_id, session_id)
        audit_service.emit(
            AuditEvent(
                tenant_id=tenant_id,
                channel=self.channel_type,
                user_id=sender_id,
                session_id=session_id,
                agent_name=runner.app_name,
                decision="allow",
                trace_id=result.get("trace_id", ""),
            )
        )

    # ---- 飞书开放平台 API ----

    async def _tenant_token(self) -> str:
        """获取并缓存 tenant_access_token（自建应用）。"""
        now = time.monotonic()
        token, expires_at = self._token_cache
        if token and now < expires_at:
            return token
        cfg = self.channel_config
        resp = await asyncio.to_thread(
            self._http_post,
            f"{self.api_base}/open-apis/auth/v3/tenant_access_token/internal",
            {"app_id": cfg.app_id, "app_secret": cfg.app_secret},
        )
        token = resp.get("tenant_access_token", "")
        expire = int(resp.get("expire", 7200))
        self._token_cache = (token, time.monotonic() + expire - 300)
        return token

    async def _send_text(self, chat_id: str, text: str) -> None:
        """主动发送文本消息（receive_id_type=chat_id）。"""
        token = await self._tenant_token()
        payload = {"receive_id": chat_id, "msg_type": "text", "content": json.dumps({"text": text})}
        resp = await asyncio.to_thread(
            self._http_post,
            f"{self.api_base}/open-apis/im/v1/messages?receive_id_type=chat_id",
            payload,
            token,
        )
        if resp.get("code", 0) != 0:
            raise RuntimeError(f"feishu send failed: {resp.get('msg', resp)}")

    async def _send_card(self, chat_id: str, reply: str, reasoning: str) -> None:
        """发送卡片消息：回答正文 + 折叠面板（灰字思考过程）。"""
        token = await self._tenant_token()
        # reasoning 截断，防超长卡片被拒
        reasoning_brief = reasoning[:3000] + ("…" if len(reasoning) > 3000 else "")
        card = {
            "schema_url": "https://open.feishu.cn/open-apis/cards/schema/v2/2.0",
            "header": {
                "title": {"tag": "plain_text", "content": "AI 回复"},
                "template": "blue",
            },
            "body": {
                "elements": [
                    {
                        "tag": "collapsible_panel",
                        "expanded": False,
                        "title": {"tag": "plain_text", "content": "思考过程（点击展开）"},
                        "border": {"color": "grey"},
                        "background_style": "default",
                        "padding": 8,
                        "elements": [
                            {
                                "tag": "markdown",
                                "content": f"<font color='grey'>{reasoning_brief}</font>",
                            }
                        ],
                    },
                    {"tag": "markdown", "content": reply},
                ]
            },
        }
        payload = {
            "receive_id": chat_id,
            "msg_type": "interactive",
            "content": json.dumps(card, ensure_ascii=False),
        }
        resp = await asyncio.to_thread(
            self._http_post,
            f"{self.api_base}/open-apis/im/v1/messages?receive_id_type=chat_id",
            payload,
            token,
        )
        if resp.get("code", 0) != 0:
            raise RuntimeError(f"feishu card send failed: {resp.get('msg', resp)}")


def body_content(payload: dict) -> str:
    """提取事件里 message.content 原始 JSON 字符串。"""
    return (payload.get("event", {}).get("message", {}) or {}).get("content", "")


def _chunk_text(text: str, limit: int = REPLY_MAX_CHARS) -> list[str]:
    return [text[i:i + limit] for i in range(0, len(text), limit)] or [""]
