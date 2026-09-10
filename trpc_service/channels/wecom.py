"""企业微信 Channel Adapter（被动回复模式）。"""
from __future__ import annotations

import asyncio
import time
import xml.etree.ElementTree as ET
from typing import Callable, Optional

from trpc_service.agent.routing import SessionRouter
from trpc_service.channels import wecom_crypto
from trpc_service.channels.base import WebhookRequest, WebhookResponse
from trpc_service.channels.dedupe import Deduper
from trpc_service.config.tenant_config import ChannelConfig, TenantConfig
from trpc_service.metrics.context import build_agent_context, new_trace_id
from trpc_service.tenant.governance.context import META_TRACE
from trpc_service.tenant.governance.user_authz import user_authz

REPLY_MAX_CHARS = 1800  # 企微被动回复文本长度上限（保守值）


def _xml_get(xml_text: str, tag: str) -> str:
    try:
        root = ET.fromstring(xml_text)
        node = root.find(tag)
        return (node.text or "") if node is not None else ""
    except ET.ParseError:
        return ""


def _build_reply_xml(encrypt: str, signature: str, timestamp: str, nonce: str) -> str:
    return (
        "<xml>"
        f"<Encrypt><![CDATA[{encrypt}]]></Encrypt>"
        f"<MsgSignature><![CDATA[{signature}]]></MsgSignature>"
        f"<TimeStamp>{timestamp}</TimeStamp>"
        f"<Nonce><![CDATA[{nonce}]]></Nonce>"
        "</xml>"
    )


def _chunk_text(text: str, limit: int = REPLY_MAX_CHARS) -> list[str]:
    return [text[i:i + limit] for i in range(0, len(text), limit)] or [""]


class WeComAdapter:
    """企业微信通道适配器（被动回复模式）。"""

    channel_type = "wecom"

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
        self.database = database  # 平台 Database（channel_binding / idempotency）

    # ---- 对外入口 ----

    async def handle_webhook(self, tenant_id: str, request: WebhookRequest) -> WebhookResponse:
        cfg = self.channel_config
        query = request.query
        msg_signature = query.get("msg_signature", "")
        timestamp = query.get("timestamp", "")
        nonce = query.get("nonce", "")

        if request.method.upper() == "GET":
            # URL 可靠性验证：解密 echostr 原样返回
            echostr = query.get("echostr", "")
            if not wecom_crypto.verify_signature(cfg.token, timestamp, nonce, echostr, msg_signature):
                return WebhookResponse(status_code=403, body="bad signature")
            plain, _ = wecom_crypto.decrypt_message(cfg.encoding_aes_key, echostr)
            return WebhookResponse(body=plain)

        encrypt = _xml_get(request.body, "Encrypt")
        if not encrypt:
            return WebhookResponse(status_code=400, body="missing Encrypt")
        if not wecom_crypto.verify_signature(cfg.token, timestamp, nonce, encrypt, msg_signature):
            return WebhookResponse(status_code=403, body="bad signature")

        try:
            plain_xml, _receiveid = wecom_crypto.decrypt_message(cfg.encoding_aes_key, encrypt)
        except wecom_crypto.WeComCryptoError:
            return WebhookResponse(status_code=400, body="decrypt failed")

        return await self._process_message(tenant_id, plain_xml, timestamp, nonce)

    # ---- 内部流程 ----

    def _sql_first_seen(self, msg_id: str) -> bool:
        """第三层幂等兜底：SQL idempotency 唯一索引。"""
        if not msg_id or self.database is None:
            return True
        from sqlalchemy.exc import IntegrityError

        from trpc_service.tenant.storage.tables import IdempotencyRow

        try:
            with self.database.session() as session:
                session.add(IdempotencyRow(idempotency_key=f"wecom:{msg_id}"))
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
            pass  # 绑定落库失败不影响消息主链路（session_id 由稳定路由保证）

    async def _process_message(
        self, tenant_id: str, plain_xml: str, timestamp: str, nonce: str
    ) -> WebhookResponse:
        from_user = _xml_get(plain_xml, "FromUserName")
        msg_type = _xml_get(plain_xml, "MsgType")
        msg_id = _xml_get(plain_xml, "MsgId") or ""
        chat_id = _xml_get(plain_xml, "ChatId") or ""  # 群聊才有
        content = _xml_get(plain_xml, "Content")

        # 幂等去重（全部消息类型）：第一层进程内/Redis，第三层 SQL 唯一索引兜底
        if msg_id and self.deduper.seen(f"wecom:{msg_id}"):
            return WebhookResponse(body="success")
        if msg_id and not await asyncio.to_thread(self._sql_first_seen, msg_id):
            return WebhookResponse(body="success")

        if not from_user:
            return WebhookResponse(body="success")

        # 撤回事件：静默 ACK（不触发 Agent、不回复、不提示用户）
        if msg_type == "revoke":
            return WebhookResponse(body="success")

        # 非文本消息：识别类型并友好回复（多媒体内容处理为预留设计）
        if msg_type != "text" or not content:
            media_hint = {
                "image": "已收到您的图片。当前仅支持文本对话，文字描述需求即可。",
                "voice": "已收到您的语音。当前仅支持文本对话，请以文字发送。",
                "video": "已收到您的视频。当前仅支持文本对话。",
                "file": "已收到您的文件。当前仅支持文本对话。",
            }
            hint = media_hint.get(msg_type, "暂不支持该消息类型，请发送文本消息。")
            return self._encrypted_reply(hint, timestamp, nonce)

        # 身份映射 + 会话路由 + 绑定落库
        session_id = SessionRouter.session_id(tenant_id, self.channel_type, from_user, chat_id)
        user_authz.bind(tenant_id, self.channel_type, from_user, session_id)
        await asyncio.to_thread(
            self._record_binding, tenant_id, from_user, chat_id, session_id
        )

        runner = self.get_runner(tenant_id)
        if runner is None:
            return WebhookResponse(status_code=503, body="agent not ready")

        # 频率限制（IM 洪峰防护，rate_limit_per_minute=0 不限制）
        from trpc_service.tenant.ratelimit import RateLimitExceeded, rate_limiter

        try:
            rate_limiter.check(tenant_id, from_user, self.tenant_config.rate_limit_per_minute)
        except RateLimitExceeded:
            return self._encrypted_reply("消息发送过于频繁，请稍后再试。", timestamp, nonce)

        # 预算前置校验（工具层 budget_limit 过滤器还有二次拦截）
        from trpc_service.tenant.budget import BudgetExceeded
        from trpc_service.tenant.governance.budget_limit import budget_manager

        try:
            budget_manager.check(tenant_id)
        except BudgetExceeded:
            from trpc_service.metrics.collector import metrics_collector
            from trpc_service.tenant.audit.model import AuditEvent
            from trpc_service.tenant.audit.service import audit_service

            metrics_collector.inc_request(tenant_id, self.channel_type, error=True)
            audit_service.emit(
                AuditEvent(
                    tenant_id=tenant_id,
                    channel=self.channel_type,
                    user_id=from_user,
                    session_id=session_id,
                    agent_name=runner.app_name,
                    decision="block",
                    error_type="budget_exceeded",
                )
            )
            return self._encrypted_reply("今日使用额度已用完，请明日再试。", timestamp, nonce)

        agent_context = build_agent_context(
            tenant_id=tenant_id,
            user_id=from_user,
            session_id=session_id,
            channel=self.channel_type,
            trace_id=new_trace_id(),
        )
        # 韧性层：重试 + 熔断（与 web chat 同一策略）
        from trpc_service.agent.resilience import resilience_policy

        outcome = await resilience_policy.execute(
            tenant_id,
            runner.run,
            user_id=session_id,  # 企微侧以会话绑定作为 user 标识
            session_id=session_id,
            message=content,
            agent_context=agent_context,
        )
        reply = outcome.text or "服务暂时不可用，请稍后重试。"

        # 审计由 web 层 AuditService 复用（channels 直接复用 audit_service）
        from trpc_service.metrics.collector import metrics_collector
        from trpc_service.tenant.audit.model import AuditEvent
        from trpc_service.tenant.audit.service import audit_service
        from trpc_service.tenant.governance.budget_limit import budget_manager

        budget_manager.record(tenant_id, api_calls=1, tokens=len(content) + len(reply))

        # 业务指标（请求量/工具调用/token/IM 投递成功率）
        metrics_collector.inc_request(tenant_id, self.channel_type, error=bool(outcome.error_type))
        metrics_collector.add_tokens(tenant_id, len(content) + len(reply))
        for call in outcome.tool_calls:
            metrics_collector.inc_tool_call(tenant_id, call.get("name", ""))

        audit_service.emit(
            AuditEvent(
                tenant_id=tenant_id,
                channel=self.channel_type,
                user_id=from_user,
                session_id=session_id,
                agent_name=runner.app_name,
                tool_name=",".join(c.get("name", "") for c in outcome.tool_calls),
                decision="error" if outcome.error_type else "allow",
                error_type=outcome.error_type,
                trace_id=agent_context.get_metadata(META_TRACE),
            )
        )

        # 加密被动回复（超长文本分片，本条返回首片）
        first_chunk = _chunk_text(reply)[0]
        response = self._encrypted_reply(first_chunk, timestamp, nonce)
        metrics_collector.inc_im_delivery(tenant_id, delivered=bool(response.delivered_reply))
        return response

    def _encrypted_reply(self, reply_text: str, timestamp: str, nonce: str) -> WebhookResponse:
        cfg = self.channel_config
        ts = timestamp or str(int(time.time()))
        inner_xml = (
            "<xml>"
            f"<ToUserName><![CDATA[{self.tenant_config.tenant_id}]]></ToUserName>"
            f"<FromUserName><![CDATA[{cfg.bot_id}]]></FromUserName>"
            f"<CreateTime>{int(time.time())}</CreateTime>"
            "<MsgType><![CDATA[text]]></MsgType>"
            f"<Content><![CDATA[{reply_text}]]></Content>"
            "</xml>"
        )
        encrypt = wecom_crypto.encrypt_message(cfg.encoding_aes_key, inner_xml, cfg.corp_id or "RECEIVEID")
        signature = wecom_crypto.sha1_signature(cfg.token, ts, nonce, encrypt)
        body = _build_reply_xml(encrypt, signature, ts, nonce)
        return WebhookResponse(content_type="application/xml", body=body, delivered_reply=reply_text)
